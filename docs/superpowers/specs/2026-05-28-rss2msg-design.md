# rss2msg — Design

Status: approved (brainstorming)
Date: 2026-05-28

## Purpose

A Go service that polls RSS, Atom, RDF, and JSON Feed sources, detects new
and updated items, and publishes a canonical change envelope to one or more
configured sinks. v1 ships with Postgres and Kafka sinks; RabbitMQ, SQS, and
SNS land behind the same `Publisher` interface in later iterations.

## Non-goals

- Feed content rendering, classification, or full-text search.
- Webhook ingestion or push-based updates (WebSub/PubSubHubbub) — polling only in v1.
- Multi-replica coordination. v1 is single-instance; the state store keeps it
  correct across restarts but not across concurrent replicas.

## Runtime model

Single Go binary with Cobra subcommands. Viper loads configuration with this
precedence (later overrides earlier): built-in defaults → config file → env
vars (prefix `RSS2MSG_`, nested keys joined with `__`).

Subcommands:

- `rss2msg serve` — long-running daemon. One goroutine per configured feed,
  each on its own poll-interval ticker. SIGINT/SIGTERM cancels the root context
  and triggers a bounded drain.
- `rss2msg run-once` — polls every feed once (bounded worker pool, default
  `min(8, len(feeds))`) and exits. Non-zero exit if any feed had an
  unrecoverable error after retries. For external schedulers
  (systemd timer, k8s CronJob).
- `rss2msg validate-config` — parses config, dials state store and each sink,
  exits 0/1. Intended for CI and pre-deploy checks.

## Package layout

```
cmd/rss2msg/main.go              cobra root, wires subcommands
internal/config/                 viper loader, typed Config struct, validation
internal/model/                  Item, Change, ChangeKind
internal/feed/                   gofeed-backed Fetcher, change detector
internal/state/                  StateStore interface
  internal/state/postgres/         v1 implementation
internal/sink/                   Publisher interface + registry
  internal/sink/postgres/          v1
  internal/sink/kafka/             v1
  internal/sink/rabbitmq/          stub, returns "not implemented"
  internal/sink/sqs/               stub
  internal/sink/sns/               stub
internal/scheduler/              per-feed ticker loop (serve) + one-shot runner
internal/retry/                  exponential backoff with jitter
internal/telemetry/              zerolog + OTEL wiring
test/e2e/                        end-to-end with testcontainers
```

## Core interfaces

Small and focused so v1 implementations and mocks stay simple:

```go
type StateStore interface {
    Get(ctx context.Context, feedURL, itemID string) (hash string, found bool, err error)
    Upsert(ctx context.Context, feedURL, itemID, hash string, seenAt time.Time) error
    Close() error
}

type Publisher interface {
    Name() string
    Publish(ctx context.Context, change model.Change) error
    Close() error
}

type FeedSource interface {                 // config-backed for v1, DB-backed later
    Feeds(ctx context.Context) ([]FeedSpec, error)
}
```

A `Registry` keyed by sink name resolves the `[]string` sink list on each
`FeedSpec` to the concrete `Publisher` instances at startup.

## Polling, change detection, delivery

Per-feed loop, identical between `serve` (driven by a ticker) and `run-once`
(driven by a worker pool):

1. **Fetch.** HTTP GET via `gofeed.Parser`. Send `If-Modified-Since` and
   `If-None-Match` headers populated from the previous run (stored next to
   state). On `304 Not Modified`, skip parsing. HTTP timeout per feed
   (default 30s, override per feed).
2. **Parse.** `gofeed` normalises RSS 2.0, Atom, RDF, and JSON Feed into a
   `*gofeed.Feed` with `Items[]`.
3. **Compare.** For each item:
   - **Identity key**: `GUID` if present, else `Link`, else
     `sha256(title || published_at)`. Stored alongside state so the same item
     is recognised across reruns.
   - **Content hash**: SHA-256 over normalised `(title, link,
     content_or_description, author, updated_at)`. Normalisation: trim and
     collapse internal whitespace so trivial diffs don't fire `updated`.
   - Look up `(feed_url, item_id)` in the state store. Outcomes:
     not found → `new`; found and hash matches → skip; found and hash
     differs → `updated`.
4. **Publish + commit.** For each detected change, fan out to the publishers
   the feed opts into. Only when **every** publish succeeds do we upsert the
   new hash to the state store. If any publisher fails after retries, leave
   state untouched — the next poll re-detects the item and retries delivery.

### Retry / backoff

Each `Publish` call goes through a per-sink retry wrapper: exponential
backoff with jitter; defaults `max_attempts: 3`, `base_delay: 500ms`,
`max_delay: 10s`. After exhausting retries the change is dropped from this
poll cycle (no in-memory queue), but state isn't committed, so the next poll
re-attempts. **Idempotency is the publisher's responsibility**: Kafka uses
the item ID as message key (downstream consumers dedupe on it if they care);
Postgres uses `INSERT … ON CONFLICT (feed_url, item_id, detected_at) DO NOTHING`,
which makes within-poll retries safe. Across poll cycles, if delivery to one
sink succeeded but another failed, the next poll's re-detection produces a
fresh `DetectedAt` and writes a second row — that's the intentional
at-least-once trade-off of "commit on full-fanout success".

### Shutdown

`context.Context` is plumbed from the root cobra command through every layer.
SIGINT/SIGTERM cancels it; in-flight fetch/publish complete with a configurable
drain timeout (default 30s) before forced exit.

## Configuration

Viper search order for the config file: `--config <path>` flag, then
`./config.yaml`, then `/etc/rss2msg/config.yaml`. Env vars take final
precedence.

```yaml
log:
  level: info            # trace|debug|info|warn|error
  format: json           # json|console

telemetry:
  service_name: rss2msg  # overrides OTEL_SERVICE_NAME if set
  traces:   { enabled: true }
  metrics:  { enabled: true }
  logs:     { enabled: false }
  prometheus:
    enabled: false
    listen: ":9090"

http:
  user_agent: "rss2msg/0.1 (+https://example.com)"
  timeout: 30s

retry:
  max_attempts: 3
  base_delay: 500ms
  max_delay: 10s

runtime:
  shutdown_drain_timeout: 30s   # serve mode
  run_once_concurrency: 8       # run-once worker pool size; 0 = min(8, len(feeds))

state:
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}   # viper resolves env vars in strings

sinks:                     # global, named, used by feeds via the name list
  - name: pg-main
    driver: postgres
    postgres:
      dsn: ${POSTGRES_DSN}
      table: feed_changes
  - name: kafka-main
    driver: kafka
    kafka:
      brokers: ["kafka-1:9092", "kafka-2:9092"]
      topic: feed.changes
      acks: all
      compression: snappy

feeds:
  - url: https://example.com/blog/rss.xml
    interval: 5m
    sinks: [pg-main, kafka-main]
  - url: https://other.example/atom.xml
    interval: 15m
    sinks: [kafka-main]
    http:
      timeout: 10s
```

### Validation (fail fast at startup)

- Every `feeds[].sinks[]` name resolves to a declared sink.
- Every `interval` ≥ 1s.
- Exactly one `state.driver` configured.
- `sinks[].name` values are unique.
- `validate-config` runs the same validator plus a `Ping`/dial against the
  state store and each sink.

## Canonical `Change` envelope

One Go struct used everywhere; each sink renders it into its own wire format.

```go
type ChangeKind string

const (
    ChangeNew     ChangeKind = "new"
    ChangeUpdated ChangeKind = "updated"
)

type Change struct {
    SchemaVersion int        `json:"schema_version"` // 1
    FeedURL       string     `json:"feed_url"`
    FeedTitle     string     `json:"feed_title,omitempty"`
    ItemID        string     `json:"item_id"`        // GUID / link / synthetic
    Kind          ChangeKind `json:"kind"`
    Title         string     `json:"title,omitempty"`
    Link          string     `json:"link,omitempty"`
    Authors       []string   `json:"authors,omitempty"`
    Summary       string     `json:"summary,omitempty"`
    Content       string     `json:"content,omitempty"`
    Categories    []string   `json:"categories,omitempty"`
    PublishedAt   *time.Time `json:"published_at,omitempty"`
    UpdatedAt     *time.Time `json:"updated_at,omitempty"`
    ContentHash   string     `json:"content_hash"`   // sha256 hex
    DetectedAt    time.Time  `json:"detected_at"`
}
```

### Per-sink rendering

- **Kafka.** `key = ItemID`, `value = json.Marshal(Change)`, headers
  `feed_url`, `kind`, `schema_version`, `traceparent` (W3C trace context).
- **Postgres.** Table
  `feed_changes(feed_url text, item_id text, kind text, payload jsonb,
  detected_at timestamptz)` with
  `PRIMARY KEY (feed_url, item_id, detected_at)`. Each detected change is
  appended; the full envelope lives in `payload`.
- **RabbitMQ / SQS / SNS.** Stubs in v1 — the registry knows the driver
  names so config validation works, but the publishers return a clear
  "not implemented in this version" error. Same `Change` will feed them once
  implemented.

## Observability

### Logging — zerolog

`github.com/rs/zerolog` with JSON output by default; pretty console output
when `log.format=console` and stdout is a TTY. The logger is injected via
context (`zerolog.Ctx`). Standard fields on every emission inside a poll:
`feed_url`, `item_id` (when applicable), `sink` (when applicable), `attempt`
(when applicable), plus `trace_id` and `span_id` from the active OTEL span so
log lines cross-link to traces.

### OpenTelemetry

OTLP exporter (gRPC default, HTTP optional). Standard env vars drive endpoint
and resource attributes (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
`OTEL_RESOURCE_ATTRIBUTES`) so the config file doesn't redeclare what the
SDK already handles. `telemetry.service_name` overrides `OTEL_SERVICE_NAME`
when set.

- **Traces.** Root span per poll cycle: `feed.poll`. Child spans:
  `feed.fetch`, `feed.parse`, `feed.detect_changes`, and one `sink.publish`
  span per publisher attempt with attributes `sink.name`, `change.kind`,
  `attempt`, `retry.final`.
- **Metrics.** OTEL meter:
  - `feed.fetches` (counter, attrs: `feed_url`, `http.status`)
  - `feed.changes` (counter, attrs: `feed_url`, `kind`)
  - `sink.publish.failures` (counter, attrs: `sink.name`)
  - `feed.fetch.duration` (histogram, ms)
  - `sink.publish.duration` (histogram, ms, attrs: `sink.name`)
- **Logs.** Optional OTLP log export from zerolog when
  `telemetry.logs.enabled=true`.
- **Prometheus.** Optional parallel scrape endpoint at `telemetry.prometheus.listen`
  when `telemetry.prometheus.enabled=true`, exporting the same OTEL metrics.

All three signals default to enabled but are no-ops when no endpoint is
configured.

## Testing

### Unit tests

Standard table-driven Go tests for pure logic: change detection (hash
normalisation, identity-key fallback chain), config validation,
retry/backoff math, envelope construction. Mocks for `StateStore` and
`Publisher` — straightforward since the interfaces are small.

### Integration tests — testcontainers-go

`github.com/testcontainers/testcontainers-go`. One container set per package,
shared via `TestMain`:

- `internal/state/postgres` — Postgres container (`postgres:16-alpine`).
  Runs schema migrations on startup; tests `Get`/`Upsert`/`Close`, conflict
  handling, and concurrent writers.
- `internal/sink/postgres` — reuses the Postgres helper; tests
  `feed_changes` inserts, JSONB round-trip, idempotency under retry.
- `internal/sink/kafka` — Kafka container in KRaft mode via the
  testcontainers Kafka module. Tests publish, key/header propagation, and
  consumer-side assertion that the JSON envelope round-trips.
- `internal/feed` — no container; an `httptest.Server` serves fixture
  RSS/Atom/JSON Feed payloads.

### End-to-end — `test/e2e`

Postgres + Kafka containers + `httptest` feed server. Runs `serve` for a
short window; mutates the served feed; asserts changes land in both sinks
with the right `kind`. Uses the in-memory OTEL exporter
(`go.opentelemetry.io/otel/sdk/trace/tracetest`) to assert span structure
(`feed.poll` → `sink.publish` parent/child) without needing a real OTLP
collector.

### Isolation and execution

- Each test gets a unique Postgres schema and Kafka topic so `go test ./...`
  in parallel doesn't collide.
- Containers cleaned up via `t.Cleanup`; the testcontainers reaper (Ryuk)
  is the safety net.
- Integration tests behind `//go:build integration`. `go test ./...` stays
  fast; `go test -tags=integration ./...` (also `make test-integration`)
  runs the full suite. CI runs both.

## Dependencies

- `github.com/spf13/cobra`, `github.com/spf13/viper`
- `github.com/mmcdole/gofeed`
- `github.com/rs/zerolog`
- `go.opentelemetry.io/otel` (+ `sdk`, `exporters/otlp/otlptrace/otlptracegrpc`,
  `exporters/otlp/otlpmetric/otlpmetricgrpc`, `exporters/prometheus`)
- `github.com/jackc/pgx/v5` for Postgres
- `github.com/twmb/franz-go` for Kafka (preferred over Sarama for modern KRaft clusters)
- `github.com/testcontainers/testcontainers-go` (+ `modules/postgres`, `modules/kafka`)

## Out of scope for v1, explicit follow-ups

- RabbitMQ, SQS, SNS publishers (stubs land in v1; implementations follow).
- DB-backed `FeedSource` (`config-first, DB later` is the agreed path).
- Multi-replica coordination / leader election.
- WebSub / PubSubHubbub push ingestion.
- Per-feed rate limiting beyond polling cadence.
