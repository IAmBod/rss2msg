# rss2msg

A Go service that polls RSS / Atom / JSON feeds, detects new and updated items,
and publishes a canonical change envelope to one or more sinks (Postgres,
Kafka, RabbitMQ, SQS, SNS). It is designed to run as a
long-lived daemon (`serve`) or as a single-shot job (`run-once`), and it can
scale to multiple instances behind a shared coordinator (Postgres advisory
locks or Redis lease) without leader election.

```
┌──────────┐   poll    ┌──────────┐   classify    ┌──────────┐   publish   ┌──────────┐
│  feeds   │──────────▶│  feed    │──────────────▶│  state   │────────────▶│  sinks   │
│ (RSS/    │  HTTP +   │ fetcher  │  new/updated  │  store   │  per-feed   │ pg/kafka │
│  Atom/   │  cache    │          │  vs seen      │ (pgxpool)│  fan-out    │ sqs/sns  │
│  JSON)   │  headers  │ detector │  + content    │          │  + retry +  │          │
└──────────┘           └──────────┘  hash         └──────────┘  DLQ        └──────────┘
                                                                     │
                                              ┌──────────────────────┘
                                              ▼
                                      ┌──────────────┐
                                      │ coordinator  │  memory | postgres | redis
                                      │ (multi-inst) │  gates poll cycles
                                      └──────────────┘
```

---

## Table of contents

- [Quickstart](#quickstart)
- [Build and run](#build-and-run)
- [CLI](#cli)
- [Configuration](#configuration)
  - [Loading order and env vars](#loading-order-and-env-vars)
  - [Top-level structure](#top-level-structure)
  - [`log`](#log)
  - [`telemetry`](#telemetry)
  - [`http`](#http)
  - [`retry`](#retry)
  - [`runtime`](#runtime)
  - [`state`](#state)
  - [`coordination`](#coordination)
  - [`sinks`](#sinks)
  - [`feeds`](#feeds)
- [The change envelope](#the-change-envelope)
- [Sink wire formats](#sink-wire-formats)
- [Telemetry](#telemetry-1)
- [Operational notes](#operational-notes)
- [Testing](#testing)
- [Design docs](#design-docs)

---

## Quickstart

```bash
# 1. Build
task build

# 2. Run Postgres + Kafka locally (skip what you don't need)
docker run -d --name pg    -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16-alpine
docker run -d --name kafka -p 9092:9092 confluentinc/cp-kafka:7.6.0

# 3. Tell the example config where to find Postgres
export POSTGRES_DSN="postgres://postgres:test@localhost:5432/postgres?sslmode=disable"

# 4. One-shot: poll every feed once, publish, exit
./rss2msg run-once --config config.example.yaml

# 5. Inspect what landed in Postgres
psql "$POSTGRES_DSN" -c 'SELECT feed_url, item_id, kind, detected_at FROM feed_changes;'

# 6. Or run as a daemon
./rss2msg serve --config config.example.yaml
```

For SQS/SNS try LocalStack:

```bash
docker run -d --name ls -p 4566:4566 localstack/localstack:3.6
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
# add endpoint_url: http://localhost:4566 under each sqs/sns sink
```

For multi-instance coordination, see [`coordination`](#coordination) below.

---

## Build and run

```bash
task build               # produces ./rss2msg
task test                # unit tests (fast, no containers)
task test-integration    # adds testcontainers (Postgres, Kafka, Redis, LocalStack)
task vet                 # go vet ./...
task tidy                # go mod tidy
task                     # list all tasks
```

Requires Go 1.22+ and [`task`](https://taskfile.dev) (`go install github.com/go-task/task/v3/cmd/task@latest`, or `brew install go-task`). Integration tests require a working Docker daemon.

---

## CLI

```
rss2msg [flags] <command>

Commands
  serve              Run as a long-lived daemon; one goroutine per feed
  run-once           Poll every feed once and exit (bounded worker pool)
  validate-config    Parse config, dial state + each sink, exit 0/1

Flags
  --config <path>    Path to config file
                     (default: ./config.yaml, then /etc/rss2msg/config.yaml)
```

`serve` exits cleanly on SIGINT/SIGTERM and waits up to
`runtime.shutdown_drain_timeout` for in-flight publishes to finish.

---

## Configuration

The full annotated example is in [`config.example.yaml`](./config.example.yaml).
The reference below documents every field — required, optional, default,
acceptable values, and what the field controls.

### Loading order and env vars

Lower wins → higher wins:

1. Built-in defaults (`config.Defaults()`).
2. The config file (found via `--config`, else `./config.yaml`, else
   `/etc/rss2msg/config.yaml`).
3. Environment variables prefixed `RSS2MSG_`, with `.` in the path replaced
   by `__`. Examples:
   - `RSS2MSG_LOG__LEVEL=warn` sets `log.level`.
   - `RSS2MSG_HTTP__TIMEOUT=10s` sets `http.timeout`.
4. `${VAR}` substitution inside string values of the loaded config. The
   loader walks the parsed tree and replaces `${VAR}` with `os.Getenv("VAR")`
   (empty if unset). Useful for secrets:
   ```yaml
   state:
     postgres:
       dsn: ${POSTGRES_DSN}
   ```

Startup runs full validation (`config.Validate`) before any side effects;
errors print a single clear line and exit 1.

### Top-level structure

```yaml
log:           # logger
telemetry:     # zerolog + OTEL traces/metrics
http:          # global HTTP defaults for feed fetching
retry:         # sink retry policy
runtime:       # shutdown + run-once concurrency
state:         # seen-item store (required)
coordination:  # multi-instance gating (optional)
sinks:         # list, at least one (Publisher destinations)
feeds:         # list, at least one
```

### `log`

| field    | type   | default | values             | notes                                       |
| -------- | ------ | ------- | ------------------ | ------------------------------------------- |
| `level`  | string | `info`  | `trace`..`fatal`   | Parsed by `zerolog.ParseLevel`.             |
| `format` | string | `json`  | `json` \| `console` | `console` is human-readable; `json` is structured. |

### `telemetry`

```yaml
telemetry:
  service_name: rss2msg
  traces:   { enabled: true }
  metrics:  { enabled: true }
  logs:     { enabled: false }
  prometheus:
    enabled: false
    listen: ":9090"
```

| field | default | notes |
| --- | --- | --- |
| `service_name`        | `rss2msg` | Set on every OTEL signal as `service.name`. |
| `traces.enabled`      | `true`    | Builds an OTLP/gRPC tracer provider when an OTLP endpoint env var is set; otherwise no-op. |
| `metrics.enabled`     | `true`    | Builds a periodic OTLP exporter when an endpoint is set. |
| `logs.enabled`        | `false`   | Reserved for future OTEL logs bridge. |
| `prometheus.enabled`  | `false`   | When true, exposes a Prometheus scrape endpoint at `prometheus.listen` + `/metrics`. |
| `prometheus.listen`   | `:9090`   | TCP listen address for the Prometheus exporter. |

OTLP transport is configured by the standard OTEL env vars — set
`OTEL_EXPORTER_OTLP_ENDPOINT=https://collector:4317` (and optional
`OTEL_EXPORTER_OTLP_HEADERS`) to enable export. Without an endpoint, the
providers are wired but no-op, so leaving the config defaults in place is
safe for local development.

The Kafka/SQS/SNS sinks all inject W3C `traceparent` (and `tracestate` when
present) so downstream consumers can stitch the trace.

### `http`

Global HTTP defaults for feed fetching. Each feed can override these under
`feeds[].http`.

| field        | default                | notes                                   |
| ------------ | ---------------------- | --------------------------------------- |
| `user_agent` | `rss2msg/0.1`          | Sent as the `User-Agent` header. Override to identify your deployment. |
| `timeout`    | `30s`                  | Per-request timeout. |

### `retry`

Per-sink retry policy applied by `sink.WithRetry`. Exponential backoff with
full jitter; runs for each publish attempt.

| field          | default | notes |
| -------------- | ------- | ----- |
| `max_attempts` | `3`     | Total tries (including the first). |
| `base_delay`   | `500ms` | Initial backoff. |
| `max_delay`    | `10s`   | Cap on the exponential delay (jitter can add up to the delay itself). |

When all retries are exhausted, the change is handed to the sink's
[dead-letter sink](#sinks) once (if declared), then dropped from the current
poll and re-detected on the next.

### `runtime`

| field                    | default | notes |
| ------------------------ | ------- | ----- |
| `shutdown_drain_timeout` | `30s`   | `serve` waits this long for in-flight publishes to finish on SIGINT/SIGTERM before forcing exit. |
| `run_once_concurrency`   | `0`     | Bounded worker pool for `run-once`. `0` means "one per feed" (no pool). |

### `state`

The state store records `(feed_url, item_id) → content_hash, last_seen_at`
so the detector can classify each polled item as new, updated, or unchanged.
It also holds per-feed HTTP cache validators (`ETag`, `Last-Modified`) so
subsequent polls send conditional requests.

```yaml
state:
  driver: postgres        # postgres | sqlite
  postgres:
    dsn: ${POSTGRES_DSN}
    tls:                  # rejected if DSN has sslmode=disable
      ca_file: /etc/ssl/pg-ca.pem
      cert_file: /etc/ssl/pg-client.pem
      key_file: /etc/ssl/pg-client.key
      server_name: pg.internal
      insecure_skip_verify: false
  # sqlite:
  #   path: ./rss2msg.db
```

| field              | required             | notes |
| ------------------ | -------------------- | ----- |
| `driver`           | yes                  | `postgres` or `sqlite`. |
| `postgres.dsn`     | yes (driver=postgres)| Standard `postgres://` DSN. The store applies its migrations idempotently on `New`. |
| `postgres.tls.*`   | no                   | Optional structured TLS config. Same field surface as `coordination.postgres.tls` — see [the Postgres TLS subsection under coordination](#postgres-tls) for the full table. Rejected when the DSN sets `sslmode=disable`. |
| `sqlite.path`      | yes (driver=sqlite)  | Filesystem path passed verbatim to the `modernc.org/sqlite` driver. `:memory:` and `?_pragma=…` query strings are accepted. |

| driver     | concurrency / scope                                              | when to use |
| ---------- | ---------------------------------------------------------------- | ----------- |
| `postgres` | Shared across instances; writers serialised by the DB.           | Production, multi-instance, or when state already lives in Postgres. |
| `sqlite`   | Single file on local disk. WAL + busy-timeout enabled by default; the store uses one connection so writes are serialised in-process. Not shared between processes/nodes. | Single-instance deployments, local dev, edge / embedded contexts. |

Schema created on first start (idempotent `CREATE TABLE IF NOT EXISTS`). The
Postgres DDL is shown; the SQLite store uses the same logical schema with
`TEXT` columns for timestamps (RFC3339Nano UTC), and `ON CONFLICT … DO
UPDATE SET col = excluded.col` upserts.

```sql
CREATE TABLE seen_items (
    feed_url     TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (feed_url, item_id)
);

CREATE TABLE feed_meta (
    feed_url      TEXT PRIMARY KEY,
    etag          TEXT NOT NULL DEFAULT '',
    last_modified TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL
);
```

### `coordination`

Gates which instance is allowed to poll a given feed in a given cycle, for
horizontally-scaled deployments. The default is single-instance (`memory`,
always grants the lease).

```yaml
coordination:
  driver: memory   # memory | postgres | redis ; default memory
  postgres:
    dsn: ${POSTGRES_DSN}     # falls back to state.postgres.dsn
    tls:                     # rejected if DSN has sslmode=disable
      ca_file: /etc/ssl/pg-ca.pem
      cert_file: /etc/ssl/pg-client.pem
      key_file: /etc/ssl/pg-client.key
      server_name: pg.internal
      insecure_skip_verify: false
  redis:
    url: ${REDIS_URL}        # e.g. redis://localhost:6379/0 or rediss://...
    lock_ttl: 30s            # optional, default 30s
    renewal_interval: 10s    # optional, default = lock_ttl / 3
    tls:                     # only valid when url is rediss://
      ca_file: /etc/ssl/redis-ca.pem
      cert_file: /etc/ssl/redis-client.pem
      key_file: /etc/ssl/redis-client.key
      server_name: redis.internal
      insecure_skip_verify: false
```

#### Postgres TLS

pgx accepts TLS parameters directly in the DSN (`sslmode`, `sslrootcert`,
`sslcert`, `sslkey`). The `coordination.postgres.tls` block is a structured
alternative that keeps secrets out of the DSN string and gives the same
field surface as the Redis backend:

| field                  | default          | notes |
| ---------------------- | ---------------- | ----- |
| `ca_file`              | system roots     | PEM CA bundle to trust instead of system roots. |
| `cert_file`, `key_file`| (none)           | PEM client cert + key for mTLS. Both or neither — validation rejects setting only one. |
| `server_name`          | DSN host         | Overrides the SNI / certificate verification hostname. |
| `insecure_skip_verify` | `false`          | Disables server cert verification. Test only — logged at warn on startup. |

When a `tls` block is set, the coordinator clears pgx's plaintext
connection fallbacks so a TLS-required connection cannot silently
downgrade. Validation rejects the combination of `tls.*` with a DSN that
explicitly says `sslmode=disable` so operators don't accidentally configure
TLS knobs that would never take effect.

#### Redis TLS

A `rediss://` URL alone gives default TLS: the system trust store for
verification, and SNI taken from the URL host. The `coordination.redis.tls`
block lets operators override that:

| field                  | default                  | notes |
| ---------------------- | ------------------------ | ----- |
| `ca_file`              | system roots             | PEM CA bundle to trust instead of system roots. |
| `cert_file`, `key_file`| (none)                   | PEM client cert + key for mTLS. Both or neither — validation rejects setting only one. |
| `server_name`          | URL host                 | Overrides the SNI / certificate verification hostname. |
| `insecure_skip_verify` | `false`                  | Disables server cert verification. Test only — logged at warn on startup. |

The TLS block is only valid when the URL uses the `rediss://` scheme;
validation rejects it for plain `redis://` so operators don't silently get
unencrypted connections.

| driver     | mechanism | crash recovery | notes |
| ---------- | --------- | -------------- | ----- |
| `memory`   | always grants the lease | n/a | Default. Use for single-instance deployments. |
| `postgres` | `pg_try_advisory_lock(int64(sha256(feed_url)[:8]))` per connection | automatic — advisory locks die with the session | Reuses the state DSN by default. No leader election. |
| `redis`    | `SET key token NX EX <lock_ttl>`, background renewal goroutine refreshes via CAS-checked `PEXPIRE`, release via CAS-checked `DEL`. Key = `rss2msg:coord:<hex(sha256(feed_url))>` | TTL-based — crashed instances release their leases after `lock_ttl` | Supports `redis://` and `rediss://`. Validation rejects unparseable URLs and `lock_ttl < 1s` or `renewal_interval >= lock_ttl`. |

The pipeline calls `coord.TryAcquire(feedURL)` before each poll. On
`(release, true, nil)` it polls and `release()` runs after; on
`(nil, false, nil)` the cycle is skipped silently (no error). On
`(nil, false, err)` the cycle is skipped, a warn is logged, and the
`feed.poll.skipped{reason="coord_error"}` counter is incremented.

The release function ignores its caller's `ctx` — it uses a fresh 5 s
background context — so a canceled poll ctx (e.g. on SIGTERM) does not leak
the lease.

### `sinks`

A non-empty list of named publishers. Each feed publishes to one or more
sinks (`feeds[].sinks: [name1, name2]`); a feed with no `sinks` list
publishes to a sink named `default` if one exists.

Common fields on every sink:

| field         | required | notes |
| ------------- | -------- | ----- |
| `name`        | yes      | Unique per config. Referenced by `feeds[].sinks` and `dead_letter`. |
| `driver`      | yes      | One of `postgres`, `kafka`, `rabbitmq`, `sqs`, `sns`. |
| `dead_letter` | no       | Name of another declared sink. On retry exhaustion the change is delivered there once, with `dlq_from_sink`, `dlq_error`, and `dlq_attempts` annotations. A sink cannot be its own DLQ. |

#### `driver: postgres`

```yaml
- name: pg-main
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}
    table: feed_changes
```

| field            | required | notes |
| ---------------- | -------- | ----- |
| `postgres.dsn`   | yes      | Standalone DSN; not required to match the state DSN. |
| `postgres.table` | yes      | Unquoted identifier (`[A-Za-z_][A-Za-z0-9_]*`, ≤ 63 chars). Validated; never interpolated raw. |

Schema created on first publish (idempotent):

```sql
CREATE TABLE <table> (
    feed_url     TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    kind         TEXT NOT NULL,
    payload      JSONB NOT NULL,            -- the full Change envelope
    detected_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (feed_url, item_id, detected_at)
);
```

The PK includes `detected_at` so re-published changes (from re-detection
after a transient sink failure) accumulate as separate rows rather than
overwriting history. Consumers dedupe on `(feed_url, item_id, content_hash)`
from the JSONB payload.

#### `driver: kafka`

```yaml
- name: kafka-main
  driver: kafka
  kafka:
    brokers: ["kafka-1:9092", "kafka-2:9092"]
    topic: feed.changes
    acks: all
    compression: snappy
```

| field         | required | default       | values |
| ------------- | -------- | ------------- | ------ |
| `brokers`     | yes      | —             | List of `host:port`. |
| `topic`       | yes      | —             | Topic name; client does not auto-create. |
| `acks`        | no       | `all`         | `all` \| `leader` \| `none`. **`none` is unsafe** (see [Operational notes](#operational-notes)). |
| `compression` | no       | `none`        | `none` \| `snappy` \| `lz4` \| `zstd` \| `gzip`. |

Record layout:
- `Key` = `Change.ItemID` (so consumers can co-partition by item).
- `Value` = JSON `Change` envelope.
- Headers: `feed_url`, `kind`, `schema_version`, optional `traceparent` /
  `tracestate`, optional `dlq_from_sink` / `dlq_error` / `dlq_attempts`.

#### `driver: sqs`

```yaml
- name: sqs-main
  driver: sqs
  sqs:
    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/feed-changes
    region: us-east-1
    # endpoint_url: http://localhost:4566   # LocalStack
    # message_group: feed_url               # FIFO only — see below
```

| field           | required | notes |
| --------------- | -------- | ----- |
| `queue_url`     | yes      | Full SQS URL. A `.fifo` suffix selects FIFO mode (see below). |
| `region`        | no       | AWS SDK falls back to env/profile. |
| `endpoint_url`  | no       | Override for LocalStack-style endpoints. |
| `message_group` | no       | FIFO only: `feed_url` (default) \| `item_id` \| `sink`. Rejected when set on a standard (non-FIFO) queue. |

Credentials come from the standard AWS SDK credential chain
(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, shared `~/.aws/credentials`,
instance metadata, etc.).

Message body = JSON `Change` envelope. Message attributes: `feed_url`,
`kind`, `schema_version`, optional `traceparent` / `tracestate`, optional
DLQ annotations.

##### FIFO queues

When `queue_url` ends with `.fifo`, the sink sets the two FIFO-required
fields on every `SendMessage` call:

- **`MessageGroupId`** — derived from `message_group`:
  - `feed_url` (default) — one group per feed: in-order per feed, parallel across feeds.
  - `item_id` — one group per item: maximum parallelism; only useful when the consumer doesn't need cross-item ordering.
  - `sink` — single group across the entire sink: strict global ordering, no parallelism.
- **`MessageDeduplicationId`** — `sha256(feed_url || item_id || content_hash)` rendered as hex. Re-publishes of an unchanged Change within SQS's 5-minute dedup window are coalesced; updates (content hash changes) produce a fresh dedup id and are delivered.

The dedup id we send is honoured regardless of the queue's
ContentBasedDeduplication setting.

#### `driver: sns`

```yaml
- name: sns-main
  driver: sns
  sns:
    topic_arn: arn:aws:sns:us-east-1:123456789012:feed-changes
    region: us-east-1
```

| field          | required | notes |
| -------------- | -------- | ----- |
| `topic_arn`    | yes      | Full SNS topic ARN. FIFO topics (`*.fifo`) are rejected by validation. |
| `region`       | no       | AWS SDK fallback chain. |
| `endpoint_url` | no       | LocalStack override. |

Message attributes mirror the SQS sink. Credentials follow the same chain.

#### `driver: rabbitmq`

```yaml
- name: rmq-main
  driver: rabbitmq
  rabbitmq:
    url: amqp://guest:guest@rabbit-1:5672/      # or amqps://... for TLS
    exchange: feed.changes
    exchange_type: topic          # direct (default) | topic | fanout | headers
    routing_key: feed.changes
    declare: true                 # declare the exchange at startup
    durable: true                 # only meaningful with declare=true
    mandatory: false              # broker returns unroutable messages (currently unhandled)
```

| field          | required | default  | notes |
| -------------- | -------- | -------- | ----- |
| `url`          | yes      | —        | Standard AMQP URL (`amqp://` or `amqps://`). User/password inline; `${ENV}` substitution works. |
| `exchange`     | no       | `""`     | Empty means RabbitMQ's default direct exchange (routes by `routing_key` to a queue with the same name). |
| `exchange_type`| no       | `direct` | `direct` \| `topic` \| `fanout` \| `headers`. Only used when `declare=true`. |
| `routing_key`  | no       | `""`     | Static routing key sent on every publish. |
| `declare`      | no       | `false`  | If true, declares the exchange at startup. Requires a non-empty `exchange`. |
| `durable`      | no       | `false`  | Durability flag for the declared exchange. |
| `mandatory`    | no       | `false`  | Publish with the AMQP mandatory flag. Returns from the broker for unroutable messages are not currently handled — turning this on without a guaranteed binding effectively drops them silently. |

Publish layout:
- Body: JSON `Change` envelope.
- `ContentType: application/json`, `DeliveryMode: 2` (persistent), `MessageId = Change.ItemID`, `Timestamp = Change.DetectedAt`.
- Headers: `feed_url`, `kind`, `schema_version`, optional `traceparent` /
  `tracestate`, optional `dlq_from_sink` / `dlq_error` / `dlq_attempts`.

Implementation notes:
- One AMQP connection + one channel per Publisher. Publishes are mutex-serialised because AMQP channels are not safe for concurrent use.
- No auto-reconnect in this version. A broker disconnect surfaces as a publish error and is handled by the sink retry+DLQ layer.

### `feeds`

A non-empty list of feeds to poll.

```yaml
feeds:
  - url: https://example.com/blog/rss.xml
    interval: 5m
    sinks: [pg-main, kafka-main]
  - url: https://other.example/atom.xml
    interval: 15m
    http:
      timeout: 10s
      headers:
        Authorization: "Bearer ${OTHER_FEED_TOKEN}"
```

| field            | required | notes |
| ---------------- | -------- | ----- |
| `url`            | yes      | RSS / Atom / JSON Feed URL (parsed by `gofeed`). |
| `interval`       | yes      | `time.Duration`. Minimum `1s`. Used by `serve`; `run-once` ignores it. |
| `sinks`          | no       | Names of declared sinks. Empty falls back to a sink named `default` (validation requires `default` to exist if any feed omits the list). |
| `http.timeout`   | no       | Per-feed override of `http.timeout`. |
| `http.headers`   | no       | Extra request headers. `If-Modified-Since` and `If-None-Match` are reserved (the fetcher manages them); validation rejects overrides of either. |

The fetcher sends `If-None-Match` / `If-Modified-Since` from `feed_meta`
when present, and updates the row from the response's `ETag` /
`Last-Modified`. A 304 short-circuits parsing.

---

## The change envelope

Every published message is a JSON `Change` (see
[`internal/model/change.go`](./internal/model/change.go)):

```json
{
  "schema_version": 1,
  "feed_url": "https://example.com/blog/rss.xml",
  "feed_title": "Example Blog",
  "item_id": "https://example.com/blog/post-1",
  "kind": "new",
  "title": "Hello world",
  "link": "https://example.com/blog/post-1",
  "authors": ["alice"],
  "summary": "...",
  "content": "...",
  "categories": ["go"],
  "published_at": "2026-05-29T12:00:00Z",
  "updated_at":   "2026-05-29T12:00:00Z",
  "content_hash": "5e88489..." ,
  "detected_at":  "2026-05-29T12:00:05Z"
}
```

- `item_id` is the stable identity: GUID if the feed provides one, else
  `link`, else `sha256(title || publishedAt)`.
- `content_hash` is the sha256 over the normalised tuple
  `(title, link, body, author, updated_at)` — whitespace runs are collapsed
  to single spaces before hashing.
- `kind` is `new` on first sighting, `updated` when the content hash
  changes for a known `item_id`. Unchanged items are not published.
- DLQ deliveries add `dlq_from_sink`, `dlq_error`, `dlq_attempts` to the
  envelope (and as headers/attributes on Kafka/SQS/SNS).

---

## Sink wire formats

| sink     | key / partition          | body                | metadata                                                          |
| -------- | ------------------------ | ------------------- | ----------------------------------------------------------------- |
| postgres | `(feed_url, item_id, detected_at)` PK | JSONB `payload`     | Columns: `feed_url`, `item_id`, `kind`, `detected_at`.            |
| kafka    | `Key = item_id`          | JSON `Change` value | Headers: `feed_url`, `kind`, `schema_version`, `traceparent?`, `tracestate?`, `dlq_*?`. |
| sqs      | n/a                      | JSON `Change` body  | MessageAttributes: same as Kafka headers.                         |
| sns      | n/a                      | JSON `Change` body  | MessageAttributes: same as Kafka headers.                         |

Postgres `payload` is the full envelope — everything else is extractable
from it; the columns are for indexing and basic SQL filtering.

---

## Telemetry

OTEL instruments registered under the meter
`github.com/iambod/rss2msg`:

| instrument               | type      | unit | attributes                                       |
| ------------------------ | --------- | ---- | ------------------------------------------------ |
| `feed.fetches`           | counter   |      | `feed_url`, `http.status` (int)                  |
| `feed.changes`           | counter   |      | `feed_url`, `kind` (`new` / `updated`)           |
| `feed.poll.skipped`      | counter   |      | `feed_url`, `reason` (`not_owner` / `coord_error`) |
| `sink.publish.failures`  | counter   |      | `sink.name`                                      |
| `feed.fetch.duration`    | histogram | ms   | `feed_url`                                       |
| `sink.publish.duration`  | histogram | ms   | `sink.name`                                      |

Traces wrap each poll cycle and each publish; downstream consumers can pick
up `traceparent` from message headers/attributes to stitch the full trace.

Zerolog is configured with the service name and is OTEL-correlated: log
records emitted inside a span carry `trace_id` and `span_id` fields.

---

## Operational notes

- **At-least-once delivery.** If one sink succeeds and another fails on the
  same poll, the next poll re-detects the item and re-publishes to *all*
  sinks. Downstream consumers should dedupe on `item_id` + `content_hash`.
- **Kafka `acks: none` is unsafe.** Combined with the commit-on-success
  model it can drop messages without state knowing they were lost. Stick
  with the default (`acks: all`) unless you accept the trade-off.
- **OTEL exporters need an OTLP endpoint.** Without
  `OTEL_EXPORTER_OTLP_ENDPOINT` (or the per-signal variants), the providers
  are wired but no-op. The Prometheus exporter is a separate flag
  (`telemetry.prometheus.enabled`).
- **Dead-letter queues.** Any sink may declare
  `dead_letter: <other-sink-name>`. On retry exhaustion the change is
  handed to the DLQ *once*, annotated with `dlq_from_sink`, `dlq_error`,
  `dlq_attempts`. If no DLQ is set or the DLQ also fails, the change is
  dropped from this poll and re-detected on the next.
- **Running multiple instances.** Use `coordination.driver=postgres` or
  `redis` and ensure every instance points at the same backend. No leader
  election — any instance may pick up any feed; losers skip the cycle
  silently. Postgres uses session-scoped advisory locks (auto-released on
  crash). Redis uses a TTL lease with a background renewer; crashed
  instances release after `lock_ttl`.
- **AWS credentials.** SQS and SNS use the AWS SDK credential chain. The
  config carries only region, queue URL / topic ARN, and an optional
  `endpoint_url` for LocalStack-style overrides. SQS FIFO queues are
  supported (see the `message_group` field under `driver: sqs`); SNS FIFO
  topics are not yet supported.
- **LocalStack for SQS/SNS.**
  ```bash
  docker run -d --name ls -p 4566:4566 localstack/localstack:3.6
  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
  ```
  Then set `endpoint_url: http://localhost:4566` on each SQS/SNS sink.
- **Shutdown.** `serve` drains in-flight publishes for up to
  `runtime.shutdown_drain_timeout` after SIGINT/SIGTERM, then forces exit.

---

## Testing

```bash
task test                # unit tests
task test-integration    # adds -tags=integration; spins:
                         #  - Postgres for state + coord/postgres + sink/postgres
                         #  - Kafka for sink/kafka
                         #  - Redis for coord/redis
                         #  - LocalStack for sink/sqs and sink/sns
```

Integration tests require a working Docker daemon (testcontainers manages
the lifecycle). Each test boots a fresh container, so the suite is slower
(~30–60 s typical) but does not require any pre-provisioned services.

End-to-end coverage lives in [`test/e2e`](./test/e2e), which runs the full
pipeline (HTTP feed → fetcher → detector → state → Postgres sink + Kafka
sink) inside one test.

---

## Design docs

- [`docs/superpowers/specs/2026-05-28-rss2msg-design.md`](./docs/superpowers/specs/2026-05-28-rss2msg-design.md)
  — original design (v1).
- [`docs/superpowers/specs/2026-05-28-rss2msg-v1.5-design.md`](./docs/superpowers/specs/2026-05-28-rss2msg-v1.5-design.md)
  — multi-instance coordination + SQS/SNS.
- [`docs/superpowers/specs/2026-05-28-rss2msg-coord-redis-design.md`](./docs/superpowers/specs/2026-05-28-rss2msg-coord-redis-design.md)
  — Redis coordinator backend.
- Implementation plans live alongside the specs under
  `docs/superpowers/plans/`.
