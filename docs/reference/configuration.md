---
title: Configuration Reference
type: reference
tags: [rss2msg/docs, configuration]
summary: Loading order, environment variables, and every config field except sinks, coordination, and feeds.
updated: 2026-06-02
---

# Configuration Reference

The full annotated example is in [`config.example.yaml`](../../examples/config.example.yaml).
The reference below documents every field — required, optional, default,
acceptable values, and what the field controls.

## Loading order and env vars

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

## Top-level structure

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

- [`coordination`](../how-to/run-multiple-instances.md) — multi-instance gating (optional).
- [`sinks`](../how-to/choose-a-sink.md) — list, at least one (Publisher destinations).
- [`feeds`](../how-to/configure-feeds.md) — list, at least one.

## `log`

| field    | type   | default | values             | notes                                       |
| -------- | ------ | ------- | ------------------ | ------------------------------------------- |
| `level`  | string | `info`  | `trace`..`fatal`   | Parsed by `zerolog.ParseLevel`.             |
| `format` | string | `json`  | `json` \| `console` | `console` is human-readable; `json` is structured. |

## `telemetry`

```yaml
telemetry:
  service_name: rss2msg
  traces:   { enabled: true }
  metrics:  { enabled: true }
  logs:     { enabled: false }
  prometheus:
    enabled: false
    listen: ":9090"
  graphite:
    enabled: false
    address: "localhost:2003"
    prefix: "rss2msg"
    interval: 10s
  sentry:
    enabled: false
    # dsn: ${SENTRY_DSN}
    level: error
    sample_rate: 1.0
    traces_sample_rate: 0.0
    debug: false
  cloudwatch:
    enabled: false
    # region: us-east-1
    # endpoint_url: ""
    logs:
      enabled: false
      # log_group: /rss2msg/app
      level: info
      batch_interval: 5s
      create_group: false
    metrics:
      enabled: false
      namespace: rss2msg
      interval: 60s
```

| field | default | notes |
| --------------------- | --------- | ----- |
| `service_name`        | `rss2msg` | Set on every OTEL signal as `service.name`. |
| `instance_id`         | hostname  | Set on every OTEL signal as `service.instance.id`, and added as a dimension/tag on CloudWatch and Graphite metrics so replicas don't collide into one series. Falls back to `OTEL_SERVICE_INSTANCE_ID`, then the hostname. |
| `traces.enabled`      | `true`    | Builds an OTLP/gRPC tracer provider when an OTLP endpoint env var is set; otherwise no-op. |
| `metrics.enabled`     | `true`    | Builds a periodic OTLP exporter when an endpoint is set. |
| `logs.enabled`        | `false`   | Reserved for future OTEL logs bridge. |
| `prometheus.enabled`  | `false`   | When true, exposes a Prometheus scrape endpoint at `prometheus.listen` + `/metrics`. |
| `prometheus.listen`   | `:9090`   | TCP listen address for the Prometheus exporter. |
| `graphite.enabled`    | `false`   | When true, pushes metrics to a Carbon (Graphite) endpoint over the plaintext protocol. |
| `graphite.address`    | `localhost:2003` | Carbon plaintext TCP endpoint (`host:port`). Required when enabled. |
| `graphite.prefix`     | `rss2msg` | Metric-path prefix prepended to every metric (e.g. `rss2msg.feed.fetches`). |
| `graphite.interval`   | `10s`     | Push cadence. `0` uses the OTEL SDK default. |

### Graphite (Carbon) export

When `graphite.enabled` is true, an OTEL `PeriodicReader` collects metrics on
`graphite.interval` and pushes them to `graphite.address` using the Carbon
**plaintext protocol** — one `"<path> <value> <unix-seconds>"` line per data
point — over a short-lived TCP connection per push. No external OTEL Collector
is required.

- OTEL metric names already use dots (`feed.fetches`), which map onto Carbon's
  dotted hierarchy; the configured `prefix` is prepended.
- Metric **attributes** fold into Graphite tags: `path;key=value;…` (sorted by
  key; spaces and `;`/`=` are sanitized to `_`).
- **Histograms** are emitted as `.count`, `.sum`, and `.min`/`.max` series.
- Values use cumulative temporality; apply `nonNegativeDerivative` in Graphite
  to recover rates.

`graphite` and `prometheus` can be enabled together; both read from the same
meter provider.

OTLP transport is configured by the standard OTEL env vars — set
`OTEL_EXPORTER_OTLP_ENDPOINT=https://collector:4317` (and optional
`OTEL_EXPORTER_OTLP_HEADERS`) to enable export. Without an endpoint, the
providers are wired but no-op, so leaving the config defaults in place is
safe for local development.

The Kafka/SQS/SNS sinks all inject W3C `traceparent` (and `tracestate` when
present) so downstream consumers can stitch the trace.

### `telemetry.sentry`

Optional [Sentry](https://sentry.io) error/crash reporting, disabled by default.
When enabled, log events at or above `level` are forwarded to Sentry as events,
and unrecovered panics are reported before the process exits.

| field | default | notes |
| -------------------- | ------- | ----- |
| `enabled`            | `false` | Master switch. |
| `dsn`                | `""`    | Sentry DSN; falls back to the `SENTRY_DSN` env var when empty. If neither resolves while enabled, Sentry is skipped with a warning (startup is not aborted). |
| `environment`        | `""`    | Falls back to `SENTRY_ENVIRONMENT`. |
| `release`            | `""`    | Falls back to `SENTRY_RELEASE`. |
| `server_name`        | `""`    | Optional host/instance label. |
| `level`              | `error` | Minimum zerolog level forwarded (`trace`..`panic`). Events carry the log message; an OTEL span context adds `trace_id`/`span_id` tags. |
| `sample_rate`        | `1.0`   | Error-event sampling, `[0.0, 1.0]`. |
| `traces_sample_rate` | `0.0`   | Performance/transaction sampling, `[0.0, 1.0]`. |
| `debug`              | `false` | Sentry SDK debug logging to stdout. |

> Only the log message, level, and trace tags reach Sentry — zerolog hooks do not
> expose structured fields or the underlying `err` object.

### `telemetry.posthog`

Optional [PostHog](https://posthog.com) telemetry, disabled by default. When
enabled, log events at or above `level` are forwarded to PostHog: events at
`error` and above are sent as `$exception` events (PostHog Error Tracking), and
lower levels (reachable only when `level` is lowered) are sent as a `log`
capture event.

| field            | default                    | notes |
| ---------------- | -------------------------- | ----- |
| `enabled`        | `false`                    | Master switch. |
| `api_key`        | `""`                       | PostHog **project** API key; falls back to the `POSTHOG_API_KEY` env var when empty. If neither resolves while enabled, PostHog is skipped with a warning (startup is not aborted). |
| `endpoint`       | `https://us.i.posthog.com` | Ingestion host; falls back to `POSTHOG_ENDPOINT`. Use `https://eu.i.posthog.com` for EU Cloud. |
| `distinct_id`    | service name / hostname    | Distinct ID attached to every event. |
| `level`          | `error`                    | Minimum zerolog level forwarded (`trace`..`panic`). Events carry `level` and `message` properties; an OTEL span context adds `trace_id`/`span_id`. |
| `flush_interval` | `0`                        | Batch flush cadence (e.g. `10s`); `0` uses the SDK default. Buffered events also flush on shutdown. |

> Only the log message, level, and trace tags reach PostHog — zerolog hooks do not
> expose structured fields or the underlying `err` object.

### `telemetry.cloudwatch`

Optional [AWS CloudWatch](https://aws.amazon.com/cloudwatch/) telemetry, disabled
by default. Two surfaces toggle independently: **Logs** ships log events to a
CloudWatch Logs group/stream, and **Metrics** pushes the OTEL instruments to
CloudWatch Metrics. `region` and `endpoint_url` are shared by both; AWS
credentials are resolved through the default SDK chain (environment, shared
config, IAM role), like the SQS/SNS sinks.

| field          | default     | notes |
| -------------- | ----------- | ----- |
| `enabled`      | `false`     | Master switch for the block. |
| `region`       | `""`        | AWS region; empty uses the default SDK chain. |
| `endpoint_url` | `""`        | Optional endpoint override (e.g. LocalStack). |

**Logs** (`telemetry.cloudwatch.logs`):

| field            | default  | notes |
| ---------------- | -------- | ----- |
| `enabled`        | `false`  | Ship log events to CloudWatch Logs. |
| `log_group`      | `""`     | Log group name. **Required** when logs are enabled. |
| `log_stream`     | hostname | Log stream name; defaults to the hostname. |
| `level`          | `info`   | Minimum zerolog level forwarded (`trace`..`panic`). |
| `batch_interval` | `5s`     | Flush cadence; `0` uses the default. |
| `create_group`   | `false`  | Auto-create the group/stream if missing (needs `logs:CreateLogGroup`/`CreateLogStream`). |

Events are batched on a buffered channel and flushed by a background goroutine
via `PutLogEvents` (sorted chronologically, chunked at the API limit), so the
logging path never blocks on network I/O. If the buffer fills, events are
dropped and the count is reported. An OTEL span context adds `trace_id`/`span_id`
to the message. The buffer flushes on shutdown. Shipment is best-effort:
`PutLogEvents` failures are logged but never abort the application.

**Metrics** (`telemetry.cloudwatch.metrics`):

| field       | default   | notes |
| ----------- | --------- | ----- |
| `enabled`   | `false`   | Push metrics to CloudWatch Metrics. |
| `namespace` | `rss2msg` | CloudWatch namespace for every datum. |
| `interval`  | `60s`     | Push cadence; `0` uses the OTEL SDK default. |

An OTEL `PeriodicReader` collects metrics on `interval` and pushes them via
`PutMetricData`. Sums and gauges map to a datum `Value`; histograms map to a
`StatisticSet` (count/sum/min/max). Metric **attributes** fold into CloudWatch
`Dimensions` (sorted by key, capped at the 30-dimension limit), and datums are
chunked into 1000-per-call requests. Requires `telemetry.metrics.enabled=true`;
needs the `cloudwatch:PutMetricData` IAM permission.

> Only the log message, level, and trace context reach CloudWatch Logs — zerolog
> hooks do not expose structured fields or the underlying `err` object.

## `http`

Global HTTP defaults for feed fetching. Each feed can override these under
`feeds[].http`.

| field        | default                | notes                                   |
| ------------ | ---------------------- | --------------------------------------- |
| `user_agent` | `rss2msg/0.1`          | Sent as the `User-Agent` header. Override to identify your deployment. |
| `timeout`    | `30s`                  | Per-request timeout. |

## `retry`

Per-sink retry policy applied by `sink.WithRetry`. Exponential backoff with
full jitter; runs for each publish attempt.

| field          | default | notes |
| -------------- | ------- | ----- |
| `max_attempts` | `3`     | Total tries (including the first). |
| `base_delay`   | `500ms` | Initial backoff. |
| `max_delay`    | `10s`   | Cap on the exponential delay (jitter can add up to the delay itself). |

When all retries are exhausted, the change is handed to the sink's
[dead-letter sink](../how-to/choose-a-sink.md) once (if declared), then dropped from the current
poll and re-detected on the next.

## `runtime`

| field                    | default | notes |
| ------------------------ | ------- | ----- |
| `shutdown_drain_timeout` | `30s`   | `serve` waits this long for in-flight publishes to finish on SIGINT/SIGTERM before forcing exit. |
| `run_once_concurrency`   | `0`     | Bounded worker pool for `run-once`. `0` means "one per feed" (no pool). |
| `deliver_timeout`        | `0s`    | Bounds a single sink delivery — all retry attempts plus the DLQ handoff — so one wedged sink can't stall a feed's poll loop. `0` (or omitted) disables it; a positive value (e.g. `60s`) caps each delivery. |

## `state`

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
| `postgres.dsn`     | yes (driver=postgres) | Standard `postgres://` DSN. The store applies its migrations idempotently on `New`. |
| `postgres.tls.*`   | no                   | Optional structured TLS config. Same field surface as `coordination.postgres.tls` — see [Secure Connections (TLS)](../how-to/secure-connections-tls.md) for the full table. Rejected when the DSN sets `sslmode=disable`. |
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

## Related

- [CLI](cli.md) — flags that point at this config file.
- [Configure Feeds](../how-to/configure-feeds.md) — the `feeds` list.
- [Choose a Sink](../how-to/choose-a-sink.md) — the `sinks` list.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — the `coordination` block.
- [Secure Connections (TLS)](../how-to/secure-connections-tls.md) — TLS for Postgres state and coordination.
