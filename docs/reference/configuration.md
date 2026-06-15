---
title: Configuration Reference
type: reference
tags: [rss2msg/docs, configuration]
summary: Loading order, environment variables, and every config field except sinks, coordination, state, feeds, and feed_sources.
updated: 2026-06-09
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
health:        # Kubernetes-style probe endpoints (serve)
state:         # seen-item store (required)
coordination:  # multi-instance gating (optional)
sinks:         # list, at least one (Publisher destinations)
feeds:         # static feed list (at least one of feeds / feed_sources)
feed_sources:  # dynamic feed list, reconciled at runtime (optional)
```

- [`state`](../how-to/choose-a-state-store.md) — seen-item store (required).
- [`coordination`](../how-to/run-multiple-instances.md) — multi-instance gating (optional).
- [`sinks`](../how-to/choose-a-sink.md) — list, at least one (Publisher destinations).
- [`feeds`](../how-to/configure-feeds.md) — the static feed list (at least one of `feeds` / `feed_sources`).
- [`feed_sources`](../how-to/load-feeds-dynamically.md) — dynamic feed list reconciled at runtime (optional).

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
  posthog:
    enabled: false
    # api_key: ${POSTHOG_API_KEY}
    endpoint: https://us.i.posthog.com
    level: error
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

## `health`

Kubernetes-style probe endpoints served by the `serve` daemon. Omitting the
block entirely yields the defaults below; the listener is not started under
`run-once`. See [Configure Kubernetes Health Probes](../how-to/configure-kubernetes-health-probes.md)
for probe semantics and a sample Deployment.

```yaml
health:
  enabled: true
  listen: ":8080"               # probe listener address
  liveness_path: /healthz       # 200 while the process is alive
  readiness_path: /readyz       # 200 when started, not draining, deps reachable
  startup_path: /startupz       # 503 until boot completes, then 200
```

| field            | default      | notes |
| ---------------- | ------------ | ----- |
| `enabled`        | `true`       | `false` starts no probe listener. |
| `listen`         | `:8080`      | Probe listener address. Required when `enabled: true`. |
| `liveness_path`  | `/healthz`   | `200` while the process is alive. |
| `readiness_path` | `/readyz`    | `200` when started, not draining, and dependencies are reachable. |
| `startup_path`   | `/startupz`  | `503` until boot completes, then `200`. |

Validation rules: each path must start with `/`; the three paths must be
distinct; `listen` is required when `enabled: true`. If
`telemetry.prometheus.enabled` is set and `health.listen` equals
`telemetry.prometheus.listen`, validation warns that one server will fail to bind.

## `feed_sources`

An ordered list of feed sources for the `serve` daemon. Each entry has a `type`
plus its own fields. See [Load Feeds Dynamically](../how-to/load-feeds-dynamically.md)
for reload semantics and the `file`, `postgres`, and `static` types. The sections
below cover the `http` and `kubernetes` source types.

### `feed_sources[].http` (type: http)

| field                                   | default | notes |
| --------------------------------------- | ------- | ----- |
| `feed_sources[].http.url`               | —       | **Required** for `type: http`. HTTP(S) endpoint returning `{"feeds":[...]}`. `${ENV}` expands. |
| `feed_sources[].http.timeout`           | `30s`   | Per-request timeout. |
| `feed_sources[].http.headers`           | —       | Arbitrary request headers for auth (e.g. `Authorization`, `X-API-Key`). The reserved conditional-GET headers `If-None-Match` and `If-Modified-Since` are managed by the source and are rejected if set here. |
| `feed_sources[].http.tls.ca_file`       | `""`    | Path to a PEM CA bundle for server-certificate verification. |
| `feed_sources[].http.tls.cert_file`     | `""`    | Client certificate for mTLS. Must be set together with `key_file` (both or neither). |
| `feed_sources[].http.tls.key_file`      | `""`    | Private key for the client certificate. Must be set together with `cert_file`. |
| `feed_sources[].http.tls.server_name`   | `""`    | SNI / cert-verify hostname override. |
| `feed_sources[].http.tls.insecure_skip_verify` | `false` | Disable server-certificate verification. For testing only. |

### `type: kubernetes`

Watches `Feed` custom resources (group `rss2msg.io`, version `v1`) via a
dynamic informer. Requires the `feeds.rss2msg.io` CRD to be installed.

```yaml
feed_sources:
  - type: kubernetes
    kubernetes:
      namespace: ""          # empty = all namespaces (cluster-wide watch)
      kubeconfig: ""         # empty = in-cluster config (pod's ServiceAccount)
      label_selector: ""     # optional; e.g. "app=myservice"
      resync_interval: 10m   # optional; default 10m
      write_status: true     # optional; default true
```

| field              | type     | default | notes |
| ------------------ | -------- | ------- | ----- |
| `namespace`        | string   | `""`    | Namespace to watch. Empty = cluster-wide watch across all namespaces. |
| `kubeconfig`       | string   | `""`    | Path to a kubeconfig file. Empty = in-cluster config (pod's ServiceAccount). |
| `label_selector`   | string   | `""`    | Kubernetes label selector to filter `Feed` CRs. Must parse via `labels.Parse`; rejected at startup if invalid. |
| `resync_interval`  | duration | `10m`   | Informer resync period. Must be at least `1s` if set. |
| `write_status`     | bool     | `true`  | When true, rss2msg writes poll results (`lastPollTime`, `lastChangeCount`, `lastError`, `Ready` condition) back to each `Feed` CR's `.status` subresource. Requires `feeds/status` RBAC. |

See [Get Feeds from Kubernetes](../how-to/get-feeds-from-kubernetes.md) for CRD
installation, RBAC, the `Feed` CR schema, and status field details.

## Related

- [CLI](cli.md) — flags that point at this config file.
- [Configure Feeds](../how-to/configure-feeds.md) — the `feeds` list.
- [Load Feeds Dynamically](../how-to/load-feeds-dynamically.md) — the `feed_sources` list.
- [Choose a Sink](../how-to/choose-a-sink.md) — the `sinks` list.
- [Choose a State Store](../how-to/choose-a-state-store.md) — the `state` block.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — the `coordination` block.
- [Secure Connections (TLS)](../how-to/secure-connections-tls.md) — TLS for Postgres state and coordination.
