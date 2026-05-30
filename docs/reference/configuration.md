---
title: Configuration Reference
type: reference
tags: [rss2msg/docs, configuration]
summary: Loading order, environment variables, and every config field except sinks, coordination, and feeds.
updated: 2026-05-30
---

# Configuration Reference

The full annotated example is in [`config.example.yaml`](../../config.example.yaml).
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
| `postgres.dsn`     | yes (driver=postgres)| Standard `postgres://` DSN. The store applies its migrations idempotently on `New`. |
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
