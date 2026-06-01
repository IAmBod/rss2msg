---
title: Telemetry
type: reference
tags: [rss2msg/docs, observability]
summary: OTEL instruments, their attributes, and trace/log correlation.
updated: 2026-06-01
---

# Telemetry

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

## Sentry

Optional error/crash reporting (disabled by default). When
`telemetry.sentry.enabled` is set and a DSN resolves (from config or
`SENTRY_DSN`), a zerolog hook forwards log events at or above
`telemetry.sentry.level` (default `error`) to Sentry, and unrecovered panics
are captured before the process exits. Events inside a span carry `trace_id` /
`span_id` tags, cross-linking Sentry issues to traces. See the
[`telemetry.sentry` config block](configuration.md#telemetrysentry) for all
fields.

## Related

- [Configuration Reference](configuration.md) — the `telemetry` config block and OTLP env vars.
- [Operational Notes](../explanation/operations.md) — enabling exporters in production.
