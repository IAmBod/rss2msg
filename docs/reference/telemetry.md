---
title: Telemetry
type: reference
tags: [rss2msg/docs, observability]
summary: OTEL instruments, their attributes, and trace/log correlation.
updated: 2026-06-02
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

## PostHog

Optional [PostHog](https://posthog.com) telemetry (disabled by default). When
`telemetry.posthog.enabled` is set and a project API key resolves (from config
or `POSTHOG_API_KEY`), a zerolog hook forwards log events at or above
`telemetry.posthog.level` (default `error`) to PostHog. Events at `error` and
above are sent as `$exception` events (PostHog Error Tracking); lower levels are
sent as a `log` capture event. Events inside a span carry `trace_id` / `span_id`
properties, cross-linking PostHog events to traces. Buffered events flush on
shutdown. See the
[`telemetry.posthog` config block](configuration.md#telemetryposthog) for all
fields.

## AWS CloudWatch

Optional [AWS CloudWatch](https://aws.amazon.com/cloudwatch/) telemetry (disabled
by default), with two independently-toggleable surfaces. When
`telemetry.cloudwatch.logs.enabled` is set, a zerolog hook batches log events at
or above `telemetry.cloudwatch.logs.level` (default `info`) and a background
goroutine ships them to a CloudWatch Logs group/stream via `PutLogEvents`, so the
logging path never blocks; an OTEL span context adds `trace_id` / `span_id` to
the message. When `telemetry.cloudwatch.metrics.enabled` is set, an OTEL
`PeriodicReader` pushes the instruments to CloudWatch Metrics via `PutMetricData`
(sums/gauges as values, histograms as a `StatisticSet`), folding attributes into
`Dimensions`. Credentials resolve through the default AWS SDK chain. See the
[`telemetry.cloudwatch` config block](configuration.md#telemetrycloudwatch) for
all fields.

## Related

- [Configuration Reference](configuration.md) — the `telemetry` config block and OTLP env vars.
- [Operational Notes](../explanation/operations.md) — enabling exporters in production.
