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
| `feed.poll.overran`      | counter   |      | `feed_url`                                       |
| `sink.publish.failures`  | counter   |      | `sink.name`                                      |
| `feed.fetch.duration`    | histogram | ms   | `feed_url`                                       |
| `sink.publish.duration`  | histogram | ms   | `sink.name`                                      |

## OTLP export

Traces and metrics are exported over OTLP when an endpoint is configured via the
standard OpenTelemetry environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT` or the
per-signal `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`),
provided `telemetry.traces` / `telemetry.metrics` are enabled (both default on).
Endpoint, headers (`OTEL_EXPORTER_OTLP_HEADERS`), TLS, and compression are read from
those standard variables.

The transport is selected by `OTEL_EXPORTER_OTLP_PROTOCOL`, with the per-signal
`OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` / `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`
overriding it:

| `OTEL_EXPORTER_OTLP_PROTOCOL` | transport                    |
| ---------------------------- | ---------------------------- |
| unset (default)              | `grpc`                       |
| `grpc`                       | OTLP over gRPC               |
| `http/protobuf`              | OTLP over HTTP with protobuf |

The unset default is `grpc`, which deliberately differs from the OpenTelemetry
specification's default (`http/protobuf`) to preserve historical behavior. Any
other value is rejected at startup. The HTTP/protobuf transport is what
[Grafana Cloud](../how-to/send-to-grafana-cloud.md) requires.

Traces wrap each poll cycle and each publish; downstream consumers can pick
up `traceparent` from message headers/attributes to stitch the full trace.

Zerolog is configured with the service name and is OTEL-correlated: log
records emitted inside a span carry `trace_id` and `span_id` fields.

## Multi-instance deployments

Every metric also carries the resource attribute `service.instance.id`
(`telemetry.instance_id`, defaulting to `OTEL_SERVICE_INSTANCE_ID` then the
hostname). The push-based exporters fold it into each metric so that two
replicas reporting the same instrument + attributes stay distinct rather than
collapsing into one series:

- **CloudWatch** — `service.instance.id` is added as a metric dimension.
- **Graphite** — `service.instance.id` is added as a tag.
- **Prometheus** — already per-instance: each replica exposes its own
  `/metrics` endpoint and Prometheus adds the `instance` label at scrape time,
  so no extra dimension is emitted (configure one scrape target per replica).
- **OTLP** — the resource is sent intact; the backend keys series by resource.

See [Run Multiple Instances](../how-to/run-multiple-instances.md) for the
coordinator setup that gates which replica polls each feed.

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
