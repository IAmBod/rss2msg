---
title: Send Telemetry to Grafana Cloud
type: how-to
tags: [rss2msg/docs, observability, grafana]
summary: Export rss2msg metrics and traces to Grafana Cloud over OTLP — directly via HTTP/protobuf, or through a Grafana Alloy / OpenTelemetry Collector bridge.
updated: 2026-06-09
---

# Send Telemetry to Grafana Cloud

rss2msg emits [OpenTelemetry](https://opentelemetry.io) metrics and traces and can
push them to [Grafana Cloud](https://grafana.com/products/cloud/) over OTLP. There
are two ways to do it:

- **[Direct push](#direct-push-httpprotobuf)** — rss2msg sends straight to the
  Grafana Cloud OTLP gateway. Simplest; no extra moving parts.
- **[Alloy / Collector bridge](#alloy--collector-bridge)** — rss2msg sends to a
  local [Grafana Alloy](https://grafana.com/docs/alloy/latest/) (or
  [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)) that forwards
  to Grafana Cloud. Use this when you want batching/retry buffering at the edge, or
  to ship **logs** as well (rss2msg does not export logs over OTLP).

Grafana Cloud's OTLP gateway accepts **OTLP over HTTP/protobuf only** — it does not
accept OTLP over gRPC (see the
[Grafana Cloud OTLP docs](https://grafana.com/docs/grafana-cloud/send-data/otlp/send-data-otlp/)).
rss2msg defaults to the gRPC transport, so direct push requires selecting the HTTP
transport explicitly (the bridge keeps gRPC locally and converts).

## Choosing the OTLP transport

rss2msg honors the standard OpenTelemetry transport variable
`OTEL_EXPORTER_OTLP_PROTOCOL`, with the per-signal
`OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` and `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`
overriding it for that signal. Accepted values are `grpc` and `http/protobuf`.

| Value | Transport |
| --- | --- |
| unset (default) | `grpc` |
| `grpc` | OTLP over gRPC |
| `http/protobuf` | OTLP over HTTP with protobuf encoding |

> **Default note.** When the variable is unset, rss2msg uses `grpc`. This
> deliberately differs from the OpenTelemetry specification's default
> (`http/protobuf`) to preserve rss2msg's historical behavior. Any other value
> (e.g. `http`, `GRPC`) is rejected at startup with a clear error.

See [Telemetry](../reference/telemetry.md) for the full list of OTLP environment
variables rss2msg reads.

## Direct push (HTTP/protobuf)

1. In the Grafana Cloud portal, open **Connections → Add new connection →
   OpenTelemetry (OTLP)** to find your **OTLP endpoint URL**, **instance ID**, and to
   generate an **API token** (an Access Policy token with `metrics:write` and
   `traces:write` scopes).

2. Build the Basic-auth header value. The credential is
   `base64("<instanceID>:<token>")`:

   ```bash
   echo -n "<instanceID>:<token>" | base64
   ```

3. Set the standard OTLP environment variables and run rss2msg:

   ```bash
   export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
   export OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp-gateway-<zone>.grafana.net/otlp
   export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic <base64-from-step-2>"
   rss2msg serve
   ```

   Replace `<zone>` with your stack's zone (shown in the portal, e.g.
   `prod-us-east-0`). rss2msg's metric and trace exporters activate when an OTLP
   endpoint is set and `telemetry.metrics` / `telemetry.traces` are enabled (both are
   enabled by default).

Metrics and traces now flow to Grafana Cloud. Logs do **not** — use the bridge below
if you also want logs in Grafana Cloud (Loki).

## Alloy / Collector bridge

Run [Grafana Alloy](https://grafana.com/docs/alloy/latest/) (or an OpenTelemetry
Collector) next to rss2msg. rss2msg keeps the default gRPC transport and points at the
local Alloy; Alloy forwards to Grafana Cloud over HTTP/protobuf and adds Basic auth.

```
rss2msg --(OTLP/gRPC)--> Alloy --(OTLP/HTTP)--> Grafana Cloud
```

Point rss2msg at the local receiver (gRPC is the default, so no protocol variable is
needed):

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://alloy:4317
rss2msg serve
```

Minimal Alloy configuration (`config.alloy`):

```alloy
otelcol.receiver.otlp "default" {
  grpc {
    endpoint = "0.0.0.0:4317"
  }
  output {
    metrics = [otelcol.exporter.otlphttp.grafana_cloud.input]
    traces  = [otelcol.exporter.otlphttp.grafana_cloud.input]
  }
}

otelcol.auth.basic "grafana_cloud" {
  username = sys.env("GRAFANA_CLOUD_INSTANCE_ID")
  password = sys.env("GRAFANA_CLOUD_API_TOKEN")
}

otelcol.exporter.otlphttp "grafana_cloud" {
  client {
    endpoint = sys.env("GRAFANA_CLOUD_OTLP_ENDPOINT")
    auth     = otelcol.auth.basic.grafana_cloud.handler
  }
}
```

A runnable version of this stack lives in
[`examples/compose/grafana-cloud/`](../../examples/compose/grafana-cloud/).

## Related

- [Telemetry](../reference/telemetry.md) — OTLP env vars, instruments, and transport selection.
- [Run with Docker](run-with-docker.md) — running rss2msg in a container.
- [Operational Notes](../explanation/operations.md) — enabling exporters in production.
