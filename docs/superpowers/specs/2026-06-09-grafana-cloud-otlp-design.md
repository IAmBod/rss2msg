# Grafana Cloud support via OTLP (issue #63)

- **Issue:** [#63](https://github.com/IAmBod/rss2msg/issues/63) "Grafana support"
- **Date:** 2026-06-09
- **Branch:** `feat/grafana-cloud-otlp`

## Problem

Issue #63 asks for "Grafana support" (empty body). rss2msg already exposes
Prometheus metrics and emits OTLP metrics + traces, and the observability compose
example already runs a local Grafana. The remaining gap is **Grafana Cloud**:
shipping telemetry to a hosted Grafana stack.

Grafana Cloud's OTLP gateway (`https://otlp-gateway-<zone>.grafana.net/otlp`) is
**HTTP/protobuf only** and authenticates with HTTP Basic auth
(`Authorization: Basic <base64(instanceID:token)>`). It does **not** accept OTLP
over gRPC.

rss2msg's OTLP exporters are **gRPC-only** (`otlptracegrpc`, `otlpmetricgrpc`). In
OpenTelemetry Go the transport is fixed by the exporter package you instantiate —
`OTEL_EXPORTER_OTLP_PROTOCOL` does **not** switch it. So rss2msg cannot push
directly to Grafana Cloud today.

## Goal

Let rss2msg send metrics + traces to Grafana Cloud, two ways:

- **(A) Direct push** — add HTTP/protobuf OTLP transport, selectable via the
  standard `OTEL_EXPORTER_OTLP_PROTOCOL` env var.
- **(B) Alloy/Collector bridge** — rss2msg pushes OTLP/gRPC to a local Grafana
  Alloy (or OTEL Collector) sidecar, which forwards to Grafana Cloud over HTTP.
  Documented + a runnable compose example.

Non-goals: OTLP **logs** export (logs reach Grafana via the Alloy bridge or the
existing Loki/CloudWatch/stdout paths); no new config block (OTLP stays fully
env-driven, as it is today).

## Design

### 1. Transport selection (code)

OTLP today is configured entirely from standard env vars; `hasOTLPEndpoint()`
reads `OTEL_EXPORTER_OTLP_ENDPOINT` / `..._TRACES_ENDPOINT` / `..._METRICS_ENDPOINT`.
Transport selection stays in the same idiom: read the standard
`OTEL_EXPORTER_OTLP_PROTOCOL`, with per-signal overrides
`OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` and `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`
(per the OTEL spec resolution order: per-signal → general → default).

New helper in `internal/telemetry/telemetry.go`:

```go
// otlpProtocol resolves the OTLP transport for a signal ("traces" or "metrics")
// from the standard env vars, per-signal overriding general. It returns the
// resolved protocol and an error for any unrecognized value.
func otlpProtocol(signal string) (string, error)
```

- Accepted values: `grpc`, `http/protobuf`.
- **Default is `grpc`** when unset — preserves current behavior. (Note: the OTEL
  spec default is `http/protobuf`; we deviate deliberately for backward
  compatibility and document the deviation in code comments and telemetry.md.)
- Unrecognized value (e.g. `http`, `GRPC`) → **return an error** so `Setup` fails
  fast with a clear message:
  `unsupported OTEL_EXPORTER_OTLP_<SIGNAL>PROTOCOL %q (want "grpc" or "http/protobuf")`.

`newTracerProvider` and the OTLP branch of `newMeterProvider` switch on the
resolved protocol:

- `grpc` → `otlptracegrpc.New(ctx)` / `otlpmetricgrpc.New(ctx)` (unchanged).
- `http/protobuf` → `otlptracehttp.New(ctx)` / `otlpmetrichttp.New(ctx)`.

Endpoint, headers, TLS, and compression continue to come from the standard
`OTEL_EXPORTER_OTLP_*` env vars — both the gRPC and HTTP exporter families read
them natively — so Grafana Cloud needs only:

```
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp-gateway-<zone>.grafana.net/otlp
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic <base64(instanceID:token)>
```

New dependencies (pinned to v1.44.0, matching the existing OTLP deps):

- `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp`
- `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp`

### 2. Tests (TDD)

Unit tests in `internal/telemetry/telemetry_test.go`:

- `otlpProtocol` resolution:
  - unset → `grpc`
  - `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` → `http/protobuf`
  - per-signal `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` beats the general var
  - per-signal applies only to its signal (traces override does not affect metrics)
  - unrecognized value → error
- Constructor smoke (no network): with `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`
  and an endpoint set, `Setup` builds providers and returns a working shutdown fn
  (mirrors `TestSetupReturnsShutdownAndLogger`). Exporter construction is lazy /
  does not dial on `New`, so this stays hermetic.

All tests use `t.Setenv` and make no network calls (`task test`).

### 3. Docs

- **New how-to:** `docs/how-to/send-to-grafana-cloud.md`, standard frontmatter
  (`title`, `type: how-to`, `tags`, `summary`, `updated`) + `## Related` footer.
  Two sections:
  - **Direct push** — the three env vars above; where to get the OTLP endpoint
    zone and generate the instance ID + token in the Grafana Cloud console; note
    that metrics + traces flow, logs do not (point to the bridge for logs).
  - **Alloy/Collector bridge** — rss2msg → OTLP/gRPC → Alloy → OTLP/HTTP →
    Grafana Cloud; link to the compose example.
- **Update** `docs/reference/telemetry.md` — document `OTEL_EXPORTER_OTLP_PROTOCOL`
  (+ per-signal overrides), accepted values, and the default-`grpc` deviation.
- **Compose example:** `examples/compose/grafana-cloud/` with `docker-compose.yml`,
  `config.yaml`, `alloy/config.alloy`, and `README.md`. rss2msg exports gRPC to the
  Alloy service; Alloy forwards to Grafana Cloud using `GRAFANA_CLOUD_*` env vars
  the README explains. Add a pointer from `examples/compose/README.md`.
- Run `bash scripts/check-doc-links.sh` (must print `OK: all relative doc links resolve`).

### 4. Issue hygiene

Backfill issue #63's empty body with this spec so it is a self-contained,
standalone description an implementer can act on (per AGENTS.md issue convention).

## Acceptance criteria

- [ ] `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` makes rss2msg export traces +
      metrics over HTTP/protobuf; default (unset) remains gRPC — no behavior change
      for existing users.
- [ ] Per-signal protocol overrides work and are tested.
- [ ] Unrecognized protocol value fails `Setup` with a clear error.
- [ ] `task test`, `task vet`, `task lint` pass; `task tidy` run (deps changed).
- [ ] New how-to + telemetry.md update + compose example added; doc link checker passes.
- [ ] Issue #63 body holds this spec.
