# Observability (Prometheus + Grafana)

rss2msg exposing its **Prometheus `/metrics`** endpoint, scraped by a **Prometheus**
container and graphed in a pre-provisioned **Grafana**. See the
[Telemetry reference](../../../docs/reference/telemetry.md) and
[Kubernetes health probes](../../../docs/how-to/kubernetes-health-probes.md).

## Run it

```bash
docker compose up
```

Then open:

- **Prometheus** — http://localhost:9090. Go to **Status → Targets** to confirm the
  `rss2msg` target is `UP`, then explore the `rss2msg_*` series in the expression
  browser.
- **Grafana** — http://localhost:3000. Anonymous access is on (no login), and the
  **Prometheus** datasource is already wired up — open **Explore**, pick the Prometheus
  datasource, and chart any `rss2msg_*` metric.

Tear down with `docker compose down -v`.

## How metrics get exposed

Two telemetry settings work together (see [`config.yaml`](config.yaml)):

```yaml
telemetry:
  metrics:    { enabled: true }    # build the OTEL meter provider
  prometheus: { enabled: true, listen: ":9090" }   # serve it at /metrics
```

`metrics.enabled` builds the meter provider; `prometheus.enabled` attaches a Prometheus
reader and starts the `/metrics` HTTP server on `listen`. Prometheus scrapes
`rss2msg:9090` over the internal Compose network — see
[`prometheus/prometheus.yml`](prometheus/prometheus.yml).

## What's here

- **`rss2msg`** — metrics + Prometheus enabled; `/metrics` on `:9090` (internal only).
- **`prometheus`** — `prom/prometheus`, scraping the `rss2msg` job every 15s.
- **`grafana`** — `grafana/grafana` with the Prometheus datasource provisioned from
  [`grafana/provisioning/`](grafana/provisioning/) and anonymous admin access for the demo.

## Try changing it

- **Add OTLP traces** — set the standard `OTEL_EXPORTER_OTLP_ENDPOINT` env var and
  `telemetry.traces.enabled: true` to ship spans to a collector.
- **Build a dashboard** — add a dashboard provider under `grafana/provisioning/dashboards/`
  and drop JSON dashboards beside it.
- **Production note** — disable anonymous Grafana access; this example trades auth for
  zero-click convenience.
