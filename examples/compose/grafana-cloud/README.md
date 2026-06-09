# Grafana Cloud (Alloy bridge)

Ship rss2msg's **metrics and traces to [Grafana Cloud](https://grafana.com/products/cloud/)**
through a [Grafana Alloy](https://grafana.com/docs/alloy/latest/) sidecar.

```
rss2msg --(OTLP/gRPC)--> Alloy --(OTLP/HTTP)--> Grafana Cloud
```

rss2msg keeps its **default gRPC transport** and points at the local Alloy; Alloy
converts to **OTLP/HTTP** and adds Basic auth, which is what Grafana Cloud's OTLP
gateway requires. (To skip Alloy and push **directly** from rss2msg, set
`OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` instead — see the
[how-to](../../../docs/how-to/send-to-grafana-cloud.md#direct-push-httpprotobuf).)

## Run it

1. In the Grafana Cloud portal open **Connections → Add new connection →
   OpenTelemetry (OTLP)** to get your **OTLP endpoint URL** and **instance ID**, and
   to generate an **API token** (Access Policy token with `metrics:write` and
   `traces:write` scopes).

2. Supply the credentials. Compose reads them from the environment — a `.env` file in
   this directory is the easiest:

   ```bash
   cat > .env <<'EOF'
   GRAFANA_CLOUD_OTLP_ENDPOINT=https://otlp-gateway-<zone>.grafana.net/otlp
   GRAFANA_CLOUD_INSTANCE_ID=<your-instance-id>
   GRAFANA_CLOUD_API_TOKEN=<your-access-policy-token>
   EOF
   ```

3. Start the stack:

   ```bash
   docker compose up        # add -d to detach
   ```

   On the first poll the state store is empty, so every current feed item is emitted
   as `new` and exported. Open your Grafana Cloud stack and query the `rss2msg_*`
   metrics (e.g. `feed_fetches_total`) or look for traces under the `rss2msg` service.

Tear down with `docker compose down -v`.

## What's here

- [`config.yaml`](config.yaml) — bind-mounted to `/etc/rss2msg/config.yaml`. Traces
  and metrics enabled; a `stdout` sink so you also see changes locally; SQLite state on
  a tmpfs.
- [`docker-compose.yml`](docker-compose.yml) — the `rss2msg` service
  (`OTEL_EXPORTER_OTLP_ENDPOINT=http://alloy:4317`) and the `alloy` service.
- [`alloy/config.alloy`](alloy/config.alloy) — an OTLP gRPC receiver wired to an
  OTLP/HTTP exporter with Basic auth to Grafana Cloud.

## Inspect the bridge

The Alloy UI is exposed at <http://localhost:12345> — use it to confirm the OTLP
receiver and the Grafana Cloud exporter are healthy and passing data.

## Related

- [Send Telemetry to Grafana Cloud](../../../docs/how-to/send-to-grafana-cloud.md) — direct push and bridge, in depth.
- [Telemetry](../../../docs/reference/telemetry.md) — instruments and OTLP env vars.
