---
title: Deploy on Azure Container Apps
type: how-to
tags: [rss2msg/docs, operations, deployment, azure]
summary: Run rss2msg on Azure Container Apps — a single-replica daemon with secrets, HTTP health probes, and a scheduled Container Apps job for run-once.
updated: 2026-06-01
---

# Deploy on Azure Container Apps

Run the published rss2msg image on Azure Container Apps (ACA) as a long-lived
`serve` daemon. For the config, secrets, and observability model, see
[Deploy in Production](../deploy.md).

## Packaging config

ACA has no host bind mount, so supply `config.yaml` one of two ways:

1. **Bake a config image** — layer your config onto the published base (keep
   secrets out; they arrive as env vars via `${VAR}`):

   ```dockerfile
   FROM ghcr.io/iambod/rss2msg:latest
   COPY config.yaml /etc/rss2msg/config.yaml
   ```

   Push it to a registry ACA can pull from (Azure Container Registry, or GHCR with
   registry credentials). The base already resolves `/etc/rss2msg/config.yaml`.
2. **Mount an Azure Files volume** at `/etc/rss2msg` via an ACA storage mount.

## Create the app

The container's default command is `serve`. Store secrets as ACA secrets and expose
them as environment variables — rss2msg expands `${VAR}` in the config.

```bash
az containerapp create \
  --name rss2msg \
  --resource-group rss2msg-rg \
  --environment rss2msg-env \
  --image <registry>/rss2msg:latest \
  --min-replicas 1 --max-replicas 1 \
  --secrets postgres-dsn=<your-dsn> \
  --env-vars "POSTGRES_DSN=secretref:postgres-dsn" \
  --ingress internal --target-port 8080
```

`--min-replicas 1` keeps the poller running; `--max-replicas 1` prevents ACA from
scaling out and double-polling (see [Scaling](#scaling)). Ingress is optional — set
`--target-port 8080` if you want to reach the health endpoints, or drop `--ingress`
entirely for a headless worker.

## Health probes

ACA supports `Liveness`, `Readiness`, and `Startup` probes. Map them to the daemon's
HTTP endpoints on port `8080` (the image has no shell, so use HTTP probes, not
command probes):

| ACA probe | Path | Port |
| --- | --- | --- |
| Liveness | `/healthz` | 8080 |
| Readiness | `/readyz` | 8080 |
| Startup | `/startupz` | 8080 |

See [Configure Kubernetes Health Probes](../configure-kubernetes-health-probes.md) for what each endpoint
means and how to tune the startup window.

## Scheduled runs

For a cron-style run instead of a daemon, use a **Container Apps job** with a
schedule trigger and the `run-once` command:

```bash
az containerapp job create \
  --name rss2msg-run-once \
  --resource-group rss2msg-rg \
  --environment rss2msg-env \
  --image <registry>/rss2msg:latest \
  --trigger-type Schedule --cron-expression "*/5 * * * *" \
  --replica-timeout 600 \
  --secrets postgres-dsn=<your-dsn> \
  --env-vars "POSTGRES_DSN=secretref:postgres-dsn" \
  --command "run-once"
```

## Scaling

There is no leader election: extra `serve` replicas without a shared coordinator
each poll every feed. Keep `--max-replicas 1` (and don't attach a scale rule), or
point all replicas at a shared Postgres/Redis coordinator — see
[Run Multiple Instances](../run-multiple-instances.md).

## Related

- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
- [Azure Service Bus](../sinks/azureservicebus.md) — Azure-native sink.
- [Run Multiple Instances](../run-multiple-instances.md) — shared coordinator setup.
- [Configure Kubernetes Health Probes](../configure-kubernetes-health-probes.md) — probe endpoint semantics.
- [CLI](../../reference/cli.md) — `serve`, `run-once`, `validate-config`.
