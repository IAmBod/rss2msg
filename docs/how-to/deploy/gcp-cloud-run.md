---
title: Deploy on GCP Cloud Run
type: how-to
tags: [rss2msg/docs, operations, deployment, gcp]
summary: Run rss2msg on Cloud Run — a daemon service with always-on CPU and the health listener on the serving port, or run-once as a Cloud Run job triggered by Cloud Scheduler.
updated: 2026-06-01
---

# Deploy on GCP Cloud Run

rss2msg is a background poller, not a request/response service, so there are two
ways to run it on Cloud Run: a **service** with always-on CPU for the long-lived
`serve` daemon, or a **job** for scheduled `run-once` runs. For the config, secrets,
and observability model, see [Deploy in Production](../deploy.md).

## Service (long-lived daemon)

Cloud Run requires the container to listen on the port in `$PORT` (default `8080`).
The health listener already defaults to `:8080` (`/healthz`, `/readyz`,
`/startupz`), so it satisfies that contract — no app change needed.

Cloud Run throttles CPU outside request handling by default, which would stall the
background poller. Run with **CPU always allocated** and at least one instance so the
daemon keeps polling:

```bash
gcloud run deploy rss2msg \
  --image ghcr.io/iambod/rss2msg:latest \
  --port 8080 \
  --no-cpu-throttling \
  --min-instances 1 --max-instances 1 \
  --set-secrets "POSTGRES_DSN=rss2msg-postgres-dsn:latest"
```

`--no-cpu-throttling` keeps the CPU allocated between requests; `--min-instances 1`
keeps the poller alive; `--max-instances 1` prevents double-polling (see
[Scaling](#scaling)). Secrets from Secret Manager arrive as environment variables,
which rss2msg expands via `${VAR}`.

### Config

Cloud Run has no host bind mount, so supply `config.yaml` one of two ways:

1. **Mount the whole file from Secret Manager** at the path the binary reads:

   ```bash
   gcloud run deploy rss2msg ... \
     --set-secrets "/etc/rss2msg/config.yaml=rss2msg-config:latest"
   ```

2. **Bake a config image** — layer your config onto the published base (keep
   secrets out; they arrive as env vars):

   ```dockerfile
   FROM ghcr.io/iambod/rss2msg:latest
   COPY config.yaml /etc/rss2msg/config.yaml
   ```

The base already resolves `/etc/rss2msg/config.yaml` by default (see
[Run with Docker](../run-with-docker.md)).

## Job (scheduled run-once)

For a cron-style run, deploy a Cloud Run **job** with the `run-once` command and
trigger it from Cloud Scheduler. Jobs run to completion and need no serving port:

```bash
gcloud run jobs create rss2msg-run-once \
  --image ghcr.io/iambod/rss2msg:latest \
  --command rss2msg --args run-once \
  --set-secrets "POSTGRES_DSN=rss2msg-postgres-dsn:latest"

gcloud scheduler jobs create http rss2msg-trigger \
  --schedule "*/5 * * * *" \
  --uri "https://<region>-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/<project>/jobs/rss2msg-run-once:run" \
  --http-method POST --oauth-service-account-email <sa>@<project>.iam.gserviceaccount.com
```

## Credentials for GCP sinks

The Cloud Pub/Sub sink uses Application Default Credentials, so it picks up the
Cloud Run service account automatically — grant it `roles/pubsub.publisher`. See
[Cloud Pub/Sub](../sinks/gcp-pubsub.md) for sink config.

## Scaling

There is no leader election: extra `serve` instances without a shared coordinator
each poll every feed. Keep `--max-instances 1`, or point all instances at a shared
Postgres/Redis coordinator — see [Run Multiple Instances](../run-multiple-instances.md).

## Related

- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
- [Cloud Pub/Sub](../sinks/gcp-pubsub.md) — GCP-native sink.
- [Run Multiple Instances](../run-multiple-instances.md) — shared coordinator setup.
- [Kubernetes Health Probes](../kubernetes-health-probes.md) — health endpoint semantics.
- [CLI](../../reference/cli.md) — `serve`, `run-once`, `validate-config`.
