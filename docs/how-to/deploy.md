---
title: Deploy in Production
type: how-to
tags: [rss2msg/docs, operations, deployment]
summary: Configure, validate, run, and observe rss2msg in production — config resolution, secrets, daemon vs job, and monitoring.
updated: 2026-05-31
---

# Deploy in Production

This page ties together the operational surface for running rss2msg as a service.
For the behavioral guarantees behind these knobs (delivery semantics, DLQs,
crash recovery), see [Operational Notes](../explanation/operations.md).

## 1. Provide configuration

rss2msg resolves its config file in this order (see [CLI](../reference/cli.md)):

1. `--config <path>` if given,
2. else `./config.yaml`,
3. else `/etc/rss2msg/config.yaml`.

Override individual fields with `RSS2MSG_`-prefixed environment variables (`.` →
`__`), and inject secrets with `${VAR}` substitution inside string values — keep
DSNs and tokens out of the file itself:

```yaml
state:
  postgres:
    dsn: ${POSTGRES_DSN}
```

The full field surface is the [Configuration Reference](../reference/configuration.md).

## 2. Validate before rollout

`validate-config` parses the config and dials the state store and every sink,
exiting non-zero on any failure. Run it in CI or as a pre-start gate:

```bash
rss2msg validate-config --config /etc/rss2msg/config.yaml
```

## 3. Run it

- **Daemon** — `rss2msg serve`: one goroutine per feed, polling on each feed's
  `interval`. It drains in-flight publishes for up to `runtime.shutdown_drain_timeout`
  on `SIGINT`/`SIGTERM`, then exits. Use this under systemd, a container
  supervisor, or Kubernetes.
- **Scheduled job** — `rss2msg run-once`: polls every feed once and exits (bounded
  by `runtime.run_once_concurrency`). Use this from cron or a Kubernetes CronJob
  when you don't want a long-lived process.

### Container image

The release pipeline publishes a multi-arch, rootless distroless image to GHCR on
every version tag (the `production` stage of the single [`Dockerfile`](../../Dockerfile)):

```bash
docker pull ghcr.io/iambod/rss2msg:latest   # or pin a version, e.g. :v1.2.3
docker run --rm -v "$PWD/config.yaml:/etc/rss2msg/config.yaml:ro" ghcr.io/iambod/rss2msg:latest serve
```

See [Run with Docker](run-with-docker.md) for the hot-reload development image, the
Docker Compose dev stack, and how to build a production image locally; see
[Releasing](../development/releasing.md) for how the image is built and published.

### Platform recipes

Step-by-step guides for the common runtimes — each covers packaging config,
injecting secrets, health probes, and a scheduled `run-once` variant:

- [Docker Compose](deploy/docker-compose.md) — the published image as a service.
- [Kubernetes](deploy/kubernetes.md) — ConfigMap, Secret, Deployment, Service, CronJob.
- [Helm chart](deploy/helm.md) — the same manifests as a templated, versioned chart.
- [AWS ECS (Fargate)](deploy/aws-ecs.md) — task definition, Secrets Manager, task-role IAM.
- [AWS Lambda](deploy/aws-lambda.md) — scheduled `run-once` as a container function.
- [Azure Container Apps](deploy/azure-container-apps.md) — daemon app and scheduled job.
- [GCP Cloud Run](deploy/gcp-cloud-run.md) — always-on service and Cloud Scheduler job.

They all start from the same model: keep secrets out of `config.yaml` (reference
them as `${VAR}`), supply the file by baking a thin image (`FROM
ghcr.io/iambod/rss2msg:latest` + `COPY config.yaml /etc/rss2msg/config.yaml`) or a
platform-native volume/secret mount, and define health checks against the HTTP
probe endpoints since the image has no shell.

## 4. Scale out

Running more than one instance? Point them all at a shared coordinator so they
don't double-poll — see [Run Multiple Instances](run-multiple-instances.md). There
is no leader election; losers skip a cycle silently.

## 5. Secure connections

Use TLS for the Postgres state store and the Postgres/Redis coordinators — see
[Secure Connections (TLS)](secure-connections-tls.md). Sink credentials (AWS, AMQP,
webhook tokens) follow each driver's mechanism in [Choose a Sink](choose-a-sink.md).

## 6. Observe it

- **OTLP** — set `OTEL_EXPORTER_OTLP_ENDPOINT` (and optional
  `OTEL_EXPORTER_OTLP_HEADERS`) to export traces and metrics. Without an endpoint
  the providers are wired but no-op.
- **Prometheus** — set `telemetry.prometheus.enabled: true` to expose `/metrics`
  on `telemetry.prometheus.listen` (default `:9090`).

The instruments to watch (fetch counts/durations, change counts, skipped polls,
sink publish failures/durations) are listed in [Telemetry](../reference/telemetry.md).

## Related

- [Run with Docker](run-with-docker.md) — multi-stage image, Compose, hot reload.
- [Operational Notes](../explanation/operations.md) — delivery semantics, DLQs, shutdown.
- [Run Multiple Instances](run-multiple-instances.md) — coordinator setup.
- [Secure Connections (TLS)](secure-connections-tls.md) — transport security.
- [Telemetry](../reference/telemetry.md) — what to monitor.
- [CLI](../reference/cli.md) — commands and flags.
