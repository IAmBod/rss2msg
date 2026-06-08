---
title: Deploy with the Helm chart
type: how-to
tags: [rss2msg/docs, operations, deployment, kubernetes, helm]
summary: Install rss2msg on Kubernetes with the bundled Helm chart — pick a serve Deployment or a run-once CronJob, render config into a ConfigMap, inject secrets, and expose Prometheus metrics.
updated: 2026-06-09
---

# Deploy with the Helm chart

The repository ships a Helm chart at [`deploy/helm/rss2msg`](../../../deploy/helm/rss2msg/README.md)
that packages the manifests from [Deploy on Kubernetes](kubernetes.md) — a ConfigMap
for `config.yaml`, an optional Secret for DSNs/tokens, the workload, a metrics Service,
and an optional Prometheus `ServiceMonitor`. Reach for the raw manifests when you want
to read every object; reach for the chart when you want one templated, versioned unit.

## Install

```bash
helm install rss2msg ./deploy/helm/rss2msg -f my-values.yaml
```

Pin the image in production (the tag defaults to the chart's `appVersion`):

```bash
helm install rss2msg ./deploy/helm/rss2msg --set image.tag=v0.1.0 -f my-values.yaml
```

## Choose a workload mode

`mode` selects which workload is rendered — only one is created:

- `mode: deployment` (default) runs a long-lived `serve` daemon with startup,
  liveness, and readiness probes (see
  [Configure Kubernetes Health Probes](../configure-kubernetes-health-probes.md)) and a metrics Service.
- `mode: cronjob` runs `run-once` on `cronjob.schedule`, polling every feed once and
  exiting — the same scheduled pattern as the
  [Kubernetes CronJob recipe](kubernetes.md#5-scheduled-runs-with-a-cronjob).

```yaml
# my-values.yaml — scheduled run-once every 15 minutes
mode: cronjob
cronjob:
  schedule: "*/15 * * * *"
```

## Provide config and secrets

The chart renders `.Values.config` into a ConfigMap mounted at
`/etc/rss2msg/config.yaml`. Keep secrets out of it: reference them as `${VAR}` and
supply the values through `secrets` (rendered into a Secret and injected via
`envFrom`), or point at an externally-managed Secret with `existingSecret`:

```yaml
# my-values.yaml
secrets:
  POSTGRES_DSN: postgres://rss2msg:change-me@postgres:5432/rss2msg?sslmode=require
config:
  state:
    driver: postgres
    postgres:
      dsn: ${POSTGRES_DSN}
  feeds:
    - url: https://example.com/feed.xml
      interval: 5m
      sinks: [out]
  sinks:
    - name: out
      driver: stdout
```

Already managing config or secrets out of band? Set `existingConfigMap` and/or
`existingSecret` and the chart skips rendering its own. The full field surface lives in
the [Configuration Reference](../../reference/configuration.md), and every chart value
is documented inline in [values.yaml](../../../deploy/helm/rss2msg/values.yaml).

## Metrics

With `metricsService.enabled: true` (default) the chart exposes a `-metrics` Service on
port 9090. Metrics are only produced when `config.telemetry.prometheus.enabled` is true
(the chart default). If you run the Prometheus Operator, set
`serviceMonitor.enabled: true` to scrape it automatically. The instruments are listed in
[Telemetry](../../reference/telemetry.md).

## Persistence

The default config uses the SQLite state store. For a `serve` Deployment that should
survive restarts, set `persistence.enabled: true` to bind a PVC at the directory holding
`config.state.sqlite.path`. Postgres-backed state needs no volume.

## Scaling

rss2msg has **no leader election**. Running more than one `serve` replica without a
shared coordinator makes every replica poll every feed. Keep `deployment.replicaCount: 1`,
or configure a shared Postgres/Redis coordinator in `config` — see
[Run Multiple Instances](../run-multiple-instances.md).

## Verify before installing

```bash
helm lint ./deploy/helm/rss2msg
helm template rss2msg ./deploy/helm/rss2msg -f my-values.yaml
```

## Related

- [Deploy on Kubernetes](kubernetes.md) — the raw manifests this chart packages.
- [Configure Kubernetes Health Probes](../configure-kubernetes-health-probes.md) — probe endpoint semantics.
- [Run Multiple Instances](../run-multiple-instances.md) — shared coordinator for multiple replicas.
- [Telemetry](../../reference/telemetry.md) — Prometheus listener and metrics.
- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
