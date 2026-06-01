# rss2msg Helm chart

A Helm chart for [rss2msg](https://github.com/IAmBod/rss2msg) — poll RSS/Atom feeds,
detect changes, and publish items to message sinks.

This chart packages the manifest set described in
[Deploy on Kubernetes](../../../docs/how-to/deploy/kubernetes.md): a ConfigMap for
`config.yaml`, an optional Secret for DSNs/tokens, the workload (a long-running
`serve` Deployment **or** a scheduled `run-once` CronJob), a metrics Service, and an
optional Prometheus `ServiceMonitor`.

## Install

From a checkout of the repository:

```bash
helm install rss2msg ./deploy/helm/rss2msg -f my-values.yaml
```

## Workload mode

`mode` selects which workload is rendered — only one is created:

- `mode: deployment` (default) — a long-running `serve` daemon with health probes
  and a metrics Service.
- `mode: cronjob` — a `run-once` CronJob on `cronjob.schedule` that polls every feed
  once and exits.

## Configuration

The application config (`config.yaml`) is rendered from `.Values.config` into a
ConfigMap and mounted at `/etc/rss2msg/config.yaml`. Keep secrets out of it: reference
them as `${VAR}` and supply the values through `secrets` (rendered into a Secret and
injected via `envFrom`) or point at an externally-managed Secret with `existingSecret`.
Likewise, `existingConfigMap` lets you supply your own ConfigMap instead of rendering
`config`.

See the [Configuration Reference](../../../docs/reference/configuration.md) for the full
field surface.

### Key values

| Key | Default | Description |
| --- | --- | --- |
| `mode` | `deployment` | `deployment` (serve daemon) or `cronjob` (scheduled run-once). |
| `image.repository` | `ghcr.io/iambod/rss2msg` | Container image. |
| `image.tag` | `""` (chart `appVersion`) | Image tag. Pin a version in production. |
| `deployment.replicaCount` | `1` | `serve` replicas. **No leader election** — see Scaling. |
| `config` | minimal stdout config | Rendered into `config.yaml`. |
| `existingConfigMap` | `""` | Use this ConfigMap instead of rendering `config`. |
| `secrets` | `{}` | Key/value pairs rendered into a Secret and injected as env vars. |
| `existingSecret` | `""` | Inject this Secret's keys instead of rendering `secrets`. |
| `probes.*` | enabled | Startup/liveness/readiness probes (deployment mode). |
| `metricsService.enabled` | `true` | Prometheus metrics Service (deployment mode). |
| `serviceMonitor.enabled` | `false` | Prometheus Operator `ServiceMonitor` (needs the CRD). |
| `persistence.enabled` | `false` | PVC for the SQLite state store (deployment mode). |
| `cronjob.schedule` | `*/5 * * * *` | Schedule when `mode: cronjob`. |

The full set is documented inline in [values.yaml](values.yaml).

## Scaling

rss2msg has **no leader election**. Running more than one `serve` replica without a
shared coordinator makes every replica poll every feed. Keep `deployment.replicaCount: 1`,
or configure a shared Postgres/Redis coordinator in `config` — see
[Run Multiple Instances](../../../docs/how-to/run-multiple-instances.md).

## Verify the chart

```bash
helm lint ./deploy/helm/rss2msg
helm template rss2msg ./deploy/helm/rss2msg -f deploy/helm/rss2msg/ci/deployment-values.yaml
helm template rss2msg ./deploy/helm/rss2msg -f deploy/helm/rss2msg/ci/cronjob-values.yaml
```

`task helm-lint` runs lint plus a render of both `ci/` value sets.
