---
title: Deploy on Kubernetes
type: how-to
tags: [rss2msg/docs, operations, deployment, kubernetes]
summary: Run rss2msg on Kubernetes — a ConfigMap for config.yaml, a Secret for DSNs and tokens, a Deployment with health probes, a metrics Service, and a CronJob for scheduled runs.
updated: 2026-06-01
---

# Deploy on Kubernetes

This recipe assembles the full manifest set for running rss2msg on Kubernetes. It
builds on [Kubernetes Health Probes](../kubernetes-health-probes.md), which covers
the liveness/readiness/startup endpoints in detail, and
[Deploy in Production](../deploy.md) for the config and secrets model.

## 1. Config in a ConfigMap

rss2msg resolves its config from `--config`, then `./config.yaml`, then
`/etc/rss2msg/config.yaml` (see [CLI](../../reference/cli.md)). Mount a ConfigMap at
that path and reference secrets as `${VAR}` so they stay out of the ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: rss2msg-config
data:
  config.yaml: |
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

## 2. Secrets in a Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: rss2msg-secrets
type: Opaque
stringData:
  POSTGRES_DSN: postgres://rss2msg:change-me@postgres:5432/rss2msg?sslmode=require
```

## 3. Deployment

The container injects the Secret as environment variables (consumed by `${VAR}`
substitution) and mounts the ConfigMap read-only. The published image is already
rootless and distroless, so no extra `securityContext` is required for it to run as
non-root. The probe block is summarized here — see
[Kubernetes Health Probes](../kubernetes-health-probes.md) for what each endpoint means.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rss2msg
spec:
  replicas: 1   # see "Scaling" below before raising this
  selector:
    matchLabels:
      app: rss2msg
  template:
    metadata:
      labels:
        app: rss2msg
    spec:
      containers:
        - name: rss2msg
          image: ghcr.io/iambod/rss2msg:latest   # pin a version in production
          args: ["serve", "--config", "/etc/rss2msg/config.yaml"]
          envFrom:
            - secretRef:
                name: rss2msg-secrets
          ports:
            - name: health
              containerPort: 8080
            - name: metrics
              containerPort: 9090
          startupProbe:
            httpGet: { path: /startupz, port: health }
            failureThreshold: 30
            periodSeconds: 2
          livenessProbe:
            httpGet: { path: /healthz, port: health }
          readinessProbe:
            httpGet: { path: /readyz, port: health }
          volumeMounts:
            - name: config
              mountPath: /etc/rss2msg
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: rss2msg-config
```

## 4. Metrics Service (optional)

Expose the Prometheus listener (set `telemetry.prometheus.enabled: true`, default
listen `:9090`) for scraping:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: rss2msg-metrics
  labels:
    app: rss2msg
spec:
  selector:
    app: rss2msg
  ports:
    - name: metrics
      port: 9090
      targetPort: metrics
```

Point a `ServiceMonitor`/`PodMonitor` (or a `prometheus.io/scrape` annotation) at
`/metrics` on this port. The instruments to watch are listed in
[Telemetry](../../reference/telemetry.md).

## 5. Scheduled runs with a CronJob

Prefer a periodic job over a daemon? Run `run-once`, which polls every feed once and
exits:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: rss2msg-run-once
spec:
  schedule: "*/5 * * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: rss2msg
              image: ghcr.io/iambod/rss2msg:latest
              args: ["run-once", "--config", "/etc/rss2msg/config.yaml"]
              envFrom:
                - secretRef:
                    name: rss2msg-secrets
              volumeMounts:
                - name: config
                  mountPath: /etc/rss2msg
                  readOnly: true
          volumes:
            - name: config
              configMap:
                name: rss2msg-config
```

## Scaling

There is no leader election. Running more than one `serve` replica without a shared
coordinator makes every replica poll every feed. Keep `replicas: 1`, or point all
replicas at a shared Postgres/Redis coordinator — see
[Run Multiple Instances](../run-multiple-instances.md).

## Related

- [Deploy with the Helm chart](helm.md) — these manifests packaged as a templated, versioned chart.
- [Kubernetes Health Probes](../kubernetes-health-probes.md) — endpoint semantics and probe tuning.
- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
- [Run Multiple Instances](../run-multiple-instances.md) — shared coordinator for multiple replicas.
- [Secure Connections (TLS)](../secure-connections-tls.md) — TLS to Postgres/Redis.
- [Telemetry](../../reference/telemetry.md) — Prometheus listener and metrics.
