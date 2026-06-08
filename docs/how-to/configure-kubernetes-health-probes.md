---
title: Configure Kubernetes Health Probes
type: how-to
tags: [rss2msg/docs, operations, kubernetes]
summary: Configure the serve daemon's liveness, readiness, and startup HTTP probes and wire them into a Kubernetes Deployment.
updated: 2026-06-09
---

# Configure Kubernetes Health Probes

The `serve` daemon exposes three Kubernetes-style HTTP health endpoints so an
orchestrator can tell when the process is alive, ready to receive traffic, and
finished booting. The probe listener is enabled by default and runs alongside the
scheduler.

## Endpoints

| Path | Probe | Meaning |
| --- | --- | --- |
| `/healthz` | Liveness | Always `200 ok` while the process is running. A failure here means the process is wedged and should be restarted. |
| `/readyz` | Readiness | `200 ok` once boot has completed, the daemon is not draining, and every dependency check passes. Returns `503` otherwise. |
| `/startupz` | Startup | `503 starting` until boot completes, then `200 ok`. Lets slow-starting pods avoid premature liveness restarts. |

Readiness runs a `Ping` against the state store on every request and returns
`503 state: <error>` when the store is unreachable. When the coordinator backend
supports reachability checks, a `coordination` check is added the same way. On
shutdown (SIGINT/SIGTERM) readiness flips to `503 draining` so Kubernetes stops
routing traffic before the daemon exits, while liveness stays `200` throughout.

## Configuration

```yaml
health:
  enabled: true
  listen: ":8080"               # probe listener address
  liveness_path: /healthz       # 200 while the process is alive
  readiness_path: /readyz       # 200 when started, not draining, deps reachable
  startup_path: /startupz       # 503 until boot completes, then 200
```

These are the defaults — omitting the `health:` block entirely yields the same
behavior. Set `enabled: false` to disable the probe listener.

Validation rules:

- Each path must start with `/`.
- The three paths must be distinct.
- `listen` is required when `enabled: true`.
- If `telemetry.prometheus.enabled` is set and `health.listen` equals
  `telemetry.prometheus.listen`, validation warns that one server will fail to
  bind.

## Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rss2msg
spec:
  replicas: 1
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
          image: rss2msg:latest
          args: ["serve", "--config", "/etc/rss2msg/config.yaml"]
          ports:
            - name: health
              containerPort: 8080
          startupProbe:
            httpGet:
              path: /startupz
              port: health
            failureThreshold: 30
            periodSeconds: 2
          livenessProbe:
            httpGet:
              path: /healthz
              port: health
          readinessProbe:
            httpGet:
              path: /readyz
              port: health
```

The `startupProbe` allows up to 60s (30 × 2s) for boot before the liveness probe
takes over, so a slow first connection to the state store or coordinator does not
trigger a restart loop.

## Related

- [Deploy in Production](deploy.md) — config resolution, secrets, daemon vs job.
- [Configuration](../reference/configuration.md) — full config reference.
- [Telemetry](../reference/telemetry.md) — metrics, traces, and the Prometheus listener.
