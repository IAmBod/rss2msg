---
title: Deploy with Docker Compose
type: how-to
tags: [rss2msg/docs, operations, deployment, docker]
summary: Run the published rss2msg image as a long-lived service with Docker Compose — pinned image, read-only config, secrets via env, and optional Postgres/Redis backing services.
updated: 2026-06-01
---

# Deploy with Docker Compose

This is the production counterpart to the hot-reload dev stack in
[Run with Docker](../run-with-docker.md): a Compose file that runs the **published
image** (not a source build) as a long-lived `serve` daemon. For the underlying
config, secrets, and observability surface, see
[Deploy in Production](../deploy.md).

## Compose file

```yaml
# compose.yaml — production rss2msg
services:
  rss2msg:
    image: ghcr.io/iambod/rss2msg:latest   # pin a version in production, e.g. :v1.2.3
    command: ["serve"]                      # the image's default command
    restart: unless-stopped
    # Config is resolved from --config, then ./config.yaml, then
    # /etc/rss2msg/config.yaml; mount yours where the binary looks for it.
    volumes:
      - ./config.yaml:/etc/rss2msg/config.yaml:ro
    # Keep DSNs and tokens out of the file — config.yaml references them as ${VAR}.
    env_file: .env
    ports:
      - "9090:9090"   # Prometheus /metrics, when telemetry.prometheus.enabled
      # - "8080:8080" # health probes / feed-sink HTTP, if you expose them
    # The binary probes its own /readyz endpoint — no shell or curl needed.
    healthcheck:
      test: ["CMD", "rss2msg", "healthcheck"]
      interval: 30s
      timeout: 3s
      start_period: 10s
      retries: 3
    depends_on:
      postgres:
        condition: service_healthy

  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: rss2msg
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: rss2msg
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U rss2msg"]
      interval: 5s
      timeout: 3s
      retries: 5

volumes:
  pgdata:
```

Your `config.yaml` references secrets as `${VAR}` so the values live in `.env`,
not the image or the file:

```yaml
state:
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}
```

```dotenv
# .env (git-ignored)
POSTGRES_PASSWORD=change-me
POSTGRES_DSN=postgres://rss2msg:change-me@postgres:5432/rss2msg?sslmode=disable
```

Bring it up and validate first:

```bash
docker compose run --rm rss2msg validate-config   # exits non-zero on any failure
docker compose up -d
```

## Notes

- **Health checks.** The image is distroless with no shell or `curl`, so a
  classic `healthcheck` command can't shell out. The binary covers this itself:
  `rss2msg healthcheck` probes its own endpoint over HTTP and exits `0`/non-zero,
  which is what the Compose `healthcheck` above (and the image's built-in
  `HEALTHCHECK`) run. It defaults to `/readyz`; use `--probe liveness` or
  `--probe startup` for the others. `restart: unless-stopped` still covers fatal
  exits. See [Run with Docker](../run-with-docker.md#health-checks) for the
  subcommand and [Configure Kubernetes Health Probes](../configure-kubernetes-health-probes.md) for
  endpoint semantics.
- **One poller per feed set.** A single instance is the simplest setup. To run more
  than one replica, point them at a shared coordinator so they don't double-poll —
  see [Run Multiple Instances](../run-multiple-instances.md).
- **Scheduled runs.** For a cron-style job instead of a daemon, run
  `docker compose run --rm rss2msg run-once` from your host scheduler rather than
  keeping `serve` up.

## Related

- [Run with Docker](../run-with-docker.md) — the hot-reload development stack and image build.
- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
- [Run Multiple Instances](../run-multiple-instances.md) — shared coordinator setup.
- [CLI](../../reference/cli.md) — `serve`, `run-once`, `validate-config`.
