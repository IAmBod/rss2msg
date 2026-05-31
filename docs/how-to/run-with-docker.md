---
title: Run with Docker
type: how-to
tags: [rss2msg/docs, operations, docker]
summary: Build and run rss2msg with the multi-stage Dockerfile — a hot-reload development image, a small distroless production image, and a Docker Compose dev stack.
updated: 2026-06-01
---

# Run with Docker

The repo ships a multi-stage [`Dockerfile`](../../Dockerfile) with two targets:

| Target | Base | Use it for |
| --- | --- | --- |
| `development` | `golang:1.25-bookworm` + [`air`](https://github.com/air-verse/air) | Local development with hot reload. |
| `production` | `gcr.io/distroless/static-debian12:nonroot` | A small, rootless runtime image for deployment. |

Because rss2msg uses a pure-Go SQLite driver, the binary builds with `CGO_ENABLED=0`
and needs no libc at runtime — the production image is just the static binary on a
distroless base.

## Build the images

Pick a target explicitly with `--target`:

```bash
# Production: static binary on distroless
docker build --target production -t rss2msg:latest .

# Development: full toolchain + hot reload
docker build --target development -t rss2msg:dev .
```

Or use the task shortcuts:

```bash
task docker-build       # rss2msg:latest (production)
task docker-build-dev   # rss2msg:dev    (development)
```

## Develop with hot reload (Docker Compose)

[`docker-compose.yml`](../../docker-compose.yml) runs the `development` image with
the source tree bind-mounted, so `air` rebuilds and restarts on every change.

```bash
cp config.example.yaml config.yaml   # the app reads ./config.yaml from the mount
docker compose up --build            # or: task docker-up
```

The app listens on the ports it's configured to use:

- `8080` — feed-sink HTTP, when a [feed sink](sinks/feed.md) is configured.
- `9090` — Prometheus `/metrics`, when `telemetry.prometheus.enabled` is `true`.

Need Postgres or Redis to exercise those drivers? Start the optional services with
the `full` profile:

```bash
docker compose --profile full up --build
```

This adds `postgres` (on `5432`) and `redis` (on `6379`). Point your `config.yaml`
at them with `${VAR}` substitution, e.g.:

```yaml
state:
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}
coordination:
  driver: redis
  redis:
    url: ${REDIS_URL}
```

```bash
export POSTGRES_DSN="postgres://rss2msg:rss2msg@postgres:5432/rss2msg?sslmode=disable"
export REDIS_URL="redis://redis:6379"
```

Stop and remove the stack with `docker compose down` (or `task docker-down`).

## Run the production image

The production image's entrypoint is the `rss2msg` binary with a default command of
`serve`. Config is resolved from `--config`, then `./config.yaml`, then
`/etc/rss2msg/config.yaml` (see [CLI](../reference/cli.md)), so mount your config
where the binary will find it:

```bash
docker run --rm \
  -v "$PWD/config.yaml:/etc/rss2msg/config.yaml:ro" \
  -p 9090:9090 \
  rss2msg:latest serve
```

Override the command for one-shot or validation runs:

```bash
# Poll every feed once and exit
docker run --rm -v "$PWD/config.yaml:/etc/rss2msg/config.yaml:ro" rss2msg:latest run-once

# Validate config and reachability, then exit non-zero on failure
docker run --rm -v "$PWD/config.yaml:/etc/rss2msg/config.yaml:ro" rss2msg:latest validate-config
```

Inject secrets as environment variables rather than baking them into the image —
DSNs and tokens are read via `${VAR}` substitution and `RSS2MSG_`-prefixed
overrides (see [Deploy in Production](deploy.md)):

```bash
docker run --rm \
  -e POSTGRES_DSN \
  -v "$PWD/config.yaml:/etc/rss2msg/config.yaml:ro" \
  rss2msg:latest serve
```

The image runs as the `nonroot` user. It contains no shell or package manager, so
define container health probes in your orchestrator (a Kubernetes liveness probe,
a Compose `healthcheck` against `/metrics`, etc.) rather than via `docker exec`.

## Related

- [Deploy in Production](deploy.md) — config resolution, secrets, scaling, observability.
- [Run Multiple Instances](run-multiple-instances.md) — shared coordinator setup.
- [Building and Testing](../development/building-and-testing.md) — build from source without Docker.
- [CLI](../reference/cli.md) — commands and config resolution.
