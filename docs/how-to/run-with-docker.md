---
title: Run with Docker
type: how-to
tags: [rss2msg/docs, operations, docker]
summary: Build and run rss2msg with the multi-stage Dockerfile — a hot-reload development image, the distroless production image published to GHCR by the release pipeline, a Docker Compose dev stack, and the binary's built-in healthcheck.
updated: 2026-06-03
---

# Run with Docker

The repo ships a single multi-stage [`Dockerfile`](../../Dockerfile) with two stages
you use directly:

| Stage | Base | Use it for |
| --- | --- | --- |
| `development` | `golang:1.25-bookworm` + [`air`](https://github.com/air-verse/air) | Local development with hot reload — built from source. |
| `production` (final stage) | `gcr.io/distroless/static-debian12:nonroot` | The small, rootless runtime image. Published to GHCR by the [release pipeline](../development/releasing.md). |

Because rss2msg uses a pure-Go SQLite driver, the binary builds with `CGO_ENABLED=0`
and needs no libc at runtime — the production image is just the static binary on a
distroless base.

The `production` stage does **not** compile from source: it packages a binary that
GoReleaser cross-compiles and stages in the build context (`COPY $TARGETPLATFORM/rss2msg`).
So a plain `docker build --target production .` won't work — there's no prebuilt binary
in a bare build context. Build a production image locally through GoReleaser instead.

## Build the images

The **development** image builds from source, so a plain `docker build` works:

```bash
docker build --target development -t rss2msg:dev .   # or: task docker-build-dev
```

The **production** image is built by GoReleaser (it supplies the prebuilt binary). For
a local one, run a GoReleaser snapshot — it produces per-arch images tagged
`ghcr.io/iambod/rss2msg:<version>-next-<arch>`:

```bash
task docker-build       # goreleaser release --snapshot --skip=publish
```

The released production image is published to GHCR on every version tag — see
[Releasing](../development/releasing.md). To just pull and run it, skip the build:

```bash
docker pull ghcr.io/iambod/rss2msg:latest
```

## Develop with hot reload (Docker Compose)

[`docker-compose.yml`](../../docker-compose.yml) runs the `development` image with
the source tree bind-mounted, so `air` rebuilds and restarts on every change.

```bash
cp examples/config.example.yaml config.yaml   # the app reads ./config.yaml from the mount
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
  ghcr.io/iambod/rss2msg:latest serve
```

Override the command for one-shot or validation runs:

```bash
# Poll every feed once and exit
docker run --rm -v "$PWD/config.yaml:/etc/rss2msg/config.yaml:ro" ghcr.io/iambod/rss2msg:latest run-once

# Validate config and reachability, then exit non-zero on failure
docker run --rm -v "$PWD/config.yaml:/etc/rss2msg/config.yaml:ro" ghcr.io/iambod/rss2msg:latest validate-config
```

Inject secrets as environment variables rather than baking them into the image —
DSNs and tokens are read via `${VAR}` substitution and `RSS2MSG_`-prefixed
overrides (see [Deploy in Production](deploy.md)):

```bash
docker run --rm \
  -e POSTGRES_DSN \
  -v "$PWD/config.yaml:/etc/rss2msg/config.yaml:ro" \
  ghcr.io/iambod/rss2msg:latest serve
```

The image runs as the `nonroot` user. It contains no shell or package manager, so
container health probes can't shell out to `curl`/`wget`. The binary covers this
itself — see below — and orchestrator-native probes (a Kubernetes liveness probe
against `/healthz`, etc.) work as usual.

## Health checks

The serve daemon exposes Kubernetes-style probe endpoints — `/healthz` (liveness),
`/readyz` (readiness, which also checks the state store and coordinator), and
`/startupz` (startup) — on `health.listen` (default `:8080`). See
[Kubernetes health probes](kubernetes-health-probes.md) for the endpoint details.

Because the distroless image has no shell, `curl`, or `wget`, the classic
`HEALTHCHECK CMD curl …` can't run. Instead the binary ships a `healthcheck`
subcommand that probes its own endpoint over HTTP and exits `0` (healthy) or
non-zero (unhealthy). It reads the same config as `serve`, so it targets whatever
`health.listen` binds (rewriting a wildcard host like `:8080` to `127.0.0.1`):

```bash
# Inside the container, or via docker exec on a runtime that allows it:
rss2msg healthcheck                 # readiness probe (default)
rss2msg healthcheck --probe liveness
rss2msg healthcheck --timeout 5s
```

The production image wires this into its `HEALTHCHECK` instruction, so
`docker ps` and orchestrators that honor image health report status out of the box.
To set it yourself in Compose, use the exec form (there's no shell to interpret a
string command):

```yaml
healthcheck:
  test: ["CMD", "rss2msg", "healthcheck"]
  interval: 30s
  timeout: 3s
  start_period: 10s
  retries: 3
```

## Related

- [Deploy in Production](deploy.md) — config resolution, secrets, scaling, observability.
- [Run Multiple Instances](run-multiple-instances.md) — shared coordinator setup.
- [Building and Testing](../development/building-and-testing.md) — build from source without Docker.
- [CLI](../reference/cli.md) — commands and config resolution.
