# Docker Compose examples

Copy-pasteable [Docker Compose](https://docs.docker.com/compose/) stacks that run
rss2msg in common deployment shapes. Each subdirectory is self-contained — a
`docker-compose.yml`, the config it needs, and a `README.md` explaining what it shows
and how to run it.

| Example | What it demonstrates |
| --- | --- |
| [`minimal/`](minimal/) | The "hello world": a single rss2msg container with a `stdout` sink, configured from a **mounted file**. |
| [`env-config/`](env-config/) | The 12-factor path — configuration driven entirely by **environment variables** (`RSS2MSG_*` overrides + `${VAR}` substitution). |
| [`kafka/`](kafka/) | rss2msg → a single-node **Kafka** (KRaft) broker via the `kafka` sink, with a console consumer to watch messages land. |
| [`rabbitmq/`](rabbitmq/) | rss2msg → **RabbitMQ** via the `rabbitmq` sink, with a pre-declared queue and the management UI exposed. |
| [`postgres/`](postgres/) | **Postgres** as both the **state store** and a `postgres` **sink** — the change-log / datastore pattern. |
| [`horizontal-scaling/`](horizontal-scaling/) | Multiple rss2msg replicas behind a shared **Redis coordinator** with **Postgres state** — safe multi-instance scaling without leader election. |
| [`observability/`](observability/) | rss2msg's Prometheus `/metrics`, scraped by **Prometheus** and graphed in a pre-provisioned **Grafana**. |

## Conventions shared by every example

- **They run the published image.** Each stack pulls
  `ghcr.io/iambod/rss2msg:latest` rather than building. The production image is
  supplied by the release pipeline (GoReleaser) and can't be built from a bare
  context — see [Run with Docker](../../docs/how-to/run-with-docker.md). Override the
  tag with the `RSS2MSG_IMAGE` environment variable, e.g.
  `RSS2MSG_IMAGE=ghcr.io/iambod/rss2msg:v1.2.3 docker compose up`.
- **Config lives at `/etc/rss2msg/config.yaml`.** The binary resolves config from
  `--config`, then `./config.yaml`, then `/etc/rss2msg/config.yaml`, so each example
  bind-mounts its `config.yaml` there read-only (except `env-config/`, which leans on
  env vars).
- **Secrets are injected, not baked in.** Connection strings and tokens are passed via
  `${VAR}` substitution and `RSS2MSG_`-prefixed overrides.
- **Backing services use healthchecks.** rss2msg waits on
  `depends_on: { condition: service_healthy }` so it starts only once its broker /
  datastore is ready.
- **Output is visible on first run.** On an empty state store every current feed item
  is classified as `new` and published, so you'll see envelopes immediately — then only
  genuinely new or changed items thereafter.

> These are **deployment-shaped** examples. For local development with hot reload, use
> the repo-root [`docker-compose.yml`](../../docker-compose.yml) instead (see
> [Run with Docker](../../docs/how-to/run-with-docker.md)).

## Running one

```bash
cd <example>
docker compose up            # add -d to detach
# ... watch it work ...
docker compose down -v       # -v also drops the data volumes
```

Each example's README covers the specifics (which ports it exposes, how to view the
output, and what to tweak).
