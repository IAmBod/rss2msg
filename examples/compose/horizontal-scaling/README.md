# Horizontal scaling (shared coordinator)

Three rss2msg replicas behind a shared **Redis coordinator** with a shared **Postgres
state store** — the multi-instance pattern from
[Run multiple instances](../../../docs/how-to/run-multiple-instances.md). The
coordinator gates poll cycles so each feed is polled by **one replica at a time**: no
leader election, no duplicate publishes, and a replica dying just hands its feeds to the
others.

## Run it

```bash
docker compose up                      # 3 replicas
docker compose up --scale rss2msg=5    # or scale to taste
```

Watch the logs interleave across replicas:

```bash
docker compose logs -f rss2msg
```

Each line carries a distinct instance identity (the container hostname). You'll see
different replicas pick up different feeds across cycles — and no item published twice.

Tear down with `docker compose down -v`.

## Why both a coordinator *and* shared state

These two pieces solve different problems, and you need both to scale safely:

- **Coordinator (Redis)** — decides *who polls what, when*, so replicas don't all hit
  the same feed in the same cycle.
- **Shared state (Postgres)** — the common "already seen" memory. Without it, each
  replica keeps its own SQLite and would re-emit items the others already published.

Pointing replicas at a coordinator while leaving them on per-container SQLite is a
misconfiguration the binary warns about — see
[Operational notes](../../../docs/explanation/operations.md).

## What's here

- **`redis`** — `redis:7-alpine`, the lease coordinator (`coordination.driver: redis`).
- **`postgres`** — `postgres:17-alpine`, the shared state store.
- **`rss2msg`** — `deploy.replicas: 3`, each reading the same `config.yaml` and getting
  `${REDIS_URL}` / `${POSTGRES_DSN}` from the environment.

## Try changing it

- **Swap the coordinator** — Postgres advisory locks (`coordination.driver: postgres`)
  or DynamoDB leases work too; see [Run multiple instances](../../../docs/how-to/run-multiple-instances.md).
- **Add real sinks** — replace the `stdout` sink with Kafka/RabbitMQ/Postgres from the
  sibling examples.
- **Redis HA** — point at Sentinel or a cluster via `coordination.redis.mode`.
