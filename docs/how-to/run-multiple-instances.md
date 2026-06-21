---
title: Run Multiple Instances
type: how-to
tags: [rss2msg/docs, coordination, scaling]
summary: Gate poll cycles across horizontally-scaled instances with the memory, postgres, redis, dynamodb, or cosmosdb coordinator.
updated: 2026-06-09
---

# Run Multiple Instances

Gates which instance is allowed to poll a given feed in a given cycle, for
horizontally-scaled deployments. The default is single-instance (`memory`,
always grants the lease).

> [!warning]
> **A distributed coordinator needs a shared state store.** The coordinator only
> serialises *polling*; deduplication of already-seen items lives in the
> [state store](choose-a-state-store.md). The `sqlite` state store is a
> local per-instance file, so each instance keeps its own seen-items set: instance B
> will republish every item instance A already sent. When you set
> `coordination.driver` to `redis`, `postgres`, `dynamodb`, or `cosmosdb`, also set a
> shared state store (`state.driver: postgres`, `dynamodb`, or `cosmosdb`) so every
> instance shares one dedup set. Validation emits a warning if it sees a distributed
> coordinator paired with `state.driver: sqlite`.

## Choose a coordinator

```yaml
coordination:
  driver: memory   # memory | postgres | redis | dynamodb | cosmosdb ; default memory
```

Each backend has its own page with the full config block, TLS notes, and edge
cases:

| driver | mechanism | crash recovery | page |
| ---------- | --------- | -------------- | ---- |
| `memory`   | always grants the lease | n/a | [Memory](coordinators/memory.md) |
| `postgres` | `pg_try_advisory_lock(int64(sha256(feed_url)[:8]))` per connection | automatic — advisory locks die with the session | [Postgres](coordinators/postgres.md) |
| `redis`    | `SET key token NX EX <lock_ttl>`, background renewal via CAS-checked `PEXPIRE`, release via CAS-checked `DEL`. Key = `rss2msg:coord:<hex(sha256(feed_url))>` | TTL-based — crashed instances release their leases after `lock_ttl` | [Redis](coordinators/redis.md) |
| `dynamodb` | conditional `PutItem` of a lease item `{pk, owner, lease_expiry}` with condition `attribute_not_exists(pk) OR lease_expiry < now`; release is a conditional `DeleteItem` on `owner = :me`. Key = `rss2msg:coord:<hex(sha256(feed_url))>` | expiry-based — a peer reclaims a crashed instance's lock once `lease_expiry` passes (after `lease_duration`) | [DynamoDB](coordinators/dynamodb.md) |
| `cosmosdb` | `CreateItem` of a lease document `{id, pk, owner, lease_expiry}`; on 409 Conflict an expired lease is reclaimed with an ETag-guarded `ReplaceItem` (`If-Match`), release is an ETag-guarded `DeleteItem`. Key = `rss2msg:coord:<hex(sha256(feed_url))>` | expiry-based — a peer reclaims a crashed instance's lock once `lease_expiry` passes (after `lease_duration`) | [Cosmos DB](coordinators/cosmosdb.md) |

`memory` is the default and needs no shared backend. The `postgres`, `redis`,
`dynamodb`, and `cosmosdb` coordinators serialise polling across instances and have
**no leader election** — losing instances skip the cycle silently. The `postgres`
coordinator reuses the state DSN by default; the `redis` coordinator offers `single`
(default), `sentinel` (tested), and `cluster` (best-effort) topologies; the `dynamodb`
and `cosmosdb` coordinators enforce lease liveness with an explicit `lease_expiry`
rather than trusting native TTL.

## Lock mechanics

The pipeline calls `coord.TryAcquire(feedURL)` before each poll. On
`(release, true, nil)` it polls and `release()` runs after; on
`(nil, false, nil)` the cycle is skipped silently (no error). On
`(nil, false, err)` the cycle is skipped, a warn is logged, and the
`feed.poll.skipped{reason="coord_error"}` counter is incremented.

The release function ignores its caller's `ctx` — it uses a fresh 5 s
background context — so a canceled poll ctx (e.g. on SIGTERM) does not leak
the lease.

## Metrics across instances

Each instance only records `feed.fetches` / `feed.changes` for the feeds it
owns that cycle (non-owners increment `feed.poll.skipped` instead), so counts
don't double. To keep replicas distinct in push-based metric backends, every
signal carries `service.instance.id` — set it with `telemetry.instance_id`
(default: `OTEL_SERVICE_INSTANCE_ID`, then the hostname). The CloudWatch and
Graphite exporters add it as a dimension/tag; Prometheus is already per-instance
via separate scrape targets. See
[Telemetry → Multi-instance deployments](../reference/telemetry.md#multi-instance-deployments).

## Related

- [Memory coordinator](coordinators/memory.md) — the single-instance default.
- [Postgres coordinator](coordinators/postgres.md) — advisory-lock gating, reuses the state DSN.
- [Redis coordinator](coordinators/redis.md) — TTL lock with single/sentinel/cluster topologies.
- [DynamoDB coordinator](coordinators/dynamodb.md) — conditional-write lease.
- [Cosmos DB coordinator](coordinators/cosmosdb.md) — ETag-guarded lease document.
- [Secure Connections (TLS)](secure-connections-tls.md) — TLS for the postgres/redis coordinators.
- [Choose a State Store](choose-a-state-store.md) — the state store, which the postgres coordinator's DSN falls back to.
- [Operational Notes](../explanation/operations.md) — no-leader-election semantics and crash recovery.
