---
title: Choose a State Store
type: how-to
tags: [rss2msg/docs, state, scaling]
summary: Persist seen-item state and HTTP cache validators with the sqlite, postgres, dynamodb, or cosmosdb state store.
updated: 2026-06-22
---

# Choose a State Store

The state store records `(feed_url, item_id) → content_hash, last_seen_at` so the
detector can classify each polled item as new, updated, or unchanged. It also holds
per-feed HTTP cache validators (`ETag`, `Last-Modified`) so subsequent polls send
conditional requests. The state store is **required**.

```yaml
state:
  driver: postgres        # postgres | sqlite | dynamodb | cosmosdb
```

## Choose a backend

Each backend has its own page with the full config block and field reference:

| driver | concurrency / scope | when to use | page |
| ------ | ------------------- | ----------- | ---- |
| `sqlite` | Single file on local disk. WAL + busy-timeout enabled by default; the store uses one connection so writes are serialised in-process. Not shared between processes/nodes. | Single-instance deployments, local dev, edge / embedded contexts. | [SQLite](state-stores/sqlite.md) |
| `postgres` | Shared across instances; writers serialised by the DB. | Production, multi-instance, or when state already lives in Postgres. | [Postgres](state-stores/postgres.md) |
| `dynamodb` | Shared, distributed-safe table; strongly-consistent reads. A feed's meta and items share a partition (`feed_url`) with the meta row under a reserved `#META` sort key. Optional TTL auto-pruning of old seen-items. | Production, multi-instance, AWS-native / serverless deployments. | [DynamoDB](state-stores/dynamodb.md) |
| `cosmosdb` | Shared, distributed-safe container partitioned on `/feed_url`. Item rows are keyed by `sha256(item_id)`; a feed's meta row uses the reserved id `__meta__`. Optional per-item `ttl` auto-pruning of old seen-items. | Production, multi-instance, Azure-native / serverless deployments. | [Cosmos DB](state-stores/cosmosdb.md) |

> [!warning]
> **Multiple instances need a shared store.** The `sqlite` store is a local
> per-instance file, so each instance keeps its own seen-items set and republishes
> items its peers already sent. When you run more than one instance with a
> distributed coordinator (`redis`, `postgres`, `dynamodb`, or `cosmosdb`), use
> `postgres`, `dynamodb`, or `cosmosdb` for state too. Validation warns if it sees a
> distributed coordinator
> paired with `state.driver: sqlite`. See
> [Run Multiple Instances](run-multiple-instances.md).

## Schema

Schema created on first start (idempotent `CREATE TABLE IF NOT EXISTS`). The
Postgres DDL is shown; the SQLite store uses the same logical schema with
`TEXT` columns for timestamps (RFC3339Nano UTC), and `ON CONFLICT … DO
UPDATE SET col = excluded.col` upserts.

```sql
CREATE TABLE seen_items (
    feed_url     TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (feed_url, item_id)
);

CREATE TABLE feed_meta (
    feed_url      TEXT PRIMARY KEY,
    etag          TEXT NOT NULL DEFAULT '',
    last_modified TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL
);
```

## Retention and cleanup

Set `state.item_ttl` to automatically remove seen-item rows that have not been observed for the configured duration. `0` (the default) keeps rows forever — this is the behavior prior to this feature.

```yaml
state:
  driver: sqlite       # or postgres | dynamodb | cosmosdb
  item_ttl: 720h       # delete rows last seen more than 30 days ago; 0/unset = keep forever
  sqlite:
    cleanup_interval: 1h   # SQL-only: how often to sweep (default 1h when item_ttl > 0)
```

**Anchor: `last_seen_at`, not first-seen.** The expiry clock is reset on every `UpsertItem` call. An item that is still present in a feed has its `last_seen_at` refreshed on every poll, so it is never eligible for pruning while it remains in the feed. Only items that have fallen off the feed for the full `item_ttl` duration are deleted.

> [!warning]
> **Set `item_ttl` comfortably longer than any feed's re-publication window.** If an item disappears from a feed and reappears within the `item_ttl` window it is safe — `last_seen_at` will have been refreshed. But if the TTL expires before the item reappears, the row is deleted; the next poll re-detects the item as new and re-publishes it. Very short TTLs (under one hour) trigger a validation warning for this reason.

**Backend behaviour:**

| backend | how pruning works |
| ------- | ----------------- |
| `sqlite` | App-side: a background goroutine runs `DELETE FROM seen_items WHERE last_seen_at < now - item_ttl` on every `cleanup_interval` tick. An immediate sweep runs at startup. |
| `postgres` | Same app-side sweep as SQLite; `cleanup_interval` controls the cadence. |
| `dynamodb` | Native: each item row is written with an epoch-seconds expiry; DynamoDB prunes expired rows automatically. Requires `state.dynamodb.ttl_attribute` to name the attribute. |
| `cosmosdb` | Native: each item row is written with a Cosmos `ttl` property; the service prunes expired documents automatically. Requires TTL to be enabled on the container. |

`feed_meta` rows (ETag / Last-Modified per feed) are **never** pruned by any backend.

**Scaled-mode note (SQL backends).** The `DELETE` is partitioned by time, so multiple instances can run their sweeps concurrently without a coordinator lock — overlapping deletes simply remove the same already-eligible rows.

## Related

- [SQLite state store](state-stores/sqlite.md) — the single-instance default.
- [Postgres state store](state-stores/postgres.md) — shared store for multi-instance.
- [DynamoDB state store](state-stores/dynamodb.md) — shared store with DynamoDB-native TTL pruning.
- [Cosmos DB state store](state-stores/cosmosdb.md) — Azure-native shared store with Cosmos-native TTL pruning.
- [Run Multiple Instances](run-multiple-instances.md) — pairing a shared store with a distributed coordinator.
- [Secure Connections (TLS)](secure-connections-tls.md) — TLS for the Postgres state store.
- [Configuration Reference](../reference/configuration.md) — loading order, env vars, and every other field.
