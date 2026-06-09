---
title: Choose a State Store
type: how-to
tags: [rss2msg/docs, state, scaling]
summary: Persist seen-item state and HTTP cache validators with the sqlite, postgres, dynamodb, or cosmosdb state store.
updated: 2026-06-09
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

## Related

- [SQLite state store](state-stores/sqlite.md) — the single-instance default.
- [Postgres state store](state-stores/postgres.md) — shared store for multi-instance.
- [DynamoDB state store](state-stores/dynamodb.md) — shared store with optional TTL pruning.
- [Cosmos DB state store](state-stores/cosmosdb.md) — Azure-native shared store with optional per-item TTL.
- [Run Multiple Instances](run-multiple-instances.md) — pairing a shared store with a distributed coordinator.
- [Secure Connections (TLS)](secure-connections-tls.md) — TLS for the Postgres state store.
- [Configuration Reference](../reference/configuration.md) — loading order, env vars, and every other field.
