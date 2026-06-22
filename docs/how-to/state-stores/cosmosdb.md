---
title: Cosmos DB state store
type: how-to
tags: [rss2msg/docs, state, cosmosdb, azure]
summary: Persist seen-item state in a shared Azure Cosmos DB container with optional per-item TTL pruning.
updated: 2026-06-22
---

# Cosmos DB state store

A shared, distributed-safe container partitioned on `/feed_url`. Item rows are keyed
by `sha256(item_id)`; a feed's meta row uses the reserved id `__meta__`. Old
seen-items can be auto-pruned with Cosmos-native TTL (the meta row never expires).

## Configure

```yaml
state:
  driver: cosmosdb
  # item_ttl: 720h            # retention since last_seen_at; 0/unset = keep forever (default)
  cosmosdb:
    # exactly one of endpoint (Entra ID) or connection_string (account key)
    endpoint: ${COSMOS_ENDPOINT}             # Entra ID auth; OR connection_string below
    connection_string: ${COSMOS_CONNECTION}  # account-key auth (mutually exclusive)
    database: rss2msg
    container: feed_state    # default feed_state; partitioned on /feed_url
    create_if_missing: false # create db/container (TTL-enabled) on startup (dev/test)
    throughput: 0            # manual RU/s when creating; 0 = serverless/shared
```

| field | required | notes |
| --- | --- | --- |
| `cosmosdb.endpoint` | one-of | Cosmos account endpoint; authenticates with `DefaultAzureCredential`. Mutually exclusive with `connection_string`. |
| `cosmosdb.connection_string` | one-of | Account-key connection string. Mutually exclusive with `endpoint`. |
| `cosmosdb.database` | yes | Database name. |
| `cosmosdb.container` | no | Container name; defaults to `feed_state`. Partitioned on `/feed_url`; provision out of band (with TTL enabled if you use `state.item_ttl`) unless `create_if_missing`. |
| `cosmosdb.create_if_missing` | no | Create the database/container on startup (TTL-enabled when `state.item_ttl` is set). Dev/test convenience; pre-provision in production. |
| `cosmosdb.throughput` | no | Manual RU/s applied when creating the container; `0` leaves it unset (serverless / shared). |
| `state.item_ttl` | no | Universal retention duration (e.g. `720h`) since an item was last seen. `0`/unset keeps rows forever. When set, each item row is written with Cosmos' reserved `ttl` property (seconds); the service prunes expired documents automatically. Requires the container to have TTL enabled. Meta rows never expire. See [Choose a State Store — Retention and cleanup](../choose-a-state-store.md#retention-and-cleanup). |

**When to use:** production, multi-instance, Azure-native / serverless deployments.

## Related

- [Choose a State Store](../choose-a-state-store.md) — the overview, comparison table, and shared schema.
- [Cosmos DB coordinator](../coordinators/cosmosdb.md) — the matching Azure-native coordinator.
- [Run Multiple Instances](../run-multiple-instances.md) — pairing a shared store with a distributed coordinator.
