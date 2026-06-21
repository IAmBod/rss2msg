---
title: Cosmos DB coordinator
type: how-to
tags: [rss2msg/docs, coordination, scaling, cosmosdb, azure]
summary: Gate polling across instances with an ETag-guarded lease document in an Azure Cosmos DB container.
updated: 2026-06-09
---

# Cosmos DB coordinator

Serialises polling across instances with an optimistic-concurrency lease. Acquire is
a `CreateItem` of a lease document `{id, pk, owner, lease_expiry}`; on a 409 Conflict
an expired lease is reclaimed with an ETag-guarded `ReplaceItem` (`If-Match`), and
release is an ETag-guarded `DeleteItem`. The key is `rss2msg:coord:<sha256-hex(feed_url)>`
(the same scheme as the Redis and Postgres coordinators; hashing also keeps the
`/`, `?` and `#` of feed URLs out of the Cosmos document id) and
each process uses an owner token (`hostname-pid-randomhex`). Crash recovery is
expiry-based — a peer reclaims a crashed instance's lock once `lease_expiry` passes
(after `lease_duration`). There is no leader election.

> [!warning]
> **Pair it with a shared state store.** The coordinator only serialises *polling*;
> deduplication of already-seen items lives in the
> [state store](../choose-a-state-store.md). Set `state.driver: postgres`,
> `dynamodb`, or `cosmosdb` so every instance shares one dedup set — otherwise each
> instance keeps its own seen-items set and republishes items its peers already sent.
> Validation warns if it sees a distributed coordinator paired with
> `state.driver: sqlite`.

## Configure

```yaml
coordination:
  driver: cosmosdb
  cosmosdb:
    # exactly one of endpoint (Entra ID) or connection_string (account key)
    endpoint: ${COSMOS_ENDPOINT}            # e.g. https://acct.documents.azure.com:443/
    connection_string: ${COSMOS_CONNECTION} # account-key auth (mutually exclusive with endpoint)
    database: rss2msg                       # required
    container: coordination_locks           # optional, default coordination_locks
    create_if_missing: false                # create db/container on startup (dev/test)
    throughput: 0                           # manual RU/s when creating; 0 = serverless/shared
    lease_duration: 60s                     # optional, default 60s; MUST exceed worst-case poll time
```

The Cosmos DB coordinator authenticates with either an account-key
`connection_string` or an `endpoint` plus `DefaultAzureCredential` (env / workload
identity / managed identity) — set **exactly one**. Each feed lock is a document keyed
by the SHA-256 hex of the feed URL and partitioned on `/pk`. Like DynamoDB it enforces lease liveness
with an explicit `lease_expiry` (Cosmos native TTL is not trusted for locks), and the
same `lease_duration` warning applies: it **must safely exceed your worst-case per-feed
poll time** — if a poll outruns the lease, a peer may steal the lock mid-poll and both
instances poll the same feed concurrently. Provision the database/container ahead of
time, or set `create_if_missing: true` for dev/test.

| Property | Value |
| --- | --- |
| Mechanism | `CreateItem` lease `{id, pk, owner, lease_expiry}`; expired leases reclaimed via ETag-guarded `ReplaceItem`; release is ETag-guarded `DeleteItem`. |
| Crash recovery | Expiry-based — a peer reclaims the lock once `lease_expiry` passes. |
| Shared state store required | Yes (`state.driver: postgres`, `dynamodb`, or `cosmosdb`) |

## Related

- [Run Multiple Instances](../run-multiple-instances.md) — the coordinator overview, comparison table, and lock mechanics.
- [Cosmos DB state store](../state-stores/cosmosdb.md) — the matching Azure-native state store.
- [Choose a State Store](../choose-a-state-store.md) — the state store.
