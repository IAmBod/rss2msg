---
title: Azure Cosmos DB sink
type: how-to
tags: [rss2msg/docs, sinks, azure, cosmosdb, nosql]
summary: Persist Changes as JSON documents in an Azure Cosmos DB (NoSQL) container; account-key or Azure AD auth.
updated: 2026-06-08
---

# Azure Cosmos DB sink

Persists each `Change` as a JSON document in an Azure Cosmos DB **NoSQL (Core
API)** container via the official `azcosmos` SDK. Delivery is **idempotent**:
documents are keyed by a stable id derived from `(feed_url, item_id)`, so a
re-delivered change is a no-op rather than a duplicate.

```yaml
- name: cosmos-main
  driver: cosmosdb
  cosmosdb:
    # auth — set exactly one of connection_string or endpoint:
    connection_string: ${AZURE_COSMOS_CONNECTION_STRING}     # account-key auth
    # endpoint: https://my-acct.documents.azure.com:443/     # Azure AD (DefaultAzureCredential)
    database: rss2msg
    container: feed_changes        # default: feed_changes
    create_if_missing: false       # create db/container on startup (dev/test only)
    # throughput: 400              # manual RU/s when creating; omit for serverless/shared
  dead_letter: dlq-main
```

| field               | required | default        | notes |
| ------------------- | -------- | -------------- | ----- |
| `connection_string` | one-of   | —              | Cosmos DB account-key connection string (`AccountEndpoint=...;AccountKey=...;`). Supports `${ENV}` substitution. Mutually exclusive with `endpoint`. |
| `endpoint`          | one-of   | —              | Account endpoint (`https://<acct>.documents.azure.com:443/`). Authenticates with `DefaultAzureCredential` (environment, workload identity, or managed identity). Mutually exclusive with `connection_string`. |
| `database`          | yes      | —              | Database (id) that holds the container. |
| `container`         | no       | `feed_changes` | Container the documents are written to. Partitioned on `/feed_url`. |
| `create_if_missing` | no       | `false`        | When true, the database and container are created on startup if absent. Intended for dev/test — production should pre-provision with the right throughput. |
| `throughput`        | no       | `0`            | Manual RU/s for the container created by `create_if_missing`. `0` leaves throughput unset (serverless or database-shared throughput). Must not be negative. |

Exactly one auth field must be set and `database` is required; both are checked
at config-validation time and again when the sink is constructed.

Document layout:

- The body is the JSON `Change` envelope with an added `id` field.
- `id` is `sha256(feed_url || NUL || item_id)` (hex) — stable and
  collision-resistant; re-publishing the same change overwrites nothing and
  returns success.
- Partition key: `/feed_url` (one logical partition per feed).

Implementation notes:

- One `*azcosmos.Client` per Publisher. The client is HTTP-based and holds no
  long-lived connections, so `Close()` is a no-op.
- Publish uses `CreateItem`; a `409 Conflict` (document already exists) is
  treated as a successful, already-delivered no-op — the same semantics as the
  postgres sink's `ON CONFLICT DO NOTHING`.
- Azure AD auth (`endpoint`) uses `DefaultAzureCredential`, which resolves
  credentials from the environment, workload identity, or managed identity in
  that order — no secret needs to live in the config file.
- A transient write failure surfaces as a publish error and is handled by the
  sink retry + DLQ layer.
- The integration test (`-tags=integration`) runs against the official Azure
  Cosmos DB Linux emulator (`vnext-preview`) via testcontainers (requires
  Docker).

Partitioning note: all documents for one feed live in a single logical
partition, which Cosmos DB caps at 20 GB. This is ample for typical feed-change
volumes; very high-volume feeds that approach the limit should use item TTL or a
different partitioning strategy (tracked for the state-store backend).

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
