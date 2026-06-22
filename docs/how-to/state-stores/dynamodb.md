---
title: DynamoDB state store
type: how-to
tags: [rss2msg/docs, state, dynamodb, aws]
summary: Persist seen-item state in a shared DynamoDB table with DynamoDB-native TTL auto-pruning.
updated: 2026-06-22
---

# DynamoDB state store

A shared, distributed-safe table with strongly-consistent reads. A feed's meta and
items share a partition (`feed_url`), with the meta row under a reserved `#META`
sort key. Old seen-items can be auto-pruned with DynamoDB-native TTL.

## Configure

```yaml
state:
  driver: dynamodb
  # item_ttl: 720h            # retention since last_seen_at; 0/unset = keep forever (default)
  dynamodb:
    table: rss2msg-state    # PK feed_url (S) + SK item_id (S), provisioned out of band
    region: us-east-1
    endpoint_url:           # LocalStack / DynamoDB Local override
    ttl_attribute: expires_at   # required when state.item_ttl is set
```

| field | required | notes |
| --- | --- | --- |
| `dynamodb.table` | yes | DynamoDB table name. Provision it out of band with partition key `feed_url` (String) and sort key `item_id` (String); the store does not create it. |
| `dynamodb.region` | no | AWS region. Empty uses the SDK default chain (env, shared config, instance metadata). |
| `dynamodb.endpoint_url` | no | Service endpoint override for LocalStack / DynamoDB Local. |
| `dynamodb.ttl_attribute` | required when `state.item_ttl > 0` | Names the DynamoDB TTL attribute (epoch seconds) written on item rows. Must match the table's `TimeToLiveSpecification` for DynamoDB-native pruning to take effect. |
| `state.item_ttl` | no | Universal retention duration (e.g. `720h`) since an item was last seen. `0`/unset keeps rows forever. When set, each item row is written with an epoch-seconds expiry in `ttl_attribute`; DynamoDB prunes expired rows automatically. See [Choose a State Store — Retention and cleanup](../choose-a-state-store.md#retention-and-cleanup). |

**When to use:** production, multi-instance, AWS-native / serverless deployments.

## Related

- [Choose a State Store](../choose-a-state-store.md) — the overview, comparison table, and shared schema.
- [Run Multiple Instances](../run-multiple-instances.md) — pairing a shared store with a distributed coordinator.
