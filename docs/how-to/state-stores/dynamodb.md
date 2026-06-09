---
title: DynamoDB state store
type: how-to
tags: [rss2msg/docs, state, dynamodb, aws]
summary: Persist seen-item state in a shared DynamoDB table with optional TTL auto-pruning.
updated: 2026-06-09
---

# DynamoDB state store

A shared, distributed-safe table with strongly-consistent reads. A feed's meta and
items share a partition (`feed_url`), with the meta row under a reserved `#META`
sort key. Old seen-items can be auto-pruned with optional TTL.

## Configure

```yaml
state:
  driver: dynamodb
  dynamodb:
    table: rss2msg-state    # PK feed_url (S) + SK item_id (S), provisioned out of band
    region: us-east-1
    endpoint_url:           # LocalStack / DynamoDB Local override
    ttl_attribute: expires_at
    item_ttl: 720h
```

| field | required | notes |
| --- | --- | --- |
| `dynamodb.table` | yes | DynamoDB table name. Provision it out of band with partition key `feed_url` (String) and sort key `item_id` (String); the store does not create it. |
| `dynamodb.region` | no | AWS region. Empty uses the SDK default chain (env, shared config, instance metadata). |
| `dynamodb.endpoint_url` | no | Service endpoint override for LocalStack / DynamoDB Local. |
| `dynamodb.ttl_attribute` | no | Names the DynamoDB TTL attribute (epoch seconds) written on item rows. Must match the table's `TimeToLiveSpecification` for auto-pruning to take effect. Requires `item_ttl`. |
| `dynamodb.item_ttl` | yes (with `ttl_attribute`) | How long an item row lives after its last seen time, e.g. `720h`. Must be set together with `ttl_attribute`. |

**When to use:** production, multi-instance, AWS-native / serverless deployments.

## Related

- [Choose a State Store](../choose-a-state-store.md) — the overview, comparison table, and shared schema.
- [Run Multiple Instances](../run-multiple-instances.md) — pairing a shared store with a distributed coordinator.
