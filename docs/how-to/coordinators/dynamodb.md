---
title: DynamoDB coordinator
type: how-to
tags: [rss2msg/docs, coordination, scaling, dynamodb, aws]
summary: Gate polling across instances with a conditional-write lease in a DynamoDB table.
updated: 2026-06-09
---

# DynamoDB coordinator

Serialises polling across instances with a conditional-write lease. Acquire is a
conditional `PutItem` of a lease item `{pk, owner, lease_expiry}` with condition
`attribute_not_exists(pk) OR lease_expiry < now`; release is a conditional
`DeleteItem` on `owner = :me`. The key is `rss2msg:coord:<feed_url>` and each
process uses an owner token (`hostname-pid-randomhex`). Crash recovery is
expiry-based — a peer reclaims a crashed instance's lock once `lease_expiry` passes
(after `lease_duration`). There is no leader election.

> [!warning]
> **Pair it with a shared state store.** The coordinator only serialises *polling*;
> deduplication of already-seen items lives in the
> [state store](../../reference/configuration.md#state). Set `state.driver: postgres`
> so every instance shares one dedup set — otherwise each instance keeps its own
> seen-items set and republishes items its peers already sent. Validation warns if it
> sees a distributed coordinator paired with `state.driver: sqlite`.

## Configure

```yaml
coordination:
  driver: dynamodb
  dynamodb:
    table: rss2msg-coord-locks   # required; lock table with partition key "pk"
    region: us-east-1            # optional; SDK default chain when empty
    endpoint_url: ${AWS_ENDPOINT_URL}  # optional; LocalStack / VPC endpoint override
    lease_duration: 60s          # optional, default 60s; MUST exceed worst-case poll time
```

The DynamoDB coordinator resolves AWS credentials via the default SDK chain (env,
shared config, IRSA / instance profile). The lock table must already exist with a
partition-key attribute named `pk` (type `S`); the coordinator does not create it.
`lease_duration` **must safely exceed your worst-case per-feed poll time** — if a
poll outruns the lease, a peer may steal the lock mid-poll and both instances poll
the same feed concurrently. The coordinator does **not** rely on DynamoDB native TTL
for lock liveness (native TTL deletion can lag up to ~48h); expiry is enforced inside
the conditional write.

| Property | Value |
| --- | --- |
| Mechanism | Conditional `PutItem` lease `{pk, owner, lease_expiry}`; release is conditional `DeleteItem` on `owner`. |
| Crash recovery | Expiry-based — a peer reclaims the lock once `lease_expiry` passes. |
| Shared state store required | Yes (`state.driver: postgres`) |

## Related

- [Run Multiple Instances](../run-multiple-instances.md) — the coordinator overview, comparison table, and lock mechanics.
- [Configuration Reference](../../reference/configuration.md#state) — the state store.
