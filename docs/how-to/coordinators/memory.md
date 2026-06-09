---
title: Memory coordinator
type: how-to
tags: [rss2msg/docs, coordination, scaling, memory]
summary: The default single-instance coordinator — always grants the lease, no shared backend.
updated: 2026-06-09
---

# Memory coordinator

The default coordinator. It always grants the lease, so every poll cycle runs —
correct for a **single-instance** deployment. Running more than one instance with
the memory coordinator makes each instance poll every feed independently.

## Configure

```yaml
coordination:
  driver: memory
```

`memory` needs no shared backend and no extra configuration. Existing configs with
no `coordination` block use it implicitly.

| Property | Value |
| --- | --- |
| Mechanism | Always grants the lease. |
| Crash recovery | n/a |
| Shared state store required | No |

To scale horizontally, switch to a distributed coordinator
([postgres](postgres.md), [redis](redis.md), or [dynamodb](dynamodb.md)) and pair it
with a shared [state store](../choose-a-state-store.md).

## Related

- [Run Multiple Instances](../run-multiple-instances.md) — the coordinator overview, comparison table, and lock mechanics.
- [Choose a State Store](../choose-a-state-store.md) — the state store.
