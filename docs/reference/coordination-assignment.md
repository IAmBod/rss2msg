---
title: Coordination Assignment Reference
type: reference
tags: [rss2msg/docs, coordination, scaling, assignment, reference]
summary: Reference for every coordination.assignment.* configuration key — type, default, meaning, and constraints.
updated: 2026-06-21
---

# Coordination Assignment Reference

The `coordination.assignment` block enables membership-based feed partitioning
across multiple instances. When enabled, each feed is owned by exactly one
instance at a time (determined by rendezvous hashing over the live member set),
replacing the N×M try-acquire pattern with M lease attempts per cycle.

Requires a distributed coordinator driver (`postgres`, `redis`, `dynamodb`, or
`cosmosdb`). With `driver: memory` the feature is a no-op (single instance owns
every feed — identical to today's behavior) and a warning is emitted.

## Example

```yaml
coordination:
  driver: redis            # memory | postgres | redis | dynamodb | cosmosdb
  assignment:
    enabled: false
    strategy: rendezvous      # only "rendezvous" is supported today
    heartbeat_interval: 10s
    member_ttl: 30s           # must exceed heartbeat_interval; dead members expire after this
    rebalance_grace: 5s
```

This block is also shown in the full annotated example at
[`examples/config.example.yaml`](../../examples/config.example.yaml).

## Keys

### `coordination.assignment.enabled`

| | |
|---|---|
| **Type** | bool |
| **Default** | `false` |

When `false` (the default), assignment is disabled: every instance schedules
tickers for every feed and the per-tick `TryAcquire` lease decides who polls.
Behavior is identical to versions before assignment was introduced.

When `true`, each instance registers itself as a member, heartbeats on
`heartbeat_interval`, and only schedules tickers for the feeds it owns under
the rendezvous hash. The per-tick `TryAcquire` guard is retained as a
correctness backstop.

Requires `coordination.driver` to be `postgres`, `redis`, `dynamodb`, or
`cosmosdb`. Setting `enabled: true` with `driver: memory` emits a warning and
is a no-op (a single instance always owns every feed).

Only affects the long-lived `serve` daemon. One-shot modes (`run-once`,
`lambda`, `azure`) ignore assignment entirely.

---

### `coordination.assignment.strategy`

| | |
|---|---|
| **Type** | string |
| **Default** | `rendezvous` |
| **Allowed values** | `rendezvous` |

The partitioning algorithm. Currently `rendezvous` (highest-random-weight /
HRW hashing) is the only supported value; any other value is a validation
error. The key is reserved so future strategies can be added without a
breaking config change.

Under `rendezvous`, for each (member, feed) pair a score is computed as
`hash64(member + "\x00" + feedURL)`; the member with the highest score owns
that feed. Ties are broken by the larger member ID (stable, deterministic).
This gives the minimal-churn property: a feed's owner changes only when the
member it hashed highest for joins or leaves.

---

### `coordination.assignment.heartbeat_interval`

| | |
|---|---|
| **Type** | duration |
| **Default** | `10s` |
| **Constraint** | must be `> 0` |

How often each instance refreshes its coordinator-side membership lease and
checks for membership-set changes. Must be positive. Must be less than
`member_ttl` (validated).

A smaller value means faster detection of peer arrivals and departures (lower
reassignment latency), at the cost of more coordinator traffic. For typical
feeds (poll interval in minutes) the default is appropriate. For very
low-latency feeds (poll interval of seconds) where freshness during a failover
matters, consider lowering this alongside `member_ttl`.

Membership traffic scales with the member count and `heartbeat_interval`,
**not** with the number of feeds.

---

### `coordination.assignment.member_ttl`

| | |
|---|---|
| **Type** | duration |
| **Default** | `30s` |
| **Constraint** | must be `> 0`; must be `> heartbeat_interval` (validated — error if violated) |

How long a member entry lives in the coordinator without a heartbeat refresh
before peers consider it dead and reassign its feeds. This is the backstop for
crash recovery (SIGKILL, network partition): a crashed instance's feeds are
reassigned within `member_ttl`.

On graceful shutdown (`Deregister` is called), reassignment happens within
`heartbeat_interval` — no `member_ttl` wait.

Set to a small multiple of `heartbeat_interval` (the default is 3×: 30s / 10s)
so a single slow or missed heartbeat does not falsely evict a live member.
Liveness is judged by coordinator-side TTL, not local wall clock, so clock skew
between instances is irrelevant.

---

### `coordination.assignment.rebalance_grace`

| | |
|---|---|
| **Type** | duration |
| **Default** | `5s` |
| **Constraint** | must be `> 0` |

Documents the transition window that the always-on per-tick `TryAcquire` guard
covers. Currently reserved for a future lock-free steady-state mode (where the
lease would be skipped outside this window); today the guard runs on every tick
regardless of this value.

## Constraint summary

| Relationship | Enforced |
|---|---|
| `member_ttl` > `heartbeat_interval` | validation error |
| `strategy` = `rendezvous` (or empty) | validation error otherwise |
| `heartbeat_interval`, `member_ttl`, `rebalance_grace` > 0 | validation error |
| `enabled: true` with `driver: memory` | warning (no-op) |

## Related

- [Coordination Assignment Model](../explanation/coordination-assignment.md) — explains the problem, the three design pieces, rebalance walkthroughs, N>M standbys, and parameter relationships.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — coordinator driver options and the underlying lock mechanics.
