---
title: Coordination Assignment Model
type: explanation
tags: [rss2msg/docs, coordination, scaling, assignment]
summary: How rendezvous-hash partitioning and membership leases reduce N×M lock amplification to M per cycle when running multiple instances.
updated: 2026-06-21
---

# Coordination Assignment Model

The assignment model is an optional layer on top of the existing coordinator that
partitions feeds across instances: each feed is scheduled by exactly one **owner
instance** per cycle, rather than every instance racing for every feed's lease.

## The problem: N×M wakeup and lock amplification

Coordination is **try-acquire-per-tick**: every instance schedules its own ticker
for *every* feed, and at each tick the pipeline races to acquire a distributed
lease for that feed (`coord.TryAcquire`). Whoever wins polls; the rest log
`PollSkipped{reason="not_owner"}` and skip.

This is correct but does not scale. With **N** instances and **M** feeds, each
cycle produces **N×M** timer wakeups and **N×M** lock attempts, of which only **M**
do useful work. The wasted N×M − M attempts are pure coordinator round-trips, and
at high N×M the coordinator becomes the bottleneck. Ownership also clumps wherever
the race happens to be won, with no load awareness.

## Design overview

An **assignment layer** sits on top of the existing coordinator. It has three
independently testable pieces:

1. **`internal/assign`** — a pure, I/O-free rendezvous-hash (HRW) assignment function.
2. **`coord.Membership`** — a new interface implemented by each backend: register this
   instance under a TTL lease and enumerate the live member set.
3. **An owner-filtering `FeedProvider`** that wraps the existing aggregator and feeds
   only this instance's owned feeds into the unchanged `ServeDynamic` reconcile loop.

The per-tick `TryAcquire` guard in `cmd/rss2msg/pipeline.go` is **kept exactly
as-is** as a belt-and-suspenders correctness backstop during rebalance windows.
`pipeline.go` is not modified.

## 1. Rendezvous (HRW) assignment

For each (member, feed) pair, a score is computed as
`hash64(member + "\x00" + feedURL)`. The member with the highest score owns that
feed; ties are broken by the larger member ID (stable, deterministic).

This gives the **minimal-churn property**: a feed's owner changes only when the
specific member it hashed highest for joins or leaves. Removing one of N members
reassigns only that member's ~M/N feeds; adding one member pulls only the feeds
that now hash highest to it. All other feeds keep their owners unchanged.

`assign.Owned(self, feeds, members)` returns the subset of feeds owned by this
instance given the live member set. If `self` is not in `members`, or `members` is
empty, it returns nil.

## 2. Membership with TTL leases and deregister-on-shutdown

Each instance heartbeats on `heartbeat_interval` (default 10s): it calls
`Membership.Heartbeat`, which refreshes its coordinator-side lease and returns
the current live member set. When the live set changes from the last snapshot,
`Changes()` is signalled and the reconcile loop re-runs.

**Self-ID** reuses the existing owner-token scheme (`hostname-pid-randomhex`),
generated once per process. Liveness is judged by **coordinator-side TTL**, never
local wall clock, so clock skew between instances is irrelevant.

### Departure: graceful shutdown vs crash

A departing instance's feeds must return to the live fleet:

- **Graceful shutdown** (SIGTERM/SIGINT): the daemon first drains its own tickers,
  then calls `Membership.Deregister`, which **deletes** this instance's member entry
  (e.g. Redis `DEL`, Postgres `DELETE`). Peers observe the smaller member set on
  their next heartbeat (≤ `heartbeat_interval`) and reassign its feeds promptly —
  no `member_ttl` wait.
- **Crash / SIGKILL / partition:** no deregister is possible, so the entry lingers
  and peers fall back to **`member_ttl` expiry** (default 30s).

There is a benign gap on graceful shutdown between "this instance stopped its
tickers" and "a peer picked them up": those feeds are simply polled one interval
later, which is harmless. If the windows overlap, the per-tick guard prevents any
double-poll.

## 3. Owner-filtering FeedProvider

`ServeDynamic` consumes a `FeedProvider` (`Desired()` + `Changes()`) and reconciles
the running ticker set on every change. The assignment layer plugs in as a
**decorator** — no scheduler-core change.

`OwnerProvider.Desired()` calls the inner provider for the full feed set, then
filters it through `assign.Owned` to return only this instance's owned feeds.
The `Changes()` channel is the **merge** of the inner provider's `Changes()`
(feed-set edits / SIGHUP) and the membership-change signal, so `ServeDynamic`
reconciles owned tickers on either trigger.

When `assignment.enabled: false`, the aggregator is passed to `ServeDynamic`
directly — unchanged from today's behavior.

## Always-on guard and the scaling trade-off

The per-tick `TryAcquire` lease is **retained on every tick**, even in steady
state, as a correctness backstop. During a rebalance, two instances can briefly
disagree about ownership (one heartbeat of propagation skew); the existing lease
makes that window safe — the loser logs `PollSkipped{reason="not_owner"}` and
skips, so no feed is double-published.

**Explicit trade-off:** coordinator request rate is **not** independent of
`pollInterval`. The assignment layer removes the **N×M → M** wakeup-and-lock
amplification (only the M owners tick and attempt the lease each cycle), which is
the dominant scaling win, but each owner still performs one lease round-trip per
poll. The "coordinator traffic independent of pollInterval" goal is therefore
relaxed to "reduced from N×M to M per cycle." A future lock-free steady-state
mode (guarding only within `rebalance_grace` of a membership change) can be
layered on later without changing the current interfaces.

## Rebalance walkthroughs

There is no central rebalancer. Every instance derives ownership locally from the
same shared member set with the same deterministic HRW function, so they converge
without coordinating.

### Scale-up — instance C joins `{A, B}`

1. C's first heartbeat registers its member entry and reads the live set `{A, B, C}`;
   C immediately computes `Owned(C, …)` and starts its tickers.
2. A and B learn about C on their next heartbeat (≤ `heartbeat_interval`), see the
   set changed, and signal `Changes()`.
3. Each recomputes `Owned`. A feed owned by A moves only if C now outscores A; B's
   score was already below A's and is unchanged, so **feeds move only A/B → C, never
   A↔B**. ~M/3 feeds migrate; the rest stay put.
4. A/B stop the moved feeds' tickers (with drain); C starts them.

### Scale-down — instance B leaves `{A, B, C}`

1. Peers learn B is gone either by **deregister** (graceful, ≤ `heartbeat_interval`)
   or by **`member_ttl` expiry** (crash, ~30s).
2. A and C enumerate `{A, C}`, see the set shrank, signal `Changes()`.
3. Each recomputes `Owned`. Only feeds B previously owned move — to whichever of
   `{A, C}` now scores highest, spread roughly evenly. Feeds A and C already owned
   are untouched.
4. A/C start the reassigned tickers. There is a brief **unpolled gap** for B's feeds
   between B stopping and a survivor picking them up; the feed is simply polled one
   cycle late — no items are dropped, since change-detection state in the store means
   the next poll still sees everything since the last successful poll.

### Feed-set change (membership unchanged)

When feeds are added, removed, or edited (SIGHUP / config reload / dynamic source),
the inner `feedsource.Aggregator` signals `Changes()`, which `OwnerProvider` merges
and forwards:

- **Added feed** hashes deterministically to exactly one current member — every
  instance agrees on the owner, so precisely one starts its ticker; no race.
- **Removed feed** drops out of every `Desired()`; its owner stops the ticker.
- **Edited feed** (same URL) keeps its owner — HRW keys on the **URL**, so
  ownership does not move on a config change; the owner restarts the ticker with the
  new config, non-owners unaffected.

Because HRW keys on URL alone, feed-set churn and membership churn are
**orthogonal**: adding or removing feed X never disturbs the ownership of feed Y,
and a feed-set change with stable membership migrates **nothing** between instances.

## N > M: more instances than feeds

HRW assigns each feed independently to exactly one live member, so **every feed
always has an owner** regardless of fleet size — there is no unowned state. When
there are more instances than feeds, at most M instances own a feed each, leaving
the remaining N − M instances **idle hot standbys**: they still heartbeat (cheap,
membership-only traffic) and participate in the member set, so when an owner leaves
they immediately become eligible to pick up its feeds on the next reconcile. This
is desirable over-provisioning for failover headroom. Distribution is per-feed, so
with few feeds the split can be uneven (e.g. 2 feeds, 5 instances → 2 busy, 3
idle); that is inherent to having fewer units of work than workers.

## Parameter relationships

The membership params and the per-feed poll interval are deliberately independent
axes. The only **hard** relationships are:

- `member_ttl` must be `>` `heartbeat_interval` (validated) — and ideally a small
  multiple (defaults 30s / 10s = 3×) so a single slow or missed heartbeat does not
  falsely evict a live member.
- The pre-existing **lease vs poll-time** rule is unchanged: the per-feed coordinator
  lease (`lease_duration` 60s DynamoDB/Cosmos DB, `lock_ttl` 30s Redis) must exceed
  worst-case poll time so a peer cannot steal a lock mid-poll. The new membership
  params do not enter this rule.

There is **no hard constraint tying membership timing to the feed interval.** The
relationship is a **soft tuning** one: `heartbeat_interval` / `member_ttl` set the
worst-case **reassignment delay** when an instance disappears (≤ `heartbeat_interval`
graceful, ≤ `member_ttl` crash). For typical feeds (interval in minutes) that delay
is negligible — leave the defaults. For very low-latency feeds (interval of seconds)
where freshness *during* a failover matters, lower `heartbeat_interval` / `member_ttl`
so the gap stays small relative to the interval, at the cost of more membership
traffic and more false-eviction sensitivity.

Two facts keep this from forcing aggressive tuning:

- A reassigned feed is polled **immediately** on the new owner (the ticker fires once
  on start), so the gap is just the detection delay, not detection delay + one
  interval.
- The gap is **lossless** at any interval — change-detection state lives in the store,
  so the next poll still sees everything since the last successful poll.
- The gap is bounded by heartbeat/TTL, **not** by `lease_duration`: the pipeline
  releases the lease at the end of each poll, so a graceful move leaves no stale
  lease blocking the new owner.

## Poll duration vs lock TTL

The assignment layer **shrinks** the existing poll-duration vs lease-TTL hazard
rather than adding to it. In steady state only the owner ticks a feed, so even an
expired DynamoDB/Cosmos DB lease cannot be stolen — no peer is polling that feed.
The poll-time-vs-lease-TTL concern now only matters during a **rebalance overlap
window** (≤ heartbeat propagation), not on every cycle. The guidance is unchanged
and now scoped: size `lease_duration` / `lock_ttl` above worst-case poll time.

## Related

- [Coordination Assignment Reference](../reference/coordination-assignment.md) — every `coordination.assignment.*` key, defaults, and constraints.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — coordinator drivers and the lock mechanics the assignment layer builds on.
- [Operational Notes](./operations.md) — no-leader-election semantics and crash recovery.
