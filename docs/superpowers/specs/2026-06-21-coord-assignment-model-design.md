# Partition/assignment model for scaling to many instances — design

- **Status:** approved (design)
- **Date:** 2026-06-21
- **Issue:** [#183](https://github.com/IAmBod/rss2msg/issues/183)
- **Branch / worktree:** `feat/coord-assignment-model` in `.worktrees/assignment-model`

## Problem

Coordination is **try-acquire-per-tick**: every instance schedules its own
ticker for *every* feed (`internal/scheduler/dynamic.go` via the
`feedsource.Aggregator` provider), and at each tick the pipeline races to
acquire a distributed lease for that feed
(`cmd/rss2msg/pipeline.go:51` `p.coord.TryAcquire`). Whoever wins polls; the
rest log `PollSkipped{reason="not_owner"}` and skip.

This is correct but does not scale. With **N** instances and **M** feeds, each
cycle produces **N×M** timer wakeups and **N×M** lock attempts, of which only
**M** do useful work. The wasted N×M − M attempts are pure coordinator
round-trips, and at high N×M the coordinator becomes the bottleneck. Ownership
also clumps wherever the race happens to be won, with no load awareness.

## Goals

- Each feed is scheduled by exactly one **owner instance** over a stable
  window; non-owners do not start a ticker for it.
- Roughly even feed distribution across instances; **minimal churn** on
  membership change — adding/removing one instance moves only ~`1/|members|` of
  feeds, the rest stay put.
- Crash/rebalance safety: a dead instance's feeds are reassigned within a
  bounded window; no feed polled by two owners for long, no feed dropped.
- **Backward compatible default:** `assignment.enabled: false` (and the memory
  coordinator) behave **identically** to today.

## Non-goals

- Replacing per-feed change-detection or state-store semantics.
- Cross-feed work stealing / intra-feed sharding.
- Multi-region active/active ownership.

## Design overview

An **assignment layer** sits on top of the existing coordinator. It has three
pieces, each independently testable:

1. **`internal/assign`** — a pure, I/O-free rendezvous-hash (HRW) assignment
   function.
2. **`coord.Membership`** — a new interface (alongside `Coordinator`) that each
   backend implements: register-this-instance-with-a-lease + enumerate-the-live-set.
3. **An owner-filtering `FeedProvider`** that wraps the existing aggregator and
   feeds only this instance's owned feeds into the *unchanged* `ServeDynamic`
   reconcile loop.

The per-tick `TryAcquire` guard in `cmd/rss2msg/pipeline.go` is **kept exactly
as-is** as a belt-and-suspenders correctness backstop during rebalance windows
(see "Guard model" below). `pipeline.go` is not modified.

### 1. `internal/assign` — rendezvous (HRW) assignment

Pure functions, no I/O:

```go
package assign

// Owner returns the member that owns feedURL under highest-random-weight
// (rendezvous) hashing, and false if members is empty.
func Owner(feedURL string, members []string) (string, bool)

// Owned returns the subset of feeds owned by self given the live member set.
// If self is not in members, or members is empty, returns nil.
func Owned(self string, feeds, members []string) []string
```

HRW: for each member, compute `score = hash64(member + "\x00" + feedURL)`; the
highest score owns, ties broken by the larger member ID (stable, deterministic).
Hash is `xxhash`/`fnv` over the concatenation — chosen for speed and uniformity;
the exact algorithm is an internal detail with no on-wire format.

**Minimal-churn property** (the reason for HRW over plain modulo): a feed's
owner only changes when the specific member it hashed highest for joins or
leaves. Removing one of N members reassigns only that member's feeds
(~`M/N`); adding one member pulls only the feeds that now hash highest to it.

Unit tests (no Docker):

- **Distribution:** with 10 members and 10 000 feeds, each member owns within
  ±20 % of `M/N`.
- **Minimal churn:** removing one member moves only its former feeds; every
  other feed keeps its owner. Adding one member moves ≤ `M/N · (1+ε)` feeds.
- **Determinism:** same inputs → same output regardless of member-slice order.
- **Edges:** empty members → no owner; single member → owns everything;
  `self` not in members → owns nothing.

### 2. `coord.Membership` — per-backend liveness

New interface in `internal/coord/coord.go`:

```go
// Membership tracks the live set of rss2msg instances sharing a coordinator.
// Implementations register this instance under a TTL lease and return the
// currently-live members. Safe for concurrent use.
type Membership interface {
    // Heartbeat refreshes this instance's lease and returns the current live
    // member set, including self. Called every heartbeat_interval. On error
    // the caller keeps the last-known member set (fail-static).
    Heartbeat(ctx context.Context) ([]string, error)
    // Deregister removes this instance's member entry so peers reassign its
    // feeds promptly on a graceful shutdown, rather than waiting for member_ttl.
    // Best-effort: a failure (e.g. coordinator already gone) is logged, not
    // fatal — the TTL is the backstop. Called from Close().
    Deregister(ctx context.Context) error
    Close() error
}
```

### Departure: deregister-on-shutdown vs TTL backstop

A departing instance's feeds must return to the live fleet. Two paths:

- **Graceful shutdown** (SIGTERM/SIGINT → `ctx` cancelled): `serve` calls
  `Membership.Deregister` (via `Close`), which **deletes** this instance's member
  entry (Redis `DEL`, PG `DELETE`, Dynamo/Cosmos `DeleteItem`). Peers observe the
  smaller member set on their next `Heartbeat` (≤ `heartbeat_interval`, ~10s) and
  reassign its feeds promptly — no `member_ttl` wait. Ordering: the daemon first
  drains its own tickers (existing `ServeDynamic` drain), *then* deregisters, so
  it never deregisters while still polling.
- **Crash / SIGKILL / partition:** no deregister is possible, so the entry lingers
  and peers fall back to **`member_ttl` expiry** (~30s). This is the unavoidable
  case the TTL exists for.

There is a benign gap on graceful shutdown between "this instance stopped its
tickers" and "a peer picked them up": those feeds are simply polled one interval
later, which is harmless for RSS polling. If the windows overlap, the per-tick
guard prevents any double-poll.

**Self-ID** reuses the existing owner-token scheme already used by the
DynamoDB/CosmosDB coordinators: `hostname-pid-randomhex`, generated once per
process. Liveness is always judged by **coordinator-side TTL**, never local wall
clock, so clock skew between instances is irrelevant.

Per-backend mechanism (membership traffic scales with member count and
heartbeat interval, **not** with M or pollInterval):

| Backend | Register (per heartbeat) | Enumerate live |
| --- | --- | --- |
| memory | none | returns `[]string{self}` (single member) |
| redis | `SET rss2msg:coord:member:<id> "<exp>" EX member_ttl` | `SCAN MATCH rss2msg:coord:member:*` |
| postgres | `INSERT … ON CONFLICT (id) DO UPDATE SET last_seen=now()` into a `coordination_members` table | `SELECT id FROM coordination_members WHERE last_seen > now() - member_ttl` |
| dynamodb | `PutItem pk="member:<id>", lease_expiry=now+ttl` | `Scan` with `begins_with(pk,"member:")` filtered to `lease_expiry > now` |
| cosmosdb | upsert doc `id/pk="member:<id>", lease_expiry=now+ttl` | `SELECT c.id FROM c WHERE STARTSWITH(c.id,'member:') AND c.lease_expiry > @now` |

Notes / trade-offs:

- **memory** is the trivial single-member case → owns every feed → exactly
  today's behavior. Zero added coordinator traffic.
- **dynamodb** `Scan` reads the whole table; acceptable because it runs once per
  `heartbeat_interval` (default 10s) and the member set is tiny relative to
  feeds. Documented as the cost of not adding a GSI / second table.
- **postgres** adds a `coordination_members` table, auto-created on startup
  (same pattern the PG coordinator already uses for its own objects).
- Members share the *same* lock table/keyspace/container as the existing
  coordinator (prefix-namespaced), so no new infrastructure is provisioned.

Stale members self-expire via TTL (`member_ttl`); no active reaping needed for
correctness. Redis keys expire natively; PG rows are filtered by `last_seen`
and opportunistically deleted when stale; Dynamo/Cosmos items are filtered by
`lease_expiry`.

### 3. Owner-filtering `FeedProvider`

`ServeDynamic` already consumes a `FeedProvider` (`Desired()` + `Changes()`) and
reconciles the running ticker set on every change, with bounded drain on stop
(`internal/scheduler/dynamic.go`). The assignment layer plugs in here as a
**decorator** — no scheduler-core change:

```go
// internal/scheduler/assignprovider.go (or internal/assign)
type OwnerProvider struct { /* inner FeedProvider, Membership, self, strategy */ }

func (o *OwnerProvider) Desired(ctx) ([]config.FeedConfig, error) {
    feeds, err := o.inner.Desired(ctx)           // full feed set
    if err != nil { return nil, err }
    owned := assign.Owned(o.self, urls(feeds), o.members())   // filter
    return filterByURL(feeds, owned), nil
}
func (o *OwnerProvider) Changes() <-chan struct{} { return o.merged }
```

A heartbeat goroutine ticks every `heartbeat_interval`: it calls
`Membership.Heartbeat`, and when the live member set changes from the last
snapshot it signals `Changes()`. The signal channel is the **merge** of the
inner provider's `Changes()` (feed-set edits / SIGHUP) and the membership-change
signal, so `ServeDynamic` reconciles owned tickers on either trigger. Start/stop
of the now-owned / no-longer-owned tickers — including the existing per-feed
drain — is entirely handled by the unchanged reconcile loop.

When `assignment.enabled: false`, `bootstrap`/`wire` does **not** wrap the
aggregator; `ServeDynamic` receives the raw aggregator exactly as today.

### Rebalance walkthroughs

There is no central rebalancer. Every instance derives ownership locally from
the *same* shared member set with the *same* deterministic HRW function, so they
converge without coordinating. HRW's minimal-churn property — a feed's owner
changes only when the specific member it hashes highest for joins or leaves —
means each event moves only ~`M/|members|` feeds and leaves everyone else's
assignments untouched.

**Scale-up — instance C joins `{A, B}`:**

1. C's first `Heartbeat` registers its member entry and reads the live set
   `{A, B, C}`; C immediately computes `Owned(C, …)` and starts its tickers — it
   works as soon as it is up, without waiting to be "assigned" anything.
2. A and B learn about C on their next heartbeat (≤ `heartbeat_interval`), see
   the set changed, and signal `Changes()`.
3. Each recomputes `Owned`. A feed `f` owned by A moves only if C now outscores
   A; B's score for `f` was already below A's and is unchanged, so **feeds move
   only A/B → C, never A↔B**. ~`M/3` feeds migrate; the rest stay put.
4. Reconcile: A/B stop the moved feeds' tickers (with drain); C starts them.

**Scale-down — instance B leaves `{A, B, C}`:**

1. Peers learn B is gone either by **deregister** (graceful, ≤
   `heartbeat_interval`) or by **`member_ttl` expiry** (crash, ~30s).
2. A and C enumerate `{A, C}`, see the set shrank, signal `Changes()`.
3. Each recomputes `Owned`. Only feeds **B** previously owned move — to whichever
   of `{A, C}` now scores highest, spread roughly evenly. Feeds A and C already
   owned are untouched.
4. Reconcile: A/C start the reassigned tickers. There is a brief **unpolled gap**
   for B's feeds between B stopping and a survivor picking them up (≤
   `heartbeat_interval` graceful, ≤ `member_ttl` crash); the feed is simply
   polled one cycle late — no items are dropped, since change-detection state in
   the store means the next poll still sees everything since the last successful
   poll.

In both directions, momentary **overlap** (two owners briefly) is made safe by
the per-tick guard, and momentary **gaps** (no owner briefly) cost only a delayed
poll. See "Guard model" below.

### More instances than feeds (N > M)

HRW assigns each feed independently to exactly one live member, so **every feed
always has an owner** regardless of fleet size — there is no error or unowned
state. When there are more instances than feeds, at most M instances own a feed
each (and, by hash collisions, possibly fewer), leaving the remaining N − M
instances **idle hot standbys**: they still heartbeat (cheap, membership-only
traffic) and participate in the member set, so when an owner leaves they
immediately become eligible to pick up its feeds on the next reconcile. This is
desirable over-provisioning for failover headroom, not a problem to guard
against. Distribution is per-feed, so with few feeds the split can be uneven
(e.g. 2 feeds, 5 instances → 2 busy, 3 idle); that is inherent to having fewer
units of work than workers.

### Guard model (decision: always-on guard)

The per-tick `TryAcquire` lease in `pipeline.go` is **retained on every tick**,
even in steady state, as a correctness backstop. During a rebalance, two
instances can briefly disagree about ownership (one heartbeat of propagation
skew); the existing lease makes that window safe — the loser logs
`PollSkipped{reason="not_owner"}` and skips, so no feed is double-published.

**Consequence (explicit, accepted):** coordinator request rate in steady state
is **not** strictly independent of `pollInterval`. The assignment layer removes
the **N×M → M** wakeup-and-lock amplification (only the M owners tick and
attempt the lease each cycle), which is the dominant scaling win, but each owner
still performs one lease round-trip per poll. We consciously choose this
belt-and-suspenders safety over eliminating the steady-state lease. The issue's
"coordinator traffic independent of pollInterval" criterion is therefore
**relaxed to "reduced from N×M to M per cycle."** A future lock-free steady-state
mode (guarding only within `rebalance_grace` of a membership change) can be
layered on later without changing this design's interfaces.

Because the guard is unchanged, **`cmd/rss2msg/pipeline.go` is not modified.**

## Configuration (config-first)

New block under the existing `coordination` config (note: the repo key is
`coordination`, not `coordinator`):

```yaml
coordination:
  driver: redis            # memory | postgres | redis | dynamodb | cosmosdb
  assignment:
    enabled: false         # default off -> today's try-acquire-everywhere behavior
    strategy: rendezvous   # rendezvous (only value for now; reserved for future)
    heartbeat_interval: 10s
    member_ttl: 30s        # member considered dead after this (must be > heartbeat_interval)
    rebalance_grace: 5s    # reserved: documents the transition window covered by the guard
```

Go struct: `CoordinationAssignmentConfig` added to `CoordinationConfig`.
Defaults registered in `internal/config/load.go` and `Default()`:
`enabled:false`, `strategy:"rendezvous"`, `heartbeat_interval:10s`,
`member_ttl:30s`, `rebalance_grace:5s`.

Validation (`internal/config/validate.go`):

- `assignment.enabled: true` requires a distributed driver (redis / postgres /
  dynamodb / cosmosdb); with `driver: memory` it is a no-op and emits a
  **warning** (single instance → assignment is vacuous), not an error.
- `member_ttl` must be `> heartbeat_interval` (else members expire before they
  can refresh) — error if violated.
- `strategy` must be `rendezvous` (or empty → default) — error otherwise.
- `heartbeat_interval`, `member_ttl`, `rebalance_grace` must be `> 0` when set.

Both example configs (`examples/config.example.yaml` and
`internal/config/example.yaml`) get the new block and **must stay
byte-identical** (an existing test enforces this).

## Wiring (`cmd/rss2msg`)

- `openMembership(ctx, cc, sc, self)` — new factory parallel to
  `openCoordinator`, switching on `cc.Driver`, returning a `coord.Membership`.
  Built only when `cc.Assignment.Enabled`.
- `serve.go` — when assignment is enabled, wrap `agg` in `OwnerProvider` before
  passing to `ServeDynamic`; start the heartbeat goroutine; after
  `ServeDynamic` returns (tickers drained), `Close()` membership, which
  deregisters this instance so peers reassign its feeds without a `member_ttl`
  wait. When disabled, unchanged.
- The membership self-ID is generated once and shared with the coordinator's
  owner token where applicable.
- One-shot modes (`run-once`, `lambda`, `azure`) ignore assignment entirely —
  they resolve the full feed set and run once; assignment only affects the
  long-lived `serve` daemon.

## Metrics

Add to `telemetry.Instruments`:

- `AssignedFeeds` — observable gauge: number of feeds this instance owns.
- `MembershipSize` — observable gauge: live member count as last seen.
- `RebalanceEvents` — counter: incremented each time the owned set changes.

`PollSkipped{reason="not_owner"}` is retained (the guard path).

## Testing

- **`internal/assign`** — unit tests: distribution, minimal churn, determinism,
  edges (no Docker).
- **Membership backends** — unit tests with the existing fake clients
  (dynamodb/cosmosdb already have fakes; redis/postgres get table-driven unit
  tests where feasible) for register/enumerate/expiry-filter logic.
- **Integration (`-tags=integration`)** — per backend (redis, postgres,
  dynamodb, cosmosdb), reusing the existing testcontainer helpers
  (`tcredis.Run`, `tcpg.Run`, `test/awslocal`, `tccosmos.Run`): two members
  register, both enumerate the same live set; one **deregisters** and is dropped
  from the live set immediately (no TTL wait); a member that stops heartbeating
  without deregistering is dropped after `member_ttl`.
- **Scheduler** — unit test that `OwnerProvider` over two distinct self-IDs and
  the same membership yields **disjoint, complete** owned sets (every feed owned
  by exactly one instance), and that a membership change signals `Changes()`.
- Gates before PR: `task test`, `task vet`, `task lint`; `task test-integration`
  for the new backend membership tests (Docker); `scripts/check-doc-links.sh`
  for docs.

## Docs

- `docs/explanation/` — an explanation page on the assignment/partition model
  (membership, HRW, the guard, the scaling trade-off), linked from the
  coordinator hub.
- `docs/reference/` — reference for the new `coordination.assignment.*` keys.
- Update the coordinator hub / scaling docs to point at it. Run the link
  checker.

## Acceptance criteria (from #183, as implemented)

- [x] `assignment.enabled: true` → each feed scheduled by exactly one owner in
  steady state (total active tickers ≈ M, not N×M).
- [~] Coordinator request rate **reduced from N×M to M per cycle** (relaxed from
  "independent of pollInterval" — see Guard model).
- [x] Membership change reassigns affected feeds within `member_ttl`, moving
  only ~`1/|members|` of feeds (HRW), never dropping a feed.
- [x] No feed double-published in steady state; the per-tick guard covers the
  transition window.
- [x] `assignment.enabled: false` and memory coordinator behave identically to
  today.
- [x] Implemented across redis, postgres, dynamodb, cosmosdb; memory is the
  trivial single-member case.
- [x] Metrics: assigned-feed count, membership size, rebalance events;
  `PollSkipped{not_owner}` retained.
- [x] Unit tests (assignment function) + integration tests (membership/rebalance
  per backend).
- [x] Docs + both example configs updated (byte-identical); link checker passes.

## Edge cases / defaults

- **Clock skew:** liveness uses coordinator-side TTL, not local wall clock.
- **Split brain / partition:** prefer brief double-poll (idempotent publish +
  guard) over a dropped feed; documented.
- **Config reload** changing the feed set: `OwnerProvider.Desired` recomputes
  ownership over the new feed set; no restart.
- **Single instance / memory:** assignment is a no-op, zero added coordinator
  traffic. `Deregister` is a no-op (nothing to remove).
- **Graceful vs crash departure:** graceful shutdown deregisters (peers reassign
  within ~`heartbeat_interval`); a crash falls back to `member_ttl` expiry. See
  "Departure" above.
- **More instances than feeds (N > M):** every feed still has exactly one owner;
  surplus instances are idle hot standbys (heartbeat only) ready for failover.
  See "More instances than feeds" above.
- **All peers gone (enumerate returns only self / empty on error):** fail-static
  to last-known members; if truly alone, this instance owns everything (safe —
  degrades to single-instance behavior rather than dropping feeds).
