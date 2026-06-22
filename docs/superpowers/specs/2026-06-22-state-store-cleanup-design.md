# State store cleanup — TTL-based pruning of seen items — design

- **Status:** approved (design)
- **Date:** 2026-06-22
- **Issue:** [#186](https://github.com/IAmBod/rss2msg/issues/186) — "State store clean up: after configured time, delete seen items. can be disabled"
- **Branch / worktree:** `feat/state-store-cleanup` in `.worktrees/state-store-cleanup`

## Problem

The state store records every item the service has already published so it can
deduplicate across polls. Each `UpsertItem` writes a row keyed on
`(feed_url, item_id)` with a `last_seen_at` timestamp
(`internal/state/state.go`).

The four backends are split on retention:

- **DynamoDB** (`internal/state/dynamodb/dynamodb.go`) and **Cosmos DB**
  (`internal/state/cosmosdb/cosmosdb.go`) already support a per-backend
  `item_ttl`. On every write they embed a write-time expiry (epoch seconds /
  `ttl` property) and the **service** auto-prunes expired rows. No application
  code runs.
- **SQLite** (`internal/state/sqlite/sqlite.go`) and **Postgres**
  (`internal/state/postgres/postgres.go`) have **no cleanup at all**.
  `seen_items` rows accumulate indefinitely, so the state DB grows without bound
  for any long-running deployment.

This design fills the SQLite/Postgres gap with a periodic app-side sweep, and
unifies the retention knob so operators configure "how long to keep seen items"
in one place regardless of backend.

## Behavior

When `state.item_ttl > 0`, seen-item rows that have not been seen for longer
than `item_ttl` are deleted:

- **SQLite / Postgres:** a background goroutine periodically deletes
  `seen_items` whose `last_seen_at` is older than `now - item_ttl`.
- **DynamoDB / Cosmos:** unchanged service-managed pruning, but the TTL value is
  now read from the unified `state.item_ttl` key instead of a per-backend key.

When `state.item_ttl == 0` (the default), nothing is ever deleted — this is
today's behavior, so existing deployments are unaffected.

### Safety: prune by `last_seen_at`, never by first-seen

The anchor is **`last_seen_at`**, the timestamp refreshed on every
`UpsertItem`. As long as an item is still present in a feed, each poll refreshes
its `last_seen_at`, so it is never eligible for pruning. Only items that have
fallen off the feed for the full `item_ttl` are deleted.

This is the correctness property of the feature. Pruning by a first-seen
timestamp would risk deleting an item that is *still in the feed*; the next poll
would then re-detect it as new and **re-publish it**, producing a duplicate
notification. `last_seen_at` makes that impossible: a row only ages out after
the item has stopped appearing in the feed.

Operators must still set `item_ttl` comfortably longer than the window in which
an item can disappear from and reappear in a feed. The validator warns on
suspiciously short values (see below) but cannot know each feed's churn, so this
remains an operator responsibility.

## Configuration (config-first)

`item_ttl` is universal and lives at the top of the `state` block. The sweep
cadence (`cleanup_interval`) only exists for the SQL backends that actually run
an app-side sweep; it is **not** a key on the native-TTL backends, where it
would be meaningless.

```yaml
# SQL backend — needs an app-side sweep, so cleanup_interval lives here
state:
  driver: sqlite
  item_ttl: 720h            # universal: retention since last_seen_at (0 = disabled, default)
  sqlite:
    path: ./rss2msg.db
    cleanup_interval: 1h    # SQL-only: how often this instance sweeps (default 1h when item_ttl > 0)
```

```yaml
state:
  driver: postgres
  item_ttl: 720h
  postgres:
    dsn: postgres://...
    cleanup_interval: 1h
```

```yaml
# Native-TTL backend — no cleanup_interval key; the service prunes
state:
  driver: dynamodb
  item_ttl: 720h
  dynamodb:
    table: rss2msg-state
    region: us-east-1
    ttl_attribute: expires_at   # which attribute carries the epoch expiry
```

```yaml
state:
  driver: cosmosdb
  item_ttl: 720h
  cosmosdb:
    endpoint: https://...
    database: rss2msg
    container: state
    create_if_missing: true
```

### Config struct changes (`internal/config/config.go`)

- Add `ItemTTL time.Duration` to `StateConfig` (`mapstructure:"item_ttl"`,
  default `0`).
- Add `CleanupInterval time.Duration` to `SQLiteStateConfig` and
  `PostgresStateConfig` (`mapstructure:"cleanup_interval"`, default `1h` when
  `item_ttl > 0`).
- **Remove** `ItemTTL` from `DynamoDBStateConfig` and `CosmosDBStateConfig`;
  `wire.go` now passes `c.ItemTTL` (the unified value) into the
  DynamoDB/Cosmos options. `DynamoDBStateConfig.TTLAttribute` stays — it is the
  only DynamoDB-specific knob.

Because rss2msg has no installed user base, the per-backend `item_ttl` keys are
moved cleanly to `state.item_ttl` rather than kept as aliases.

### Validation (`internal/config/validate.go`)

- Reject negative `item_ttl` or `cleanup_interval`.
- Reject `cleanup_interval > 0` together with `item_ttl == 0` (a SQL backend
  configured to sweep with nothing to sweep — a misconfiguration).
- For DynamoDB, keep the existing rule that `ttl_attribute` is required when
  `item_ttl > 0` (the attribute name is needed to write the expiry). Drop the
  old "both or neither" coupling now that `item_ttl` is unified.
- **Warn** (do not block) when `item_ttl` is set but very short
  (e.g. `< 1h`), flagging the duplicate-republish hazard.

## Interface + backend implementation

Add one method to `state.Store` (`internal/state/state.go`):

```go
// PruneItemsBefore deletes seen_items whose last_seen_at is older than cutoff.
// It returns the number of rows removed.
PruneItemsBefore(ctx context.Context, cutoff time.Time) (removed int64, err error)
```

- **SQLite:** `DELETE FROM seen_items WHERE last_seen_at < ?` (cutoff formatted
  the same way `last_seen_at` is stored). Returns `RowsAffected`.
- **Postgres:** `DELETE FROM seen_items WHERE last_seen_at < $1`. Returns the
  command tag's `RowsAffected`.
- **DynamoDB / Cosmos:** implemented as a **no-op returning `(0, nil)`** — these
  backends prune natively via the write-time TTL, so there is nothing for the
  app to scan or delete (and a scan would be costly).

`feed_meta` is also pruned when `item_ttl > 0`, anchored on `updated_at` (the
last time the feed's HTTP cache validators changed). For SQL backends the same
sweep issues `DELETE FROM feed_meta WHERE updated_at < cutoff`; for
DynamoDB/Cosmos the meta row is written with the same TTL attribute/property as
item rows, so the service prunes it natively.

**304 caveat:** a still-polled feed that only ever returns `304 Not Modified`
does not refresh `updated_at`, so its cached validators may be pruned after
`item_ttl` and re-fetched once on the next successful poll. This is harmless:
`seen_items` still deduplicates, so no duplicate publishes occur.

## The cleanup loop (`cmd/rss2msg/serve.go`)

After `openStateStore` succeeds, if `item_ttl > 0` **and** the driver is a SQL
backend (`sqlite` / `postgres`), launch a goroutine modeled on the existing
ticker pattern in `internal/scheduler/ownerprovider.go` (`OwnerProvider.Run`):

- `time.NewTicker(cleanup_interval)`, stopped on exit.
- An **immediate first sweep** before entering the loop.
- `select` on `ctx.Done()` (clean shutdown) and `ticker.C`.
- Each tick calls `store.PruneItemsBefore(ctx, time.Now().Add(-item_ttl))`.
- Log rows deleted at **info** when `> 0`, **debug** when `0`; log and continue
  on error (a failed sweep must not crash the service — the next tick retries).

The native-TTL backends do not get a goroutine (the wiring checks the driver),
so DynamoDB/Cosmos incur zero extra cost.

### Scaled mode

The DELETE is idempotent and partitioned by time
(`last_seen_at < cutoff`), so it is safe for every instance to run its own sweep
concurrently — no coordinator lock is required. Overlapping deletes simply
remove the same already-eligible rows. This will be stated explicitly in the
docs.

## Testing (TDD)

- **Unit (`task test`, no containers):**
  - SQLite `PruneItemsBefore`: insert rows with old and fresh `last_seen_at`
    plus a `feed_meta` row; prune; assert only old `seen_items` rows are gone and
    both fresh `seen_items` and the `feed_meta` row survive (this method only
    touches `seen_items`); assert the returned count.
  - SQLite `PruneFeedMetaBefore` (feed_meta addendum): insert two `feed_meta`
    rows plus a `seen_items` row, backdate one meta row's `updated_at` past the
    cutoff; prune; assert only the old meta row is gone and the fresh meta row
    **and** the `seen_items` row survive; assert the returned count.
  - DynamoDB / Cosmos `PruneItemsBefore` and `PruneFeedMetaBefore`: assert each
    returns `(0, nil)` and makes no remote calls (no-op; the service prunes
    items and meta rows via their write-time TTL). Also assert the meta
    row/document carries the native TTL when `item_ttl` is configured and none
    when it is unset.
  - Config validation: negative durations rejected; `cleanup_interval > 0` with
    `item_ttl == 0` rejected; short-`item_ttl` warning emitted; DynamoDB
    `ttl_attribute`-required rule.
- **Integration (`task test-integration`, Docker):**
  - Postgres `PruneItemsBefore` and `PruneFeedMetaBefore` against a real
    database via testcontainers (same shape as the SQLite unit tests).
- Run `task test`, `task vet`, `task lint`, and `task test-integration` (this
  change touches the state store) before opening the PR.

## Docs / config examples

- Update `examples/config.example.yaml` **and** `internal/config/example.yaml`
  together (they must stay byte-identical) with the new `state.item_ttl` and the
  SQL `cleanup_interval` keys, and migrate the existing dynamodb/cosmos
  `item_ttl` examples to the unified key.
- Update the state-store reference docs under `docs/` to document `item_ttl`
  (universal), `cleanup_interval` (SQL-only), the `last_seen_at` safety
  semantics, the duplicate-republish hazard, and the scaled-mode note. Run
  `bash scripts/check-doc-links.sh`.

## Out of scope

- ~~Pruning `feed_meta`.~~ (Now in scope: `feed_meta` is pruned by `item_ttl` on `updated_at` — see addendum 2026-06-22.)
- Per-feed TTL overrides (single global `state.item_ttl` only).
- Active scanning/TTL emulation for DynamoDB/Cosmos (the service handles it).
- Coordinator-elected single-sweeper (the idempotent DELETE makes it
  unnecessary).
