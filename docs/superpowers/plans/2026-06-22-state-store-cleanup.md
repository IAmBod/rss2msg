# State Store Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add TTL-based pruning of old "seen items" to the state store: SQLite/Postgres prune via a periodic background sweep; DynamoDB/Cosmos keep their native service-side TTL, now driven by one unified config knob.

**Architecture:** A new `state.Store.PruneItemsBefore(ctx, cutoff)` method deletes `seen_items` rows whose `last_seen_at` is older than `cutoff`. SQLite/Postgres implement a real DELETE; DynamoDB/Cosmos implement a no-op (the service prunes from a write-time TTL). A small `internal/statecleanup` package runs the sweep on a ticker, wired into `serve` only for SQL backends when `state.item_ttl > 0`. Retention is configured once via `state.item_ttl`; the SQL sweep cadence is `state.<sqlite|postgres>.cleanup_interval`.

**Tech Stack:** Go 1.25, zerolog, Viper/mapstructure config, modernc.org/sqlite, jackc/pgx v5, testcontainers (Postgres integration test), Cobra.

## Global Constraints

- Go 1.25; run `task test`, `task vet`, `task lint` before the PR; run `task test-integration` (this change touches the state store) — Docker required.
- **Prune anchor is `last_seen_at` only.** Never prune by a first-seen timestamp — a still-live item would be re-detected as new and re-published.
- **`feed_meta` is never pruned.** Only `seen_items` rows are deleted.
- **SQLite stores `last_seen_at` as RFC3339Nano text.** A naive `last_seen_at < ?` string comparison is WRONG (variable-width fractional seconds sort incorrectly — verified: `.1Z` sorts after `.11Z`). Use `datetime(last_seen_at) < datetime(?)`, which normalizes both sides to second precision. Second-level granularity is fine for an hours-scale TTL.
- **No installed users** (per repo policy): remove the old per-backend `item_ttl` config keys cleanly; do not keep aliases.
- Conventional Commits. Commit with explicit pathspecs only — never `git add -A`/`.` (Obsidian vault auto-staging hazard). Run `git status` before each commit.
- `examples/config.example.yaml` and `internal/config/example.yaml` must stay **byte-identical** (a test enforces this) — edit both with the same content.
- Module import path is `github.com/iambod/rss2msg`.

---

### Task 1: SQLite `PruneItemsBefore`

**Files:**
- Modify: `internal/state/sqlite/sqlite.go`
- Test: `internal/state/sqlite/sqlite_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `func (s *Store) PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error)` on `*sqlite.Store`.

- [ ] **Step 1: Write the failing test**

Add to `internal/state/sqlite/sqlite_test.go` (reuses the existing `newStore` helper):

```go
func TestPruneItemsBefore(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// Two old items with DIFFERENT fractional-second widths straddling the
	// naive-string-compare trap, plus one fresh item, plus a feed_meta row.
	old1 := base.Add(-48 * time.Hour).Add(100 * time.Millisecond) // ...:00.1Z
	old2 := base.Add(-48 * time.Hour).Add(120 * time.Millisecond) // ...:00.12Z
	fresh := base.Add(-1 * time.Minute)
	if err := s.UpsertItem(ctx, "f", "old1", "h", old1); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertItem(ctx, "f", "old2", "h", old2); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertItem(ctx, "f", "fresh", "h", fresh); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertFeedMeta(ctx, "f", state.FeedMeta{ETag: "e"}); err != nil {
		t.Fatal(err)
	}

	cutoff := base.Add(-24 * time.Hour)
	n, err := s.PruneItemsBefore(ctx, cutoff)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 2 {
		t.Fatalf("removed = %d, want 2", n)
	}
	// Fresh item survives.
	if _, found, err := s.GetItem(ctx, "f", "fresh"); err != nil || !found {
		t.Fatalf("fresh item gone: found=%v err=%v", found, err)
	}
	// Old items deleted.
	if _, found, _ := s.GetItem(ctx, "f", "old1"); found {
		t.Fatal("old1 not pruned")
	}
	// feed_meta is never pruned.
	if _, found, err := s.GetFeedMeta(ctx, "f"); err != nil || !found {
		t.Fatalf("feed_meta gone: found=%v err=%v", found, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/state/sqlite/ -run TestPruneItemsBefore -v`
Expected: FAIL — `s.PruneItemsBefore undefined`.

- [ ] **Step 3: Write minimal implementation**

Add to `internal/state/sqlite/sqlite.go` (after `UpsertItem`):

```go
// PruneItemsBefore deletes seen_items whose last_seen_at is older than cutoff
// and returns the number of rows removed. feed_meta is never touched.
//
// last_seen_at is stored as RFC3339Nano text; datetime() normalizes both sides
// to a canonical second-precision form so the comparison is correct regardless
// of fractional-second width (a plain string "<" is not).
func (s *Store) PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM seen_items WHERE datetime(last_seen_at) < datetime(?)`,
		cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("state/sqlite: prune: %w", err)
	}
	return res.RowsAffected()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/state/sqlite/ -run TestPruneItemsBefore -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/state/sqlite/sqlite.go internal/state/sqlite/sqlite_test.go
git commit -m "feat(state): add PruneItemsBefore to sqlite store"
```

---

### Task 2: Postgres `PruneItemsBefore`

**Files:**
- Modify: `internal/state/postgres/postgres.go`
- Test: `internal/state/postgres/postgres_test.go` (integration, `//go:build integration`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `func (s *Store) PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error)` on `*postgres.Store`.

- [ ] **Step 1: Write the failing test**

Add to `internal/state/postgres/postgres_test.go` (reuses the existing `setupStore` helper):

```go
func TestPruneItemsBefore(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)
	old := base.Add(-48 * time.Hour)
	fresh := base.Add(-1 * time.Minute)
	if err := store.UpsertItem(ctx, "f", "old", "h", old); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertItem(ctx, "f", "fresh", "h", fresh); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertFeedMeta(ctx, "f", state.FeedMeta{ETag: "e"}); err != nil {
		t.Fatal(err)
	}

	n, err := store.PruneItemsBefore(ctx, base.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 1 {
		t.Fatalf("removed = %d, want 1", n)
	}
	if _, found, _ := store.GetItem(ctx, "f", "old"); found {
		t.Fatal("old not pruned")
	}
	if _, found, _ := store.GetItem(ctx, "f", "fresh"); !found {
		t.Fatal("fresh pruned")
	}
	if _, found, _ := store.GetFeedMeta(ctx, "f"); !found {
		t.Fatal("feed_meta pruned")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags=integration ./internal/state/postgres/ -run TestPruneItemsBefore -v`
Expected: FAIL — `store.PruneItemsBefore undefined`. (Needs Docker.)

- [ ] **Step 3: Write minimal implementation**

Add to `internal/state/postgres/postgres.go` (after `UpsertItem`):

```go
// PruneItemsBefore deletes seen_items whose last_seen_at is older than cutoff
// and returns the number of rows removed. feed_meta is never touched.
func (s *Store) PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM seen_items WHERE last_seen_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("state/postgres: prune: %w", err)
	}
	return tag.RowsAffected(), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags=integration ./internal/state/postgres/ -run TestPruneItemsBefore -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/state/postgres/postgres.go internal/state/postgres/postgres_test.go
git commit -m "feat(state): add PruneItemsBefore to postgres store"
```

---

### Task 3: DynamoDB & Cosmos no-op `PruneItemsBefore`

**Files:**
- Modify: `internal/state/dynamodb/dynamodb.go`
- Modify: `internal/state/cosmosdb/cosmosdb.go`
- Test: `internal/state/dynamodb/dynamodb_test.go` (package `dynamodb`)
- Test: `internal/state/cosmosdb/cosmosdb_unit_test.go` (package `cosmosdb`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `func (s *Store) PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error)` on both `*dynamodb.Store` and `*cosmosdb.Store`, each a no-op returning `(0, nil)`.

- [ ] **Step 1: Write the failing tests**

Add to `internal/state/dynamodb/dynamodb_test.go`:

```go
func TestPruneItemsBeforeIsNoOp(t *testing.T) {
	s := &Store{} // no client needed; method must not touch it
	n, err := s.PruneItemsBefore(context.Background(), time.Now())
	if err != nil || n != 0 {
		t.Fatalf("got (%d, %v), want (0, nil)", n, err)
	}
}
```

Add to `internal/state/cosmosdb/cosmosdb_unit_test.go`:

```go
func TestPruneItemsBeforeIsNoOp(t *testing.T) {
	s := &Store{} // no client needed; method must not touch it
	n, err := s.PruneItemsBefore(context.Background(), time.Now())
	if err != nil || n != 0 {
		t.Fatalf("got (%d, %v), want (0, nil)", n, err)
	}
}
```

(Ensure `context` and `time` are imported in each test file.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/state/dynamodb/ ./internal/state/cosmosdb/ -run TestPruneItemsBeforeIsNoOp -v`
Expected: FAIL — `PruneItemsBefore undefined`.

- [ ] **Step 3: Write minimal implementations**

Add to `internal/state/dynamodb/dynamodb.go` (after `UpsertItem`):

```go
// PruneItemsBefore is a no-op for DynamoDB: old item rows are pruned by the
// service from the write-time TTL attribute (see ItemTTL), so the application
// never scans or deletes. It always returns (0, nil) to satisfy state.Store.
func (s *Store) PruneItemsBefore(_ context.Context, _ time.Time) (int64, error) {
	return 0, nil
}
```

Add to `internal/state/cosmosdb/cosmosdb.go` (after `UpsertItem`):

```go
// PruneItemsBefore is a no-op for Cosmos DB: old item rows are pruned by the
// service from the write-time `ttl` property (see ItemTTL), so the application
// never scans or deletes. It always returns (0, nil) to satisfy state.Store.
func (s *Store) PruneItemsBefore(_ context.Context, _ time.Time) (int64, error) {
	return 0, nil
}
```

(Both files already import `context` and `time`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/state/dynamodb/ ./internal/state/cosmosdb/ -run TestPruneItemsBeforeIsNoOp -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/state/dynamodb/dynamodb.go internal/state/dynamodb/dynamodb_test.go internal/state/cosmosdb/cosmosdb.go internal/state/cosmosdb/cosmosdb_unit_test.go
git commit -m "feat(state): add no-op PruneItemsBefore to dynamodb and cosmosdb stores"
```

---

### Task 4: Add `PruneItemsBefore` to the `state.Store` interface

**Files:**
- Modify: `internal/state/state.go`

**Interfaces:**
- Consumes: the four `PruneItemsBefore` methods from Tasks 1–3.
- Produces: `PruneItemsBefore(ctx context.Context, cutoff time.Time) (removed int64, err error)` as a method on the `state.Store` interface — the contract `internal/statecleanup` (Task 6) and `serve` (Task 7) depend on.

- [ ] **Step 1: Add the method to the interface**

In `internal/state/state.go`, inside the `Store` interface, add after `UpsertItem`:

```go
	// PruneItemsBefore deletes per-item seen-state whose LastSeenAt is older
	// than cutoff and returns the number of rows removed. Feed metadata is
	// never pruned. Backends with service-managed TTL (DynamoDB, Cosmos) may
	// implement this as a no-op returning (0, nil).
	PruneItemsBefore(ctx context.Context, cutoff time.Time) (removed int64, err error)
```

- [ ] **Step 2: Verify the whole module compiles and tests pass**

Run: `go build ./... && go test ./internal/state/...`
Expected: PASS — all four backends already satisfy the new method, so the interface change compiles cleanly. If any mock/fake `state.Store` exists elsewhere it will fail to compile here; add the same no-op method to it.

- [ ] **Step 3: Find and fix any other `state.Store` implementers**

Run: `grep -rln "state.Store" --include=*.go internal cmd | xargs grep -l "func.*UpsertItem" 2>/dev/null`
For any fake/mock found (e.g. in scheduler or pipeline tests) that does not yet have `PruneItemsBefore`, add:

```go
func (m *fakeStore) PruneItemsBefore(context.Context, time.Time) (int64, error) { return 0, nil }
```

Then re-run `go build ./... && go vet ./...`. Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add internal/state/state.go
# plus any test mock files you had to update:
# git add <path/to/fake_test.go>
git commit -m "feat(state): require PruneItemsBefore on the Store interface"
```

---

### Task 5: Config struct + validation + wire unification

**Files:**
- Modify: `internal/config/config.go` (StateConfig + SQLite/Postgres/DynamoDB/Cosmos config structs)
- Modify: `internal/config/validate.go`
- Modify: `cmd/rss2msg/wire.go` (`openStateStore`)
- Test: `internal/config/validate_test.go`

This task moves together because removing the per-backend `item_ttl` fields breaks `wire.go` and `validate.go`; doing all three in one task keeps the build green at the task boundary.

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `config.StateConfig.ItemTTL time.Duration`; `config.SQLiteStateConfig.CleanupInterval time.Duration`; `config.PostgresStateConfig.CleanupInterval time.Duration`. Removes `DynamoDBStateConfig.ItemTTL` and `CosmosDBStateConfig.ItemTTL`. These are read by Task 7.

- [ ] **Step 1: Write the failing validation tests**

Add to `internal/config/validate_test.go` (follow the existing table/test style in that file; construct a minimal valid `Config` with `State.Driver` set):

```go
func TestValidateStateItemTTLNegative(t *testing.T) {
	c := minimalValidConfig() // helper already used by other tests in this file
	c.State.Driver = "sqlite"
	c.State.SQLite.Path = "x.db"
	c.State.ItemTTL = -1
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for negative state.item_ttl")
	}
}

func TestValidateCleanupIntervalWithoutTTL(t *testing.T) {
	c := minimalValidConfig()
	c.State.Driver = "sqlite"
	c.State.SQLite.Path = "x.db"
	c.State.ItemTTL = 0
	c.State.SQLite.CleanupInterval = time.Hour
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error: cleanup_interval set but item_ttl=0")
	}
}

func TestValidateShortItemTTLWarns(t *testing.T) {
	c := minimalValidConfig()
	c.State.Driver = "sqlite"
	c.State.SQLite.Path = "x.db"
	c.State.ItemTTL = 30 * time.Minute
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "item_ttl") {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a short-item_ttl warning")
	}
}

func TestValidateDynamoTTLRequiresAttribute(t *testing.T) {
	c := minimalValidConfig()
	c.State.Driver = "dynamodb"
	c.State.DynamoDB.Table = "t"
	c.State.ItemTTL = 720 * time.Hour // set but no ttl_attribute
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error: dynamodb item_ttl set without ttl_attribute")
	}
}
```

> If `minimalValidConfig()` does not exist in the test file, build the `Config` inline the way the neighboring tests do. Check the top of `validate_test.go` first and match the existing convention; do not invent a helper.
> Ensure `strings` and `time` are imported in the test file.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestValidateState|TestValidateCleanup|TestValidateShort|TestValidateDynamoTTL' -v`
Expected: FAIL/compile error — new fields don't exist yet.

- [ ] **Step 3: Edit the config structs**

In `internal/config/config.go`:

Add `ItemTTL` to `StateConfig`:

```go
type StateConfig struct {
	Driver   string                 `mapstructure:"driver"`
	ItemTTL  time.Duration          `mapstructure:"item_ttl"` // 0 = disabled (default); retention since last_seen_at, honored by all backends
	Postgres PostgresStateConfig    `mapstructure:"postgres"`
	SQLite   SQLiteStateConfig      `mapstructure:"sqlite"`
	DynamoDB DynamoDBStateConfig    `mapstructure:"dynamodb"`
	CosmosDB CosmosDBStateConfig    `mapstructure:"cosmosdb"`
	Extra    map[string]interface{} `mapstructure:",remain"`
}
```

Add `CleanupInterval` to the SQL configs:

```go
type PostgresStateConfig struct {
	DSN             string           `mapstructure:"dsn"`
	TLS             StatePGTLSConfig `mapstructure:"tls"`
	CleanupInterval time.Duration    `mapstructure:"cleanup_interval"` // SQL-only: sweep cadence; 0 -> 1h when item_ttl > 0
}

type SQLiteStateConfig struct {
	Path            string        `mapstructure:"path"`
	CleanupInterval time.Duration `mapstructure:"cleanup_interval"` // SQL-only: sweep cadence; 0 -> 1h when item_ttl > 0
}
```

Remove the `ItemTTL` field from `DynamoDBStateConfig` (keep `TTLAttribute`) and from `CosmosDBStateConfig`. Update their doc comments to say `item_ttl` now lives at `state.item_ttl`.

- [ ] **Step 4: Update `wire.go` `openStateStore`**

In `cmd/rss2msg/wire.go`, change the dynamodb and cosmosdb cases to read the unified TTL:

```go
	case "dynamodb":
		return statedynamodb.New(ctx, statedynamodb.Options{
			Table:        c.DynamoDB.Table,
			Region:       c.DynamoDB.Region,
			EndpointURL:  c.DynamoDB.EndpointURL,
			TTLAttribute: c.DynamoDB.TTLAttribute,
			ItemTTL:      c.ItemTTL,
		})
	case "cosmosdb":
		return statecosmos.New(ctx, statecosmos.Options{
			Endpoint:         c.CosmosDB.Endpoint,
			ConnectionString: c.CosmosDB.ConnectionString,
			Database:         c.CosmosDB.Database,
			Container:        c.CosmosDB.Container,
			CreateIfMissing:  c.CosmosDB.CreateIfMissing,
			Throughput:       c.CosmosDB.Throughput,
			ItemTTL:          c.ItemTTL,
		})
```

- [ ] **Step 5: Update validation**

In `internal/config/validate.go`, replace the per-backend `item_ttl` checks. Remove the old coupling block in the `dynamodb` case (lines that reference `d.ItemTTL` / "ttl_attribute and item_ttl must both be set") and the `cosmosdb` `d.ItemTTL < 0` block. After the `switch c.State.Driver { … }` block (just before the coordination checks), add unified checks:

```go
	// Unified state retention (item_ttl) + SQL sweep cadence (cleanup_interval).
	if c.State.ItemTTL < 0 {
		return *warnings, fmt.Errorf("state.item_ttl %v must not be negative", c.State.ItemTTL)
	}
	sqlCleanup := map[string]time.Duration{
		"sqlite":   c.State.SQLite.CleanupInterval,
		"postgres": c.State.Postgres.CleanupInterval,
	}
	for drv, iv := range sqlCleanup {
		if c.State.Driver != drv {
			continue
		}
		if iv < 0 {
			return *warnings, fmt.Errorf("state.%s.cleanup_interval %v must not be negative", drv, iv)
		}
		if iv > 0 && c.State.ItemTTL == 0 {
			return *warnings, fmt.Errorf("state.%s.cleanup_interval is set but state.item_ttl is 0 (cleanup disabled)", drv)
		}
	}
	// DynamoDB needs the attribute name to write the expiry epoch.
	if c.State.Driver == "dynamodb" && c.State.ItemTTL > 0 &&
		strings.TrimSpace(c.State.DynamoDB.TTLAttribute) == "" {
		return *warnings, fmt.Errorf("state.dynamodb.ttl_attribute is required when state.item_ttl is set")
	}
	// Short TTLs risk pruning items still in the feed (duplicate re-publish).
	if c.State.ItemTTL > 0 && c.State.ItemTTL < time.Hour {
		*warnings = append(*warnings,
			fmt.Sprintf("state.item_ttl %v is very short; items still present in a feed may be pruned and re-published", c.State.ItemTTL))
	}
```

Ensure `time` is imported in `validate.go` (add it if missing).

- [ ] **Step 6: Run config tests + full build**

Run: `go build ./... && go test ./internal/config/ -v`
Expected: PASS (new validation tests green; existing config tests still green). If an existing test set `state.dynamodb.item_ttl` / `state.cosmosdb.item_ttl`, migrate it to `state.item_ttl`.

- [ ] **Step 7: Commit**

```bash
git add internal/config/config.go internal/config/validate.go internal/config/validate_test.go cmd/rss2msg/wire.go
git commit -m "feat(config): unify state.item_ttl and add SQL cleanup_interval"
```

---

### Task 6: `internal/statecleanup` sweep loop

**Files:**
- Create: `internal/statecleanup/statecleanup.go`
- Test: `internal/statecleanup/statecleanup_test.go`

**Interfaces:**
- Consumes: the `PruneItemsBefore` contract (Task 4).
- Produces: `statecleanup.Run(ctx context.Context, interval, ttl time.Duration, p Pruner, onResult func(removed int64, err error))` and `type Pruner interface { PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error) }`. Called by Task 7.

- [ ] **Step 1: Write the failing test**

Create `internal/statecleanup/statecleanup_test.go`:

```go
package statecleanup_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/statecleanup"
)

type fakePruner struct {
	mu      sync.Mutex
	cutoffs []time.Time
}

func (f *fakePruner) PruneItemsBefore(_ context.Context, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cutoffs = append(f.cutoffs, cutoff)
	return 1, nil
}

func (f *fakePruner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cutoffs)
}

func TestRunSweepsImmediatelyThenOnTick(t *testing.T) {
	p := &fakePruner{}
	var results int
	var rmu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		statecleanup.Run(ctx, 20*time.Millisecond, time.Hour, p, func(removed int64, err error) {
			rmu.Lock()
			results++
			rmu.Unlock()
		})
		close(done)
	}()

	// Immediate sweep + at least one tick within ~70ms.
	time.Sleep(70 * time.Millisecond)
	cancel()
	<-done

	if p.count() < 2 {
		t.Fatalf("sweeps = %d, want >= 2 (immediate + tick)", p.count())
	}
	// Cutoff must be ~now-ttl (an hour in the past), proving ttl is applied.
	if d := time.Since(p.cutoffs[0]); d < 50*time.Minute || d > 70*time.Minute {
		t.Fatalf("first cutoff age = %v, want ~1h", d)
	}
	rmu.Lock()
	defer rmu.Unlock()
	if results < 2 {
		t.Fatalf("onResult calls = %d, want >= 2", results)
	}
}

func TestRunReturnsImmediatelyWhenDisabled(t *testing.T) {
	p := &fakePruner{}
	statecleanup.Run(context.Background(), 0, time.Hour, p, nil) // interval<=0
	statecleanup.Run(context.Background(), time.Hour, 0, p, nil) // ttl<=0
	if p.count() != 0 {
		t.Fatalf("sweeps = %d, want 0 when disabled", p.count())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/statecleanup/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write the implementation**

Create `internal/statecleanup/statecleanup.go`:

```go
// Package statecleanup runs a periodic sweep that deletes seen-item state older
// than a TTL. It carries no logging or config dependency so it can be tested in
// isolation; the caller injects what each sweep reports via onResult.
package statecleanup

import (
	"context"
	"time"
)

// Pruner is the subset of state.Store this loop needs.
type Pruner interface {
	PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

// Run blocks until ctx is cancelled. It performs an immediate sweep, then one
// sweep per interval. Each sweep deletes items last seen before now-ttl and, if
// onResult is non-nil, reports the rows removed and any error. Run returns
// immediately if interval or ttl is non-positive.
func Run(ctx context.Context, interval, ttl time.Duration, p Pruner, onResult func(removed int64, err error)) {
	if interval <= 0 || ttl <= 0 {
		return
	}
	sweep := func() {
		n, err := p.PruneItemsBefore(ctx, time.Now().Add(-ttl))
		if onResult != nil {
			onResult(n, err)
		}
	}
	sweep() // clear the backlog at startup instead of waiting a full interval
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sweep()
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/statecleanup/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/statecleanup/statecleanup.go internal/statecleanup/statecleanup_test.go
git commit -m "feat(state): add statecleanup periodic sweep loop"
```

---

### Task 7: Wire the sweep into `serve`

**Files:**
- Modify: `cmd/rss2msg/wire.go` (add `stateCleanupInterval` helper)
- Modify: `cmd/rss2msg/serve.go` (launch the goroutine)

**Interfaces:**
- Consumes: `config.StateConfig.ItemTTL` / `.SQLite.CleanupInterval` / `.Postgres.CleanupInterval` (Task 5); `statecleanup.Run` + `statecleanup.Pruner` (Task 6); `w.store` of type `state.Store` (existing).
- Produces: a running background sweep for SQL backends when `state.item_ttl > 0`.

- [ ] **Step 1: Add the interval helper**

In `cmd/rss2msg/wire.go`, add (near `openStateStore`):

```go
// stateCleanupInterval returns the effective background-sweep interval for SQL
// state backends (default 1h when unset), or 0 for backends that prune natively
// (dynamodb, cosmosdb) and therefore need no application sweep.
func stateCleanupInterval(c config.StateConfig) time.Duration {
	var iv time.Duration
	switch c.Driver {
	case "sqlite":
		iv = c.SQLite.CleanupInterval
	case "postgres":
		iv = c.Postgres.CleanupInterval
	default:
		return 0
	}
	if iv <= 0 {
		iv = time.Hour
	}
	return iv
}
```

Confirm `time` is imported in `wire.go` (add if missing).

- [ ] **Step 2: Launch the goroutine in `serve.go`**

In `cmd/rss2msg/serve.go`, add this block after the heartbeat block (after the `if cfg.Heartbeat.Enabled { … }` closes, before the SIGHUP block):

```go
			// Opt-in state cleanup: periodically delete seen-items not seen
			// within state.item_ttl. Only SQL backends sweep here; DynamoDB and
			// Cosmos prune natively (stateCleanupInterval returns 0 for them).
			// The DELETE is idempotent and time-partitioned, so every instance
			// can sweep independently — no coordinator lock is needed.
			if cfg.State.ItemTTL > 0 {
				if iv := stateCleanupInterval(cfg.State); iv > 0 {
					go statecleanup.Run(ctx, iv, cfg.State.ItemTTL, w.store, func(removed int64, err error) {
						if err != nil {
							tel.Logger.Error().Err(err).
								Str("component", "state-cleanup").
								Msg("state cleanup sweep failed")
							return
						}
						ev := tel.Logger.Debug()
						if removed > 0 {
							ev = tel.Logger.Info()
						}
						ev.Int64("removed", removed).
							Str("component", "state-cleanup").
							Msg("state cleanup sweep complete")
					})
				}
			}
```

Add the import `"github.com/iambod/rss2msg/internal/statecleanup"` to `serve.go`.

- [ ] **Step 3: Build and vet**

Run: `go build ./... && go vet ./...`
Expected: clean. (`w.store` satisfies `statecleanup.Pruner` via the interface method added in Task 4.)

- [ ] **Step 4: Manual smoke check (SQLite)**

Run:
```bash
go run ./cmd/rss2msg validate-config --config examples/config.example.yaml
```
Expected: exits 0 (no validation error from the new fields). Full daemon run is covered by existing e2e; no new e2e required.

- [ ] **Step 5: Commit**

```bash
git add cmd/rss2msg/wire.go cmd/rss2msg/serve.go
git commit -m "feat(serve): run TTL state cleanup sweep for SQL backends"
```

---

### Task 8: Config examples + docs

**Files:**
- Modify: `examples/config.example.yaml`
- Modify: `internal/config/example.yaml` (must stay byte-identical to the above)
- Modify: the state-store reference doc(s) under `docs/`

**Interfaces:**
- Consumes: the final config surface from Task 5.
- Produces: documentation; no code.

- [ ] **Step 1: Update the example config (both files identically)**

Replace the `state:` block in `examples/config.example.yaml` (and apply the EXACT same text to `internal/config/example.yaml`) so it documents the unified knob:

```yaml
state:
  driver: sqlite   # postgres | sqlite | dynamodb | cosmosdb
  # item_ttl: 720h            # retention since an item was last seen; 0/unset = keep forever (default).
                              # Honored by every backend: SQL sweeps; DynamoDB/Cosmos prune natively.
  sqlite:
    path: ./rss2msg.db    # filesystem path; ":memory:" and "?_pragma=..." are accepted
    # cleanup_interval: 1h    # SQL-only: how often this instance sweeps expired items (default 1h when item_ttl > 0)
  # postgres:
  #   dsn: ${POSTGRES_DSN}
  #   cleanup_interval: 1h    # SQL-only: sweep cadence (default 1h when item_ttl > 0)
  #   tls:                  # only takes effect when DSN does not set sslmode=disable
  #     ca_file: /etc/ssl/pg-ca.pem
  #     cert_file: /etc/ssl/pg-client.pem   # mTLS: both cert_file and key_file or neither
  #     key_file:  /etc/ssl/pg-client.key
  #     server_name: pg.internal             # SNI / cert-verify hostname override
  #     insecure_skip_verify: false          # true = disable cert verification (test only)
  # dynamodb:                # shared, distributed-safe store (good with a redis/postgres coordinator)
  #   table: rss2msg-state   # required; table with PK feed_url (S) + SK item_id (S), provisioned out of band
  #   region: us-east-1      # optional; SDK default chain (env / shared config) when empty
  #   endpoint_url:          # optional; LocalStack / DynamoDB Local override
  #   ttl_attribute: expires_at   # required when state.item_ttl is set; names the TTL attribute the table prunes on
```

- [ ] **Step 2: Verify the two example files are byte-identical**

Run: `diff examples/config.example.yaml internal/config/example.yaml && echo IDENTICAL`
Expected: prints `IDENTICAL` (no diff). Then run the drift-guard test:
Run: `go test ./internal/config/ -run Example -v`
Expected: PASS.

- [ ] **Step 3: Update the state-store reference docs**

Find the state-store doc(s):
Run: `grep -rln "item_ttl\|state store\|seen_items" docs/`
In the matched reference page(s), document: `state.item_ttl` (universal retention; 0 = disabled), `cleanup_interval` (SQL-only sweep cadence, default 1h), that DynamoDB/Cosmos prune natively (and DynamoDB needs `ttl_attribute`), the `last_seen_at` anchor and the duplicate-republish hazard of too-short TTLs, and the scaled-mode note (idempotent sweep, no coordinator lock). Move any existing per-backend `item_ttl` docs to the unified key. Ground every statement in the config/code; do not invent options.

- [ ] **Step 4: Check doc links**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`.

- [ ] **Step 5: Commit**

```bash
git add examples/config.example.yaml internal/config/example.yaml docs/
git commit -m "docs(state): document unified item_ttl and SQL cleanup_interval"
```

---

## Final verification (run before opening the PR)

- [ ] `task test` — unit suite (race) green.
- [ ] `task vet` — clean.
- [ ] `task lint` — golangci-lint clean.
- [ ] `task test-integration` — Postgres prune integration test green (Docker required). State store changed, so this is required, not optional.
- [ ] `bash scripts/check-doc-links.sh` — `OK`.
- [ ] `diff examples/config.example.yaml internal/config/example.yaml` — no output.
- [ ] `git status` — only intended files staged across commits; no Obsidian vault noise.

## Self-review notes (plan vs. spec)

- **Spec coverage:** unified `state.item_ttl` (Task 5); SQL-only `cleanup_interval` (Tasks 5, 7); `PruneItemsBefore` interface + 4 backends (Tasks 1–4); SQL DELETE + Dynamo/Cosmos no-op (Tasks 1–3); `feed_meta` never pruned (asserted in Tasks 1–2 tests); `last_seen_at` anchor + short-TTL warning (Tasks 5, 6); cleanup loop with immediate sweep + ctx cancel (Task 6); serve wiring gated on SQL driver + `item_ttl>0` (Task 7); scaled-mode comment (Task 7); validation rules (Task 5); example.yaml + docs sync (Task 8); TDD + integration test for Postgres (Task 2). All spec sections map to a task.
- **Type consistency:** `PruneItemsBefore(ctx, cutoff time.Time) (int64, error)` is identical across the interface (Task 4), all four backends (Tasks 1–3), and `statecleanup.Pruner` (Task 6). `stateCleanupInterval` and `statecleanup.Run` signatures match their call site in Task 7.
- **Build-green boundaries:** the only field removals (Dynamo/Cosmos `ItemTTL`) happen in the same task (5) that updates `wire.go` and `validate.go`, so every task ends compiling.
