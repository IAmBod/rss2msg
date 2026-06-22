# State Store Cleanup — feed_meta Addendum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax. This extends the merged-into-PR-#191 feature on branch `feat/state-store-cleanup`.

**Goal:** Also bound `feed_meta` growth: prune `feed_meta` rows whose `updated_at` is older than `state.item_ttl`, reusing the same knob and sweep as `seen_items`. SQL backends delete in the sweep; DynamoDB/Cosmos prune natively by writing a TTL on the meta row.

**Architecture:** Add a sibling `PruneFeedMetaBefore(ctx, cutoff)` to `state.Store` (real DELETE for SQLite/Postgres anchored on `updated_at`; no-op for DynamoDB/Cosmos). DynamoDB/Cosmos start writing a native TTL on the meta row (mirroring how item rows already work), so the service prunes stale meta. The `internal/statecleanup` sweep calls both prune methods with the same cutoff (`now - item_ttl`).

**Tech Stack:** Go 1.25, modernc.org/sqlite, pgx v5, AWS SDK v2 (DynamoDB), azcosmos, zerolog, testcontainers.

## Global Constraints

- Reuse `state.item_ttl` (no new config knob). Same sweep, same cutoff for items and meta.
- feed_meta prune anchor is **`updated_at`** (the only freshness column on feed_meta). Accepted tradeoff (decided with the user): `updated_at` is not refreshed on HTTP 304, so a stable 304-only feed may have its cached validators pruned every `item_ttl` and do one extra full fetch — self-healing, bandwidth-only, no duplicate publishes (seen_items still dedups). Orphaned/removed feeds are pruned correctly.
- SQLite compares `updated_at` via `datetime(updated_at) < datetime(?)` (raw string `<` mis-sorts RFC3339Nano fractional seconds). Postgres uses direct `updated_at < $1` (TIMESTAMPTZ).
- DynamoDB meta TTL requires `ttl_attribute` (same attribute as items); only written when `item_ttl > 0`. Cosmos meta TTL uses the reserved `ttl` property; container already has TTL enabled.
- `PruneItemsBefore` is unchanged and still only touches `seen_items`; its "feed_meta never touched" doc comment stays accurate at the method level.
- Conventional Commits; explicit-pathspec staging only (Obsidian vault auto-staging hazard); `git status` before every commit. Work in worktree `/home/iambod/Documents/Workspace/rss2msg/.worktrees/state-store-cleanup` on branch `feat/state-store-cleanup`.
- Module path `github.com/iambod/rss2msg`.

---

### Task 9: SQLite `PruneFeedMetaBefore`

**Files:** Modify `internal/state/sqlite/sqlite.go`; Test `internal/state/sqlite/sqlite_test.go`.

**Interfaces:**
- Produces: `func (s *Store) PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (int64, error)` on `*sqlite.Store`.

- [ ] **Step 1: Write the failing test** — add to `sqlite_test.go`:

```go
func TestPruneFeedMetaBefore(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	// feed_meta.updated_at is set to time.Now() inside UpsertFeedMeta, so we
	// cannot backdate it directly through the API. Insert rows with explicit
	// updated_at via the store's db is not exposed; instead upsert two metas,
	// then backdate one with a raw UPDATE through a fresh connection.
	if err := s.UpsertFeedMeta(ctx, "old-feed", state.FeedMeta{ETag: "e-old"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertFeedMeta(ctx, "fresh-feed", state.FeedMeta{ETag: "e-fresh"}); err != nil {
		t.Fatal(err)
	}
	// Keep a seen_items row to prove it is NOT pruned by this method.
	if err := s.UpsertItem(ctx, "old-feed", "i1", "h", time.Now()); err != nil {
		t.Fatal(err)
	}
	// Backdate old-feed's updated_at well past the cutoff.
	if err := s.SetFeedMetaUpdatedAtForTest(ctx, "old-feed", time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}

	n, err := s.PruneFeedMetaBefore(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 1 {
		t.Fatalf("removed = %d, want 1", n)
	}
	if _, found, _ := s.GetFeedMeta(ctx, "old-feed"); found {
		t.Fatal("old-feed meta not pruned")
	}
	if _, found, _ := s.GetFeedMeta(ctx, "fresh-feed"); !found {
		t.Fatal("fresh-feed meta wrongly pruned")
	}
	if _, found, _ := s.GetItem(ctx, "old-feed", "i1"); !found {
		t.Fatal("seen_items wrongly pruned by PruneFeedMetaBefore")
	}
}
```

This test needs a tiny test-only helper to backdate `updated_at` (UpsertFeedMeta always stamps `now`). Add it in the implementation step below.

- [ ] **Step 2: Run test to verify it fails** — `go test ./internal/state/sqlite/ -run TestPruneFeedMetaBefore -v` → FAIL (methods undefined).

- [ ] **Step 3: Implement** — add to `sqlite.go` after `UpsertFeedMeta`:

```go
// PruneFeedMetaBefore deletes feed_meta rows whose updated_at is older than
// cutoff and returns the number of rows removed. seen_items is not touched.
// updated_at is stored as RFC3339Nano text; datetime() normalizes both sides
// so the comparison is correct regardless of fractional-second width.
func (s *Store) PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM feed_meta WHERE datetime(updated_at) < datetime(?)`,
		cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("state/sqlite: prune feed_meta: %w", err)
	}
	return res.RowsAffected()
}

// SetFeedMetaUpdatedAtForTest overwrites a feed's updated_at. Test-only seam:
// UpsertFeedMeta always stamps time.Now(), so tests need a way to backdate.
func (s *Store) SetFeedMetaUpdatedAtForTest(ctx context.Context, feedURL string, t time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE feed_meta SET updated_at=? WHERE feed_url=?`,
		t.UTC().Format(time.RFC3339Nano), feedURL)
	return err
}
```

- [ ] **Step 4: Run test to verify it passes** — `go test ./internal/state/sqlite/ -run TestPruneFeedMetaBefore -v` → PASS. Also run the whole sqlite package: `go test ./internal/state/sqlite/`.

- [ ] **Step 5: Commit**
```bash
git add internal/state/sqlite/sqlite.go internal/state/sqlite/sqlite_test.go
git commit -m "feat(state): add PruneFeedMetaBefore to sqlite store"
```

---

### Task 10: Postgres `PruneFeedMetaBefore`

**Files:** Modify `internal/state/postgres/postgres.go`; Test `internal/state/postgres/postgres_test.go` (`//go:build integration`).

**Interfaces:** Produces `func (s *Store) PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (int64, error)` on `*postgres.Store`.

- [ ] **Step 1: Write the failing test** — add to `postgres_test.go`:

```go
func TestPruneFeedMetaBefore(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	if err := store.UpsertFeedMeta(ctx, "old-feed", state.FeedMeta{ETag: "e-old"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertFeedMeta(ctx, "fresh-feed", state.FeedMeta{ETag: "e-fresh"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertItem(ctx, "old-feed", "i1", "h", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.SetFeedMetaUpdatedAtForTest(ctx, "old-feed", time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}

	n, err := store.PruneFeedMetaBefore(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 1 {
		t.Fatalf("removed = %d, want 1", n)
	}
	if _, found, _ := store.GetFeedMeta(ctx, "old-feed"); found {
		t.Fatal("old-feed meta not pruned")
	}
	if _, found, _ := store.GetFeedMeta(ctx, "fresh-feed"); !found {
		t.Fatal("fresh-feed meta wrongly pruned")
	}
	if _, found, _ := store.GetItem(ctx, "old-feed", "i1"); !found {
		t.Fatal("seen_items wrongly pruned")
	}
}
```

- [ ] **Step 2: Run to verify it fails** — `go test -tags=integration ./internal/state/postgres/ -run TestPruneFeedMetaBefore -v` (needs Docker) → FAIL (undefined).

- [ ] **Step 3: Implement** — add to `postgres.go` after `UpsertFeedMeta`:

```go
// PruneFeedMetaBefore deletes feed_meta rows whose updated_at is older than
// cutoff and returns the number of rows removed. seen_items is not touched.
func (s *Store) PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM feed_meta WHERE updated_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("state/postgres: prune feed_meta: %w", err)
	}
	return tag.RowsAffected(), nil
}

// SetFeedMetaUpdatedAtForTest overwrites a feed's updated_at. Test-only seam:
// UpsertFeedMeta always stamps NOW(), so tests need a way to backdate.
func (s *Store) SetFeedMetaUpdatedAtForTest(ctx context.Context, feedURL string, t time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE feed_meta SET updated_at=$1 WHERE feed_url=$2`, t, feedURL)
	return err
}
```

- [ ] **Step 4: Run to verify it passes** — `go test -tags=integration ./internal/state/postgres/ -run TestPruneFeedMetaBefore -v` → PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/state/postgres/postgres.go internal/state/postgres/postgres_test.go
git commit -m "feat(state): add PruneFeedMetaBefore to postgres store"
```

---

### Task 11: DynamoDB + Cosmos — native TTL on meta rows + no-op `PruneFeedMetaBefore`

**Files:** Modify `internal/state/dynamodb/dynamodb.go`, `internal/state/cosmosdb/cosmosdb.go`; Tests `internal/state/dynamodb/dynamodb_test.go` (pkg `dynamodb`), `internal/state/cosmosdb/cosmosdb_unit_test.go` (pkg `cosmosdb`).

**Interfaces:** Produces no-op `PruneFeedMetaBefore(_ context.Context, _ time.Time) (int64, error)` on both stores. Behavior change: meta rows now carry a native TTL when `item_ttl > 0`.

- [ ] **Step 1: Write failing tests.**

DynamoDB (`dynamodb_test.go`):
```go
func TestPruneFeedMetaBeforeIsNoOp(t *testing.T) {
	s := &Store{}
	n, err := s.PruneFeedMetaBefore(context.Background(), time.Now())
	if err != nil || n != 0 {
		t.Fatalf("got (%d, %v), want (0, nil)", n, err)
	}
}

func TestUpsertFeedMetaWritesTTLWhenConfigured(t *testing.T) {
	// A store configured with a TTL attribute + item_ttl must write that
	// attribute on the meta row so DynamoDB prunes stale feed_meta.
	s := &Store{ttlAttribute: "expires_at", itemTTL: time.Hour}
	item := s.buildFeedMetaItem("https://f", state.FeedMeta{ETag: "e"})
	if _, ok := item["expires_at"]; !ok {
		t.Fatal("expected ttl attribute on meta item when item_ttl set")
	}
}

func TestUpsertFeedMetaNoTTLWhenUnset(t *testing.T) {
	s := &Store{} // no ttlAttribute / itemTTL
	item := s.buildFeedMetaItem("https://f", state.FeedMeta{ETag: "e"})
	if _, ok := item["expires_at"]; ok {
		t.Fatal("did not expect a ttl attribute when item_ttl unset")
	}
}
```

Cosmos (`cosmosdb_unit_test.go`):
```go
func TestPruneFeedMetaBeforeIsNoOp(t *testing.T) {
	s := &Store{}
	n, err := s.PruneFeedMetaBefore(context.Background(), time.Now())
	if err != nil || n != 0 {
		t.Fatalf("got (%d, %v), want (0, nil)", n, err)
	}
}

func TestMetaDocTTLWhenConfigured(t *testing.T) {
	s := &Store{itemTTL: time.Hour, now: time.Now}
	doc := s.buildMetaDoc("https://f", state.FeedMeta{ETag: "e"})
	if doc.TTL == nil || *doc.TTL < 1 {
		t.Fatalf("expected positive ttl on meta doc, got %v", doc.TTL)
	}
}

func TestMetaDocNoTTLWhenUnset(t *testing.T) {
	s := &Store{now: time.Now} // itemTTL == 0
	doc := s.buildMetaDoc("https://f", state.FeedMeta{ETag: "e"})
	if doc.TTL != nil {
		t.Fatalf("expected no ttl on meta doc, got %v", *doc.TTL)
	}
}
```

- [ ] **Step 2: Run to verify they fail** — `go test ./internal/state/dynamodb/ ./internal/state/cosmosdb/ -run 'PruneFeedMetaBeforeIsNoOp|FeedMeta|MetaDoc' -v` → FAIL (undefined methods).

- [ ] **Step 3: Implement DynamoDB.** Refactor `UpsertFeedMeta` to build the item via a helper that sets the TTL attribute, and add the no-op prune. Replace the body of `UpsertFeedMeta` (the `item := map[...]{...}` construction through the `if !meta.LastModified.IsZero()` block) so it calls the helper:

```go
// buildFeedMetaItem assembles the meta row. When a TTL attribute and item_ttl
// are configured, it stamps an expiry (updated_at + item_ttl) so DynamoDB
// prunes stale feed_meta the same way it prunes item rows.
func (s *Store) buildFeedMetaItem(feedURL string, meta state.FeedMeta) map[string]ddbtypes.AttributeValue {
	now := time.Now().UTC()
	item := map[string]ddbtypes.AttributeValue{
		pkAttr:       &ddbtypes.AttributeValueMemberS{Value: feedURL},
		skAttr:       &ddbtypes.AttributeValueMemberS{Value: metaSK},
		"etag":       &ddbtypes.AttributeValueMemberS{Value: meta.ETag},
		"updated_at": &ddbtypes.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
	}
	if !meta.LastModified.IsZero() {
		item["last_modified"] = &ddbtypes.AttributeValueMemberS{Value: meta.LastModified.UTC().Format(time.RFC3339Nano)}
	}
	if s.ttlAttribute != "" && s.itemTTL > 0 {
		expiry := now.Add(s.itemTTL).Unix()
		item[s.ttlAttribute] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expiry)}
	}
	return item
}

// PruneFeedMetaBefore is a no-op for DynamoDB: stale meta rows are pruned by
// the service from the write-time TTL attribute (see buildFeedMetaItem).
func (s *Store) PruneFeedMetaBefore(_ context.Context, _ time.Time) (int64, error) {
	return 0, nil
}
```
And change `UpsertFeedMeta` to:
```go
func (s *Store) UpsertFeedMeta(ctx context.Context, feedURL string, meta state.FeedMeta) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      s.buildFeedMetaItem(feedURL, meta),
	})
	if err != nil {
		return fmt.Errorf("state/dynamodb: PutItem (meta): %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Implement Cosmos.** Add `TTL *int json:"ttl,omitempty"` to `metaDoc`; extract a `buildMetaDoc` helper that sets TTL; add the no-op prune. Add to the `metaDoc` struct:
```go
	TTL          *int   `json:"ttl,omitempty"`
```
Add the helper + no-op (place near `UpsertFeedMeta`):
```go
// buildMetaDoc assembles the meta document. When item_ttl is configured it sets
// the reserved `ttl` property so Cosmos prunes stale feed_meta the same way it
// prunes item rows. ttl is relative to last write, so each upsert extends it.
func (s *Store) buildMetaDoc(feedURL string, meta state.FeedMeta) metaDoc {
	doc := metaDoc{
		ID:        metaID,
		FeedURL:   feedURL,
		ETag:      meta.ETag,
		UpdatedAt: s.now().UTC().Format(time.RFC3339Nano),
	}
	if !meta.LastModified.IsZero() {
		doc.LastModified = meta.LastModified.UTC().Format(time.RFC3339Nano)
	}
	if s.itemTTL > 0 {
		secs := int(s.itemTTL.Seconds())
		if secs < 1 {
			secs = 1
		}
		doc.TTL = &secs
	}
	return doc
}

// PruneFeedMetaBefore is a no-op for Cosmos DB: stale meta rows are pruned by
// the service from the write-time `ttl` property (see buildMetaDoc).
func (s *Store) PruneFeedMetaBefore(_ context.Context, _ time.Time) (int64, error) {
	return 0, nil
}
```
Then change `UpsertFeedMeta` to marshal `s.buildMetaDoc(feedURL, meta)` instead of building the `metaDoc` inline (keep the existing marshal/upsert call and partition-key logic; only the doc construction moves into the helper). Verify the existing `UpsertFeedMeta` still sets the partition key and id exactly as before.

- [ ] **Step 5: Run tests to verify they pass** — `go test ./internal/state/dynamodb/ ./internal/state/cosmosdb/` → PASS (whole packages, incl. existing tests).

- [ ] **Step 6: Commit**
```bash
git add internal/state/dynamodb/dynamodb.go internal/state/dynamodb/dynamodb_test.go internal/state/cosmosdb/cosmosdb.go internal/state/cosmosdb/cosmosdb_unit_test.go
git commit -m "feat(state): prune feed_meta via native TTL on dynamodb/cosmosdb meta rows"
```

---

### Task 12: Interface + statecleanup sweep wiring

**Files:** Modify `internal/state/state.go`, `internal/statecleanup/statecleanup.go`, `internal/statecleanup/statecleanup_test.go`, and the two mocks (`internal/feed/detector_test.go`, `cmd/rss2msg/pipeline_test.go`).

**Interfaces:** Adds `PruneFeedMetaBefore(ctx, cutoff) (int64, error)` to `state.Store` and to `statecleanup.Pruner`; the sweep calls both with one cutoff.

- [ ] **Step 1: Add to the `state.Store` interface** (`state.go`, after `PruneItemsBefore`):
```go
	// PruneFeedMetaBefore deletes per-feed HTTP cache metadata whose UpdatedAt
	// is older than cutoff and returns the number of rows removed. Backends with
	// service-managed TTL (DynamoDB, Cosmos) may implement this as a no-op.
	PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (removed int64, err error)
```

- [ ] **Step 2: Extend `statecleanup.Pruner` + `Run`** (`statecleanup.go`):
```go
type Pruner interface {
	PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error)
	PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (int64, error)
}
```
Change the `sweep` closure to prune both with one cutoff and report the combined count:
```go
	sweep := func() {
		cutoff := time.Now().Add(-ttl)
		nItems, err := p.PruneItemsBefore(ctx, cutoff)
		if err != nil {
			if onResult != nil {
				onResult(nItems, err)
			}
			return
		}
		nMeta, err := p.PruneFeedMetaBefore(ctx, cutoff)
		if onResult != nil {
			onResult(nItems+nMeta, err)
		}
	}
```
Update the `Run` doc comment to say it prunes seen-items and feed metadata older than `now-ttl`.

- [ ] **Step 3: Update statecleanup test** (`statecleanup_test.go`): add a `PruneFeedMetaBefore` method to `fakePruner` (mirror `PruneItemsBefore`, recording cutoffs / returning a count), and assert it is called per sweep with the same cutoff as items. Keep the existing assertions. Example additions:
```go
func (f *fakePruner) PruneFeedMetaBefore(_ context.Context, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metaCutoffs = append(f.metaCutoffs, cutoff)
	return 1, nil
}
```
Add `metaCutoffs []time.Time` to the struct and, in `TestRunSweepsImmediatelyThenOnTick`, assert `len(f.metaCutoffs) >= 2` and that the combined `removed` reported to `onResult` is the sum (items+meta) — i.e. each call reports `2` from the fake (1 item + 1 meta). Adjust the existing `results`/count expectations accordingly.

- [ ] **Step 4: Update the two mocks** — add to `memStore` (`internal/feed/detector_test.go`) and `fakeStore` (`cmd/rss2msg/pipeline_test.go`):
```go
func (m *memStore) PruneFeedMetaBefore(context.Context, time.Time) (int64, error) { return 0, nil }
```
(use the correct receiver name/type for each).

- [ ] **Step 5: Build + test** — `go build ./... && go vet ./...`; `go test ./internal/statecleanup/ -race ./internal/feed/ ./cmd/rss2msg/`. All green. (serve.go needs no change — `w.store` already satisfies the extended `Pruner`, and the combined count flows through the existing `onResult`.)

- [ ] **Step 6: Commit**
```bash
git add internal/state/state.go internal/statecleanup/statecleanup.go internal/statecleanup/statecleanup_test.go internal/feed/detector_test.go cmd/rss2msg/pipeline_test.go
git commit -m "feat(state): sweep feed_meta alongside seen_items in statecleanup"
```

---

### Task 13: Docs + comments (reverse the "never pruned" statements)

**Files:** Modify `docs/how-to/choose-a-state-store.md`; `internal/state/dynamodb/dynamodb.go` + `internal/state/cosmosdb/cosmosdb.go` package doc comments; `docs/superpowers/specs/2026-06-22-state-store-cleanup-design.md` (the "feed_meta never pruned" / out-of-scope lines). Grep first: `grep -rln "feed_meta is never\|feed_meta.*never\|never.*feed_meta\|meta rows are never\|never given a TTL\|never given a ttl" docs/ internal/`.

- [ ] **Step 1:** In `docs/how-to/choose-a-state-store.md`, change the statement that `feed_meta` is never pruned to: `feed_meta` is also bounded by `item_ttl`, anchored on its `updated_at` (last time the feed's HTTP validators changed). Add the operator-facing caveat: a still-polled feed that only returns `304 Not Modified` does not refresh `updated_at`, so its cached validators may be pruned after `item_ttl` and re-fetched once on the next poll — harmless (no duplicate publishes; `seen_items` still dedups). Update the backend table so the DynamoDB/Cosmos rows note that meta rows now also carry the native TTL. Bump `updated:` to 2026-06-22.

- [ ] **Step 2:** Update the DynamoDB package comment (`dynamodb.go` lines ~14-17) and Cosmos package comment (`cosmosdb.go` lines ~15-19): replace "meta rows are never given a TTL" with a statement that meta rows also receive the TTL (attribute / `ttl` property) when `item_ttl` is configured, so the service prunes stale feed metadata too.

- [ ] **Step 3:** In the spec (`...-design.md`), update the "feed_meta is never pruned" statements and remove/curtail the "Pruning `feed_meta`" out-of-scope bullet, noting feed_meta is now pruned by `item_ttl` on `updated_at` with the 304 caveat. (The plan files are historical; leave the original plan as-is — this addendum documents the change.)

- [ ] **Step 4:** Run `bash scripts/check-doc-links.sh` → `OK`. (No example.yaml change: `item_ttl` already documents it bounds retention; optionally add one clause to the `item_ttl` comment in BOTH example files that it also bounds feed_meta — if you do, keep them byte-identical and re-run `go test ./internal/config/ -run Example`.)

- [ ] **Step 5: Commit**
```bash
git add docs/how-to/choose-a-state-store.md internal/state/dynamodb/dynamodb.go internal/state/cosmosdb/cosmosdb.go docs/superpowers/specs/2026-06-22-state-store-cleanup-design.md
# plus the two example files if you edited them
git commit -m "docs(state): document feed_meta cleanup via item_ttl"
```

---

## Final verification (disk permitting; else CI on PR #191)
- [ ] `go build ./... && go vet ./...`
- [ ] `task test` (race) / `task test-integration` (Postgres feed_meta prune) — if disk allows; else note CI runs them.
- [ ] `bash scripts/check-doc-links.sh` → OK; example-config drift identical if examples touched.
- [ ] `git status` clean staging per commit.

## Self-review notes (plan vs. intent)
- Reuses `item_ttl`, anchors feed_meta on `updated_at` (Task 9/10 SQL DELETE; Task 11 native TTL). 304 caveat documented (Task 13) and accepted.
- `PruneFeedMetaBefore` signature identical across interface (Task 12), all 4 backends (Tasks 9-11), and `statecleanup.Pruner` (Task 12). Sweep uses one cutoff for both prunes.
- Build-green boundaries: each backend method added before the interface (Task 12) requires it; mocks updated in the same task as the interface.
- DynamoDB/Cosmos meta now carries native TTL — the only behavior reversal; covered by Task 11 tests and Task 13 docs/comments.
