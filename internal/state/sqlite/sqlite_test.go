package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/state"
	sqlitestate "github.com/iambod/rss2msg/internal/state/sqlite"
)

func newStore(t *testing.T) *sqlitestate.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := sqlitestate.New(context.Background(), filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPingAfterOpen(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestItemMissingThenUpsertedThenUpdated(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	if _, found, err := s.GetItem(ctx, "https://e/f", "i1"); err != nil || found {
		t.Fatalf("expected miss, got found=%v err=%v", found, err)
	}

	t1 := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	if err := s.UpsertItem(ctx, "https://e/f", "i1", "h1", t1); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.GetItem(ctx, "https://e/f", "i1")
	if err != nil || !found {
		t.Fatalf("expected hit, got found=%v err=%v", found, err)
	}
	if got.ContentHash != "h1" || !got.LastSeenAt.Equal(t1) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	t2 := t1.Add(5 * time.Minute)
	if err := s.UpsertItem(ctx, "https://e/f", "i1", "h2", t2); err != nil {
		t.Fatal(err)
	}
	got, _, err = s.GetItem(ctx, "https://e/f", "i1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ContentHash != "h2" || !got.LastSeenAt.Equal(t2) {
		t.Fatalf("update mismatch: %+v", got)
	}
}

func TestFeedMetaMissingThenUpsertedWithAndWithoutLastModified(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	if _, found, err := s.GetFeedMeta(ctx, "https://e/f"); err != nil || found {
		t.Fatalf("expected miss, got found=%v err=%v", found, err)
	}

	// Without LastModified.
	if err := s.UpsertFeedMeta(ctx, "https://e/f", state.FeedMeta{ETag: `W/"abc"`}); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.GetFeedMeta(ctx, "https://e/f")
	if err != nil || !found {
		t.Fatalf("expected hit, got found=%v err=%v", found, err)
	}
	if got.ETag != `W/"abc"` || !got.LastModified.IsZero() {
		t.Fatalf("unexpected meta: %+v", got)
	}

	// With LastModified.
	lm := time.Date(2026, 5, 29, 10, 0, 0, 0, time.UTC)
	if err := s.UpsertFeedMeta(ctx, "https://e/f", state.FeedMeta{ETag: `W/"xyz"`, LastModified: lm}); err != nil {
		t.Fatal(err)
	}
	got, _, err = s.GetFeedMeta(ctx, "https://e/f")
	if err != nil {
		t.Fatal(err)
	}
	if got.ETag != `W/"xyz"` || !got.LastModified.Equal(lm) {
		t.Fatalf("unexpected updated meta: %+v", got)
	}
}

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

func TestSchemaIsIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")

	s1, err := sqlitestate.New(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	_ = s1.Close()

	s2, err := sqlitestate.New(context.Background(), path)
	if err != nil {
		t.Fatalf("second open should be idempotent: %v", err)
	}
	_ = s2.Close()
}

func TestInMemoryDatabase(t *testing.T) {
	t.Parallel()
	s, err := sqlitestate.New(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}
