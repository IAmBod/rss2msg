//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/iambod/rss2msg/internal/state"
	statepg "github.com/iambod/rss2msg/internal/state/postgres"
)

func setupStore(t *testing.T) *statepg.Store {
	t.Helper()
	ctx := context.Background()

	pgC, err := tcpg.Run(ctx, "postgres:16-alpine",
		tcpg.WithDatabase("rss2msg"),
		tcpg.WithUsername("test"),
		tcpg.WithPassword("test"),
		tcpg.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })

	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	store, err := statepg.New(ctx, statepg.Options{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestStoreItemRoundTrip(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	_, found, err := store.GetItem(ctx, "feed-a", "item-1")
	if err != nil || found {
		t.Fatalf("expected not-found, got found=%v err=%v", found, err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	if err := store.UpsertItem(ctx, "feed-a", "item-1", "hash-1", now); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.GetItem(ctx, "feed-a", "item-1")
	if err != nil || !found {
		t.Fatalf("expected found, got found=%v err=%v", found, err)
	}
	if got.ContentHash != "hash-1" || !got.LastSeenAt.Equal(now) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	if err := store.UpsertItem(ctx, "feed-a", "item-1", "hash-2", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, _, _ = store.GetItem(ctx, "feed-a", "item-1")
	if got.ContentHash != "hash-2" {
		t.Fatalf("expected hash-2 after update, got %q", got.ContentHash)
	}
}

func TestStoreFeedMetaRoundTrip(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	if err := store.UpsertFeedMeta(ctx, "feed-a", state.FeedMeta{ETag: `"abc"`, LastModified: now}); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.GetFeedMeta(ctx, "feed-a")
	if err != nil || !found {
		t.Fatalf("expected found, err=%v", err)
	}
	if got.ETag != `"abc"` || !got.LastModified.Equal(now) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

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
