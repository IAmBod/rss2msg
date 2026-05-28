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
	store, err := statepg.New(ctx, dsn)
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
