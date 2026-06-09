//go:build integration

package cosmosdb

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tccosmos "github.com/testcontainers/testcontainers-go/modules/azure/cosmosdb"

	"github.com/iambod/rss2msg/internal/state"
)

const emulatorImage = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview"

// setup starts the Cosmos DB emulator and returns a ready Store plus the
// connection string / client options routing to the emulator (self-signed
// cert trusted).
func setup(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	ctr, err := tccosmos.Run(ctx, emulatorImage)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	connStr, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := tccosmos.NewContainerPolicy(ctx, ctr)
	if err != nil {
		t.Fatal(err)
	}
	clientOpts := policy.ClientOptions()

	s, err := New(ctx, Options{
		ConnectionString: connStr,
		Database:         "rss2msg",
		Container:        "feed_state",
		CreateIfMissing:  true,
		Throughput:       400,
		ItemTTL:          time.Hour,
		ClientOptions:    clientOpts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := setup(t)

	const feed = "https://example.com/feed"

	if err := s.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Item: missing -> upsert -> get -> overwrite.
	if _, found, err := s.GetItem(ctx, feed, "item-1"); err != nil || found {
		t.Fatalf("missing item: found=%v err=%v", found, err)
	}
	seen := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	if err := s.UpsertItem(ctx, feed, "item-1", "hash-1", seen); err != nil {
		t.Fatalf("upsert item: %v", err)
	}
	got, found, err := s.GetItem(ctx, feed, "item-1")
	if err != nil || !found {
		t.Fatalf("get item: found=%v err=%v", found, err)
	}
	if got.ContentHash != "hash-1" || !got.LastSeenAt.Equal(seen) {
		t.Fatalf("item round-trip mismatch: %+v", got)
	}
	if err := s.UpsertItem(ctx, feed, "item-1", "hash-2", seen); err != nil {
		t.Fatalf("re-upsert item: %v", err)
	}
	if got, _, _ := s.GetItem(ctx, feed, "item-1"); got.ContentHash != "hash-2" {
		t.Fatalf("overwrite hash=%q, want hash-2", got.ContentHash)
	}

	// Meta: missing -> upsert -> get.
	if _, found, err := s.GetFeedMeta(ctx, feed); err != nil || found {
		t.Fatalf("missing meta: found=%v err=%v", found, err)
	}
	lm := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	if err := s.UpsertFeedMeta(ctx, feed, state.FeedMeta{ETag: `"abc"`, LastModified: lm}); err != nil {
		t.Fatalf("upsert meta: %v", err)
	}
	meta, found, err := s.GetFeedMeta(ctx, feed)
	if err != nil || !found {
		t.Fatalf("get meta: found=%v err=%v", found, err)
	}
	if meta.ETag != `"abc"` || !meta.LastModified.Equal(lm) {
		t.Fatalf("meta round-trip mismatch: %+v", meta)
	}

	// Item and meta coexist in the same partition.
	if _, found, _ := s.GetItem(ctx, feed, "item-1"); !found {
		t.Fatal("item lost after meta writes")
	}
}
