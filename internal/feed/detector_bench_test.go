package feed

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mmcdole/gofeed"
)

// benchFeed builds a feed of n items with distinct identities, published times,
// content and author, delivered newest-first as real feeds do. It exercises the
// detector's per-item hot path: identity-key derivation, content hashing, the
// state lookup, and the published-ascending sort.
func benchFeed(n int) *gofeed.Feed {
	items := make([]*gofeed.Item, n)
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		pub := base.Add(time.Duration(i) * time.Minute)
		// Fill newest-first (index 0 == newest) to mirror a real feed page.
		items[n-1-i] = &gofeed.Item{
			GUID:            fmt.Sprintf("guid-%d", i),
			Title:           fmt.Sprintf("Item %d", i),
			Link:            fmt.Sprintf("https://example.test/item/%d", i),
			Content:         fmt.Sprintf("Body content for item %d with enough words to hash.", i),
			Author:          &gofeed.Person{Name: "Bench Author"},
			PublishedParsed: &pub,
		}
	}
	return &gofeed.Feed{Title: "Bench Feed", Items: items}
}

// BenchmarkDetectAllNew measures the cold path where every item is unseen. The
// store is never written by Detect, so a single empty store yields "not found"
// on every lookup across all iterations.
func BenchmarkDetectAllNew(b *testing.B) {
	det := NewDetector()
	feed := benchFeed(50)
	st := newMemStore()
	when := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := det.Detect(ctx, "https://example.test/feed", feed, st, when); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDetectSteadyState measures the common case: a poll where every item
// has been seen before and is unchanged, so the detector hashes, looks up, and
// emits nothing. This is the path that runs on the vast majority of poll cycles.
func BenchmarkDetectSteadyState(b *testing.B) {
	det := NewDetector()
	feed := benchFeed(50)
	st := newMemStore()
	when := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()

	// Seed the store so every item is already known with a matching hash.
	first, err := det.Detect(ctx, "https://example.test/feed", feed, st, when)
	if err != nil {
		b.Fatal(err)
	}
	for _, c := range first {
		if err := st.UpsertItem(ctx, c.FeedURL, c.ItemID, c.ContentHash, c.DetectedAt); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		changes, err := det.Detect(ctx, "https://example.test/feed", feed, st, when)
		if err != nil {
			b.Fatal(err)
		}
		if len(changes) != 0 {
			b.Fatalf("steady state should yield no changes, got %d", len(changes))
		}
	}
}
