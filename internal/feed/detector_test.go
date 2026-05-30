package feed

import (
	"context"
	"testing"
	"time"

	"github.com/mmcdole/gofeed"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/state"
)

type memStore struct {
	items map[string]state.ItemState
}

func newMemStore() *memStore { return &memStore{items: map[string]state.ItemState{}} }
func (m *memStore) GetItem(ctx context.Context, feed, id string) (state.ItemState, bool, error) {
	s, ok := m.items[feed+"|"+id]
	return s, ok, nil
}
func (m *memStore) UpsertItem(ctx context.Context, feed, id, hash string, seenAt time.Time) error {
	m.items[feed+"|"+id] = state.ItemState{ContentHash: hash, LastSeenAt: seenAt}
	return nil
}
func (m *memStore) GetFeedMeta(ctx context.Context, _ string) (state.FeedMeta, bool, error) {
	return state.FeedMeta{}, false, nil
}
func (m *memStore) UpsertFeedMeta(ctx context.Context, _ string, _ state.FeedMeta) error {
	return nil
}
func (m *memStore) Ping(ctx context.Context) error { return nil }
func (m *memStore) Close() error                   { return nil }

func sampleFeed(items ...*gofeed.Item) *gofeed.Feed {
	return &gofeed.Feed{Title: "Sample", Items: items}
}

func TestDetectFirstSeenIsNew(t *testing.T) {
	t.Parallel()
	det := NewDetector()
	st := newMemStore()
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	feed := sampleFeed(&gofeed.Item{GUID: "a", Title: "Hello", Link: "https://e/a", Content: "body"})

	changes, err := det.Detect(context.Background(), "https://e/feed", feed, st, when)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Kind != model.ChangeNew {
		t.Fatalf("expected one new change, got %+v", changes)
	}
	if changes[0].ItemID != "a" || changes[0].DetectedAt != when {
		t.Fatalf("bad envelope: %+v", changes[0])
	}
}

func TestDetectUnchangedYieldsNothing(t *testing.T) {
	t.Parallel()
	det := NewDetector()
	st := newMemStore()
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	feed := sampleFeed(&gofeed.Item{GUID: "a", Title: "Hello", Link: "https://e/a", Content: "body"})

	first, _ := det.Detect(context.Background(), "https://e/feed", feed, st, when)
	// Commit state for that change, as the scheduler would.
	for _, c := range first {
		if err := st.UpsertItem(context.Background(), c.FeedURL, c.ItemID, c.ContentHash, c.DetectedAt); err != nil {
			t.Fatal(err)
		}
	}

	second, _ := det.Detect(context.Background(), "https://e/feed", feed, st, when.Add(time.Minute))
	if len(second) != 0 {
		t.Fatalf("expected no changes second pass, got %+v", second)
	}
}

func TestDetectChangedYieldsUpdated(t *testing.T) {
	t.Parallel()
	det := NewDetector()
	st := newMemStore()
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	feedV1 := sampleFeed(&gofeed.Item{GUID: "a", Title: "Hello", Link: "https://e/a", Content: "body one"})
	first, _ := det.Detect(context.Background(), "https://e/feed", feedV1, st, when)
	for _, c := range first {
		_ = st.UpsertItem(context.Background(), c.FeedURL, c.ItemID, c.ContentHash, c.DetectedAt)
	}

	feedV2 := sampleFeed(&gofeed.Item{GUID: "a", Title: "Hello", Link: "https://e/a", Content: "body two"})
	second, _ := det.Detect(context.Background(), "https://e/feed", feedV2, st, when.Add(time.Minute))
	if len(second) != 1 || second[0].Kind != model.ChangeUpdated {
		t.Fatalf("expected one updated change, got %+v", second)
	}
}

func TestDetectOrdersByPublishedAscending(t *testing.T) {
	t.Parallel()
	det := NewDetector()
	st := newMemStore()
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	t1 := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	// Feed delivers newest-first, as real feeds (e.g. vg.hu) do.
	feed := sampleFeed(
		&gofeed.Item{GUID: "c", Title: "Newest", PublishedParsed: &t3},
		&gofeed.Item{GUID: "a", Title: "Oldest", PublishedParsed: &t1},
		&gofeed.Item{GUID: "b", Title: "Middle", PublishedParsed: &t2},
	)

	changes, err := det.Detect(context.Background(), "https://e/feed", feed, st, when)
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := []string{changes[0].ItemID, changes[1].ItemID, changes[2].ItemID}
	wantIDs := []string{"a", "b", "c"}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("expected published-ascending order %v, got %v", wantIDs, gotIDs)
		}
	}
}

func TestDetectSyntheticIdentityKey(t *testing.T) {
	t.Parallel()
	det := NewDetector()
	st := newMemStore()
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	item := &gofeed.Item{Title: "No-id"}
	changes, _ := det.Detect(context.Background(), "https://e/feed", sampleFeed(item), st, when)
	if len(changes) != 1 || changes[0].ItemID == "" || len(changes[0].ItemID) != 64 {
		t.Fatalf("expected sha256 synthetic id, got %+v", changes)
	}
}
