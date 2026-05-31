package feed

import (
	"context"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

func chg(feed, id string, at time.Time) model.Change {
	return model.Change{FeedURL: feed, ItemID: id, Kind: model.ChangeNew, Title: id, DetectedAt: at}
}

func TestMemoryStore_UpsertDedupAndOrder(t *testing.T) {
	ctx := context.Background()
	s := newMemoryStore(3)
	base := time.Unix(1000, 0)
	_ = s.Write(ctx, chg("f", "a", base))
	_ = s.Write(ctx, chg("f", "b", base.Add(time.Second)))
	_ = s.Write(ctx, chg("f", "a", base.Add(2*time.Second))) // update a -> jumps to top
	got, _ := s.Recent(ctx, 10)
	if len(got) != 2 {
		t.Fatalf("want 2 deduped rows, got %d", len(got))
	}
	if got[0].ItemID != "a" || got[1].ItemID != "b" {
		t.Fatalf("want a (updated) newest, then b; got %s,%s", got[0].ItemID, got[1].ItemID)
	}
}

func TestMemoryStore_RetentionBound(t *testing.T) {
	ctx := context.Background()
	s := newMemoryStore(2)
	base := time.Unix(1000, 0)
	for i, id := range []string{"a", "b", "c"} {
		_ = s.Write(ctx, chg("f", id, base.Add(time.Duration(i)*time.Second)))
	}
	got, _ := s.Recent(ctx, 10)
	if len(got) != 2 || got[0].ItemID != "c" || got[1].ItemID != "b" {
		t.Fatalf("want newest 2 (c,b), got %+v", got)
	}
}
