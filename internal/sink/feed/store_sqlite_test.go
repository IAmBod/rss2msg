package feed

import (
	"context"
	"testing"
	"time"
)

func TestSQLiteStore_UpsertOrderRetention(t *testing.T) {
	ctx := context.Background()
	s, err := newSQLiteStore(ctx, ":memory:", "feed_output", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Unix(2000, 0).UTC()
	for i, id := range []string{"a", "b", "c"} {
		if err := s.Write(ctx, chg("f", id, base.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Write(ctx, chg("f", "c", base.Add(10*time.Second))); err != nil { // update c
		t.Fatal(err)
	}
	got, err := s.Recent(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ItemID != "c" || got[1].ItemID != "b" {
		t.Fatalf("want [c,b], got %+v", got)
	}
}

func TestSQLiteStore_SubSecondOrdering(t *testing.T) {
	// Regression: detected_at is TEXT ordered lexically, so sub-second times
	// must use a fixed-width format. 0.300s and 0.350s would misorder under
	// time.RFC3339Nano (trailing-zero trimming). Newest-first must hold.
	ctx := context.Background()
	s, err := newSQLiteStore(ctx, ":memory:", "feed_output", 10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Unix(2000, 0).UTC()
	earlier := base.Add(300 * time.Millisecond) // 0.300s
	later := base.Add(350 * time.Millisecond)   // 0.350s
	if err := s.Write(ctx, chg("f", "earlier", earlier)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, chg("f", "later", later)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Recent(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ItemID != "later" || got[1].ItemID != "earlier" {
		t.Fatalf("sub-second ordering wrong; want [later, earlier], got %+v", got)
	}
}

func TestSQLiteStore_RetentionPrunes(t *testing.T) {
	old := sqlitePruneEvery
	sqlitePruneEvery = 1 // prune on every write
	defer func() { sqlitePruneEvery = old }()
	ctx := context.Background()
	s, err := newSQLiteStore(ctx, ":memory:", "feed_output", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Unix(2000, 0).UTC()
	for i, id := range []string{"a", "b", "c", "d"} {
		if err := s.Write(ctx, chg("f", id, base.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.Recent(ctx, 100)
	if len(got) != 2 || got[0].ItemID != "d" || got[1].ItemID != "c" {
		t.Fatalf("want pruned to newest 2 [d,c], got %+v", got)
	}
}
