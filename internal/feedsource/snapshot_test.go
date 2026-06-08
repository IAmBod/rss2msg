package feedsource

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

func TestSnapshotMergesAndDedupesByPrecedence(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://x", 1*time.Minute), feed("https://y", time.Minute)}, nil
	})
	b := newFake("b", func() ([]config.FeedConfig, error) {
		// https://x collides with a (earlier source wins); https://z is new.
		return []config.FeedConfig{feed("https://x", 9*time.Minute), feed("https://z", time.Minute)}, nil
	})
	got, err := Snapshot(context.Background(), a, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 deduped feeds, got %d: %+v", len(got), got)
	}
	// a wins the https://x collision, so its 1m interval is kept.
	for _, fc := range got {
		if fc.URL == "https://x" && fc.Interval != 1*time.Minute {
			t.Fatalf("earlier source should win collision; got interval %s", fc.Interval)
		}
	}
}

func TestSnapshotPropagatesSourceError(t *testing.T) {
	good := newFake("good", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://x", time.Minute)}, nil
	})
	bad := newFake("bad", func() ([]config.FeedConfig, error) {
		return nil, errors.New("boom")
	})
	_, err := Snapshot(context.Background(), good, bad)
	if err == nil {
		t.Fatal("expected a source error to abort the snapshot")
	}
	if !contains(err.Error(), "bad") || !contains(err.Error(), "boom") {
		t.Fatalf("error should name the failing source and cause, got %v", err)
	}
}

func TestSnapshotEmptyIsNotAnError(t *testing.T) {
	got, err := Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
