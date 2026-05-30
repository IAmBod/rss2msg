package feedsource

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

// fakeSource is a test Source with controllable feeds/error and a manual signal.
type fakeSource struct {
	name string
	ch   chan struct{}
	fn   func() ([]config.FeedConfig, error)
}

func newFake(name string, fn func() ([]config.FeedConfig, error)) *fakeSource {
	return &fakeSource{name: name, ch: make(chan struct{}, 1), fn: fn}
}
func (f *fakeSource) Name() string                                       { return f.name }
func (f *fakeSource) Feeds(context.Context) ([]config.FeedConfig, error) { return f.fn() }
func (f *fakeSource) Changes() <-chan struct{}                           { return f.ch }
func (f *fakeSource) signal()                                            { f.ch <- struct{}{} }

func feed(url string, interval time.Duration) config.FeedConfig {
	return config.FeedConfig{URL: url, Interval: interval}
}

func TestAggregatorMergesAndDedupsByURL(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://x", time.Minute), feed("https://x", time.Minute)}, nil
	})
	b := newFake("b", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://y", time.Minute)}, nil
	})
	agg := NewAggregator(a, b)
	got, err := agg.Desired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 deduped feeds, got %d: %+v", len(got), got)
	}
}

func TestAggregatorEarlierSourceWinsOnCollision(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://x", 1*time.Minute)}, nil
	})
	b := newFake("b", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://x", 9*time.Minute)}, nil
	})
	agg := NewAggregator(a, b) // a has precedence
	got, _ := agg.Desired(context.Background())
	if len(got) != 1 || got[0].Interval != 1*time.Minute {
		t.Fatalf("want winner from a (1m), got %+v", got)
	}
}

func TestAggregatorFailingSourceKeepsLastKnownGood(t *testing.T) {
	fail := false
	a := newFake("a", func() ([]config.FeedConfig, error) {
		if fail {
			return nil, errors.New("boom")
		}
		return []config.FeedConfig{feed("https://x", time.Minute)}, nil
	})
	agg := NewAggregator(a)

	if _, err := agg.Desired(context.Background()); err != nil { // primes last-known-good
		t.Fatal(err)
	}
	fail = true
	got, err := agg.Desired(context.Background())
	if err != nil {
		t.Fatalf("aggregator should not surface source error: %v", err)
	}
	if len(got) != 1 || got[0].URL != "https://x" {
		t.Fatalf("want last-known-good retained, got %+v", got)
	}
}

func TestAggregatorEmptySetAccepted(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) { return nil, nil })
	agg := NewAggregator(a)
	got, err := agg.Desired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty set, got %+v", got)
	}
}

func TestAggregatorChangesFanIn(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) { return nil, nil })
	agg := NewAggregator(a)
	a.signal()
	select {
	case <-agg.Changes():
	case <-time.After(time.Second):
		t.Fatal("expected aggregator to forward source signal")
	}
}
