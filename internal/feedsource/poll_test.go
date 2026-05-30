package feedsource

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

func TestPollSourceTicksAndFetches(t *testing.T) {
	var calls int32
	p := NewPoll("poll", 20*time.Millisecond, func(context.Context) ([]config.FeedConfig, error) {
		atomic.AddInt32(&calls, 1)
		return []config.FeedConfig{feed("https://e/1", time.Minute)}, nil
	})
	t.Cleanup(p.Close)

	got, err := p.Feeds(context.Background())
	if err != nil || len(got) != 1 {
		t.Fatalf("feeds=%+v err=%v", got, err)
	}

	signals := 0
	deadline := time.After(200 * time.Millisecond)
	for signals < 2 {
		select {
		case <-p.Changes():
			signals++
		case <-deadline:
			t.Fatalf("want >=2 tick signals, got %d", signals)
		}
	}
	if atomic.LoadInt32(&calls) < 1 {
		t.Fatal("fetch never called")
	}
}

func TestPollSourceNonPositiveIntervalAndDoubleClose(t *testing.T) {
	p := NewPoll("poll", 0, func(context.Context) ([]config.FeedConfig, error) { return nil, nil })
	p.Close()
	p.Close() // must not panic
}
