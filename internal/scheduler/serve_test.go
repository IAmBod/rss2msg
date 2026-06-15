package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

type stubPipeline struct {
	mu     sync.Mutex
	calls  int32
	failed bool
}

func (s *stubPipeline) RunOnce(ctx context.Context, feedURL string, at time.Time) ([]model.Change, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	atomic.AddInt32(&s.calls, 1)
	if s.failed {
		return nil, errors.New("boom")
	}
	return nil, nil
}

func (s *stubPipeline) FeedURL() string { return "https://e/1" }

// slowPipeline's RunOnce takes `delay` to complete, simulating a poll that
// overruns its interval.
type slowPipeline struct{ delay time.Duration }

func (s *slowPipeline) RunOnce(ctx context.Context, _ string, _ time.Time) ([]model.Change, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
	}
	return nil, nil
}
func (s *slowPipeline) FeedURL() string { return "https://e/slow" }

func TestServeReportsPollOverrun(t *testing.T) {
	t.Parallel()
	p := &slowPipeline{delay: 60 * time.Millisecond}
	const interval = 20 * time.Millisecond

	var overruns int32
	var mu sync.Mutex
	var gotURL string
	var gotTook, gotInterval time.Duration

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		_ = Serve(ctx, ServeConfig{
			Pipelines:    []FeedPipeline{p},
			Intervals:    map[string]time.Duration{"https://e/slow": interval},
			DrainTimeout: 200 * time.Millisecond,
			OnPollOverrun: func(feedURL string, took, iv time.Duration) {
				atomic.AddInt32(&overruns, 1)
				mu.Lock()
				gotURL, gotTook, gotInterval = feedURL, took, iv
				mu.Unlock()
			},
		})
		close(done)
	}()
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	if atomic.LoadInt32(&overruns) < 1 {
		t.Fatalf("expected at least one poll-overrun callback, got 0")
	}
	mu.Lock()
	defer mu.Unlock()
	if gotURL != "https://e/slow" {
		t.Fatalf("callback feedURL = %q", gotURL)
	}
	if gotTook <= gotInterval {
		t.Fatalf("expected took (%v) > interval (%v)", gotTook, gotInterval)
	}
}

func TestServeNoOverrunWhenPollIsFast(t *testing.T) {
	t.Parallel()
	p := &stubPipeline{} // returns immediately
	var overruns int32

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		_ = Serve(ctx, ServeConfig{
			Pipelines:    []FeedPipeline{p},
			Intervals:    map[string]time.Duration{"https://e/1": 20 * time.Millisecond},
			DrainTimeout: 100 * time.Millisecond,
			OnPollOverrun: func(string, time.Duration, time.Duration) {
				atomic.AddInt32(&overruns, 1)
			},
		})
		close(done)
	}()
	time.Sleep(90 * time.Millisecond)
	cancel()
	<-done

	if got := atomic.LoadInt32(&overruns); got != 0 {
		t.Fatalf("expected no overruns for a fast poll, got %d", got)
	}
}

func TestServeRunsEachFeedOnSchedule(t *testing.T) {
	t.Parallel()
	p := &stubPipeline{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan struct{})
	go func() {
		_ = Serve(ctx, ServeConfig{
			Pipelines:    []FeedPipeline{p},
			Intervals:    map[string]time.Duration{"https://e/1": 20 * time.Millisecond},
			DrainTimeout: 100 * time.Millisecond,
		})
		close(done)
	}()

	time.Sleep(90 * time.Millisecond)
	cancel()
	<-done
	if got := atomic.LoadInt32(&p.calls); got < 2 {
		t.Fatalf("expected the feed to tick at least twice, got %d", got)
	}
}

// nChangePipeline always returns n model.Change values from RunOnce.
type nChangePipeline struct {
	url string
	n   int
}

func (p *nChangePipeline) FeedURL() string { return p.url }
func (p *nChangePipeline) RunOnce(_ context.Context, _ string, _ time.Time) ([]model.Change, error) {
	return make([]model.Change, p.n), nil
}

func TestServeFiresOnPollComplete(t *testing.T) {
	p := &nChangePipeline{url: "https://e/x", n: 3}
	got := make(chan int, 1)
	cfg := ServeConfig{
		Pipelines:    []FeedPipeline{p},
		Intervals:    map[string]time.Duration{"https://e/x": time.Hour},
		DrainTimeout: time.Second,
		OnPollComplete: func(feedURL string, changeCount int, err error, when time.Time) {
			select {
			case got <- changeCount:
			default:
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = Serve(ctx, cfg) }()
	select {
	case n := <-got:
		if n != 3 {
			t.Fatalf("changeCount = %d, want 3", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnPollComplete never fired")
	}
	cancel()
}
