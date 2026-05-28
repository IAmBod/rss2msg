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
