package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

type countingPipeline struct {
	url   string
	calls int32
	err   error
}

func (c *countingPipeline) FeedURL() string { return c.url }
func (c *countingPipeline) RunOnce(ctx context.Context, _ string, _ time.Time) ([]model.Change, error) {
	atomic.AddInt32(&c.calls, 1)
	return nil, c.err
}

func TestRunOncePollsEveryPipelineOnce(t *testing.T) {
	t.Parallel()
	ps := []FeedPipeline{
		&countingPipeline{url: "a"},
		&countingPipeline{url: "b"},
		&countingPipeline{url: "c"},
	}
	if err := RunOnce(context.Background(), RunOnceConfig{Pipelines: ps, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	for _, p := range ps {
		if got := atomic.LoadInt32(&p.(*countingPipeline).calls); got != 1 {
			t.Fatalf("%s: expected 1 call, got %d", p.FeedURL(), got)
		}
	}
}

func TestRunOnceReturnsJoinedErrors(t *testing.T) {
	t.Parallel()
	ps := []FeedPipeline{
		&countingPipeline{url: "a", err: errors.New("a fail")},
		&countingPipeline{url: "b"},
		&countingPipeline{url: "c", err: errors.New("c fail")},
	}
	err := RunOnce(context.Background(), RunOnceConfig{Pipelines: ps, Concurrency: 8})
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "a fail") || !strings.Contains(msg, "c fail") {
		t.Fatalf("joined message missing: %q", msg)
	}
}

func TestRunOnceDefaultConcurrencyCapsAtEight(t *testing.T) {
	t.Parallel()
	ps := make([]FeedPipeline, 20)
	for i := range ps {
		ps[i] = &countingPipeline{url: "u"}
	}
	cfg := RunOnceConfig{Pipelines: ps, Concurrency: 0}
	if got := effectiveConcurrency(cfg); got != 8 {
		t.Fatalf("expected 8, got %d", got)
	}
	cfg2 := RunOnceConfig{Pipelines: ps[:3], Concurrency: 0}
	if got := effectiveConcurrency(cfg2); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}
