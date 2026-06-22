package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/model"
)

type pokePipeline struct {
	url   string
	polls *int32
}

func (p *pokePipeline) FeedURL() string { return p.url }
func (p *pokePipeline) RunOnce(ctx context.Context, _ string, _ time.Time) ([]model.Change, error) {
	atomic.AddInt32(p.polls, 1)
	return nil, nil
}

func TestPollNowTriggersExtraPoll(t *testing.T) {
	var polls int32
	url := "https://example.com/feed"
	pollNow := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ServeDynamic(ctx, DynamicConfig{
		Provider: newStaticProvider([]config.FeedConfig{{URL: url, Interval: time.Hour}}),
		Factory:  func(config.FeedConfig) (FeedPipeline, error) { return &pokePipeline{url: url, polls: &polls}, nil },
		PollNow:  pollNow,
	})

	// Wait for the initial immediate tick.
	waitFor(t, func() bool { return atomic.LoadInt32(&polls) == 1 })
	pollNow <- url
	waitFor(t, func() bool { return atomic.LoadInt32(&polls) == 2 })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
