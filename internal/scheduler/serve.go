package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

// FeedPipeline is the per-feed unit the scheduler ticks. The scheduler does
// not need to know about Fetchers, Detectors, or Sinks directly.
type FeedPipeline interface {
	FeedURL() string
	RunOnce(ctx context.Context, feedURL string, at time.Time) ([]model.Change, error)
}

type ServeConfig struct {
	Pipelines    []FeedPipeline
	Intervals    map[string]time.Duration // keyed by feed URL
	DrainTimeout time.Duration            // grace period after ctx is cancelled
}

// Serve runs one goroutine per pipeline, each on its own ticker. It returns
// when ctx is cancelled and all in-flight ticks complete, or when DrainTimeout
// elapses (whichever comes first). Errors from RunOnce are returned as a
// joined error after shutdown.
func Serve(ctx context.Context, cfg ServeConfig) error {
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}

	var (
		wg     sync.WaitGroup
		errsMu sync.Mutex
		joined []error
	)
	collect := func(err error) {
		if err == nil {
			return
		}
		errsMu.Lock()
		joined = append(joined, err)
		errsMu.Unlock()
	}

	for _, p := range cfg.Pipelines {
		p := p
		interval, ok := cfg.Intervals[p.FeedURL()]
		if !ok || interval <= 0 {
			collect(errors.New("scheduler: no interval for feed " + p.FeedURL()))
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runFeedLoop(ctx, p, interval, collect)
		}()
	}

	<-ctx.Done()

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(cfg.DrainTimeout):
		collect(errors.New("scheduler: drain timeout exceeded"))
	}

	return errors.Join(joined...)
}

func runFeedLoop(ctx context.Context, p FeedPipeline, interval time.Duration, collect func(error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	// Tick once immediately on start so users don't wait an interval before the
	// first fetch happens.
	tick(ctx, p, collect)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			_ = now
			tick(ctx, p, collect)
		}
	}
}

func tick(ctx context.Context, p FeedPipeline, collect func(error)) {
	_, err := p.RunOnce(ctx, p.FeedURL(), time.Now().UTC())
	if err != nil && !errors.Is(err, context.Canceled) {
		collect(err)
	}
}
