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
	// OnPollOverrun, if set, is called when a single poll takes longer than the
	// feed's interval, so the effective polling rate is below what's configured.
	// Optional.
	OnPollOverrun func(feedURL string, took, interval time.Duration)
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
		var onOverrun func(took time.Duration)
		if cfg.OnPollOverrun != nil {
			url := p.FeedURL()
			onOverrun = func(took time.Duration) { cfg.OnPollOverrun(url, took, interval) }
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runFeedLoop(ctx, p, interval, collect, onOverrun)
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

func runFeedLoop(ctx context.Context, p FeedPipeline, interval time.Duration, collect func(error), onOverrun func(took time.Duration)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	// Tick once immediately on start so users don't wait an interval before the
	// first fetch happens.
	runTick(ctx, p, interval, collect, onOverrun)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			_ = now
			runTick(ctx, p, interval, collect, onOverrun)
		}
	}
}

// runTick performs one poll and, when it overran the interval, reports it. A
// poll that ends only because ctx was cancelled is not counted as an overrun.
func runTick(ctx context.Context, p FeedPipeline, interval time.Duration, collect func(error), onOverrun func(took time.Duration)) {
	start := time.Now()
	tick(ctx, p, collect)
	took := time.Since(start)
	if onOverrun != nil && took > interval && ctx.Err() == nil {
		onOverrun(took)
	}
}

func tick(ctx context.Context, p FeedPipeline, collect func(error)) {
	_, err := p.RunOnce(ctx, p.FeedURL(), time.Now().UTC())
	if err != nil && !errors.Is(err, context.Canceled) {
		collect(err)
	}
}
