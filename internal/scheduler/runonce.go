package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"
)

type RunOnceConfig struct {
	Pipelines   []FeedPipeline
	Concurrency int // 0 = min(8, len(pipelines))
}

func RunOnce(ctx context.Context, cfg RunOnceConfig) error {
	workers := effectiveConcurrency(cfg)
	jobs := make(chan FeedPipeline)

	var (
		wg     sync.WaitGroup
		errsMu sync.Mutex
		errs   []error
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if ctx.Err() != nil {
					return
				}
				if _, err := p.RunOnce(ctx, p.FeedURL(), time.Now().UTC()); err != nil && !errors.Is(err, context.Canceled) {
					errsMu.Lock()
					errs = append(errs, err)
					errsMu.Unlock()
				}
			}
		}()
	}

	for _, p := range cfg.Pipelines {
		select {
		case <-ctx.Done():
		case jobs <- p:
		}
	}
	close(jobs)
	wg.Wait()
	return errors.Join(errs...)
}

func effectiveConcurrency(cfg RunOnceConfig) int {
	if cfg.Concurrency > 0 {
		return cfg.Concurrency
	}
	n := len(cfg.Pipelines)
	if n == 0 {
		return 1
	}
	if n < 8 {
		return n
	}
	return 8
}
