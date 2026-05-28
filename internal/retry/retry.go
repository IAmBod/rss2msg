package retry

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type Result struct {
	Attempts int
	Err      error
}

// Do invokes fn up to cfg.MaxAttempts times, sleeping between attempts with
// exponential backoff and full jitter. It returns immediately on success or
// when ctx is cancelled.
func Do(ctx context.Context, cfg Config, fn func(ctx context.Context) error) Result {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 100 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 || cfg.MaxDelay < cfg.BaseDelay {
		cfg.MaxDelay = 10 * cfg.BaseDelay
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				return Result{Attempts: attempt - 1, Err: err}
			}
			return Result{Attempts: attempt - 1, Err: errors.Join(lastErr, err)}
		}
		err := fn(ctx)
		if err == nil {
			return Result{Attempts: attempt, Err: nil}
		}
		lastErr = err
		if attempt == cfg.MaxAttempts {
			break
		}
		delay := delayFor(cfg, attempt)
		select {
		case <-ctx.Done():
			return Result{Attempts: attempt, Err: errors.Join(lastErr, ctx.Err())}
		case <-time.After(delay):
		}
	}
	return Result{Attempts: cfg.MaxAttempts, Err: lastErr}
}

// delayFor returns base * 2^(attempt-1) capped at MaxDelay, plus jitter in
// [0, delay).
func delayFor(cfg Config, attempt int) time.Duration {
	d := cfg.BaseDelay << (attempt - 1)
	if d <= 0 || d > cfg.MaxDelay {
		d = cfg.MaxDelay
	}
	jitter := time.Duration(rand.Int63n(int64(d)))
	return d + jitter
}
