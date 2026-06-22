// Package statecleanup runs a periodic sweep that deletes seen-item state older
// than a TTL. It carries no logging or config dependency so it can be tested in
// isolation; the caller injects what each sweep reports via onResult.
package statecleanup

import (
	"context"
	"time"
)

// Pruner is the subset of state.Store this loop needs.
type Pruner interface {
	PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

// Run blocks until ctx is cancelled. It performs an immediate sweep, then one
// sweep per interval. Each sweep deletes items last seen before now-ttl and, if
// onResult is non-nil, reports the rows removed and any error. Run returns
// immediately if interval or ttl is non-positive.
func Run(ctx context.Context, interval, ttl time.Duration, p Pruner, onResult func(removed int64, err error)) {
	if interval <= 0 || ttl <= 0 {
		return
	}
	sweep := func() {
		n, err := p.PruneItemsBefore(ctx, time.Now().Add(-ttl))
		if onResult != nil {
			onResult(n, err)
		}
	}
	sweep() // clear the backlog at startup instead of waiting a full interval
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sweep()
		}
	}
}
