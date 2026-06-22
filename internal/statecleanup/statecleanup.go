// Package statecleanup runs a periodic sweep that deletes seen-item state and
// feed metadata older than a TTL. It carries no logging or config dependency so
// it can be tested in isolation; the caller injects what each sweep reports via
// onResult.
package statecleanup

import (
	"context"
	"time"
)

// Pruner is the subset of state.Store this loop needs.
type Pruner interface {
	PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error)
	PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

// Run blocks until ctx is cancelled. It performs an immediate sweep, then one
// sweep per interval. Each sweep deletes seen-items and feed metadata older than
// now-ttl and, if onResult is non-nil, reports the combined rows removed and any
// error. Run returns immediately if interval or ttl is non-positive.
func Run(ctx context.Context, interval, ttl time.Duration, p Pruner, onResult func(removed int64, err error)) {
	if interval <= 0 || ttl <= 0 {
		return
	}
	sweep := func() {
		cutoff := time.Now().Add(-ttl)
		nItems, err := p.PruneItemsBefore(ctx, cutoff)
		if err != nil {
			if onResult != nil {
				onResult(nItems, err)
			}
			return
		}
		nMeta, err := p.PruneFeedMetaBefore(ctx, cutoff)
		if onResult != nil {
			onResult(nItems+nMeta, err)
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
