// Package heartbeat provides an opt-in liveness signal: a callback invoked on a
// fixed interval for as long as a context is live. It carries no logging or
// configuration dependency so it can be tested in isolation; the caller injects
// what each beat does via emit.
package heartbeat

import (
	"context"
	"time"
)

// Run blocks until ctx is cancelled, calling emit once per interval. The first
// call happens after the first full interval elapses (it does not fire
// immediately). Run returns immediately if interval is non-positive.
func Run(ctx context.Context, interval time.Duration, emit func()) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			emit()
		}
	}
}
