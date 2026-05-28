package coord

import "context"

// ReleaseFunc is returned by TryAcquire on successful lease acquisition.
// Callers must invoke it (typically via defer) before their next poll
// attempt for the same feed.
type ReleaseFunc func(ctx context.Context) error

// Coordinator gates which instance is allowed to poll a given feed in a
// given cycle. Implementations must be safe for concurrent use across many
// goroutines.
type Coordinator interface {
	// TryAcquire returns (release, true, nil) if this instance just won the
	// lease for feedURL.
	//
	// Returns (nil, false, nil) if another instance currently holds the
	// lease. The caller should skip this poll cycle.
	//
	// Returns (nil, false, err) on infrastructure failure. The caller treats
	// this the same as not-acquired (skip), logs at warn, and increments the
	// coord_error metric.
	TryAcquire(ctx context.Context, feedURL string) (ReleaseFunc, bool, error)

	Close() error
}
