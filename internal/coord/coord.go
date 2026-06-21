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

// Membership tracks the live set of rss2msg instances sharing a coordinator.
// Implementations register this instance under a TTL lease and return the
// currently-live member IDs (including self). Safe for concurrent use.
type Membership interface {
	// Heartbeat refreshes this instance's lease and returns the current live
	// member set, including self. Called every heartbeat_interval. On error the
	// caller keeps the last-known member set (fail-static).
	Heartbeat(ctx context.Context) ([]string, error)
	// Deregister removes this instance's member entry so peers reassign its
	// feeds promptly on graceful shutdown instead of waiting for the TTL.
	// Best-effort: callers log failures rather than treating them as fatal.
	Deregister(ctx context.Context) error
	Close() error
}

// MembershipProvider is implemented by coordinators that support the assignment
// layer. Membership returns a Membership bound to this instance's member ID,
// reusing the coordinator's existing client/connection.
type MembershipProvider interface {
	Membership(self string) (Membership, error)
}
