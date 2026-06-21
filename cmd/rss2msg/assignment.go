package main

import (
	"context"
	"fmt"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/coord"
	"github.com/iambod/rss2msg/internal/scheduler"
	"github.com/iambod/rss2msg/internal/telemetry"
)

// maybeWrapProvider wraps inner with an OwnerProvider when assignment is
// enabled, reusing the coordinator's client via coord.MembershipProvider.
//
// Returns:
//   - (inner, nil, nil) when assignment is disabled (no-op; today's behavior).
//   - (op, op, nil) when enabled and the driver supports membership.
//   - (nil, nil, err) when the driver doesn't support membership or membership
//     construction fails.
//
// The caller is responsible for calling op.Run(ctx) in a goroutine and
// op.Close(ctx) on shutdown (after ServeDynamic drains).
func maybeWrapProvider(
	cfg config.Config,
	cd coord.Coordinator,
	inner scheduler.FeedProvider,
	self string,
	instr telemetry.Instruments,
) (scheduler.FeedProvider, *scheduler.OwnerProvider, error) {
	a := cfg.Coordination.Assignment
	if !a.Enabled {
		return inner, nil, nil
	}

	mp, ok := cd.(coord.MembershipProvider)
	if !ok {
		return nil, nil, fmt.Errorf(
			"coordination.assignment.enabled but driver %q does not support membership",
			cfg.Coordination.Driver,
		)
	}

	m, err := mp.Membership(self)
	if err != nil {
		return nil, nil, fmt.Errorf("build membership: %w", err)
	}

	onRebalance := func(members, owned int) {
		ctx := context.Background()
		if instr.MembershipSize != nil {
			instr.MembershipSize.Record(ctx, int64(members))
		}
		if instr.AssignedFeeds != nil {
			instr.AssignedFeeds.Record(ctx, int64(owned))
		}
		if instr.RebalanceEvents != nil {
			instr.RebalanceEvents.Add(ctx, 1)
		}
	}

	op := scheduler.NewOwnerProvider(inner, m, self, a.HeartbeatInterval, onRebalance)
	return op, op, nil
}
