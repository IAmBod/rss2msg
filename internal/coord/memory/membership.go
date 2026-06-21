package memory

import (
	"context"

	"github.com/iambod/rss2msg/internal/coord"
)

// Membership implements coord.MembershipProvider for the in-process coordinator:
// the fleet is always exactly this one instance, so it owns every feed.
func (Coordinator) Membership(self string) (coord.Membership, error) {
	return staticMembership{self: self}, nil
}

type staticMembership struct{ self string }

func (m staticMembership) Heartbeat(context.Context) ([]string, error) { return []string{m.self}, nil }
func (staticMembership) Deregister(context.Context) error              { return nil }
func (staticMembership) Close() error                                  { return nil }
