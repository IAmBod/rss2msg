package redis

import (
	"context"
	"strings"

	"github.com/iambod/rss2msg/internal/coord"
)

var _ coord.MembershipProvider = (*Coordinator)(nil)

func memberKeyPrefix() string      { return "rss2msg:coord:member:" }
func memberKey(self string) string { return memberKeyPrefix() + self }

// Membership returns a Redis-backed Membership reusing this coordinator's client.
// Each member is a key with a TTL; the live set is a SCAN of the member prefix.
// Close is a no-op because the coordinator owns the underlying client.
func (c *Coordinator) Membership(self string) (coord.Membership, error) {
	return &redisMembership{c: c, self: self}, nil
}

type redisMembership struct {
	c    *Coordinator
	self string
}

// Heartbeat refreshes this instance's TTL key and returns all currently live
// member IDs (keys matching the member prefix, with the prefix stripped).
func (m *redisMembership) Heartbeat(ctx context.Context) ([]string, error) {
	if err := m.c.client.Set(ctx, memberKey(m.self), "1", m.c.opts.MemberTTL).Err(); err != nil {
		return nil, err
	}
	prefix := memberKeyPrefix()
	var ids []string
	iter := m.c.client.Scan(ctx, 0, prefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		ids = append(ids, strings.TrimPrefix(iter.Val(), prefix))
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// Deregister removes this instance's member key immediately so peers can
// reassign its feeds without waiting for the TTL to expire.
func (m *redisMembership) Deregister(ctx context.Context) error {
	return m.c.client.Del(ctx, memberKey(m.self)).Err()
}

// Close is a no-op: the Coordinator owns the underlying client.
func (m *redisMembership) Close() error { return nil }
