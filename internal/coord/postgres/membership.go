package postgres

import (
	"context"
	"fmt"

	"github.com/iambod/rss2msg/internal/coord"
)

// Compile-time assertion: *Coordinator implements coord.MembershipProvider.
var _ coord.MembershipProvider = (*Coordinator)(nil)

const createMembersTable = `
CREATE TABLE IF NOT EXISTS coordination_members (
    id        text PRIMARY KEY,
    last_seen timestamptz NOT NULL
)`

// Membership returns a Postgres-backed Membership reusing this coordinator's
// pool. Liveness is judged by last_seen relative to member_ttl (coordinator
// clock), so instance clock skew is irrelevant. The coordination_members table
// is created on first call if it does not already exist.
func (c *Coordinator) Membership(self string) (coord.Membership, error) {
	if _, err := c.pool.Exec(context.Background(), createMembersTable); err != nil {
		return nil, fmt.Errorf("coord/postgres: create coordination_members: %w", err)
	}
	return &pgMembership{c: c, self: self}, nil
}

type pgMembership struct {
	c    *Coordinator
	self string
}

// Heartbeat upserts this instance's heartbeat row, reaps stale members (those
// whose last_seen is older than member_ttl), then returns the current live
// member IDs (including self). Two separate calls are used because pgx's
// extended-protocol Query rejects multi-statement strings.
func (m *pgMembership) Heartbeat(ctx context.Context) ([]string, error) {
	// Upsert this instance's heartbeat row.
	_, err := m.c.pool.Exec(ctx,
		`INSERT INTO coordination_members (id, last_seen) VALUES ($1, now())
		 ON CONFLICT (id) DO UPDATE SET last_seen = now()`,
		m.self,
	)
	if err != nil {
		return nil, fmt.Errorf("coord/postgres: heartbeat upsert: %w", err)
	}

	// Interval string accepted by Postgres: e.g. "30000 milliseconds".
	ttlInterval := fmt.Sprintf("%d milliseconds", m.c.memberTTL.Milliseconds())

	// Reap stale members opportunistically (best-effort; ignore errors).
	_, _ = m.c.pool.Exec(ctx,
		`DELETE FROM coordination_members WHERE last_seen < now() - $1::interval`,
		ttlInterval,
	)

	// Read the current live set.
	rows, err := m.c.pool.Query(ctx,
		`SELECT id FROM coordination_members WHERE last_seen > now() - $1::interval`,
		ttlInterval,
	)
	if err != nil {
		return nil, fmt.Errorf("coord/postgres: heartbeat select: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("coord/postgres: heartbeat scan: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Deregister removes this instance's member row immediately so peers can
// reassign its feeds without waiting for the TTL to expire.
func (m *pgMembership) Deregister(ctx context.Context) error {
	_, err := m.c.pool.Exec(ctx,
		`DELETE FROM coordination_members WHERE id = $1`, m.self,
	)
	return err
}

// Close is a no-op: the Coordinator owns the underlying pool.
func (m *pgMembership) Close() error { return nil }

