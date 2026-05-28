// Package postgres provides a Coordinator backed by pg_try_advisory_lock,
// keyed by the first 8 bytes of sha256(feed_url). Locks are session-scoped:
// they die with the connection, so crashed instances release their leases
// automatically.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/iambod/rss2msg/internal/coord"
)

type Coordinator struct {
	pool *pgxpool.Pool

	mu   sync.Mutex
	held map[*pgxpool.Conn]struct{}
}

// New opens a pool against dsn. minConns lets the caller ensure the pool can
// service a fan-out poll cycle (e.g. minConns = len(feeds)). If minConns is
// zero, pgxpool defaults apply.
func New(ctx context.Context, dsn string, minConns int) (*Coordinator, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("coord/postgres: parse dsn: %w", err)
	}
	if minConns > 0 {
		// MaxConns is what callers actually pull from; raise it to fit fan-out.
		if int32(minConns) > cfg.MaxConns {
			cfg.MaxConns = int32(minConns)
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("coord/postgres: pgxpool: %w", err)
	}
	return &Coordinator{
		pool: pool,
		held: make(map[*pgxpool.Conn]struct{}),
	}, nil
}

// Close terminates the pool. Any still-held leases are hijacked off the pool
// and their underlying *pgx.Conn is closed, which kills the Postgres session
// and releases its advisory locks. This mirrors what happens to a crashed
// instance (the OS closes the TCP socket), so peer coordinators observe lock
// availability promptly.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	held := c.held
	c.held = nil
	c.mu.Unlock()

	for pc := range held {
		// Hijack returns the underlying *pgx.Conn and removes it from the pool
		// so pool.Close() will not block waiting for it.
		raw := pc.Hijack()
		// Best-effort close; we are tearing down anyway.
		_ = raw.Close(context.Background())
	}
	c.pool.Close()
	return nil
}

func (c *Coordinator) TryAcquire(ctx context.Context, feedURL string) (coord.ReleaseFunc, bool, error) {
	key := lockKey(feedURL)

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("coord/postgres: acquire conn: %w", err)
	}

	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("coord/postgres: pg_try_advisory_lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, false, nil
	}

	c.mu.Lock()
	if c.held == nil {
		// Coordinator is closing; abandon the lease.
		c.mu.Unlock()
		conn.Release()
		return nil, false, fmt.Errorf("coord/postgres: coordinator closed")
	}
	c.held[conn] = struct{}{}
	c.mu.Unlock()

	release := func(rctx context.Context) error {
		c.mu.Lock()
		if c.held == nil {
			// Coordinator was closed; conn was hijacked, nothing to do.
			c.mu.Unlock()
			return nil
		}
		if _, ok := c.held[conn]; !ok {
			c.mu.Unlock()
			return nil
		}
		delete(c.held, conn)
		c.mu.Unlock()

		defer conn.Release()
		_, err := conn.Exec(rctx, `SELECT pg_advisory_unlock($1)`, key)
		if err != nil {
			return fmt.Errorf("coord/postgres: pg_advisory_unlock: %w", err)
		}
		return nil
	}
	return release, true, nil
}

// lockKey returns an int64 derived from sha256(feedURL). Postgres advisory
// locks take a bigint; we accept the bottom 8 bytes of the hash.
func lockKey(feedURL string) int64 {
	sum := sha256.Sum256([]byte(feedURL))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}
