// Package postgres provides a Coordinator backed by pg_try_advisory_lock,
// keyed by the first 8 bytes of sha256(feed_url). Locks are session-scoped:
// they die with the connection, so crashed instances release their leases
// automatically.
package postgres

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/coord"
)

// Options configures the Postgres-backed Coordinator.
type Options struct {
	DSN      string // required; pgx-style URL or keyword DSN
	MinConns int    // raises pgxpool.MaxConns to fit fan-out; 0 leaves pgxpool defaults

	// TLS, if non-nil, overrides whatever TLS config the DSN's sslmode
	// produced. Forces TLS by also clearing pgx fallbacks (so plaintext is
	// never attempted).
	TLS *TLSOptions
}

// TLSOptions configures custom TLS for the coordinator's Postgres pool. Same
// shape as the redis coordinator's options so operators have a consistent
// surface.
type TLSOptions struct {
	// CAFile is a PEM-encoded CA bundle to trust instead of the system roots.
	CAFile string
	// CertFile and KeyFile together enable client-certificate (mTLS) auth.
	// Either both or neither must be set.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / certificate verification hostname.
	// Defaults to the DSN host.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. For
	// local/test only — logged at warn.
	InsecureSkipVerify bool
}

type Coordinator struct {
	pool *pgxpool.Pool

	mu   sync.Mutex
	held map[*pgxpool.Conn]struct{}
}

// New opens a pool against opts.DSN.
func New(ctx context.Context, opts Options) (*Coordinator, error) {
	cfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("coord/postgres: parse dsn: %w", err)
	}
	if opts.MinConns > 0 {
		// MaxConns is what callers actually pull from; raise it to fit fan-out.
		if int32(opts.MinConns) > cfg.MaxConns {
			cfg.MaxConns = int32(opts.MinConns)
		}
	}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS, cfg.ConnConfig.Host)
		if err != nil {
			return nil, fmt.Errorf("coord/postgres: build TLS config: %w", err)
		}
		cfg.ConnConfig.TLSConfig = tc
		// Drop any plaintext fallbacks pgx may have set up from the DSN's
		// sslmode — the operator opted into TLS knobs, so plaintext must
		// never be attempted.
		cfg.ConnConfig.Fallbacks = nil
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("coord_driver", "postgres").
				Msg("coord/postgres: TLS verification disabled (insecure_skip_verify=true)")
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

// buildTLSConfig translates TLSOptions into a *tls.Config. defaultServerName
// is the host parsed out of the DSN; used as SNI when the caller did not
// override it.
func buildTLSConfig(opts TLSOptions, defaultServerName string) (*tls.Config, error) {
	tc := &tls.Config{
		ServerName:         defaultServerName,
		InsecureSkipVerify: opts.InsecureSkipVerify,
	}
	if opts.ServerName != "" {
		tc.ServerName = opts.ServerName
	}
	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %q: no PEM certificates parsed", opts.CAFile)
		}
		tc.RootCAs = pool
	}
	if opts.CertFile != "" || opts.KeyFile != "" {
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, fmt.Errorf("cert_file and key_file must both be set or both empty")
		}
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("client cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
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

	release := func(_ context.Context) error {
		// We intentionally ignore the caller's context. The pipeline defers
		// release on the poll ctx, which is canceled on SIGTERM or per-feed
		// timeout. Running the unlock on a canceled ctx would error out and
		// leak the advisory lock in the session, which is then returned to
		// the pool. Use a fresh bounded ctx instead.
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

		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, key); err != nil {
			// Unlock failed — the conn may still hold the lock. Don't return
			// it to the pool; hijack and close so the Postgres session dies
			// and the lock dies with it (same teardown pattern as Close()).
			raw := conn.Hijack()
			_ = raw.Close(context.Background())
			return fmt.Errorf("coord/postgres: pg_advisory_unlock: %w", err)
		}
		conn.Release()
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
