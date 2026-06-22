package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/state"
)

// Options configures the Postgres-backed state Store.
type Options struct {
	DSN string // required; pgx-style URL or keyword DSN

	// TLS, if non-nil, overrides whatever TLS config the DSN's sslmode
	// produced. Forces TLS by clearing pgx fallbacks (so plaintext is
	// never silently attempted).
	TLS *TLSOptions
}

// TLSOptions configures custom TLS for the state pool. Field shape mirrors
// the coord/postgres and coord/redis packages so operators have a consistent
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

const schemaSQL = `
CREATE TABLE IF NOT EXISTS seen_items (
    feed_url TEXT NOT NULL,
    item_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (feed_url, item_id)
);

CREATE TABLE IF NOT EXISTS feed_meta (
    feed_url TEXT PRIMARY KEY,
    etag TEXT NOT NULL DEFAULT '',
    last_modified TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);
`

type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool against opts.DSN and ensures the schema is
// present.
func New(ctx context.Context, opts Options) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("state/postgres: parse dsn: %w", err)
	}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS, cfg.ConnConfig.Host)
		if err != nil {
			return nil, fmt.Errorf("state/postgres: build TLS config: %w", err)
		}
		cfg.ConnConfig.TLSConfig = tc
		// Drop plaintext fallbacks pgx may have set up from the DSN's
		// sslmode — the operator opted into TLS knobs, so plaintext must
		// never be attempted.
		cfg.ConnConfig.Fallbacks = nil
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("component", "state/postgres").
				Msg("state/postgres: TLS verification disabled (insecure_skip_verify=true)")
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("state/postgres: pgxpool: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// buildTLSConfig translates TLSOptions into a *tls.Config. defaultServerName
// is the host parsed out of the DSN; used as SNI when the caller did not
// override it.
//
// Logic identical to internal/coord/postgres.buildTLSConfig — kept as a
// local copy until a fourth pgx pool wants the same surface, at which
// point it's worth extracting to a shared internal/pgtls package.
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

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

func (s *Store) Close() error                   { s.pool.Close(); return nil }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) GetItem(ctx context.Context, feedURL, itemID string) (state.ItemState, bool, error) {
	row := s.pool.QueryRow(ctx, `SELECT content_hash, last_seen_at FROM seen_items WHERE feed_url=$1 AND item_id=$2`, feedURL, itemID)
	var out state.ItemState
	if err := row.Scan(&out.ContentHash, &out.LastSeenAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return state.ItemState{}, false, nil
		}
		return state.ItemState{}, false, err
	}
	return out, true, nil
}

func (s *Store) UpsertItem(ctx context.Context, feedURL, itemID, hash string, seenAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
        INSERT INTO seen_items (feed_url, item_id, content_hash, last_seen_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (feed_url, item_id) DO UPDATE
        SET content_hash = EXCLUDED.content_hash,
            last_seen_at = EXCLUDED.last_seen_at
    `, feedURL, itemID, hash, seenAt)
	return err
}

// PruneItemsBefore deletes seen_items whose last_seen_at is older than cutoff
// and returns the number of rows removed. feed_meta is never touched.
func (s *Store) PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM seen_items WHERE last_seen_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("state/postgres: prune: %w", err)
	}
	return tag.RowsAffected(), nil
}

// PruneFeedMetaBefore deletes feed_meta rows whose updated_at is older than
// cutoff and returns the number of rows removed. seen_items is not touched.
func (s *Store) PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM feed_meta WHERE updated_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("state/postgres: prune feed_meta: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (s *Store) GetFeedMeta(ctx context.Context, feedURL string) (state.FeedMeta, bool, error) {
	row := s.pool.QueryRow(ctx, `SELECT etag, last_modified FROM feed_meta WHERE feed_url=$1`, feedURL)
	var out state.FeedMeta
	var lm *time.Time
	if err := row.Scan(&out.ETag, &lm); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return state.FeedMeta{}, false, nil
		}
		return state.FeedMeta{}, false, err
	}
	if lm != nil {
		out.LastModified = *lm
	}
	return out, true, nil
}

func (s *Store) UpsertFeedMeta(ctx context.Context, feedURL string, meta state.FeedMeta) error {
	var lm *time.Time
	if !meta.LastModified.IsZero() {
		lm = &meta.LastModified
	}
	_, err := s.pool.Exec(ctx, `
        INSERT INTO feed_meta (feed_url, etag, last_modified, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (feed_url) DO UPDATE
        SET etag = EXCLUDED.etag,
            last_modified = EXCLUDED.last_modified,
            updated_at = NOW()
    `, feedURL, meta.ETag, lm)
	return err
}
