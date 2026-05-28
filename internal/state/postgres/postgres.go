package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/iambod/rss2msg/internal/state"
)

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

// New opens a connection pool and ensures schema is present.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
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
