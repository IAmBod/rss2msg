// Package sqlite provides a Store backed by SQLite via the pure-Go
// modernc.org/sqlite driver (no CGO). Suitable for single-instance
// deployments and local development; for multi-writer or networked
// deployments use the Postgres store instead.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // register the pure-Go "sqlite" database/sql driver

	"github.com/iambod/rss2msg/internal/state"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS seen_items (
    feed_url     TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (feed_url, item_id)
);

CREATE TABLE IF NOT EXISTS feed_meta (
    feed_url      TEXT PRIMARY KEY,
    etag          TEXT NOT NULL DEFAULT '',
    last_modified TEXT,
    updated_at    TEXT NOT NULL
);
`

// pragmas applied at open. WAL gives better concurrency; busy_timeout=5000
// ms makes concurrent writers wait briefly instead of returning SQLITE_BUSY.
const initPragmasSQL = `
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = NORMAL;
`

type Store struct {
	db *sql.DB
}

// New opens the database at path and ensures the schema is present. The
// path is passed verbatim to modernc.org/sqlite, so query parameters like
// `?_pragma=...` and the special `:memory:` form are supported.
func New(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("state/sqlite: open: %w", err)
	}
	// SQLite serialises writes; a single connection avoids the database-
	// locked surprises that ad-hoc pools cause for in-process use.
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, initPragmasSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("state/sqlite: pragmas: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("state/sqlite: apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error                   { return s.db.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) GetItem(ctx context.Context, feedURL, itemID string) (state.ItemState, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT content_hash, last_seen_at FROM seen_items WHERE feed_url=? AND item_id=?`,
		feedURL, itemID)
	var hash, seenAt string
	if err := row.Scan(&hash, &seenAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return state.ItemState{}, false, nil
		}
		return state.ItemState{}, false, err
	}
	t, err := time.Parse(time.RFC3339Nano, seenAt)
	if err != nil {
		return state.ItemState{}, false, fmt.Errorf("state/sqlite: parse last_seen_at: %w", err)
	}
	return state.ItemState{ContentHash: hash, LastSeenAt: t}, true, nil
}

func (s *Store) UpsertItem(ctx context.Context, feedURL, itemID, hash string, seenAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO seen_items (feed_url, item_id, content_hash, last_seen_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (feed_url, item_id) DO UPDATE
        SET content_hash = excluded.content_hash,
            last_seen_at = excluded.last_seen_at
    `, feedURL, itemID, hash, seenAt.UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) GetFeedMeta(ctx context.Context, feedURL string) (state.FeedMeta, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT etag, last_modified FROM feed_meta WHERE feed_url=?`, feedURL)
	var etag string
	var lm sql.NullString
	if err := row.Scan(&etag, &lm); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return state.FeedMeta{}, false, nil
		}
		return state.FeedMeta{}, false, err
	}
	out := state.FeedMeta{ETag: etag}
	if lm.Valid && lm.String != "" {
		t, err := time.Parse(time.RFC3339Nano, lm.String)
		if err != nil {
			return state.FeedMeta{}, false, fmt.Errorf("state/sqlite: parse last_modified: %w", err)
		}
		out.LastModified = t
	}
	return out, true, nil
}

func (s *Store) UpsertFeedMeta(ctx context.Context, feedURL string, meta state.FeedMeta) error {
	var lm any
	if !meta.LastModified.IsZero() {
		lm = meta.LastModified.UTC().Format(time.RFC3339Nano)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO feed_meta (feed_url, etag, last_modified, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (feed_url) DO UPDATE
        SET etag = excluded.etag,
            last_modified = excluded.last_modified,
            updated_at = excluded.updated_at
    `, feedURL, meta.ETag, lm, now)
	return err
}
