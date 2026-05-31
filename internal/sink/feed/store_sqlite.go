package feed

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"

	_ "modernc.org/sqlite"

	"github.com/iambod/rss2msg/internal/model"
)

// sqlitePruneEvery: prune at most once per N writes (package var so tests can lower it).
var sqlitePruneEvery int64 = 16

// sqliteTimeFormat is a fixed-width RFC3339 variant with a constant 9-digit
// fractional part. detected_at is stored as TEXT and ordered lexically, so the
// width must be constant — time.RFC3339Nano trims trailing zeros, which makes
// "…05.3Z" sort after "…05.35Z" (wrong). Fixed width keeps lexical == chrono.
const sqliteTimeFormat = "2006-01-02T15:04:05.000000000Z07:00"

type sqliteStore struct {
	db     *sql.DB
	table  string
	max    int
	writes atomic.Int64
}

func newSQLiteStore(ctx context.Context, path, table string, max int) (*sqliteStore, error) {
	if table == "" {
		table = "feed_output"
	}
	if !validIdentifier(table) {
		return nil, fmt.Errorf("feed sqlite: invalid table %q", table)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("feed sqlite: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	pragmas := `PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA synchronous=NORMAL;`
	if _, err := db.ExecContext(ctx, pragmas); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("feed sqlite: pragmas: %w", err)
	}
	schema := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  feed_url TEXT NOT NULL,
  item_id TEXT NOT NULL,
  detected_at TEXT NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (feed_url, item_id)
);
CREATE INDEX IF NOT EXISTS %s_detected_at ON %s (detected_at DESC);`, table, table, table)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("feed sqlite: schema: %w", err)
	}
	return &sqliteStore{db: db, table: table, max: max}, nil
}

func (s *sqliteStore) Write(ctx context.Context, c model.Change) error {
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	stmt := fmt.Sprintf(`INSERT INTO %s (feed_url, item_id, detected_at, payload) VALUES (?,?,?,?)
		ON CONFLICT(feed_url, item_id) DO UPDATE SET detected_at=excluded.detected_at, payload=excluded.payload`, s.table)
	if _, err := s.db.ExecContext(ctx, stmt, c.FeedURL, c.ItemID, c.DetectedAt.UTC().Format(sqliteTimeFormat), string(payload)); err != nil {
		return err
	}
	if s.writes.Add(1)%sqlitePruneEvery == 0 {
		_ = s.prune(ctx)
	}
	return nil
}

func (s *sqliteStore) prune(ctx context.Context) error {
	stmt := fmt.Sprintf(`DELETE FROM %s WHERE (feed_url, item_id) NOT IN (
		SELECT feed_url, item_id FROM %s ORDER BY detected_at DESC LIMIT %d)`, s.table, s.table, s.max)
	_, err := s.db.ExecContext(ctx, stmt)
	return err
}

func (s *sqliteStore) Recent(ctx context.Context, n int) ([]model.Change, error) {
	stmt := fmt.Sprintf(`SELECT payload FROM %s ORDER BY detected_at DESC LIMIT ?`, s.table)
	rows, err := s.db.QueryContext(ctx, stmt, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Change
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var c model.Change
		if err := json.Unmarshal([]byte(payload), &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *sqliteStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s *sqliteStore) Close() error                   { return s.db.Close() }
