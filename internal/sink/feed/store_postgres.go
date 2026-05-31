package feed

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/iambod/rss2msg/internal/model"
)

var pgPruneEvery int64 = 16

type postgresOptions struct {
	DSN   string
	Table string
	Max   int
	TLS   *pgTLSOptions
}

type pgTLSOptions struct {
	CAFile, CertFile, KeyFile, ServerName string
	InsecureSkipVerify                    bool
}

type postgresStore struct {
	pool   *pgxpool.Pool
	table  string
	max    int
	writes atomic.Int64
}

func newPostgresStore(ctx context.Context, opts postgresOptions) (*postgresStore, error) {
	table := opts.Table
	if table == "" {
		table = "feed_output"
	}
	if !validIdentifier(table) {
		return nil, fmt.Errorf("feed postgres: invalid table %q", table)
	}
	cfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("feed postgres: parse dsn: %w", err)
	}
	if opts.TLS != nil {
		tc, err := buildTLS(opts.TLS)
		if err != nil {
			return nil, err
		}
		cfg.ConnConfig.TLSConfig = tc
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("feed postgres: pool: %w", err)
	}
	s := &postgresStore{pool: pool, table: table, max: opts.Max}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func buildTLS(o *pgTLSOptions) (*tls.Config, error) {
	c := &tls.Config{ServerName: o.ServerName, InsecureSkipVerify: o.InsecureSkipVerify} //nolint:gosec
	if o.CAFile != "" {
		pem, err := os.ReadFile(o.CAFile)
		if err != nil {
			return nil, fmt.Errorf("feed postgres: ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("feed postgres: ca_file has no certs")
		}
		c.RootCAs = pool
	}
	if o.CertFile != "" && o.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("feed postgres: client cert: %w", err)
		}
		c.Certificates = []tls.Certificate{cert}
	}
	return c, nil
}

func (s *postgresStore) migrate(ctx context.Context) error {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  feed_url TEXT NOT NULL,
  item_id TEXT NOT NULL,
  detected_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (feed_url, item_id)
);
CREATE INDEX IF NOT EXISTS %s_detected_at ON %s (detected_at DESC);`, s.table, s.table, s.table)
	_, err := s.pool.Exec(ctx, stmt)
	return err
}

func (s *postgresStore) Write(ctx context.Context, c model.Change) error {
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	stmt := fmt.Sprintf(`INSERT INTO %s (feed_url, item_id, detected_at, payload) VALUES ($1,$2,$3,$4)
		ON CONFLICT (feed_url, item_id) DO UPDATE SET detected_at=EXCLUDED.detected_at, payload=EXCLUDED.payload`, s.table)
	if _, err := s.pool.Exec(ctx, stmt, c.FeedURL, c.ItemID, c.DetectedAt, payload); err != nil {
		return err
	}
	if s.writes.Add(1)%pgPruneEvery == 0 {
		_ = s.prune(ctx)
	}
	return nil
}

func (s *postgresStore) prune(ctx context.Context) error {
	stmt := fmt.Sprintf(`DELETE FROM %s WHERE (feed_url, item_id) NOT IN (
		SELECT feed_url, item_id FROM %s ORDER BY detected_at DESC LIMIT %d)`, s.table, s.table, s.max)
	_, err := s.pool.Exec(ctx, stmt)
	return err
}

func (s *postgresStore) Recent(ctx context.Context, n int) ([]model.Change, error) {
	stmt := fmt.Sprintf(`SELECT payload FROM %s ORDER BY detected_at DESC LIMIT $1`, s.table)
	rows, err := s.pool.Query(ctx, stmt, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Change
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var c model.Change
		if err := json.Unmarshal(payload, &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *postgresStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *postgresStore) Close() error                   { s.pool.Close(); return nil }
