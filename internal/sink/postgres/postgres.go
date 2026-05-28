package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/iambod/rss2msg/internal/model"
)

type Publisher struct {
	name  string
	pool  *pgxpool.Pool
	table string
}

type Options struct {
	Name  string
	DSN   string
	Table string
}

const defaultTable = "feed_changes"

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("postgres sink: name is required")
	}
	if opts.DSN == "" {
		return nil, fmt.Errorf("postgres sink %q: dsn is required", opts.Name)
	}
	table := opts.Table
	if table == "" {
		table = defaultTable
	}
	if !validIdentifier(table) {
		return nil, fmt.Errorf("postgres sink %q: invalid table name %q", opts.Name, table)
	}
	pool, err := pgxpool.New(ctx, opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres sink %q: pgxpool: %w", opts.Name, err)
	}
	p := &Publisher{name: opts.Name, pool: pool, table: table}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error { p.pool.Close(); return nil }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	payload, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("postgres sink %q: marshal: %w", p.name, err)
	}
	// Table comes from validated config; validIdentifier guards the rest.
	stmt := fmt.Sprintf(`
        INSERT INTO %s (feed_url, item_id, kind, payload, detected_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (feed_url, item_id, detected_at) DO NOTHING
    `, p.table)
	_, err = p.pool.Exec(ctx, stmt, change.FeedURL, change.ItemID, string(change.Kind), payload, change.DetectedAt)
	return err
}

func (p *Publisher) migrate(ctx context.Context) error {
	stmt := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            feed_url TEXT NOT NULL,
            item_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload JSONB NOT NULL,
            detected_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (feed_url, item_id, detected_at)
        );`, p.table)
	_, err := p.pool.Exec(ctx, stmt)
	return err
}

func validIdentifier(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
