//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/iambod/rss2msg/internal/feedsource/sources/postgres"
)

func setupPGSource(t *testing.T, schema, query string, opts *postgres.PostgresOptions) (*postgres.Postgres, string) {
	t.Helper()
	ctx := context.Background()

	pgC, err := tcpg.Run(ctx, "postgres:16-alpine",
		tcpg.WithDatabase("rss2msg"),
		tcpg.WithUsername("test"),
		tcpg.WithPassword("test"),
		tcpg.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })

	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		t.Fatalf("seed schema: %v", err)
	}
	pool.Close()

	o := postgres.PostgresOptions{Name: "db", DSN: dsn, Query: query}
	if opts != nil {
		o = *opts
		o.DSN = dsn
	}
	src, err := postgres.NewPostgres(ctx, o)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src, dsn
}

func TestPostgresSourceReadsTextArrayTable(t *testing.T) {
	schema := `
CREATE TABLE feeds (
  url      TEXT NOT NULL,
  interval TEXT,
  sinks    TEXT[],
  note     TEXT
);
INSERT INTO feeds (url, interval, sinks, note) VALUES
  ('https://a.example/feed', '15m', ARRAY['x','y'], 'ignored'),
  ('https://b.example/feed', NULL, NULL, NULL);
`
	// Default table query path (SELECT * FROM feeds).
	src, _ := setupPGSource(t, schema, "", &postgres.PostgresOptions{Name: "db", Table: "feeds"})

	feeds, err := src.Feeds(context.Background())
	if err != nil {
		t.Fatalf("Feeds: %v", err)
	}
	if len(feeds) != 2 {
		t.Fatalf("expected 2 feeds, got %d: %+v", len(feeds), feeds)
	}

	byURL := map[string]int{}
	for i, f := range feeds {
		byURL[f.URL] = i
	}
	a, ok := byURL["https://a.example/feed"]
	if !ok {
		t.Fatal("feed a missing")
	}
	if feeds[a].Interval != 15*time.Minute {
		t.Errorf("interval: got %v want 15m", feeds[a].Interval)
	}
	if len(feeds[a].Sinks) != 2 || feeds[a].Sinks[0] != "x" || feeds[a].Sinks[1] != "y" {
		t.Errorf("sinks: got %v want [x y]", feeds[a].Sinks)
	}

	b := byURL["https://b.example/feed"]
	if feeds[b].Interval != 0 || len(feeds[b].Sinks) != 0 {
		t.Errorf("feed b should have zero interval/sinks, got %+v", feeds[b])
	}
}

func TestPostgresSourceCustomQuery(t *testing.T) {
	schema := `
CREATE TABLE catalog (
  link    TEXT NOT NULL,
  enabled BOOLEAN NOT NULL
);
INSERT INTO catalog (link, enabled) VALUES
  ('https://on.example/feed', true),
  ('https://off.example/feed', false);
`
	src, _ := setupPGSource(t, schema, "SELECT link AS url FROM catalog WHERE enabled", nil)

	feeds, err := src.Feeds(context.Background())
	if err != nil {
		t.Fatalf("Feeds: %v", err)
	}
	if len(feeds) != 1 || feeds[0].URL != "https://on.example/feed" {
		t.Fatalf("expected only the enabled feed, got %+v", feeds)
	}
}

func TestPostgresSourceQueryErrorPropagates(t *testing.T) {
	schema := `CREATE TABLE t (x INT);`
	src, _ := setupPGSource(t, schema, "SELECT * FROM does_not_exist", nil)
	if _, err := src.Feeds(context.Background()); err == nil {
		t.Fatal("expected error querying a missing table")
	}
}
