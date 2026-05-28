//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/iambod/rss2msg/internal/model"
	sinkpg "github.com/iambod/rss2msg/internal/sink/postgres"
)

func setup(t *testing.T) (*sinkpg.Publisher, *pgxpool.Pool) {
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
	pub, err := sinkpg.New(ctx, sinkpg.Options{Name: "test", DSN: dsn, Table: "feed_changes"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pub, pool
}

func TestPublishInsertsRow(t *testing.T) {
	pub, pool := setup(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew,
		Title: "hi", ContentHash: "h", DetectedAt: now,
	}
	if err := pub.Publish(ctx, c); err != nil {
		t.Fatal(err)
	}
	var (
		kind    string
		payload []byte
	)
	if err := pool.QueryRow(ctx, `SELECT kind, payload FROM feed_changes WHERE feed_url=$1 AND item_id=$2`, "f1", "i1").Scan(&kind, &payload); err != nil {
		t.Fatal(err)
	}
	if kind != "new" {
		t.Fatalf("kind=%q", kind)
	}
	var round model.Change
	if err := json.Unmarshal(payload, &round); err != nil {
		t.Fatal(err)
	}
	if round.Title != "hi" || round.ItemID != "i1" {
		t.Fatalf("round trip mismatch: %+v", round)
	}
}

func TestPublishIsIdempotentWithinSameDetectedAt(t *testing.T) {
	pub, pool := setup(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew,
		ContentHash: "h", DetectedAt: now,
	}
	for i := 0; i < 3; i++ {
		if err := pub.Publish(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM feed_changes WHERE feed_url=$1 AND item_id=$2`, "f1", "i1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row after idempotent retries, got %d", n)
	}
}
