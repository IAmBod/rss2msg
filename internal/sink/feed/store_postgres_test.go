//go:build integration

package feed

import (
	"context"
	"testing"
	"time"

	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startPostgresForTest(t *testing.T) string {
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
	return dsn
}

func TestPostgresStore_UpsertOrderRetention(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgresForTest(t) // bootstrap a throwaway postgres; mirror internal/state/postgres
	s, err := newPostgresStore(ctx, postgresOptions{DSN: dsn, Table: "feed_output", Max: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Unix(3000, 0).UTC()
	for i, id := range []string{"a", "b", "c"} {
		if err := s.Write(ctx, chg("f", id, base.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ { // exceed the prune threshold (16) to force retention
		if err := s.Write(ctx, chg("f", "c", base.Add(time.Duration(20+i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Recent(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) > 2 || got[0].ItemID != "c" {
		t.Fatalf("want newest c first and <=2 rows, got %+v", got)
	}
}
