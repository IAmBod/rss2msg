//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	coordpg "github.com/iambod/rss2msg/internal/coord/postgres"
)

func newDSN(t *testing.T) string {
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

func TestTwoCoordinatorsRaceForSameFeed(t *testing.T) {
	dsn := newDSN(t)
	ctx := context.Background()

	a, err := coordpg.New(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := coordpg.New(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	rel, ok, err := a.TryAcquire(ctx, "https://e/feed-x")
	if err != nil || !ok {
		t.Fatalf("a: expected acquire ok, got %v %v", ok, err)
	}
	defer rel(ctx)

	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-x")
	if err != nil {
		t.Fatalf("b: unexpected err: %v", err)
	}
	if okB {
		_ = relB(ctx)
		t.Fatal("b: expected NOT to acquire while a holds the lock")
	}
}

func TestCoordinatorReleaseAllowsAcquireAgain(t *testing.T) {
	dsn := newDSN(t)
	ctx := context.Background()

	a, err := coordpg.New(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	rel, ok, err := a.TryAcquire(ctx, "https://e/feed-y")
	if err != nil || !ok {
		t.Fatalf("first acquire: %v %v", ok, err)
	}
	if err := rel(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	rel2, ok2, err := a.TryAcquire(ctx, "https://e/feed-y")
	if err != nil || !ok2 {
		t.Fatalf("re-acquire after release: %v %v", ok2, err)
	}
	_ = rel2(ctx)
}

func TestReleaseWithCanceledContextStillFreesLock(t *testing.T) {
	dsn := newDSN(t)
	ctx := context.Background()

	a, err := coordpg.New(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := coordpg.New(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	rel, ok, err := a.TryAcquire(ctx, "https://e/cancel-test")
	if err != nil || !ok {
		t.Fatalf("a acquire: %v %v", ok, err)
	}

	// Simulate the pipeline shutting down: call release with a pre-canceled ctx.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_ = rel(cctx) // error is acceptable, but the lock MUST be freed.

	// Give Postgres a moment.
	time.Sleep(200 * time.Millisecond)

	rel2, ok2, err := b.TryAcquire(ctx, "https://e/cancel-test")
	if err != nil {
		t.Fatalf("b acquire after canceled release: %v", err)
	}
	if !ok2 {
		t.Fatalf("b: expected to acquire after a's canceled release, but lock is still held")
	}
	_ = rel2(ctx)
}

func TestCoordinatorClosingReleasesLocks(t *testing.T) {
	dsn := newDSN(t)
	ctx := context.Background()

	a, err := coordpg.New(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := a.TryAcquire(ctx, "https://e/feed-z")
	if err != nil || !ok {
		t.Fatalf("a acquire: %v %v", ok, err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Allow Postgres a brief moment to notice the closed sessions.
	time.Sleep(200 * time.Millisecond)

	b, err := coordpg.New(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	rel, ok, err := b.TryAcquire(ctx, "https://e/feed-z")
	if err != nil || !ok {
		t.Fatalf("b acquire after a.Close: %v %v", ok, err)
	}
	_ = rel(ctx)
}
