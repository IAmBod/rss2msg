//go:build integration

package redis_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	coordredis "github.com/iambod/rss2msg/internal/coord/redis"
)

func lockKeyFor(feedURL string) string {
	sum := sha256.Sum256([]byte(feedURL))
	return "rss2msg:coord:" + hex.EncodeToString(sum[:])
}

func newRedisURL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	rC, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rC.Terminate(ctx) })
	url, err := rC.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return url
}

func newCoord(t *testing.T, url string, ttl, renew time.Duration) *coordredis.Coordinator {
	t.Helper()
	c, err := coordredis.New(context.Background(), coordredis.Options{
		URL:             url,
		LockTTL:         ttl,
		RenewalInterval: renew,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestTwoCoordinatorsRaceForSameFeed(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 30*time.Second, 10*time.Second)
	b := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	relA, okA, err := a.TryAcquire(ctx, "https://e/feed-x")
	if err != nil || !okA {
		t.Fatalf("A acquire: ok=%v err=%v", okA, err)
	}
	defer relA(ctx)

	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-x")
	if err != nil {
		t.Fatalf("B acquire err: %v", err)
	}
	if okB {
		_ = relB(ctx)
		t.Fatalf("B should not have acquired while A holds")
	}
}

func TestReleaseAllowsAcquireAgain(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	rel, ok, err := a.TryAcquire(ctx, "https://e/feed-y")
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	if err := rel(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	rel2, ok, err := a.TryAcquire(ctx, "https://e/feed-y")
	if err != nil || !ok {
		t.Fatalf("re-acquire: ok=%v err=%v", ok, err)
	}
	_ = rel2(ctx)
}

func TestHeldLeaseSurvivesPastInitialTTL(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 1*time.Second, 300*time.Millisecond)
	b := newCoord(t, url, 1*time.Second, 300*time.Millisecond)
	ctx := context.Background()

	relA, ok, err := a.TryAcquire(ctx, "https://e/feed-z")
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}

	// Sleep past the initial TTL; renewal should keep the lease alive.
	time.Sleep(2 * time.Second)

	rB, okB, err := b.TryAcquire(ctx, "https://e/feed-z")
	if err != nil {
		t.Fatalf("B acquire err: %v", err)
	}
	if okB {
		_ = rB(ctx)
		t.Fatalf("B should still be locked out after renewal")
	}

	if err := relA(ctx); err != nil {
		t.Fatalf("A release: %v", err)
	}

	// Give a moment, then B should succeed.
	rB2, okB2, err := b.TryAcquire(ctx, "https://e/feed-z")
	if err != nil || !okB2 {
		t.Fatalf("B acquire after A release: ok=%v err=%v", okB2, err)
	}
	_ = rB2(ctx)
}

func TestLockLostExternallyMakesReleaseNoOp(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 1*time.Second, 300*time.Millisecond)
	b := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	relA, ok, err := a.TryAcquire(ctx, "https://e/feed-lost")
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}

	// Simulate external eviction: bypass the coordinator and DEL the key.
	cfg, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	side := goredis.NewClient(cfg)
	defer side.Close()
	if n, err := side.Del(ctx, lockKeyFor("https://e/feed-lost")).Result(); err != nil || n != 1 {
		t.Fatalf("DEL setup: n=%d err=%v", n, err)
	}

	// B now acquires (CAS-free SET NX succeeds because key is gone).
	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-lost")
	if err != nil || !okB {
		t.Fatalf("B acquire after eviction: ok=%v err=%v", okB, err)
	}

	// Sleep two renewal intervals so A's renewal goroutine has noticed the
	// CAS-zero and exited.
	time.Sleep(700 * time.Millisecond)

	// A's release must be a no-op and must not delete B's lease.
	if err := relA(ctx); err != nil {
		t.Fatalf("A release should be nil, got %v", err)
	}
	if err := relB(ctx); err != nil {
		t.Fatalf("B release: %v", err)
	}
}

func TestCoordinatorCloseReleasesHeldLeases(t *testing.T) {
	url := newRedisURL(t)
	a, err := coordredis.New(context.Background(), coordredis.Options{
		URL:             url,
		LockTTL:         30 * time.Second,
		RenewalInterval: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	b := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	_, ok, err := a.TryAcquire(ctx, "https://e/feed-close")
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("A close: %v", err)
	}

	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-close")
	if err != nil || !okB {
		t.Fatalf("B acquire after A.Close: ok=%v err=%v", okB, err)
	}
	_ = relB(ctx)
}

func TestCanceledReleaseCtxStillFreesLease(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 30*time.Second, 10*time.Second)
	b := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	rel, ok, err := a.TryAcquire(ctx, "https://e/feed-cancel")
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}

	doomed, cancel := context.WithCancel(ctx)
	cancel()
	if err := rel(doomed); err != nil {
		t.Fatalf("release on canceled ctx: %v", err)
	}

	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-cancel")
	if err != nil || !okB {
		t.Fatalf("B acquire after canceled release: ok=%v err=%v", okB, err)
	}
	_ = relB(ctx)
}
