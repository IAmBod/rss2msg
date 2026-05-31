//go:build integration

package redis_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

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

// startRedisSentinel brings up a single Redis master plus one Sentinel,
// both on a shared user-defined Docker network so the Sentinel can reach the
// master by container alias. It returns the master name ("mymaster") and the
// host:port the test should hand to coordredis as a Sentinel address.
//
// The single-node modules/redis helper can't model a Sentinel topology, so we
// drive raw GenericContainer requests here. As with the other helpers in this
// file the test is gated behind the `integration` build tag; on ANY container
// or network failure (most importantly: no Docker daemon) we Skipf so the
// suite stays green in environments without Docker and only runs for real in
// CI/Docker.
func startRedisSentinel(t *testing.T) (masterName string, sentinelAddrs []string) {
	t.Helper()
	ctx := context.Background()

	const (
		master = "mymaster"
		alias  = "redis-master"
	)

	net, err := network.New(ctx, network.WithAttachable())
	if err != nil {
		t.Skipf("skipping: cannot create docker network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(ctx) })

	// Master.
	masterC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			Networks:     []string{net.Name},
			NetworkAliases: map[string][]string{
				net.Name: {alias},
			},
			WaitingFor: wait.ForListeningPort("6379/tcp").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("skipping: cannot start redis master container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(masterC) })

	// Sentinel. The config is generated in-container via a small sh entrypoint
	// so the file is written next to where redis-sentinel reads it; this avoids
	// having to bake a file mount and keeps the helper hermetic.
	sentinelConf := strings.Join([]string{
		"port 26379",
		fmt.Sprintf("sentinel monitor %s %s 6379 1", master, alias),
		fmt.Sprintf("sentinel down-after-milliseconds %s 1000", master),
		fmt.Sprintf("sentinel failover-timeout %s 5000", master),
	}, "\\n")

	sentinelC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"26379/tcp"},
			Networks:     []string{net.Name},
			Entrypoint:   []string{"sh", "-c"},
			Cmd: []string{
				fmt.Sprintf("printf '%s\\n' > /etc/sentinel.conf && "+
					"redis-sentinel /etc/sentinel.conf", sentinelConf),
			},
			WaitingFor: wait.ForListeningPort("26379/tcp").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("skipping: cannot start redis sentinel container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(sentinelC) })

	host, err := sentinelC.Host(ctx)
	if err != nil {
		t.Skipf("skipping: cannot resolve sentinel host: %v", err)
	}
	port, err := sentinelC.MappedPort(ctx, "26379/tcp")
	if err != nil {
		t.Skipf("skipping: cannot resolve sentinel mapped port: %v", err)
	}

	return master, []string{fmt.Sprintf("%s:%s", host, port.Port())}
}

// TestCoordinator_Sentinel_AcquireRenewRelease exercises the full lease
// lifecycle (acquire, blocked second acquire, renewal-keeps-it-held across a
// TTL, release, re-acquire) against a real Sentinel-fronted master, proving
// the FailoverClient path in coordredis works end to end.
func TestCoordinator_Sentinel_AcquireRenewRelease(t *testing.T) {
	masterName, sentinelAddrs := startRedisSentinel(t)
	ctx := context.Background()

	const feed = "https://example.com/feed.xml"

	newSentinelCoord := func() *coordredis.Coordinator {
		c, err := coordredis.New(ctx, coordredis.Options{
			Mode:    "sentinel",
			LockTTL: 2 * time.Second, // comfortably exceeds one renewal (TTL/3)
			Sentinel: coordredis.SentinelOptions{
				MasterName: masterName,
				Addrs:      sentinelAddrs,
			},
		})
		if err != nil {
			t.Fatalf("sentinel coordinator New: %v", err)
		}
		return c
	}

	c := newSentinelCoord()
	t.Cleanup(func() { _ = c.Close() })

	rel, ok, err := c.TryAcquire(ctx, feed)
	if err != nil || !ok {
		t.Fatalf("c acquire: ok=%v err=%v", ok, err)
	}

	c2 := newSentinelCoord()
	t.Cleanup(func() { _ = c2.Close() })

	if _, ok2, _ := c2.TryAcquire(ctx, feed); ok2 {
		t.Fatal("c2 should not acquire while c holds the lease")
	}

	// Sleep past one TTL; the renewal goroutine must keep c's lease alive.
	time.Sleep(3 * time.Second)

	if _, ok2, _ := c2.TryAcquire(ctx, feed); ok2 {
		t.Fatal("c2 should still be locked out after renewal extended the lease")
	}

	if err := rel(ctx); err != nil {
		t.Fatalf("c release: %v", err)
	}

	rel2, ok2, err := c2.TryAcquire(ctx, feed)
	if err != nil || !ok2 {
		t.Fatalf("c2 acquire after c release: ok=%v err=%v", ok2, err)
	}
	if err := rel2(ctx); err != nil {
		t.Fatalf("c2 release: %v", err)
	}
}
