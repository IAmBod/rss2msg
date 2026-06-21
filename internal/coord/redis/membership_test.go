//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	coordredis "github.com/iambod/rss2msg/internal/coord/redis"
)

func TestRedisMembershipRegistersAndDeregisters(t *testing.T) {
	url := newRedisURL(t) // defined in redis_test.go (same package, same build tag)
	ctx := context.Background()

	c1, err := coordredis.New(ctx, coordredis.Options{URL: url, MemberTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := coordredis.New(ctx, coordredis.Options{URL: url, MemberTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	m1, _ := c1.Membership("inst-1")
	m2, _ := c2.Membership("inst-2")

	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(got, "inst-1") || !contains(got, "inst-2") {
		t.Fatalf("expected both members live, got %v", got)
	}

	// Graceful deregister drops inst-1 immediately.
	if err := m1.Deregister(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = m2.Heartbeat(ctx)
	if contains(got, "inst-1") {
		t.Fatalf("inst-1 should be gone after deregister, got %v", got)
	}

	// TTL expiry drops a crashed member (stop heartbeating inst-2; wait past TTL).
	time.Sleep(3 * time.Second)
	got, _ = m1.Heartbeat(ctx) // re-registers inst-1
	if contains(got, "inst-2") {
		t.Fatalf("inst-2 should have expired via TTL, got %v", got)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
