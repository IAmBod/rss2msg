//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	coordpg "github.com/iambod/rss2msg/internal/coord/postgres"
)

func TestPostgresMembershipLiveSetAndExpiry(t *testing.T) {
	dsn := newDSN(t)
	ctx := context.Background()

	c, err := coordpg.New(ctx, coordpg.Options{DSN: dsn, MemberTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	m1, err := c.Membership("inst-1")
	if err != nil {
		t.Fatal(err)
	}
	m2, err := c.Membership("inst-2")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 live members, got %v", got)
	}

	if err := m1.Deregister(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = m2.Heartbeat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "inst-2" {
		t.Fatalf("expected only inst-2 after deregister, got %v", got)
	}
}
