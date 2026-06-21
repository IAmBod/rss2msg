//go:build integration

package dynamodb_test

import (
	"context"
	"testing"
	"time"

	coordddb "github.com/iambod/rss2msg/internal/coord/dynamodb"
)

func TestDynamoMembership(t *testing.T) {
	endpoint, _ := setup(t)
	ctx := context.Background()

	c, err := coordddb.New(ctx, coordddb.Options{
		Table:       table,
		Region:      region,
		EndpointURL: endpoint,
		MemberTTL:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	m1, err := c.Membership("inst-1")
	if err != nil {
		t.Fatalf("Membership inst-1: %v", err)
	}
	m2, err := c.Membership("inst-2")
	if err != nil {
		t.Fatalf("Membership inst-2: %v", err)
	}

	// Both register via Heartbeat.
	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatalf("m1.Heartbeat: %v", err)
	}
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatalf("m2.Heartbeat: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 members, got %v", got)
	}

	// Deregister inst-1; only inst-2 remains.
	if err := m1.Deregister(ctx); err != nil {
		t.Fatalf("m1.Deregister: %v", err)
	}
	got, err = m2.Heartbeat(ctx)
	if err != nil {
		t.Fatalf("m2.Heartbeat after deregister: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 member after deregister, got %v", got)
	}
	if got[0] != "inst-2" {
		t.Fatalf("remaining member = %q, want inst-2", got[0])
	}

	// Close is a no-op.
	if err := m2.Close(); err != nil {
		t.Fatalf("m2.Close: %v", err)
	}
}
