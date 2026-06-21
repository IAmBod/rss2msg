//go:build integration

package cosmosdb

import (
	"context"
	"testing"
	"time"
)

// TestCosmosMembershipLiveSet exercises the full membership lifecycle against
// a live Cosmos DB emulator: two instances register, enumerate the live set,
// then deregister one and verify the set shrinks.
func TestCosmosMembershipLiveSet(t *testing.T) {
	ctx := context.Background()
	connStr, clientOpts := setup(t)

	c, err := New(ctx, Options{
		ConnectionString: connStr,
		Database:         "rss2msg",
		Container:        "coordination_locks",
		CreateIfMissing:  true,
		Throughput:       400,
		LeaseDuration:    30 * time.Second,
		MemberTTL:        10 * time.Second,
		ClientOptions:    clientOpts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()

	m1, err := c.Membership("inst-1")
	if err != nil {
		t.Fatalf("Membership inst-1: %v", err)
	}
	defer func() { _ = m1.Close() }()

	m2, err := c.Membership("inst-2")
	if err != nil {
		t.Fatalf("Membership inst-2: %v", err)
	}
	defer func() { _ = m2.Close() }()

	// Register inst-1.
	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatalf("m1.Heartbeat: %v", err)
	}

	// Register inst-2 and enumerate — both should appear.
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatalf("m2.Heartbeat: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 members after both heartbeat, got %v", got)
	}

	// Deregister inst-1; the next heartbeat from inst-2 should return only inst-2.
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
}
