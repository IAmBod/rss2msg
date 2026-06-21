package dynamodb

import (
	"context"
	"testing"
	"time"
)

func TestMemberPKFormat(t *testing.T) {
	if got := memberPK("h-1-ab"); got != "member:h-1-ab" {
		t.Fatalf("memberPK = %q", got)
	}
	if got := memberPKPrefix(); got != "member:" {
		t.Fatalf("memberPKPrefix = %q", got)
	}
}

// TestMembershipHeartbeatAndDeregister exercises the full membership lifecycle
// against the in-memory fakeDDB, including the Scan-based live-set enumeration.
func TestMembershipHeartbeatAndDeregister(t *testing.T) {
	f := newFakeDDB()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	c1 := newWithClient(f, Options{Table: "locks", Owner: "owner-1", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	c1.now = func() time.Time { return base }
	c2 := newWithClient(f, Options{Table: "locks", Owner: "owner-2", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	c2.now = func() time.Time { return base }

	ctx := context.Background()

	m1, err := c1.Membership("inst-1")
	if err != nil {
		t.Fatalf("Membership: %v", err)
	}
	m2, err := c2.Membership("inst-2")
	if err != nil {
		t.Fatalf("Membership: %v", err)
	}

	// Both heartbeat — each upserts its member item and returns the live set.
	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatalf("m1.Heartbeat: %v", err)
	}
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatalf("m2.Heartbeat: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 members after both heartbeat, got %v", got)
	}

	// Deregister inst-1; the next heartbeat should see only inst-2.
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

// TestMembershipExpiredExcluded checks that expired member items are filtered out
// from the Scan results.
func TestMembershipExpiredExcluded(t *testing.T) {
	f := newFakeDDB()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	c1 := newWithClient(f, Options{Table: "locks", Owner: "owner-1", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	c1.now = func() time.Time { return base }
	c2 := newWithClient(f, Options{Table: "locks", Owner: "owner-2", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	// c2 runs 2 minutes later; c1's member item is already expired by then.
	c2.now = func() time.Time { return base.Add(2 * time.Minute) }

	ctx := context.Background()

	m1, _ := c1.Membership("inst-1")
	m2, _ := c2.Membership("inst-2")

	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatalf("m1.Heartbeat: %v", err)
	}
	// m2 heartbeats at t+2m; inst-1's lease expired (set to t+60s).
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatalf("m2.Heartbeat: %v", err)
	}
	// Only inst-2 should appear; inst-1 is expired.
	for _, id := range got {
		if id == "inst-1" {
			t.Fatalf("expired member inst-1 still in live set: %v", got)
		}
	}
	if len(got) != 1 || got[0] != "inst-2" {
		t.Fatalf("expected only inst-2, got %v", got)
	}
}

// TestMembershipExpiredReaped checks that an expired member item is deleted
// (best-effort reap) from the store during a later Heartbeat call.
func TestMembershipExpiredReaped(t *testing.T) {
	f := newFakeDDB()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	c1 := newWithClient(f, Options{Table: "locks", Owner: "owner-1", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	c1.now = func() time.Time { return base }
	c2 := newWithClient(f, Options{Table: "locks", Owner: "owner-2", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	c2.now = func() time.Time { return base.Add(2 * time.Minute) }

	ctx := context.Background()

	m1, _ := c1.Membership("inst-1")
	m2, _ := c2.Membership("inst-2")

	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatalf("m1.Heartbeat: %v", err)
	}
	// Verify inst-1's member item exists before the reap.
	if f.get(memberPK("inst-1")) == nil {
		t.Fatal("expected inst-1 member item to be present before reap")
	}

	// m2 heartbeats at t+2m; inst-1's lease is expired, so it should be reaped.
	if _, err := m2.Heartbeat(ctx); err != nil {
		t.Fatalf("m2.Heartbeat: %v", err)
	}

	// inst-1's member item must now be absent (best-effort reap).
	if f.get(memberPK("inst-1")) != nil {
		t.Fatal("expired member inst-1 was not reaped during Heartbeat")
	}
}

// TestMembershipCloseIsNoOp ensures Close returns nil without panicking.
func TestMembershipCloseIsNoOp(t *testing.T) {
	f := newFakeDDB()
	c := newWithClient(f, Options{Table: "locks", Owner: "owner-1", LeaseDuration: 60 * time.Second})
	m, err := c.Membership("inst-1")
	if err != nil {
		t.Fatalf("Membership: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestMemberTTLDefaultsToLeaseDuration checks that when MemberTTL is unset
// on Options it falls back to LeaseDuration in the coordinator.
func TestMemberTTLDefaultsToLeaseDuration(t *testing.T) {
	c := newWithClient(newFakeDDB(), Options{Table: "locks", Owner: "o", LeaseDuration: 45 * time.Second})
	if c.memberTTL != 0 {
		t.Fatalf("memberTTL should be 0 when unset in Options, got %v", c.memberTTL)
	}
	// The dynamoMembership itself resolves 0 -> leaseDuration at Heartbeat time.
	m, _ := c.Membership("self")
	dm := m.(*dynamoMembership)
	ttl := dm.ttl
	if ttl <= 0 {
		t.Fatalf("effective TTL should be > 0 (fell back to leaseDuration), got %v", ttl)
	}
	if ttl != 45*time.Second {
		t.Fatalf("effective TTL = %v, want 45s (leaseDuration fallback)", ttl)
	}
}
