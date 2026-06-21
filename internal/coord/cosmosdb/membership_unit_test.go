package cosmosdb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

func TestMemberDocIDFormat(t *testing.T) {
	if got := memberDocID("h-1-ab"); got != "member:h-1-ab" {
		t.Fatalf("memberDocID = %q", got)
	}
}

// TestMembershipHeartbeatAndDeregister exercises the full membership lifecycle
// against the in-memory fakeContainer, including the query-based live-set enumeration.
func TestMembershipHeartbeatAndDeregister(t *testing.T) {
	f := newFakeContainer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	c1 := newWithContainer(f, Options{Database: "db", Owner: "owner-1", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	c1.now = func() time.Time { return base }
	c2 := newWithContainer(f, Options{Database: "db", Owner: "owner-2", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	c2.now = func() time.Time { return base }

	ctx := context.Background()

	m1, err := c1.Membership("inst-1")
	if err != nil {
		t.Fatalf("c1.Membership: %v", err)
	}
	m2, err := c2.Membership("inst-2")
	if err != nil {
		t.Fatalf("c2.Membership: %v", err)
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

// TestMembershipExpiredExcluded checks that expired member items are filtered out.
func TestMembershipExpiredExcluded(t *testing.T) {
	f := newFakeContainer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	c1 := newWithContainer(f, Options{Database: "db", Owner: "owner-1", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
	c1.now = func() time.Time { return base }
	c2 := newWithContainer(f, Options{Database: "db", Owner: "owner-2", LeaseDuration: 60 * time.Second, MemberTTL: 60 * time.Second})
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
	for _, id := range got {
		if id == "inst-1" {
			t.Fatalf("expired member inst-1 still in live set: %v", got)
		}
	}
	if len(got) != 1 || got[0] != "inst-2" {
		t.Fatalf("expected only inst-2, got %v", got)
	}
}

// TestMembershipCloseIsNoOp ensures Close returns nil without panicking.
func TestMembershipCloseIsNoOp(t *testing.T) {
	f := newFakeContainer()
	c := newWithContainer(f, Options{Database: "db", Owner: "owner-1", LeaseDuration: 60 * time.Second})
	m, err := c.Membership("inst-1")
	if err != nil {
		t.Fatalf("Membership: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestMemberTTLDefaultsToLeaseDuration checks that when MemberTTL is unset on
// Options it falls back to LeaseDuration in the coordinator.
func TestMemberTTLDefaultsToLeaseDuration(t *testing.T) {
	c := newWithContainer(newFakeContainer(), Options{Database: "db", Owner: "o", LeaseDuration: 45 * time.Second})
	if c.memberTTL != 0 {
		t.Fatalf("memberTTL should be 0 when unset in Options, got %v", c.memberTTL)
	}
	// The cosmosMembership itself resolves 0 -> leaseDuration at Membership() time.
	m, _ := c.Membership("self")
	cm := m.(*cosmosMembership)
	if cm.ttl != 45*time.Second {
		t.Fatalf("effective TTL = %v, want 45s (leaseDuration fallback)", cm.ttl)
	}
}

// --- helpers to make fakeContainer satisfy the extended containerAPI ---.

// newQueryItemsPager builds a real *runtime.Pager[azcosmos.QueryItemsResponse] from
// the fake's in-memory member docs, filtering by lease_expiry > @now.
// This satisfies the new containerAPI.NewQueryItemsPager method without bypassing the
// real pager type.
func (f *fakeContainer) NewQueryItemsPager(_ string, _ azcosmos.PartitionKey, o *azcosmos.QueryOptions) *runtime.Pager[azcosmos.QueryItemsResponse] {
	// Extract @now from the query parameters.
	var nowMs int64
	if o != nil {
		for _, p := range o.QueryParameters {
			if p.Name == "@now" {
				switch v := p.Value.(type) {
				case int64:
					nowMs = v
				case float64:
					nowMs = int64(v)
				}
			}
		}
	}

	// Snapshot matching items under the lock.
	f.mu.Lock()
	var items [][]byte
	for id, doc := range f.items {
		// Only member docs (id prefix "member:") in the "members" partition.
		if len(id) < 7 || id[:7] != "member:" {
			continue
		}
		var md memberDoc
		if err := json.Unmarshal(doc.body, &md); err != nil {
			continue
		}
		if md.PK != memberPartitionKey {
			continue
		}
		if md.LeaseExpiry <= nowMs {
			continue
		}
		b := append([]byte(nil), doc.body...)
		items = append(items, b)
	}
	f.mu.Unlock()

	fetched := false
	return runtime.NewPager(runtime.PagingHandler[azcosmos.QueryItemsResponse]{
		More: func(azcosmos.QueryItemsResponse) bool { return false },
		Fetcher: func(_ context.Context, _ *azcosmos.QueryItemsResponse) (azcosmos.QueryItemsResponse, error) {
			if fetched {
				return azcosmos.QueryItemsResponse{}, nil
			}
			fetched = true
			return azcosmos.QueryItemsResponse{Items: items}, nil
		},
	})
}
