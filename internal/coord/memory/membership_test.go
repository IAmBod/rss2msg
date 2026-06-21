package memory

import (
	"context"
	"testing"

	"github.com/iambod/rss2msg/internal/coord"
)

func TestMemoryMembershipSingleMember(t *testing.T) {
	t.Parallel()
	var c coord.MembershipProvider = New()
	m, err := c.Membership("self-1")
	if err != nil {
		t.Fatalf("membership: %v", err)
	}
	got, err := m.Heartbeat(context.Background())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if len(got) != 1 || got[0] != "self-1" {
		t.Fatalf("expected single member [self-1], got %v", got)
	}
	if err := m.Deregister(context.Background()); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
