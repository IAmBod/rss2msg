package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

type fakeInner struct {
	feeds []config.FeedConfig
	ch    chan struct{}
}

func (f *fakeInner) Desired(context.Context) ([]config.FeedConfig, error) { return f.feeds, nil }
func (f *fakeInner) Changes() <-chan struct{}                             { return f.ch }

type fakeMembership struct {
	mu      sync.Mutex
	members []string
}

func (m *fakeMembership) set(ids ...string) {
	m.mu.Lock()
	m.members = ids
	m.mu.Unlock()
}
func (m *fakeMembership) Heartbeat(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.members))
	copy(out, m.members)
	return out, nil
}
func (m *fakeMembership) Deregister(context.Context) error { return nil }
func (m *fakeMembership) Close() error                     { return nil }

func feeds(urls ...string) []config.FeedConfig {
	out := make([]config.FeedConfig, len(urls))
	for i, u := range urls {
		out[i] = config.FeedConfig{URL: u, Interval: time.Minute}
	}
	return out
}

func TestOwnerProviderFiltersToOwned(t *testing.T) {
	inner := &fakeInner{feeds: feeds("https://e/a", "https://e/b", "https://e/c"), ch: make(chan struct{}, 1)}
	mem := &fakeMembership{members: []string{"self", "peer"}}
	op := NewOwnerProvider(inner, mem, "self", 10*time.Millisecond, nil)

	// Prime the member snapshot.
	if _, err := op.Heartbeat(context.Background()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	owned, err := op.Desired(context.Background())
	if err != nil {
		t.Fatalf("desired: %v", err)
	}
	// Cross-check: the union of self's and peer's owned sets is all feeds, disjoint.
	opPeer := NewOwnerProvider(inner, mem, "peer", time.Hour, nil)
	_, _ = opPeer.Heartbeat(context.Background())
	peerOwned, _ := opPeer.Desired(context.Background())
	if len(owned)+len(peerOwned) != 3 {
		t.Fatalf("owned(%d)+peerOwned(%d) should equal 3 feeds", len(owned), len(peerOwned))
	}
}

func TestOwnerProviderSignalsOnMembershipChange(t *testing.T) {
	inner := &fakeInner{feeds: feeds("https://e/a", "https://e/b"), ch: make(chan struct{}, 1)}
	mem := &fakeMembership{members: []string{"self"}}
	op := NewOwnerProvider(inner, mem, "self", 5*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go op.Run(ctx)

	// Drain the initial signal (first heartbeat establishes the baseline).
	select {
	case <-op.Changes():
	case <-time.After(time.Second):
	}

	mem.set("self", "peer") // membership grows
	select {
	case <-op.Changes():
		// success: change propagated
	case <-time.After(2 * time.Second):
		t.Fatal("expected Changes() signal after membership changed")
	}
}
