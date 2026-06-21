package scheduler

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/iambod/rss2msg/internal/assign"
	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/coord"
)

// OwnerProvider wraps a FeedProvider so that Desired() yields only the feeds
// this instance owns under rendezvous hashing of the live coordinator members.
// A heartbeat loop refreshes membership and signals Changes() whenever the
// member set changes, so ServeDynamic reconciles the owned ticker set.
type OwnerProvider struct {
	inner       FeedProvider
	membership  coord.Membership
	self        string
	heartbeat   time.Duration
	onRebalance func(members, owned int)

	changes chan struct{}

	mu      sync.RWMutex
	members []string
	lastKey string
}

// NewOwnerProvider builds the provider. heartbeat is the membership refresh
// period; onRebalance (optional) is called with the live member count and this
// instance's owned-feed count whenever membership changes.
func NewOwnerProvider(inner FeedProvider, m coord.Membership, self string, heartbeat time.Duration, onRebalance func(members, owned int)) *OwnerProvider {
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	return &OwnerProvider{
		inner: inner, membership: m, self: self, heartbeat: heartbeat,
		onRebalance: onRebalance,
		changes:     make(chan struct{}, 1),
		members:     []string{self}, // fail-static baseline: alone until first heartbeat
	}
}

func (o *OwnerProvider) snapshot() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]string, len(o.members))
	copy(out, o.members)
	return out
}

// Heartbeat refreshes membership once and returns the live set. It updates the
// cached snapshot and signals Changes() if the set changed. Exposed for tests
// and the priming call.
func (o *OwnerProvider) Heartbeat(ctx context.Context) ([]string, error) {
	members, err := o.membership.Heartbeat(ctx)
	if err != nil {
		return o.snapshot(), err // keep last-known set
	}
	sorted := append([]string(nil), members...)
	sort.Strings(sorted)
	key := strings.Join(sorted, ",")

	o.mu.Lock()
	changed := key != o.lastKey
	o.members = sorted
	o.lastKey = key
	o.mu.Unlock()

	if changed {
		o.signal()
		if o.onRebalance != nil {
			feeds, _ := o.inner.Desired(ctx)
			owned := assign.Owned(o.self, urls(feeds), sorted)
			o.onRebalance(len(sorted), len(owned))
		}
	}
	return sorted, nil
}

func (o *OwnerProvider) signal() {
	select {
	case o.changes <- struct{}{}:
	default: // already pending; reconcile reads the latest state anyway
	}
}

// Run drives the heartbeat loop until ctx is cancelled.
func (o *OwnerProvider) Run(ctx context.Context) {
	_, _ = o.Heartbeat(ctx) // establish baseline immediately
	t := time.NewTicker(o.heartbeat)
	defer t.Stop()
	// Forward inner provider changes onto our merged channel.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-o.inner.Changes():
				o.signal()
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = o.Heartbeat(ctx)
		}
	}
}

// Desired returns only this instance's owned feeds from the inner set.
func (o *OwnerProvider) Desired(ctx context.Context) ([]config.FeedConfig, error) {
	all, err := o.inner.Desired(ctx)
	if err != nil {
		return nil, err
	}
	owned := assign.Owned(o.self, urls(all), o.snapshot())
	ownedSet := make(map[string]struct{}, len(owned))
	for _, u := range owned {
		ownedSet[u] = struct{}{}
	}
	out := make([]config.FeedConfig, 0, len(owned))
	for _, fc := range all {
		if _, ok := ownedSet[fc.URL]; ok {
			out = append(out, fc)
		}
	}
	return out, nil
}

// Changes signals when either the inner feed set or the membership changes.
func (o *OwnerProvider) Changes() <-chan struct{} { return o.changes }

// Close deregisters this instance (best-effort) and closes the membership.
func (o *OwnerProvider) Close(ctx context.Context) error {
	derr := o.membership.Deregister(ctx)
	cerr := o.membership.Close()
	if derr != nil {
		return derr
	}
	return cerr
}

func urls(feeds []config.FeedConfig) []string {
	out := make([]string, len(feeds))
	for i, fc := range feeds {
		out[i] = fc.URL
	}
	return out
}
