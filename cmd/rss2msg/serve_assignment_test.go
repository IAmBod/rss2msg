package main

import (
	"context"
	"testing"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/coord"
	coordmem "github.com/iambod/rss2msg/internal/coord/memory"
	"github.com/iambod/rss2msg/internal/scheduler"
	"github.com/iambod/rss2msg/internal/telemetry"
)

// stubFeedProvider is a minimal scheduler.FeedProvider for tests.
type stubFeedProvider struct{}

func (stubFeedProvider) Desired(_ context.Context) ([]config.FeedConfig, error) { return nil, nil }
func (stubFeedProvider) Changes() <-chan struct{}                                { return make(chan struct{}) }

// Verify the interface is satisfied at compile time.
var _ scheduler.FeedProvider = stubFeedProvider{}

func TestMaybeWrapProviderDisabledReturnsInner(t *testing.T) {
	cfg := config.Defaults()
	cfg.Coordination.Assignment.Enabled = false
	var inner scheduler.FeedProvider = stubFeedProvider{}
	got, op, err := maybeWrapProvider(cfg, coordmem.New(), inner, "self", telemetry.Instruments{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if op != nil {
		t.Fatal("expected no OwnerProvider when assignment disabled")
	}
	if got != inner {
		t.Fatal("expected the raw inner provider when assignment disabled")
	}
}

func TestMaybeWrapProviderEnabledWrapsMemory(t *testing.T) {
	cfg := config.Defaults()
	cfg.Coordination.Assignment.Enabled = true
	got, op, err := maybeWrapProvider(cfg, coordmem.New(), stubFeedProvider{}, "self", telemetry.Instruments{})
	if err != nil {
		t.Fatalf("memory implements MembershipProvider, expected no err: %v", err)
	}
	if op == nil || got == nil {
		t.Fatal("expected a wrapped provider when enabled")
	}
	// Call Heartbeat once to trigger onRebalance; confirm nil instruments don't panic.
	_, _ = op.Heartbeat(context.Background())
}

// noMembershipCoord is a fake coordinator that deliberately does NOT implement
// MembershipProvider, so we can test the error path.
type noMembershipCoord struct{}

func (noMembershipCoord) TryAcquire(_ context.Context, _ string) (coord.ReleaseFunc, bool, error) {
	return nil, false, nil
}
func (noMembershipCoord) Close() error { return nil }

func TestMaybeWrapProviderEnabledNonMembershipDriverErrors(t *testing.T) {
	cfg := config.Defaults()
	cfg.Coordination.Assignment.Enabled = true
	cfg.Coordination.Driver = "fake"
	_, _, err := maybeWrapProvider(cfg, noMembershipCoord{}, stubFeedProvider{}, "self", telemetry.Instruments{})
	if err == nil {
		t.Fatal("expected error when driver does not implement MembershipProvider")
	}
}
