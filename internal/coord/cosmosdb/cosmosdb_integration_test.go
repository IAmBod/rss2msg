//go:build integration

package cosmosdb

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/testcontainers/testcontainers-go"
	tccosmos "github.com/testcontainers/testcontainers-go/modules/azure/cosmosdb"
)

const emulatorImage = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview"

// setup starts the Cosmos DB emulator and returns a connection string plus the
// client options that route to the emulator endpoint and trust its
// self-signed certificate.
func setup(t *testing.T) (string, *azcosmos.ClientOptions) {
	t.Helper()
	ctx := context.Background()
	ctr, err := tccosmos.Run(ctx, emulatorImage)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	connStr, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := tccosmos.NewContainerPolicy(ctx, ctr)
	if err != nil {
		t.Fatal(err)
	}
	return connStr, policy.ClientOptions()
}

func newCoord(ctx context.Context, t *testing.T, connStr string, clientOpts *azcosmos.ClientOptions, lease time.Duration) *Coordinator {
	t.Helper()
	c, err := New(ctx, Options{
		ConnectionString: connStr,
		Database:         "rss2msg",
		Container:        "coordination_locks",
		CreateIfMissing:  true,
		Throughput:       400,
		LeaseDuration:    lease,
		ClientOptions:    clientOpts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestCoordinatorContention(t *testing.T) {
	ctx := context.Background()
	connStr, clientOpts := setup(t)

	a := newCoord(ctx, t, connStr, clientOpts, 60*time.Second)
	defer func() { _ = a.Close() }()
	b := newCoord(ctx, t, connStr, clientOpts, 60*time.Second)
	defer func() { _ = b.Close() }()

	const feed = "https://example.com/feed"

	// A wins; B is blocked on the live lease.
	relA, ok, err := a.TryAcquire(ctx, feed)
	if err != nil || !ok {
		t.Fatalf("a acquire: ok=%v err=%v", ok, err)
	}
	if _, ok, err := b.TryAcquire(ctx, feed); err != nil || ok {
		t.Fatalf("b should be blocked: ok=%v err=%v", ok, err)
	}

	// A releases; B can now acquire.
	if err := relA(ctx); err != nil {
		t.Fatalf("a release: %v", err)
	}
	relB, ok, err := b.TryAcquire(ctx, feed)
	if err != nil || !ok {
		t.Fatalf("b acquire after release: ok=%v err=%v", ok, err)
	}
	if err := relB(ctx); err != nil {
		t.Fatalf("b release: %v", err)
	}
}

func TestCoordinatorExpiredLeaseStolen(t *testing.T) {
	ctx := context.Background()
	connStr, clientOpts := setup(t)

	// Short lease so a crashed (never-released) holder is reclaimable quickly.
	a := newCoord(ctx, t, connStr, clientOpts, 2*time.Second)
	defer func() { _ = a.Close() }()
	b := newCoord(ctx, t, connStr, clientOpts, 2*time.Second)
	defer func() { _ = b.Close() }()

	const feed = "https://example.com/expiry"

	// A acquires and never releases (simulated crash).
	if _, ok, err := a.TryAcquire(ctx, feed); err != nil || !ok {
		t.Fatalf("a acquire: ok=%v err=%v", ok, err)
	}
	// Before expiry, B is blocked.
	if _, ok, _ := b.TryAcquire(ctx, feed); ok {
		t.Fatal("b should be blocked before lease expiry")
	}

	time.Sleep(3 * time.Second) // outlive A's 2s lease

	rel, ok, err := b.TryAcquire(ctx, feed)
	if err != nil || !ok {
		t.Fatalf("b should steal expired lease: ok=%v err=%v", ok, err)
	}
	if err := rel(ctx); err != nil {
		t.Fatalf("b release: %v", err)
	}
}
