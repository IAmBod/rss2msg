//go:build integration

package cosmosdb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/testcontainers/testcontainers-go"
	tccosmos "github.com/testcontainers/testcontainers-go/modules/azure/cosmosdb"

	"github.com/iambod/rss2msg/internal/model"
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

func TestPublishRoundTrip(t *testing.T) {
	ctx := context.Background()
	connStr, clientOpts := setup(t)

	p, err := New(ctx, Options{
		Name:             "cosmos-test",
		ConnectionString: connStr,
		Database:         "rss2msg",
		Container:        "feed_changes",
		CreateIfMissing:  true,
		Throughput:       400,
		ClientOptions:    clientOpts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = p.Close() }()

	change := model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://example.com/feed",
		ItemID:        "item-1",
		Kind:          model.ChangeNew,
		Title:         "Hello",
		ContentHash:   "abc",
	}
	if err := p.Publish(ctx, change); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Re-publishing the same change must be an idempotent no-op (409 swallowed).
	if err := p.Publish(ctx, change); err != nil {
		t.Fatalf("Publish (idempotent re-send): %v", err)
	}

	// Read the document back with a raw SDK client and assert the wire layout.
	client, err := azcosmos.NewClientFromConnectionString(connStr, clientOpts)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	container, err := client.NewContainer("rss2msg", "feed_changes")
	if err != nil {
		t.Fatalf("container: %v", err)
	}
	id := docID(change.FeedURL, change.ItemID)
	resp, err := container.ReadItem(ctx, azcosmos.NewPartitionKeyString(change.FeedURL), id, nil)
	if err != nil {
		t.Fatalf("ReadItem: %v", err)
	}

	var doc map[string]any
	if err := json.Unmarshal(resp.Value, &doc); err != nil {
		t.Fatalf("stored doc is not JSON: %v", err)
	}
	if doc["id"] != id {
		t.Errorf("stored id: got %v, want %q", doc["id"], id)
	}
	if doc["feed_url"] != change.FeedURL {
		t.Errorf("stored feed_url: got %v, want %q", doc["feed_url"], change.FeedURL)
	}

	var rt model.Change
	if err := json.Unmarshal(resp.Value, &rt); err != nil {
		t.Fatalf("stored doc is not a Change: %v", err)
	}
	if rt.ItemID != change.ItemID || rt.Title != change.Title {
		t.Errorf("round-trip mismatch: %+v", rt)
	}
}
