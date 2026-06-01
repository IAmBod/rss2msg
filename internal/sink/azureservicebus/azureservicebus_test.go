//go:build integration

package azureservicebus

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/testcontainers/testcontainers-go"
	tcservicebus "github.com/testcontainers/testcontainers-go/modules/azure/servicebus"

	"github.com/iambod/rss2msg/internal/model"
)

const (
	emulatorImage = "mcr.microsoft.com/azure-messaging/servicebus-emulator:1.1.2"
	testQueue     = "feed-changes"
)

// emulatorConfig declares the namespace + queue the sink publishes to. The
// emulator does not auto-create entities; they must be present in this config.
const emulatorConfig = `{
  "UserConfig": {
    "Namespaces": [
      {
        "Name": "sbemulatorns",
        "Queues": [
          {
            "Name": "feed-changes",
            "Properties": {
              "DeadLetteringOnMessageExpiration": false,
              "DefaultMessageTimeToLive": "PT1H",
              "DuplicateDetectionHistoryTimeWindow": "PT20S",
              "ForwardDeadLetteredMessagesTo": "",
              "ForwardTo": "",
              "LockDuration": "PT1M",
              "MaxDeliveryCount": 10,
              "RequiresDuplicateDetection": false,
              "RequiresSession": false
            }
          }
        ]
      }
    ],
    "Logging": { "Type": "File" }
  }
}`

func setup(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcservicebus.Run(ctx, emulatorImage,
		tcservicebus.WithAcceptEULA(),
		tcservicebus.WithConfig(strings.NewReader(emulatorConfig)),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	connStr, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return connStr
}

func TestPublishRoundTrip(t *testing.T) {
	ctx := context.Background()
	connStr := setup(t)

	p, err := New(Options{Name: "asb-test", ConnectionString: connStr, Queue: testQueue})
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

	// Read the message back with a raw SDK receiver and assert the wire layout.
	client, err := azservicebus.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		t.Fatalf("receiver client: %v", err)
	}
	defer func() { _ = client.Close(ctx) }()
	receiver, err := client.NewReceiverForQueue(testQueue, nil)
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer func() { _ = receiver.Close(ctx) }()

	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	msgs, err := receiver.ReceiveMessages(rctx, 1, nil)
	if err != nil {
		t.Fatalf("ReceiveMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 message, got %d", len(msgs))
	}
	got := msgs[0]
	_ = receiver.CompleteMessage(ctx, got, nil)

	if got.ContentType == nil || *got.ContentType != "application/json" {
		t.Errorf("content type: got %v", got.ContentType)
	}
	if got.MessageID != "item-1" {
		t.Errorf("message id: got %q, want %q", got.MessageID, "item-1")
	}
	if got.ApplicationProperties["feed_url"] != change.FeedURL {
		t.Errorf("feed_url prop: got %v", got.ApplicationProperties["feed_url"])
	}
	if got.ApplicationProperties["kind"] != string(model.ChangeNew) {
		t.Errorf("kind prop: got %v", got.ApplicationProperties["kind"])
	}

	var rt model.Change
	if err := json.Unmarshal(got.Body, &rt); err != nil {
		t.Fatalf("body is not Change JSON: %v", err)
	}
	if rt.ItemID != change.ItemID || rt.FeedURL != change.FeedURL || rt.Title != change.Title {
		t.Errorf("body round-trip mismatch: %+v", rt)
	}
}
