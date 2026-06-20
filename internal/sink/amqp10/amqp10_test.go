//go:build integration

package amqp10

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	goamqp "github.com/Azure/go-amqp"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/iambod/rss2msg/internal/model"
)

// setupRabbitMQ4 boots a rabbitmq:4-management container (AMQP 1.0 natively
// on port 5672) and returns its amqp:// URL, management HTTP base URL, and a
// cleanup func.
func setupRabbitMQ4(t *testing.T) (amqpURL, mgmtURL string, cleanup func()) {
	t.Helper()
	ctx := context.Background()
	c, err := tcrabbitmq.Run(ctx, "rabbitmq:4-management")
	if err != nil {
		t.Fatal(err)
	}
	u, err := c.AmqpURL(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatal(err)
	}
	hu, err := c.HttpURL(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatal(err)
	}
	return u, hu, func() { _ = c.Terminate(ctx) }
}

// declareQueueViaManagement uses the RabbitMQ management HTTP API to declare a
// classic durable queue. The path encodes vhost "/" as %2F.
func declareQueueViaManagement(t *testing.T, mgmtURL, queueName string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"durable":     true,
		"auto_delete": false,
		"arguments":   map[string]any{},
	})
	u := fmt.Sprintf("%s/api/queues/%%2F/%s", mgmtURL, queueName)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, u, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build queue declare request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("guest", "guest")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("declare queue via management API: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("declare queue: unexpected status %d", resp.StatusCode)
	}
}

func TestAMQP10PublishRoundTrips(t *testing.T) {
	amqpURL, mgmtURL, cleanup := setupRabbitMQ4(t)
	defer cleanup()

	const queueName = "itest-amqp10"

	// Declare the queue first via the management API so the sender can route to it.
	declareQueueViaManagement(t, mgmtURL, queueName)

	// AMQP 1.0 address for sending to the queue.
	target := "/queues/" + queueName

	ctx := context.Background()
	p, err := New(ctx, Options{
		Name:   "amqp10-itest",
		URL:    amqpURL,
		Target: target,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	change := model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://example.com/feed",
		ItemID:        "item-1",
		Kind:          model.ChangeNew,
		Title:         "Hello AMQP 1.0",
		ContentHash:   "abc123",
		DetectedAt:    time.Now().UTC(),
	}

	if err := p.Publish(ctx, change); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Open a separate connection to receive the message back and assert it.
	recvConn, err := goamqp.Dial(ctx, amqpURL, nil)
	if err != nil {
		t.Fatalf("receiver dial: %v", err)
	}
	defer recvConn.Close()

	recvSess, err := recvConn.NewSession(ctx, nil)
	if err != nil {
		t.Fatalf("receiver session: %v", err)
	}

	recv, err := recvSess.NewReceiver(ctx, target, nil)
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer recv.Close(ctx)

	recvCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	msg, err := recv.Receive(recvCtx, nil)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if err := recv.AcceptMessage(ctx, msg); err != nil {
		t.Fatalf("accept message: %v", err)
	}

	// Assert body round-trips.
	if len(msg.Data) == 0 || len(msg.Data[0]) == 0 {
		t.Fatal("received message has empty body")
	}
	var got model.Change
	if err := json.Unmarshal(msg.Data[0], &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if got.ItemID != change.ItemID || got.FeedURL != change.FeedURL || got.Kind != change.Kind {
		t.Errorf("body round-trip mismatch: got %+v", got)
	}

	// Assert application properties.
	if feedURL, ok := msg.ApplicationProperties["feed_url"]; !ok || feedURL != change.FeedURL {
		t.Errorf("feed_url app property: got %v", msg.ApplicationProperties["feed_url"])
	}
	if kind, ok := msg.ApplicationProperties["kind"]; !ok || kind != string(model.ChangeNew) {
		t.Errorf("kind app property: got %v", msg.ApplicationProperties["kind"])
	}
}
