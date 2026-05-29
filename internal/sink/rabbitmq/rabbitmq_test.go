//go:build integration

package rabbitmq_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/iambod/rss2msg/internal/model"
	sinkrabbitmq "github.com/iambod/rss2msg/internal/sink/rabbitmq"
)

// setup boots a rabbitmq:3-management-alpine container and returns its
// amqp:// URL plus a cleanup func.
func setup(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()
	c, err := tcrabbitmq.Run(ctx, "rabbitmq:3-management-alpine")
	if err != nil {
		t.Fatal(err)
	}
	url, err := c.AmqpURL(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatal(err)
	}
	return url, func() { _ = c.Terminate(ctx) }
}

func declareQueueAndBind(t *testing.T, url, exchange, routingKey, queue string) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	if err := ch.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		t.Fatalf("queue bind: %v", err)
	}
}

func consumeOne(t *testing.T, url, queue string, timeout time.Duration) amqp.Delivery {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	deliveries, err := ch.Consume(queue, "test-consumer", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	select {
	case d := <-deliveries:
		return d
	case <-time.After(timeout):
		t.Fatalf("no delivery within %v", timeout)
	}
	return amqp.Delivery{}
}

func TestPublishRoundTripsEnvelopeAndHeaders(t *testing.T) {
	url, cleanup := setup(t)
	defer cleanup()

	const (
		exchange   = "rss2msg.test"
		routingKey = "feed.changes"
		queue      = "rss2msg.test.q"
	)

	pub, err := sinkrabbitmq.New(sinkrabbitmq.Options{
		Name:         "rmq-test",
		URL:          url,
		Exchange:     exchange,
		ExchangeType: "topic",
		RoutingKey:   routingKey,
		Declare:      true,
		Durable:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	declareQueueAndBind(t, url, exchange, routingKey, queue)

	when := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	change := model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://e/feed",
		FeedTitle:     "Example",
		ItemID:        "i1",
		Kind:          model.ChangeNew,
		Title:         "Hello world",
		Link:          "https://e/post/1",
		ContentHash:   "deadbeef",
		DetectedAt:    when,
	}
	if err := pub.Publish(context.Background(), change); err != nil {
		t.Fatalf("publish: %v", err)
	}

	d := consumeOne(t, url, queue, 10*time.Second)

	if d.ContentType != "application/json" {
		t.Errorf("content type: want application/json, got %q", d.ContentType)
	}
	if d.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery mode: want persistent (2), got %d", d.DeliveryMode)
	}
	if d.MessageId != "i1" {
		t.Errorf("message id: want i1, got %q", d.MessageId)
	}

	if got := d.Headers["feed_url"]; got != "https://e/feed" {
		t.Errorf("header feed_url: got %v", got)
	}
	if got := d.Headers["kind"]; got != string(model.ChangeNew) {
		t.Errorf("header kind: got %v", got)
	}
	if got, _ := d.Headers["schema_version"].(int32); got != int32(model.SchemaVersion) {
		t.Errorf("header schema_version: want %d, got %v (%T)", model.SchemaVersion, d.Headers["schema_version"], d.Headers["schema_version"])
	}

	var round model.Change
	if err := json.Unmarshal(d.Body, &round); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if round.ItemID != change.ItemID || round.FeedURL != change.FeedURL || round.Kind != change.Kind || round.Title != change.Title {
		t.Errorf("body round-trip mismatch: %+v", round)
	}
	if round.SchemaVersion != model.SchemaVersion {
		t.Errorf("body schema_version: want %d, got %d", model.SchemaVersion, round.SchemaVersion)
	}
}

func TestPublishDLQHeadersWhenDLQFieldsPopulated(t *testing.T) {
	url, cleanup := setup(t)
	defer cleanup()

	const (
		exchange   = "rss2msg.dlq.test"
		routingKey = "feed.dlq"
		queue      = "rss2msg.dlq.test.q"
	)

	pub, err := sinkrabbitmq.New(sinkrabbitmq.Options{
		Name: "rmq-dlq-test", URL: url, Exchange: exchange, ExchangeType: "direct",
		RoutingKey: routingKey, Declare: true, Durable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	declareQueueAndBind(t, url, exchange, routingKey, queue)

	change := model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://e/feed",
		ItemID:        "i-dlq",
		Kind:          model.ChangeUpdated,
		ContentHash:   "h",
		DetectedAt:    time.Now().UTC(),
		DLQFromSink:   "kafka-main",
		DLQError:      "broker unreachable",
		DLQAttempts:   3,
	}
	if err := pub.Publish(context.Background(), change); err != nil {
		t.Fatalf("publish: %v", err)
	}

	d := consumeOne(t, url, queue, 10*time.Second)

	if got := d.Headers["dlq_from_sink"]; got != "kafka-main" {
		t.Errorf("dlq_from_sink: got %v", got)
	}
	if got := d.Headers["dlq_error"]; got != "broker unreachable" {
		t.Errorf("dlq_error: got %v", got)
	}
	if got, _ := d.Headers["dlq_attempts"].(int32); got != 3 {
		t.Errorf("dlq_attempts: want 3, got %v", d.Headers["dlq_attempts"])
	}
}
