package rabbitmqstream

import (
	"testing"
	"time"

	streamamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

	"github.com/iambod/rss2msg/internal/model"
)

func TestNewRejectsMissingFields(t *testing.T) {
	t.Parallel()
	if _, err := New(t.Context(), Options{URIs: []string{"rabbitmq-stream://h:5552/"}}); err == nil {
		t.Fatal("want error for missing name")
	}
	if _, err := New(t.Context(), Options{Name: "x", URIs: []string{"rabbitmq-stream://h:5552/"}}); err == nil {
		t.Fatal("want error for missing stream")
	}
	if _, err := New(t.Context(), Options{Name: "x", Stream: "s"}); err == nil {
		t.Fatal("want error for missing uris/url")
	}
}

func TestBuildMessageMapsChange(t *testing.T) {
	t.Parallel()
	ch := model.Change{
		FeedURL:       "https://example.com/feed",
		Kind:          model.ChangeNew,
		SchemaVersion: 1,
		ItemID:        "item-7",
		DetectedAt:    time.Unix(1700000000, 0),
	}
	msg := buildMessage(t.Context(), ch).(*streamamqp.AMQP10)
	if msg.ApplicationProperties["feed_url"] != "https://example.com/feed" {
		t.Fatalf("feed_url missing: %+v", msg.ApplicationProperties)
	}
	if msg.Properties == nil || msg.Properties.MessageID != "item-7" {
		t.Fatalf("MessageID not set: %+v", msg.Properties)
	}
}
