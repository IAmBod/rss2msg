package amqp10

import (
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

func TestNewRejectsMissingFields(t *testing.T) {
	t.Parallel()
	if _, err := New(t.Context(), Options{URL: "amqp://h", Target: "q"}); err == nil {
		t.Fatal("want error for missing name")
	}
	if _, err := New(t.Context(), Options{Name: "x", Target: "q"}); err == nil {
		t.Fatal("want error for missing url")
	}
	if _, err := New(t.Context(), Options{Name: "x", URL: "amqp://h"}); err == nil {
		t.Fatal("want error for missing target")
	}
}

func TestResolveAuthExplicitWins(t *testing.T) {
	t.Parallel()
	u, p, err := resolveAuth("amqp://urluser:urlpass@host:5672", "explicit", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if u != "explicit" || p != "secret" {
		t.Fatalf("explicit creds should win, got %q/%q", u, p)
	}
	u, p, _ = resolveAuth("amqp://urluser:urlpass@host:5672", "", "")
	if u != "urluser" || p != "urlpass" {
		t.Fatalf("want URL userinfo fallback, got %q/%q", u, p)
	}
}

func TestBuildMessageMapsChange(t *testing.T) {
	t.Parallel()
	ch := model.Change{
		FeedURL:       "https://example.com/feed",
		Kind:          model.ChangeNew,
		SchemaVersion: 1,
		ItemID:        "item-42",
		DetectedAt:    time.Unix(1700000000, 0),
	}
	msg := buildMessage(t.Context(), ch)
	if got := string(msg.Data[0]); got == "" {
		t.Fatal("body must be JSON-encoded change")
	}
	if msg.Properties == nil || msg.Properties.MessageID != "item-42" {
		t.Fatalf("MessageID not set, got %+v", msg.Properties)
	}
	if msg.ApplicationProperties["feed_url"] != "https://example.com/feed" {
		t.Fatalf("feed_url app property missing: %+v", msg.ApplicationProperties)
	}
	if msg.ApplicationProperties["kind"] != string(model.ChangeNew) {
		t.Fatalf("kind app property missing: %+v", msg.ApplicationProperties)
	}
}
