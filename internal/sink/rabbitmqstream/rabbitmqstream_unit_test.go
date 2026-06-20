package rabbitmqstream

import (
	"errors"
	"strings"
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

func TestNotConfirmedErrorNilCause(t *testing.T) {
	t.Parallel()
	// A not-confirmed status can carry a nil GetError(); the formatted error
	// must not render the "%!w(<nil>)" verb-mismatch artifact.
	err := notConfirmedError("mysink", nil)
	if err == nil {
		t.Fatal("want non-nil error")
	}
	got := err.Error()
	if strings.Contains(got, "%!w") || strings.Contains(got, "<nil>") {
		t.Fatalf("error string has nil-wrap artifact: %q", got)
	}
	if !strings.Contains(got, "mysink") || !strings.Contains(got, "not confirmed") {
		t.Fatalf("error string missing context: %q", got)
	}
}

func TestNotConfirmedErrorWrapsCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("stream closed")
	err := notConfirmedError("mysink", cause)
	if !errors.Is(err, cause) {
		t.Fatalf("want wrapped cause, got %v", err)
	}
	if !strings.Contains(err.Error(), "stream closed") {
		t.Fatalf("error string missing cause detail: %q", err.Error())
	}
}
