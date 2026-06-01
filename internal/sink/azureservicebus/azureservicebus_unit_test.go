package azureservicebus

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/iambod/rss2msg/internal/model"
)

func TestNewRejectsMissingName(t *testing.T) {
	t.Parallel()
	_, err := New(Options{ConnectionString: "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v", Queue: "q"})
	if err == nil || !strings.Contains(err.Error(), "name is required") {
		t.Fatalf("got %v", err)
	}
}

func TestNewRejectsMissingAuth(t *testing.T) {
	t.Parallel()
	_, err := New(Options{Name: "x", Queue: "q"})
	if err == nil || !strings.Contains(err.Error(), "connection_string or namespace") {
		t.Fatalf("got %v", err)
	}
}

func TestNewRejectsBothAuth(t *testing.T) {
	t.Parallel()
	_, err := New(Options{
		Name:             "x",
		ConnectionString: "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v",
		Namespace:        "x.servicebus.windows.net",
		Queue:            "q",
	})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("got %v", err)
	}
}

func TestNewRejectsMissingEntity(t *testing.T) {
	t.Parallel()
	_, err := New(Options{Name: "x", ConnectionString: "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v"})
	if err == nil || !strings.Contains(err.Error(), "queue or topic") {
		t.Fatalf("got %v", err)
	}
}

func TestNewRejectsBothEntities(t *testing.T) {
	t.Parallel()
	_, err := New(Options{
		Name:             "x",
		ConnectionString: "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v",
		Queue:            "q",
		Topic:            "t",
	})
	if err == nil || !strings.Contains(err.Error(), "queue and topic are mutually exclusive") {
		t.Fatalf("got %v", err)
	}
}

func TestBuildMessageLayout(t *testing.T) {
	t.Parallel()
	change := model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://example.com/feed",
		ItemID:        "item-1",
		Kind:          model.ChangeNew,
		Title:         "Hello",
	}
	msg, err := buildMessage(context.Background(), change)
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}

	if msg.ContentType == nil || *msg.ContentType != "application/json" {
		t.Errorf("content type: got %v", msg.ContentType)
	}
	if msg.MessageID == nil || *msg.MessageID != "item-1" {
		t.Errorf("message id: got %v", msg.MessageID)
	}

	// Body must be the JSON-marshaled Change.
	var got model.Change
	if err := json.Unmarshal(msg.Body, &got); err != nil {
		t.Fatalf("body is not the Change JSON: %v", err)
	}
	if got.ItemID != "item-1" || got.FeedURL != change.FeedURL {
		t.Errorf("body round-trip mismatch: %+v", got)
	}

	if msg.ApplicationProperties["feed_url"] != change.FeedURL {
		t.Errorf("feed_url prop: got %v", msg.ApplicationProperties["feed_url"])
	}
	if msg.ApplicationProperties["kind"] != string(model.ChangeNew) {
		t.Errorf("kind prop: got %v", msg.ApplicationProperties["kind"])
	}
	if msg.ApplicationProperties["schema_version"] != model.SchemaVersion {
		t.Errorf("schema_version prop: got %v", msg.ApplicationProperties["schema_version"])
	}
}

func TestBuildMessageOmitsEmptyMessageID(t *testing.T) {
	t.Parallel()
	msg, err := buildMessage(context.Background(), model.Change{FeedURL: "f", Kind: model.ChangeNew})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if msg.MessageID != nil {
		t.Errorf("expected nil MessageID for empty ItemID, got %q", *msg.MessageID)
	}
}

func TestBuildMessageDLQProperties(t *testing.T) {
	t.Parallel()

	// Without DLQ annotations, the dlq_* properties are absent.
	plain, err := buildMessage(context.Background(), model.Change{FeedURL: "f", ItemID: "i", Kind: model.ChangeNew})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if _, ok := plain.ApplicationProperties["dlq_from_sink"]; ok {
		t.Errorf("did not expect dlq_from_sink on a non-DLQ change")
	}

	// With DLQ annotations, all three are carried.
	dlq, err := buildMessage(context.Background(), model.Change{
		FeedURL:     "f",
		ItemID:      "i",
		Kind:        model.ChangeNew,
		DLQFromSink: "kafka-main",
		DLQError:    "boom",
		DLQAttempts: 3,
	})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if dlq.ApplicationProperties["dlq_from_sink"] != "kafka-main" {
		t.Errorf("dlq_from_sink: got %v", dlq.ApplicationProperties["dlq_from_sink"])
	}
	if dlq.ApplicationProperties["dlq_error"] != "boom" {
		t.Errorf("dlq_error: got %v", dlq.ApplicationProperties["dlq_error"])
	}
	if dlq.ApplicationProperties["dlq_attempts"] != 3 {
		t.Errorf("dlq_attempts: got %v", dlq.ApplicationProperties["dlq_attempts"])
	}
}

func TestBuildMessageInjectsTraceContext(t *testing.T) {
	t.Parallel()

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	msg, err := buildMessage(ctx, model.Change{FeedURL: "f", ItemID: "i", Kind: model.ChangeNew})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	tp, ok := msg.ApplicationProperties["traceparent"].(string)
	if !ok || !strings.Contains(tp, "0123456789abcdef0123456789abcdef") {
		t.Errorf("expected traceparent carrying the trace id, got %v", msg.ApplicationProperties["traceparent"])
	}
}
