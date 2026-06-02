package dapr

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/iambod/rss2msg/internal/model"
)

// capture records what the publish seam was called with.
type capture struct {
	pubsubName  string
	topic       string
	data        []byte
	contentType string
	metadata    map[string]string
	calls       int
}

// newCapturing builds a Publisher whose publish seam records into c and returns
// retErr. It bypasses the real Dapr client so the unit tests need no sidecar.
func newCapturing(t *testing.T, opts Options, c *capture, retErr error) *Publisher {
	t.Helper()
	p, err := newPublisher(opts)
	if err != nil {
		t.Fatalf("newPublisher: %v", err)
	}
	p.publish = func(_ context.Context, pubsubName, topic string, data []byte, contentType string, metadata map[string]string) error {
		c.calls++
		c.pubsubName = pubsubName
		c.topic = topic
		c.data = data
		c.contentType = contentType
		c.metadata = metadata
		return retErr
	}
	return p
}

func sampleChange() model.Change {
	pub := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	return model.Change{
		FeedURL:     "https://example.com/feed.xml",
		FeedTitle:   "Example",
		ItemID:      "item-1",
		Kind:        model.ChangeNew,
		Title:       "Hello",
		Link:        "https://example.com/1",
		PublishedAt: &pub,
		ContentHash: "abc123",
	}
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{"missing name", Options{PubsubName: "ps", Topic: "t"}},
		{"missing pubsub_name", Options{Name: "n", Topic: "t"}},
		{"missing topic", Options{Name: "n", PubsubName: "ps"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(context.Background(), tc.opts); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestPublishSendsToConfiguredTopic(t *testing.T) {
	var cap capture
	p := newCapturing(t, Options{Name: "out", PubsubName: "rss-pubsub", Topic: "rss.changes"}, &cap, nil)

	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if cap.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", cap.calls)
	}
	if cap.pubsubName != "rss-pubsub" {
		t.Errorf("pubsubName = %q, want %q", cap.pubsubName, "rss-pubsub")
	}
	if cap.topic != "rss.changes" {
		t.Errorf("topic = %q, want %q", cap.topic, "rss.changes")
	}
	if cap.contentType != "application/json" {
		t.Errorf("contentType = %q, want application/json", cap.contentType)
	}
}

func TestPublishBodyIsChangeJSON(t *testing.T) {
	var cap capture
	p := newCapturing(t, Options{Name: "out", PubsubName: "ps", Topic: "t"}, &cap, nil)
	change := sampleChange()

	if err := p.Publish(context.Background(), change); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var got model.Change
	if err := json.Unmarshal(cap.data, &got); err != nil {
		t.Fatalf("unmarshal published body: %v", err)
	}
	if got.ItemID != change.ItemID || got.FeedURL != change.FeedURL || got.Title != change.Title {
		t.Errorf("round-tripped change = %+v, want fields from %+v", got, change)
	}
	if got.SchemaVersion != model.SchemaVersion {
		t.Errorf("schema_version = %d, want %d", got.SchemaVersion, model.SchemaVersion)
	}
}

func TestPublishMetadataIncludesRoutingFields(t *testing.T) {
	var cap capture
	p := newCapturing(t, Options{Name: "out", PubsubName: "ps", Topic: "t"}, &cap, nil)

	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	want := map[string]string{
		"feed_url":       "https://example.com/feed.xml",
		"kind":           "new",
		"schema_version": "0",
	}
	for k, v := range want {
		if cap.metadata[k] != v {
			t.Errorf("metadata[%q] = %q, want %q", k, cap.metadata[k], v)
		}
	}
}

func TestPublishMergesStaticMetadata(t *testing.T) {
	var cap capture
	p := newCapturing(t, Options{
		Name: "out", PubsubName: "ps", Topic: "t",
		Metadata: map[string]string{"priority": "high", "region": "eu"},
	}, &cap, nil)

	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if cap.metadata["priority"] != "high" || cap.metadata["region"] != "eu" {
		t.Errorf("static metadata not propagated: %v", cap.metadata)
	}
	// Routing fields still present alongside static ones.
	if cap.metadata["feed_url"] == "" {
		t.Errorf("routing metadata dropped when static metadata set: %v", cap.metadata)
	}
}

func TestPublishAddsDLQMetadata(t *testing.T) {
	var cap capture
	p := newCapturing(t, Options{Name: "out", PubsubName: "ps", Topic: "t"}, &cap, nil)
	change := sampleChange()
	change.DLQFromSink = "primary"
	change.DLQError = "boom"
	change.DLQAttempts = 3

	if err := p.Publish(context.Background(), change); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if cap.metadata["dlq_from_sink"] != "primary" {
		t.Errorf("dlq_from_sink = %q, want primary", cap.metadata["dlq_from_sink"])
	}
	if cap.metadata["dlq_error"] != "boom" {
		t.Errorf("dlq_error = %q, want boom", cap.metadata["dlq_error"])
	}
	if cap.metadata["dlq_attempts"] != "3" {
		t.Errorf("dlq_attempts = %q, want 3", cap.metadata["dlq_attempts"])
	}
}

func TestPublishOmitsDLQMetadataWhenNotDLQ(t *testing.T) {
	var cap capture
	p := newCapturing(t, Options{Name: "out", PubsubName: "ps", Topic: "t"}, &cap, nil)

	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if _, ok := cap.metadata["dlq_from_sink"]; ok {
		t.Errorf("dlq_from_sink present for non-DLQ change: %v", cap.metadata)
	}
}

func TestPublishInjectsTraceContext(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator()) })

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "publish")
	defer span.End()
	if !span.SpanContext().IsValid() {
		t.Skip("no valid span context")
	}

	var cap capture
	p := newCapturing(t, Options{Name: "out", PubsubName: "ps", Topic: "t"}, &cap, nil)
	if err := p.Publish(ctx, sampleChange()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if cap.metadata["traceparent"] == "" {
		t.Errorf("traceparent not injected: %v", cap.metadata)
	}
	var _ trace.SpanContext // keep trace import meaningful
}

func TestPublishWrapsError(t *testing.T) {
	var cap capture
	wantErr := errors.New("sidecar down")
	p := newCapturing(t, Options{Name: "out", PubsubName: "ps", Topic: "t"}, &cap, wantErr)

	err := p.Publish(context.Background(), sampleChange())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error %v does not wrap %v", err, wantErr)
	}
}

func TestName(t *testing.T) {
	var cap capture
	p := newCapturing(t, Options{Name: "my-sink", PubsubName: "ps", Topic: "t"}, &cap, nil)
	if p.Name() != "my-sink" {
		t.Errorf("Name() = %q, want my-sink", p.Name())
	}
}

func TestClose(t *testing.T) {
	var cap capture
	p := newCapturing(t, Options{Name: "out", PubsubName: "ps", Topic: "t"}, &cap, nil)
	closed := false
	p.closeFn = func() error { closed = true; return nil }
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !closed {
		t.Error("Close did not invoke closeFn")
	}
}
