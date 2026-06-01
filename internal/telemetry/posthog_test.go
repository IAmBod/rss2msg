package telemetry

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	posthog "github.com/posthog/posthog-go"
	"github.com/rs/zerolog"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/iambod/rss2msg/internal/config"
)

// fakePostHog records enqueued messages in memory so tests can assert what the
// hook produced without contacting PostHog.
type fakePostHog struct {
	mu     sync.Mutex
	msgs   []posthog.Message
	closed bool
}

func (f *fakePostHog) Enqueue(m posthog.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, m)
	return nil
}

func (f *fakePostHog) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakePostHog) captured() []posthog.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]posthog.Message(nil), f.msgs...)
}

func TestPostHogHookCapturesErrorAsException(t *testing.T) {
	f := &fakePostHog{}
	hook := posthogLogHook{client: f, minLevel: zerolog.ErrorLevel, distinctID: "node-1"}

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Error().Msg("boom")

	msgs := f.captured()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 enqueued message, got %d", len(msgs))
	}
	ex, ok := msgs[0].(posthog.Exception)
	if !ok {
		t.Fatalf("expected posthog.Exception, got %T", msgs[0])
	}
	if ex.DistinctId != "node-1" {
		t.Fatalf("expected distinct id %q, got %q", "node-1", ex.DistinctId)
	}
	if len(ex.ExceptionList) != 1 || ex.ExceptionList[0].Value != "boom" {
		t.Fatalf("expected exception value %q, got %+v", "boom", ex.ExceptionList)
	}
}

func TestPostHogHookSkipsBelowThreshold(t *testing.T) {
	f := &fakePostHog{}
	hook := posthogLogHook{client: f, minLevel: zerolog.ErrorLevel, distinctID: "node-1"}

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Info().Msg("just info")
	logger.Warn().Msg("a warning")

	if msgs := f.captured(); len(msgs) != 0 {
		t.Fatalf("expected no messages below threshold, got %d", len(msgs))
	}
}

func TestPostHogHookForwardsLowerLevelAsCapture(t *testing.T) {
	f := &fakePostHog{}
	hook := posthogLogHook{client: f, minLevel: zerolog.WarnLevel, distinctID: "node-1"}

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Warn().Msg("heads up")

	msgs := f.captured()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	c, ok := msgs[0].(posthog.Capture)
	if !ok {
		t.Fatalf("expected posthog.Capture, got %T", msgs[0])
	}
	if c.Event != "log" {
		t.Fatalf("expected event %q, got %q", "log", c.Event)
	}
	if c.Properties["message"] != "heads up" {
		t.Fatalf("expected message property %q, got %v", "heads up", c.Properties["message"])
	}
	if c.Properties["level"] != "warn" {
		t.Fatalf("expected level property %q, got %v", "warn", c.Properties["level"])
	}
}

func TestPostHogHookAttachesTraceTags(t *testing.T) {
	f := &fakePostHog{}
	hook := posthogLogHook{client: f, minLevel: zerolog.ErrorLevel, distinctID: "node-1"}

	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "s")
	defer span.End()

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Error().Ctx(ctx).Msg("with trace")

	msgs := f.captured()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	ex := msgs[0].(posthog.Exception)
	want := span.SpanContext().TraceID().String()
	if got := ex.Properties["trace_id"]; got != want {
		t.Fatalf("expected trace_id %q, got %v", want, got)
	}
	if ex.Properties["span_id"] == nil {
		t.Fatalf("expected span_id property to be set")
	}
}

func TestSetupPostHogReturnsErrNoAPIKeyWhenUnset(t *testing.T) {
	t.Setenv("POSTHOG_API_KEY", "")
	_, _, err := setupPostHog(config.TelemetryPostHogConfig{Enabled: true, Level: "error"})
	if !errors.Is(err, errNoPostHogAPIKey) {
		t.Fatalf("expected errNoPostHogAPIKey, got %v", err)
	}
}

func TestSetupGracefulWhenPostHogEnabledWithoutAPIKey(t *testing.T) {
	t.Setenv("POSTHOG_API_KEY", "")
	cfg := config.Defaults()
	cfg.Telemetry.PostHog.Enabled = true
	buf := &bytes.Buffer{}
	tel, err := Setup(context.Background(), cfg, buf)
	if err != nil {
		t.Fatalf("expected graceful setup, got %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()
	if !bytes.Contains(buf.Bytes(), []byte("no API key resolved")) {
		t.Fatalf("expected warning about missing API key, got %q", buf.String())
	}
}
