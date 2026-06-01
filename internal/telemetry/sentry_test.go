package telemetry

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sentry "github.com/getsentry/sentry-go"
	"github.com/rs/zerolog"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/iambod/rss2msg/internal/config"
)

// recordingTransport is a sentry.Transport that captures events in memory so
// tests can assert what the hook produced without contacting a server.
type recordingTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (r *recordingTransport) Configure(sentry.ClientOptions) {}
func (r *recordingTransport) SendEvent(e *sentry.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}
func (r *recordingTransport) Flush(time.Duration) bool              { return true }
func (r *recordingTransport) FlushWithContext(context.Context) bool { return true }
func (r *recordingTransport) Close()                                {}
func (r *recordingTransport) captured() []*sentry.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*sentry.Event(nil), r.events...)
}

// initSentryWithTransport initializes the global Sentry SDK with a recording
// transport for the duration of a test, restoring a no-op client afterwards.
func initSentryWithTransport(t *testing.T) *recordingTransport {
	t.Helper()
	rt := &recordingTransport{}
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:        "https://public@example.com/1",
		Transport:  rt,
		SampleRate: 1.0,
	}); err != nil {
		t.Fatalf("sentry.Init: %v", err)
	}
	t.Cleanup(func() { _ = sentry.Init(sentry.ClientOptions{}) })
	return rt
}

func TestSentryHookCapturesErrorEvent(t *testing.T) {
	rt := initSentryWithTransport(t)
	hook := sentryLogHook{minLevel: zerolog.ErrorLevel}

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Error().Msg("boom")
	sentry.Flush(time.Second)

	evs := rt.captured()
	if len(evs) != 1 {
		t.Fatalf("expected 1 captured event, got %d", len(evs))
	}
	if evs[0].Message != "boom" {
		t.Fatalf("expected message %q, got %q", "boom", evs[0].Message)
	}
	if evs[0].Level != sentry.LevelError {
		t.Fatalf("expected level %q, got %q", sentry.LevelError, evs[0].Level)
	}
}

func TestSentryHookSkipsBelowThreshold(t *testing.T) {
	rt := initSentryWithTransport(t)
	hook := sentryLogHook{minLevel: zerolog.ErrorLevel}

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Info().Msg("just info")
	logger.Warn().Msg("a warning")
	sentry.Flush(time.Second)

	if evs := rt.captured(); len(evs) != 0 {
		t.Fatalf("expected no events below threshold, got %d", len(evs))
	}
}

func TestSentryHookForwardsWarnWhenThresholdLowered(t *testing.T) {
	rt := initSentryWithTransport(t)
	hook := sentryLogHook{minLevel: zerolog.WarnLevel}

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Warn().Msg("heads up")
	sentry.Flush(time.Second)

	evs := rt.captured()
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	if evs[0].Level != sentry.LevelWarning {
		t.Fatalf("expected level %q, got %q", sentry.LevelWarning, evs[0].Level)
	}
}

func TestSentryHookAttachesTraceTags(t *testing.T) {
	rt := initSentryWithTransport(t)
	hook := sentryLogHook{minLevel: zerolog.ErrorLevel}

	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "s")
	defer span.End()

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Error().Ctx(ctx).Msg("with trace")
	sentry.Flush(time.Second)

	evs := rt.captured()
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	want := span.SpanContext().TraceID().String()
	if got := evs[0].Tags["trace_id"]; got != want {
		t.Fatalf("expected trace_id tag %q, got %q", want, got)
	}
	if evs[0].Tags["span_id"] == "" {
		t.Fatalf("expected span_id tag to be set")
	}
}

func TestSetupSentryReturnsErrNoDSNWhenUnset(t *testing.T) {
	t.Setenv("SENTRY_DSN", "")
	_, _, err := setupSentry(config.TelemetrySentryConfig{Enabled: true, Level: "error"})
	if !errors.Is(err, errNoSentryDSN) {
		t.Fatalf("expected errNoSentryDSN, got %v", err)
	}
}

func TestSetupGracefulWhenSentryEnabledWithoutDSN(t *testing.T) {
	t.Setenv("SENTRY_DSN", "")
	cfg := config.Defaults()
	cfg.Telemetry.Sentry.Enabled = true
	buf := &bytes.Buffer{}
	tel, err := Setup(context.Background(), cfg, buf)
	if err != nil {
		t.Fatalf("expected graceful setup, got %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()
	if !bytes.Contains(buf.Bytes(), []byte("no DSN resolved")) {
		t.Fatalf("expected warning about missing DSN, got %q", buf.String())
	}
}

func TestSentryLevelMapping(t *testing.T) {
	cases := map[zerolog.Level]sentry.Level{
		zerolog.TraceLevel: sentry.LevelDebug,
		zerolog.DebugLevel: sentry.LevelDebug,
		zerolog.InfoLevel:  sentry.LevelInfo,
		zerolog.WarnLevel:  sentry.LevelWarning,
		zerolog.ErrorLevel: sentry.LevelError,
		zerolog.FatalLevel: sentry.LevelFatal,
		zerolog.PanicLevel: sentry.LevelFatal,
	}
	for in, want := range cases {
		if got := sentryLevel(in); got != want {
			t.Errorf("sentryLevel(%v) = %q, want %q", in, got, want)
		}
	}
}
