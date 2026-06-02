package telemetry

import (
	"bytes"
	"context"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/iambod/rss2msg/internal/config"
)

func TestInstanceIDResolution(t *testing.T) {
	t.Run("config value wins", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_INSTANCE_ID", "from-env")
		cfg := config.Defaults()
		cfg.Telemetry.InstanceID = "from-config"
		if got := instanceID(cfg); got != "from-config" {
			t.Fatalf("instanceID = %q, want from-config", got)
		}
	})
	t.Run("env when config empty", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_INSTANCE_ID", "from-env")
		cfg := config.Defaults()
		cfg.Telemetry.InstanceID = ""
		if got := instanceID(cfg); got != "from-env" {
			t.Fatalf("instanceID = %q, want from-env", got)
		}
	})
	t.Run("hostname fallback", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_INSTANCE_ID", "")
		cfg := config.Defaults()
		cfg.Telemetry.InstanceID = ""
		if got := instanceID(cfg); got == "" {
			t.Fatal("instanceID fell back to empty; want non-empty hostname")
		}
	})
}

func TestSetupReturnsShutdownAndLogger(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Log.Format = "json"
	buf := &bytes.Buffer{}
	tel, err := Setup(context.Background(), cfg, buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	tel.Logger.Info().Str("k", "v").Msg("hello")
	if !strings.Contains(buf.String(), `"k":"v"`) || !strings.Contains(buf.String(), `"message":"hello"`) {
		t.Fatalf("expected zerolog JSON output, got %q", buf.String())
	}
	if tel.Tracer == nil || tel.Meter == nil {
		t.Fatalf("expected tracer and meter to be non-nil even with telemetry disabled")
	}
}

func TestSetupConsoleFormat(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Log.Format = "console"
	buf := &bytes.Buffer{}
	tel, err := Setup(context.Background(), cfg, buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	tel.Logger.Info().Msg("hi")
	out := buf.String()
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Fatalf("expected human-readable console output, got %q", out)
	}
}

func TestLoggerCarriesTraceIDFromContext(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Log.Format = "json"
	buf := &bytes.Buffer{}
	tel, err := Setup(context.Background(), cfg, buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	// Force a real (non-noop) tracer provider so SpanContext is valid.
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("test")

	ctx, span := tracer.Start(context.Background(), "test.span")
	defer span.End()

	tel.Logger.Info().Ctx(ctx).Msg("hello")
	out := buf.String()
	if !strings.Contains(out, `"trace_id"`) || !strings.Contains(out, `"span_id"`) {
		t.Fatalf("expected trace_id and span_id in log line, got %q", out)
	}
	if !strings.Contains(out, span.SpanContext().TraceID().String()) {
		t.Fatalf("expected trace_id %s in log line, got %q", span.SpanContext().TraceID().String(), out)
	}
}

func TestNewInstrumentsAllCountersAndHistograms(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	tel, err := Setup(context.Background(), cfg, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	instr, err := NewInstruments(tel.Meter)
	if err != nil {
		t.Fatal(err)
	}
	if instr.FeedFetches == nil || instr.FeedChanges == nil ||
		instr.SinkPublishFailures == nil || instr.FeedFetchDuration == nil ||
		instr.SinkPublishDuration == nil {
		t.Fatalf("expected all instruments non-nil: %+v", instr)
	}
}
