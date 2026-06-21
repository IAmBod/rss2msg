package telemetry

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

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

func TestOTLPProtocol(t *testing.T) {
	t.Run("defaults to grpc when unset", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "")
		t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "")
		got, err := otlpProtocol("traces")
		if err != nil {
			t.Fatal(err)
		}
		if got != "grpc" {
			t.Fatalf("otlpProtocol(traces) = %q, want grpc", got)
		}
	})
	t.Run("general var selects http/protobuf", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "")
		t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "")
		got, err := otlpProtocol("metrics")
		if err != nil {
			t.Fatal(err)
		}
		if got != "http/protobuf" {
			t.Fatalf("otlpProtocol(metrics) = %q, want http/protobuf", got)
		}
	})
	t.Run("per-signal var overrides general", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
		t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "")
		got, err := otlpProtocol("traces")
		if err != nil {
			t.Fatal(err)
		}
		if got != "http/protobuf" {
			t.Fatalf("otlpProtocol(traces) = %q, want http/protobuf", got)
		}
	})
	t.Run("per-signal override scoped to its signal", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
		t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "")
		got, err := otlpProtocol("metrics")
		if err != nil {
			t.Fatal(err)
		}
		if got != "grpc" {
			t.Fatalf("otlpProtocol(metrics) = %q, want grpc (traces override must not leak)", got)
		}
	})
	t.Run("unrecognized value errors", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http")
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "")
		t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "")
		if _, err := otlpProtocol("traces"); err == nil {
			t.Fatal("expected error for unrecognized protocol, got nil")
		}
	})
}

func TestSetupBuildsHTTPProtobufExporters(t *testing.T) {
	// Selecting http/protobuf with an endpoint set must build the OTLP trace and
	// metric providers without error. OTEL exporters construct lazily (no dial on
	// New), so this stays hermetic — no server is started.
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

	cfg := config.Defaults()
	cfg.Log.Format = "json"
	tel, err := Setup(context.Background(), cfg, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Setup with http/protobuf: %v", err)
	}
	// Shutdown may error flushing to the unreachable endpoint; bound it and ignore.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tel.Shutdown(ctx)

	if tel.Tracer == nil || tel.Meter == nil {
		t.Fatal("expected tracer and meter to be non-nil")
	}
}

func TestSetupRejectsUnknownOTLPProtocol(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

	cfg := config.Defaults()
	if _, err := Setup(context.Background(), cfg, &bytes.Buffer{}); err == nil {
		t.Fatal("expected Setup to fail on unsupported OTLP protocol, got nil")
	}
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

func TestInstrumentsHasAssignmentMeters(t *testing.T) {
	mp := metricnoop.NewMeterProvider()
	in, err := NewInstruments(mp.Meter("test"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	if in.MembershipSize == nil || in.AssignedFeeds == nil || in.RebalanceEvents == nil {
		t.Fatal("expected MembershipSize, AssignedFeeds, RebalanceEvents to be initialized")
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
