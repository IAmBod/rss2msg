package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/iambod/rss2msg/internal/config"
)

type Telemetry struct {
	Logger zerolog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter

	shutdownFns []func(context.Context) error
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	for i := len(t.shutdownFns) - 1; i >= 0; i-- {
		if err := t.shutdownFns[i](ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Setup wires zerolog and OTEL providers per cfg. `out` is the writer the
// logger writes to (usually os.Stderr). Returns a Telemetry handle whose
// Shutdown must be called.
func Setup(ctx context.Context, cfg config.Config, out io.Writer) (*Telemetry, error) {
	t := &Telemetry{}
	t.Logger = buildLogger(cfg.Log, out)

	// Install the W3C TraceContext + Baggage propagator so that span context
	// can actually be injected into outbound carriers (e.g. Kafka headers).
	// Without this the OTEL global propagator is a no-op composite and
	// traceparent headers would never be produced.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithAttributes(semconv.ServiceName(serviceName(cfg))),
	)
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}

	if cfg.Telemetry.Traces.Enabled && hasOTLPEndpoint() {
		tp, err := newTracerProvider(ctx, res)
		if err != nil {
			return nil, err
		}
		otel.SetTracerProvider(tp)
		t.shutdownFns = append(t.shutdownFns, tp.Shutdown)
	}

	if cfg.Telemetry.Metrics.Enabled {
		mp, shutdown, err := newMeterProvider(ctx, cfg, res)
		if err != nil {
			return nil, err
		}
		otel.SetMeterProvider(mp)
		t.shutdownFns = append(t.shutdownFns, shutdown)
	}

	t.Tracer = otel.Tracer("github.com/iambod/rss2msg")
	t.Meter = otel.Meter("github.com/iambod/rss2msg")

	return t, nil
}

var zerologOnce sync.Once

func buildLogger(c config.LogConfig, out io.Writer) zerolog.Logger {
	zerologOnce.Do(func() {
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	})
	lvl, err := zerolog.ParseLevel(c.Level)
	if err != nil || lvl == zerolog.NoLevel {
		lvl = zerolog.InfoLevel
	}
	w := out
	if c.Format == "console" {
		w = zerolog.ConsoleWriter{Out: out}
	}
	return zerolog.New(w).Level(lvl).Hook(otelLogHook{}).With().Timestamp().Logger()
}

// otelLogHook decorates every log event whose ctx carries a valid OTEL span
// with trace_id and span_id fields, enabling cross-correlation between logs
// and traces. Callers attach a ctx via Event.Ctx(ctx) or by retrieving the
// per-context logger with zerolog.Ctx(ctx).
type otelLogHook struct{}

func (otelLogHook) Run(e *zerolog.Event, _ zerolog.Level, _ string) {
	ctx := e.GetCtx()
	if ctx == nil {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return
	}
	e.Str("trace_id", sc.TraceID().String()).Str("span_id", sc.SpanID().String())
}

func serviceName(cfg config.Config) string {
	if cfg.Telemetry.ServiceName != "" {
		return cfg.Telemetry.ServiceName
	}
	if v := os.Getenv("OTEL_SERVICE_NAME"); v != "" {
		return v
	}
	return "rss2msg"
}

func hasOTLPEndpoint() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
}

func newTracerProvider(ctx context.Context, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	return tp, nil
}

func newMeterProvider(ctx context.Context, cfg config.Config, res *resource.Resource) (*sdkmetric.MeterProvider, func(context.Context) error, error) {
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	var stopHTTP func(context.Context) error

	if hasOTLPEndpoint() {
		exp, err := otlpmetricgrpc.New(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("otlp metric exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}

	if cfg.Telemetry.Prometheus.Enabled {
		// Use a dedicated prometheus.Registry rather than the process-global
		// DefaultRegisterer so the /metrics handler only exposes OTEL meters
		// (and we don't depend on incidental global registrations).
		reg := prometheus.NewRegistry()
		promReader, err := promexp.New(promexp.WithRegisterer(reg))
		if err != nil {
			return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(promReader))
		stopHTTP, err = startPrometheusServer(cfg.Telemetry.Prometheus.Listen, reg)
		if err != nil {
			return nil, nil, err
		}
	}

	mp := sdkmetric.NewMeterProvider(opts...)
	shutdown := func(ctx context.Context) error {
		errs := []error{mp.Shutdown(ctx)}
		if stopHTTP != nil {
			errs = append(errs, stopHTTP(ctx))
		}
		return errors.Join(errs...)
	}
	return mp, shutdown, nil
}

func startPrometheusServer(listen string, gatherer prometheus.Gatherer) (func(context.Context) error, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", prometheusHTTPHandler(gatherer))
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("prometheus listen %q: %w", listen, err)
	}
	srv := &http.Server{Handler: mux}
	go func() {
		_ = srv.Serve(ln)
	}()
	return srv.Shutdown, nil
}

// Instruments groups the OTEL meters the pipeline uses. They are no-ops if
// metrics are disabled in Setup. Duration histograms record values in
// milliseconds — callers should pass float64(time.Since(start))/float64(time.Millisecond)
// (or equivalent) to preserve sub-ms precision.
type Instruments struct {
	FeedFetches         metric.Int64Counter
	FeedChanges         metric.Int64Counter
	SinkPublishFailures metric.Int64Counter
	PollSkipped         metric.Int64Counter
	FeedFetchDuration   metric.Float64Histogram
	SinkPublishDuration metric.Float64Histogram
}

func NewInstruments(meter metric.Meter) (Instruments, error) {
	var i Instruments
	var err error
	if i.FeedFetches, err = meter.Int64Counter("feed.fetches"); err != nil {
		return i, err
	}
	if i.FeedChanges, err = meter.Int64Counter("feed.changes"); err != nil {
		return i, err
	}
	if i.SinkPublishFailures, err = meter.Int64Counter("sink.publish.failures"); err != nil {
		return i, err
	}
	if i.PollSkipped, err = meter.Int64Counter("feed.poll.skipped"); err != nil {
		return i, err
	}
	if i.FeedFetchDuration, err = meter.Float64Histogram("feed.fetch.duration", metric.WithUnit("ms")); err != nil {
		return i, err
	}
	if i.SinkPublishDuration, err = meter.Float64Histogram("sink.publish.duration", metric.WithUnit("ms")); err != nil {
		return i, err
	}
	return i, nil
}
