package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
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

	// Resolve log-forwarding sinks (Sentry, PostHog) first so their hooks can be
	// attached to the logger we build below. A missing credential is non-fatal:
	// warn (once the logger exists) and skip.
	var logHooks []zerolog.Hook
	var setupWarnings []string

	if cfg.Telemetry.Sentry.Enabled {
		hook, shutdown, err := setupSentry(cfg.Telemetry.Sentry)
		switch {
		case errors.Is(err, errNoSentryDSN):
			setupWarnings = append(setupWarnings, "telemetry.sentry.enabled=true but no DSN resolved (set telemetry.sentry.dsn or SENTRY_DSN); Sentry disabled")
		case err != nil:
			return nil, err
		default:
			logHooks = append(logHooks, hook)
			t.shutdownFns = append(t.shutdownFns, shutdown)
		}
	}

	if cfg.Telemetry.PostHog.Enabled {
		hook, shutdown, err := setupPostHog(cfg.Telemetry.PostHog)
		switch {
		case errors.Is(err, errNoPostHogAPIKey):
			setupWarnings = append(setupWarnings, "telemetry.posthog.enabled=true but no API key resolved (set telemetry.posthog.api_key or POSTHOG_API_KEY); PostHog disabled")
		case err != nil:
			return nil, err
		default:
			logHooks = append(logHooks, hook)
			t.shutdownFns = append(t.shutdownFns, shutdown)
		}
	}

	if cfg.Telemetry.CloudWatch.Enabled && cfg.Telemetry.CloudWatch.Logs.Enabled {
		// Report shipment failures through a hookless logger on `out` so an
		// error never re-enters the CloudWatch hook and recurses.
		errLogger := zerolog.New(out).With().Timestamp().Logger()
		onErr := func(err error) {
			errLogger.Warn().Err(err).Msg("cloudwatch logs shipment failed")
		}
		hook, shutdown, err := setupCloudWatchLogs(ctx, cfg.Telemetry.CloudWatch, onErr)
		if err != nil {
			return nil, err
		}
		logHooks = append(logHooks, hook)
		t.shutdownFns = append(t.shutdownFns, shutdown)
	}

	t.Logger = buildLogger(cfg.Log, out, logHooks...)
	for _, w := range setupWarnings {
		t.Logger.Warn().Msg(w)
	}

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
		resource.WithAttributes(
			semconv.ServiceName(serviceName(cfg)),
			semconv.ServiceInstanceID(instanceID(cfg)),
		),
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

func buildLogger(c config.LogConfig, out io.Writer, extraHooks ...zerolog.Hook) zerolog.Logger {
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
	logger := zerolog.New(w).Level(lvl).Hook(otelLogHook{})
	for _, h := range extraHooks {
		if h != nil {
			logger = logger.Hook(h)
		}
	}
	return logger.With().Timestamp().Logger()
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

// instanceID resolves service.instance.id for the OTEL resource. Setting it is
// what keeps push-based metric exporters (CloudWatch, Graphite) from collapsing
// multiple replicas into one colliding series. Resolution order mirrors
// serviceName: explicit config, then OTEL_SERVICE_INSTANCE_ID, then hostname.
func instanceID(cfg config.Config) string {
	if cfg.Telemetry.InstanceID != "" {
		return cfg.Telemetry.InstanceID
	}
	if v := os.Getenv("OTEL_SERVICE_INSTANCE_ID"); v != "" {
		return v
	}
	return hostname()
}

func hasOTLPEndpoint() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
}

// otlpProtocol resolves the OTLP transport for a signal ("traces" or "metrics")
// from the standard OTEL env vars, with the per-signal
// OTEL_EXPORTER_OTLP_<SIGNAL>_PROTOCOL taking precedence over the general
// OTEL_EXPORTER_OTLP_PROTOCOL (per the OpenTelemetry specification's resolution
// order). It returns an error for any value other than "grpc" or "http/protobuf".
//
// The default when unset is "grpc". This deliberately deviates from the OTEL
// spec default ("http/protobuf") to preserve rss2msg's historical gRPC-only
// behavior; the HTTP transport is what Grafana Cloud's OTLP gateway requires.
func otlpProtocol(signal string) (string, error) {
	signalVar := "OTEL_EXPORTER_OTLP_" + strings.ToUpper(signal) + "_PROTOCOL"
	p := os.Getenv(signalVar)
	from := signalVar
	if p == "" {
		p = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
		from = "OTEL_EXPORTER_OTLP_PROTOCOL"
	}
	switch p {
	case "":
		return "grpc", nil
	case "grpc", "http/protobuf":
		return p, nil
	default:
		return "", fmt.Errorf("unsupported %s %q (want \"grpc\" or \"http/protobuf\")", from, p)
	}
}

func newTracerProvider(ctx context.Context, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	proto, err := otlpProtocol("traces")
	if err != nil {
		return nil, err
	}
	var exp *otlptrace.Exporter
	switch proto {
	case "http/protobuf":
		exp, err = otlptracehttp.New(ctx)
	default:
		exp, err = otlptracegrpc.New(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter (%s): %w", proto, err)
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
		proto, err := otlpProtocol("metrics")
		if err != nil {
			return nil, nil, err
		}
		var exp sdkmetric.Exporter
		switch proto {
		case "http/protobuf":
			exp, err = otlpmetrichttp.New(ctx)
		default:
			exp, err = otlpmetricgrpc.New(ctx)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("otlp metric exporter (%s): %w", proto, err)
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

	if cfg.Telemetry.Graphite.Enabled {
		exp, err := newGraphiteExporter(cfg.Telemetry.Graphite)
		if err != nil {
			return nil, nil, fmt.Errorf("graphite exporter: %w", err)
		}
		var ropts []sdkmetric.PeriodicReaderOption
		if cfg.Telemetry.Graphite.Interval > 0 {
			ropts = append(ropts, sdkmetric.WithInterval(cfg.Telemetry.Graphite.Interval))
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, ropts...)))
	}

	if cfg.Telemetry.CloudWatch.Enabled && cfg.Telemetry.CloudWatch.Metrics.Enabled {
		readerOpt, err := setupCloudWatchMetrics(ctx, cfg.Telemetry.CloudWatch)
		if err != nil {
			return nil, nil, fmt.Errorf("cloudwatch metrics exporter: %w", err)
		}
		opts = append(opts, readerOpt)
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
	PollOverran         metric.Int64Counter
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
	if i.PollOverran, err = meter.Int64Counter("feed.poll.overran"); err != nil {
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
