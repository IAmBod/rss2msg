package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	sentry "github.com/getsentry/sentry-go"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"

	"github.com/iambod/rss2msg/internal/config"
)

// sentryFlushTimeout bounds how long Shutdown waits for buffered Sentry events
// to drain when the shutdown context carries no shorter deadline.
const sentryFlushTimeout = 2 * time.Second

// errNoSentryDSN signals that Sentry is enabled but no DSN could be resolved
// from config or the SENTRY_DSN environment variable. Setup treats this as a
// graceful skip (warn + continue), not a fatal error.
var errNoSentryDSN = errors.New("no sentry dsn")

// resolveSentryDSN returns the configured DSN, falling back to the SENTRY_DSN
// environment variable when the config field is empty.
func resolveSentryDSN(c config.TelemetrySentryConfig) string {
	if strings.TrimSpace(c.DSN) != "" {
		return c.DSN
	}
	return os.Getenv("SENTRY_DSN")
}

// setupSentry initializes the global Sentry SDK from cfg and returns a zerolog
// hook that forwards log events at or above cfg.Level, plus a shutdown func that
// flushes buffered events. It returns errNoSentryDSN when no DSN is resolvable.
func setupSentry(cfg config.TelemetrySentryConfig) (zerolog.Hook, func(context.Context) error, error) {
	dsn := resolveSentryDSN(cfg)
	if dsn == "" {
		return nil, nil, errNoSentryDSN
	}

	level := strings.TrimSpace(cfg.Level)
	if level == "" {
		level = "error"
	}
	minLevel, err := zerolog.ParseLevel(strings.ToLower(level))
	if err != nil || minLevel == zerolog.NoLevel {
		return nil, nil, fmt.Errorf("parse sentry level %q: %w", cfg.Level, err)
	}

	if err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Environment:      firstNonEmpty(cfg.Environment, os.Getenv("SENTRY_ENVIRONMENT")),
		Release:          firstNonEmpty(cfg.Release, os.Getenv("SENTRY_RELEASE")),
		ServerName:       cfg.ServerName,
		SampleRate:       cfg.SampleRate,
		TracesSampleRate: cfg.TracesSampleRate,
		Debug:            cfg.Debug,
	}); err != nil {
		return nil, nil, fmt.Errorf("init sentry: %w", err)
	}

	shutdown := func(ctx context.Context) error {
		timeout := sentryFlushTimeout
		if dl, ok := ctx.Deadline(); ok {
			if d := time.Until(dl); d > 0 && d < timeout {
				timeout = d
			}
		}
		sentry.Flush(timeout)
		return nil
	}
	return sentryLogHook{minLevel: minLevel}, shutdown, nil
}

// sentryLogHook captures zerolog events at or above minLevel as Sentry events.
// When the event carries an OTEL span context, trace_id/span_id are attached as
// tags so Sentry events cross-link to traces (mirrors otelLogHook).
//
// zerolog hooks only expose the level and message — not structured fields or the
// err object — so the captured event carries the message string.
type sentryLogHook struct {
	minLevel zerolog.Level
}

func (h sentryLogHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	// NoLevel/Disabled sort numerically above the real levels; never forward
	// them regardless of the configured threshold.
	if level == zerolog.NoLevel || level == zerolog.Disabled || level < h.minLevel {
		return
	}
	ev := sentry.NewEvent()
	ev.Level = sentryLevel(level)
	ev.Message = msg
	if ctx := e.GetCtx(); ctx != nil {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			ev.Tags["trace_id"] = sc.TraceID().String()
			ev.Tags["span_id"] = sc.SpanID().String()
		}
	}
	sentry.CaptureEvent(ev)
}

// sentryLevel maps a zerolog level to the corresponding Sentry severity.
func sentryLevel(l zerolog.Level) sentry.Level {
	switch l {
	case zerolog.TraceLevel, zerolog.DebugLevel:
		return sentry.LevelDebug
	case zerolog.WarnLevel:
		return sentry.LevelWarning
	case zerolog.ErrorLevel:
		return sentry.LevelError
	case zerolog.FatalLevel, zerolog.PanicLevel:
		return sentry.LevelFatal
	default:
		return sentry.LevelInfo
	}
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}
