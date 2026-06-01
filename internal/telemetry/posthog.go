package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	posthog "github.com/posthog/posthog-go"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"

	"github.com/iambod/rss2msg/internal/config"
)

// postHogShutdownTimeout bounds how long Shutdown waits for buffered PostHog
// events to drain when Close is called.
const postHogShutdownTimeout = 2 * time.Second

// errNoPostHogAPIKey signals that PostHog is enabled but no project API key
// could be resolved from config or the POSTHOG_API_KEY environment variable.
// Setup treats this as a graceful skip (warn + continue), not a fatal error.
var errNoPostHogAPIKey = errors.New("no posthog api key")

// postHogClient is the subset of posthog.Client the hook depends on, narrowed
// for testability (a fake can satisfy it without contacting PostHog).
type postHogClient interface {
	Enqueue(posthog.Message) error
	Close() error
}

// resolvePostHogAPIKey returns the configured project API key, falling back to
// the POSTHOG_API_KEY environment variable when the config field is empty.
func resolvePostHogAPIKey(c config.TelemetryPostHogConfig) string {
	if k := strings.TrimSpace(c.APIKey); k != "" {
		return k
	}
	return strings.TrimSpace(os.Getenv("POSTHOG_API_KEY"))
}

// setupPostHog initializes a PostHog client from cfg and returns a zerolog hook
// that forwards log events at or above cfg.Level, plus a shutdown func that
// flushes buffered events. It returns errNoPostHogAPIKey when no key resolves.
func setupPostHog(cfg config.TelemetryPostHogConfig) (zerolog.Hook, func(context.Context) error, error) {
	apiKey := resolvePostHogAPIKey(cfg)
	if apiKey == "" {
		return nil, nil, errNoPostHogAPIKey
	}

	level := strings.TrimSpace(cfg.Level)
	if level == "" {
		level = "error"
	}
	minLevel, err := zerolog.ParseLevel(strings.ToLower(level))
	if err != nil || minLevel == zerolog.NoLevel {
		return nil, nil, fmt.Errorf("parse posthog level %q: %w", cfg.Level, err)
	}

	client, err := posthog.NewWithConfig(apiKey, posthog.Config{
		Endpoint:        firstNonEmpty(cfg.Endpoint, os.Getenv("POSTHOG_ENDPOINT")),
		Interval:        cfg.FlushInterval,
		ShutdownTimeout: postHogShutdownTimeout,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("init posthog: %w", err)
	}

	distinctID := firstNonEmpty(cfg.DistinctID, hostname(), "rss2msg")
	shutdown := func(context.Context) error {
		return client.Close()
	}
	return posthogLogHook{client: client, minLevel: minLevel, distinctID: distinctID}, shutdown, nil
}

// posthogLogHook captures zerolog events at or above minLevel and forwards them
// to PostHog: events at error level and above become $exception events (Error
// Tracking) so they group as exceptions in PostHog; lower levels (reachable
// only when the threshold is lowered) become a "log" capture event. When the
// event carries an OTEL span context, trace_id/span_id are attached as event
// properties so PostHog events cross-link to traces (mirrors otelLogHook).
//
// zerolog hooks only expose the level and message — not structured fields or the
// err object — so the captured event carries the message string.
type posthogLogHook struct {
	client     postHogClient
	minLevel   zerolog.Level
	distinctID string
}

func (h posthogLogHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	// NoLevel/Disabled sort numerically above the real levels; never forward
	// them regardless of the configured threshold.
	if level == zerolog.NoLevel || level == zerolog.Disabled || level < h.minLevel {
		return
	}

	props := posthog.Properties{"level": level.String()}
	if ctx := e.GetCtx(); ctx != nil {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			props["trace_id"] = sc.TraceID().String()
			props["span_id"] = sc.SpanID().String()
		}
	}

	if level >= zerolog.ErrorLevel {
		_ = h.client.Enqueue(posthog.Exception{
			DistinctId: h.distinctID,
			Timestamp:  time.Now(),
			Properties: props,
			ExceptionList: []posthog.ExceptionItem{{
				Type:  "rss2msg." + level.String(),
				Value: msg,
			}},
		})
		return
	}

	props["message"] = msg
	_ = h.client.Enqueue(posthog.Capture{
		DistinctId: h.distinctID,
		Event:      "log",
		Timestamp:  time.Now(),
		Properties: props,
	})
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
