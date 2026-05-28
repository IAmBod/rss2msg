package telemetry

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/iambod/rss2msg/internal/config"
)

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
