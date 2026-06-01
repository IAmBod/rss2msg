package config

import (
	"strings"
	"testing"
)

func TestDefaultsSentryDisabledWithErrorLevel(t *testing.T) {
	t.Parallel()
	s := Defaults().Telemetry.Sentry
	if s.Enabled {
		t.Fatalf("expected sentry disabled by default")
	}
	if s.Level != "error" {
		t.Fatalf("expected default level %q, got %q", "error", s.Level)
	}
	if s.SampleRate != 1.0 {
		t.Fatalf("expected default sample_rate 1.0, got %v", s.SampleRate)
	}
	if s.TracesSampleRate != 0.0 {
		t.Fatalf("expected default traces_sample_rate 0.0, got %v", s.TracesSampleRate)
	}
}

func TestValidateAcceptsEnabledSentry(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Sentry.Enabled = true
	c.Telemetry.Sentry.DSN = "https://public@example.com/1"
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsSampleRateOutOfRange(t *testing.T) {
	t.Parallel()
	for _, v := range []float64{-0.1, 1.5} {
		c := goodCfg()
		c.Telemetry.Sentry.Enabled = true
		c.Telemetry.Sentry.SampleRate = v
		_, err := Validate(c)
		if err == nil || !strings.Contains(err.Error(), "sample_rate") {
			t.Fatalf("sample_rate=%v: expected sample_rate error, got %v", v, err)
		}
	}
}

func TestValidateRejectsTracesSampleRateOutOfRange(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Sentry.Enabled = true
	c.Telemetry.Sentry.TracesSampleRate = 2.0
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "traces_sample_rate") {
		t.Fatalf("expected traces_sample_rate error, got %v", err)
	}
}

func TestValidateRejectsBogusSentryLevel(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Sentry.Enabled = true
	c.Telemetry.Sentry.Level = "bogus"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "level") {
		t.Fatalf("expected level error, got %v", err)
	}
}

func TestValidateIgnoresSentryWhenDisabled(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Sentry.Enabled = false
	c.Telemetry.Sentry.SampleRate = 99 // out of range but should be ignored
	c.Telemetry.Sentry.Level = "bogus"
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected disabled sentry to skip validation, got %v", err)
	}
}
