package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultsPostHogDisabledWithErrorLevel(t *testing.T) {
	t.Parallel()
	p := Defaults().Telemetry.PostHog
	if p.Enabled {
		t.Fatalf("expected posthog disabled by default")
	}
	if p.Level != "error" {
		t.Fatalf("expected default level %q, got %q", "error", p.Level)
	}
	if p.Endpoint != "https://us.i.posthog.com" {
		t.Fatalf("expected default endpoint, got %q", p.Endpoint)
	}
}

func TestValidateAcceptsEnabledPostHog(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.PostHog.Enabled = true
	c.Telemetry.PostHog.APIKey = "phc_123"
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsBogusPostHogLevel(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.PostHog.Enabled = true
	c.Telemetry.PostHog.Level = "bogus"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "level") {
		t.Fatalf("expected level error, got %v", err)
	}
}

func TestValidateRejectsNegativePostHogFlushInterval(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.PostHog.Enabled = true
	c.Telemetry.PostHog.FlushInterval = -1 * time.Second
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "flush_interval") {
		t.Fatalf("expected flush_interval error, got %v", err)
	}
}

func TestValidateIgnoresPostHogWhenDisabled(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.PostHog.Enabled = false
	c.Telemetry.PostHog.Level = "bogus"
	c.Telemetry.PostHog.FlushInterval = -1 * time.Second
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected disabled posthog to skip validation, got %v", err)
	}
}
