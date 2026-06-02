package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultsCloudWatchDisabledWithSaneDefaults(t *testing.T) {
	t.Parallel()
	cw := Defaults().Telemetry.CloudWatch
	if cw.Enabled {
		t.Fatalf("expected cloudwatch disabled by default")
	}
	if cw.Logs.Level != "info" {
		t.Fatalf("expected default logs level %q, got %q", "info", cw.Logs.Level)
	}
	if cw.Logs.BatchInterval != 5*time.Second {
		t.Fatalf("expected default batch_interval 5s, got %v", cw.Logs.BatchInterval)
	}
	if cw.Metrics.Namespace != "rss2msg" {
		t.Fatalf("expected default metrics namespace %q, got %q", "rss2msg", cw.Metrics.Namespace)
	}
	if cw.Metrics.Interval != 60*time.Second {
		t.Fatalf("expected default metrics interval 60s, got %v", cw.Metrics.Interval)
	}
}

func TestValidateAcceptsEnabledCloudWatchLogs(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.CloudWatch.Enabled = true
	c.Telemetry.CloudWatch.Logs.Enabled = true
	c.Telemetry.CloudWatch.Logs.LogGroup = "/rss2msg/app"
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsCloudWatchLogsWithoutLogGroup(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.CloudWatch.Enabled = true
	c.Telemetry.CloudWatch.Logs.Enabled = true
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "log_group") {
		t.Fatalf("expected log_group error, got %v", err)
	}
}

func TestValidateRejectsBogusCloudWatchLogsLevel(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.CloudWatch.Enabled = true
	c.Telemetry.CloudWatch.Logs.Enabled = true
	c.Telemetry.CloudWatch.Logs.LogGroup = "/rss2msg/app"
	c.Telemetry.CloudWatch.Logs.Level = "bogus"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "level") {
		t.Fatalf("expected level error, got %v", err)
	}
}

func TestValidateRejectsNegativeCloudWatchBatchInterval(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.CloudWatch.Enabled = true
	c.Telemetry.CloudWatch.Logs.Enabled = true
	c.Telemetry.CloudWatch.Logs.LogGroup = "/rss2msg/app"
	c.Telemetry.CloudWatch.Logs.BatchInterval = -1 * time.Second
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "batch_interval") {
		t.Fatalf("expected batch_interval error, got %v", err)
	}
}

func TestValidateRejectsNegativeCloudWatchMetricsInterval(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.CloudWatch.Enabled = true
	c.Telemetry.CloudWatch.Metrics.Enabled = true
	c.Telemetry.CloudWatch.Metrics.Interval = -1 * time.Second
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("expected interval error, got %v", err)
	}
}

func TestValidateWarnsWhenCloudWatchMetricsButSignalDisabled(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Metrics.Enabled = false
	c.Telemetry.CloudWatch.Enabled = true
	c.Telemetry.CloudWatch.Metrics.Enabled = true
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !containsSubstr(warnings, "telemetry.metrics.enabled=false") {
		t.Fatalf("expected warning about metrics signal disabled, got %v", warnings)
	}
}

func TestValidateWarnsWhenCloudWatchEnabledButNoSurface(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.CloudWatch.Enabled = true
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !containsSubstr(warnings, "neither") {
		t.Fatalf("expected warning that no surface is enabled, got %v", warnings)
	}
}

func TestValidateIgnoresCloudWatchWhenDisabled(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.CloudWatch.Enabled = false
	c.Telemetry.CloudWatch.Logs.Enabled = true // no log_group, but block disabled
	c.Telemetry.CloudWatch.Logs.Level = "bogus"
	c.Telemetry.CloudWatch.Metrics.Interval = -1 * time.Second
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected disabled cloudwatch to skip validation, got %v", err)
	}
}

func containsSubstr(haystack []string, needle string) bool {
	for _, h := range haystack {
		if strings.Contains(h, needle) {
			return true
		}
	}
	return false
}
