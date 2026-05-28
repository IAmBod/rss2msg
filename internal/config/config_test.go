package config

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	t.Parallel()
	d := Defaults()
	if d.Log.Level != "info" || d.Log.Format != "json" {
		t.Fatalf("bad log defaults: %+v", d.Log)
	}
	if d.HTTP.Timeout != 30*time.Second {
		t.Fatalf("bad http timeout default: %v", d.HTTP.Timeout)
	}
	if d.Retry.MaxAttempts != 3 || d.Retry.BaseDelay != 500*time.Millisecond {
		t.Fatalf("bad retry defaults: %+v", d.Retry)
	}
	if d.Runtime.ShutdownDrainTimeout != 30*time.Second {
		t.Fatalf("bad shutdown default: %v", d.Runtime.ShutdownDrainTimeout)
	}
}
