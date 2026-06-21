package main

import (
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

func TestEffectiveFetchRetryInheritsGlobal(t *testing.T) {
	global := config.RetryConfig{MaxAttempts: 3, BaseDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second}
	got := effectiveFetchRetry(global, config.RetryConfig{}) // per-feed empty
	if got.MaxAttempts != 3 || got.BaseDelay != 500*time.Millisecond || got.MaxDelay != 10*time.Second {
		t.Fatalf("expected global inherited, got %+v", got)
	}
	if got.Retryable == nil {
		t.Fatalf("Retryable predicate must be set")
	}
}

func TestEffectiveFetchRetryPerFeedOverrides(t *testing.T) {
	global := config.RetryConfig{MaxAttempts: 3, BaseDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second}
	perFeed := config.RetryConfig{MaxAttempts: 5} // only attempts overridden
	got := effectiveFetchRetry(global, perFeed)
	if got.MaxAttempts != 5 {
		t.Fatalf("expected per-feed max_attempts=5, got %d", got.MaxAttempts)
	}
	if got.BaseDelay != 500*time.Millisecond || got.MaxDelay != 10*time.Second {
		t.Fatalf("expected base/max inherited, got %+v", got)
	}
}
