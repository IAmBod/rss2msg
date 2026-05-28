package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoSucceedsFirstAttempt(t *testing.T) {
	t.Parallel()
	calls := 0
	res := Do(context.Background(), Config{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}, func(ctx context.Context) error {
		calls++
		return nil
	})
	if res.Err != nil || res.Attempts != 1 || calls != 1 {
		t.Fatalf("got %+v calls=%d", res, calls)
	}
}

func TestDoRetriesAndStops(t *testing.T) {
	t.Parallel()
	want := errors.New("boom")
	calls := 0
	res := Do(context.Background(), Config{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}, func(ctx context.Context) error {
		calls++
		return want
	})
	if !errors.Is(res.Err, want) {
		t.Fatalf("got err %v", res.Err)
	}
	if res.Attempts != 3 || calls != 3 {
		t.Fatalf("expected 3 attempts, got attempts=%d calls=%d", res.Attempts, calls)
	}
}

func TestDoRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	res := Do(ctx, Config{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: time.Second}, func(ctx context.Context) error {
		calls++
		return errors.New("nope")
	})
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", res.Err)
	}
	if res.Attempts != 0 {
		t.Fatalf("expected Attempts=0 when ctx pre-cancelled, got %d", res.Attempts)
	}
	if calls != 0 {
		t.Fatalf("expected fn to not be called when ctx pre-cancelled, got %d calls", calls)
	}
}

func TestDelayForGrowsAndCaps(t *testing.T) {
	t.Parallel()
	cfg := Config{MaxAttempts: 10, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond}
	for i := 1; i <= 10; i++ {
		d := delayFor(cfg, i)
		if d < 0 || d > cfg.MaxDelay*2 { // jitter can add up to delay itself
			t.Fatalf("attempt %d: delay %v out of expected range", i, d)
		}
	}
}
