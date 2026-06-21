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

func TestDoStopsOnNonRetryableError(t *testing.T) {
	stop := errors.New("permanent")
	calls := 0
	res := Do(context.Background(), Config{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		Retryable:   func(error) bool { return false },
	}, func(context.Context) error {
		calls++
		return stop
	})
	if calls != 1 {
		t.Fatalf("expected fn called once, got %d", calls)
	}
	if res.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", res.Attempts)
	}
	if !errors.Is(res.Err, stop) {
		t.Fatalf("expected stop error, got %v", res.Err)
	}
}

func TestDoRetriesWhenPredicateAllows(t *testing.T) {
	calls := 0
	res := Do(context.Background(), Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		Retryable:   func(error) bool { return true },
	}, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if res.Err != nil {
		t.Fatalf("expected success, got %v", res.Err)
	}
}

func TestDoNilPredicateRetriesAll(t *testing.T) {
	calls := 0
	res := Do(context.Background(), Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
	}, func(context.Context) error {
		calls++
		return errors.New("boom")
	})
	if calls != 3 {
		t.Fatalf("expected 3 calls (nil predicate retries all), got %d", calls)
	}
	if res.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", res.Attempts)
	}
}
