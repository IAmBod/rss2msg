package heartbeat

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunFiresOnInterval(t *testing.T) {
	var count atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		Run(ctx, 10*time.Millisecond, func() { count.Add(1) })
		close(done)
	}()

	// Allow roughly 5 intervals to elapse, then stop.
	time.Sleep(55 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return promptly after context cancellation")
	}

	if got := count.Load(); got < 3 {
		t.Fatalf("expected at least 3 beats over ~5 intervals, got %d", got)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	var count atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		Run(ctx, time.Hour, func() { count.Add(1) })
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	if got := count.Load(); got != 0 {
		t.Fatalf("expected no beats before first interval, got %d", got)
	}
}

func TestRunReturnsImmediatelyForNonPositiveInterval(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		done := make(chan struct{})
		go func() {
			Run(context.Background(), d, func() { t.Errorf("emit should not be called for interval %v", d) })
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("Run did not return immediately for interval %v", d)
		}
	}
}
