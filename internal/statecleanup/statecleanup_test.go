package statecleanup_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/statecleanup"
)

type fakePruner struct {
	mu      sync.Mutex
	cutoffs []time.Time
}

func (f *fakePruner) PruneItemsBefore(_ context.Context, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cutoffs = append(f.cutoffs, cutoff)
	return 1, nil
}

func (f *fakePruner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cutoffs)
}

func TestRunSweepsImmediatelyThenOnTick(t *testing.T) {
	p := &fakePruner{}
	var results int
	var rmu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		statecleanup.Run(ctx, 20*time.Millisecond, time.Hour, p, func(removed int64, err error) {
			rmu.Lock()
			results++
			rmu.Unlock()
		})
		close(done)
	}()

	// Immediate sweep + at least one tick within ~70ms.
	time.Sleep(70 * time.Millisecond)
	cancel()
	<-done

	if p.count() < 2 {
		t.Fatalf("sweeps = %d, want >= 2 (immediate + tick)", p.count())
	}
	// Cutoff must be ~now-ttl (an hour in the past), proving ttl is applied.
	if d := time.Since(p.cutoffs[0]); d < 50*time.Minute || d > 70*time.Minute {
		t.Fatalf("first cutoff age = %v, want ~1h", d)
	}
	rmu.Lock()
	defer rmu.Unlock()
	if results < 2 {
		t.Fatalf("onResult calls = %d, want >= 2", results)
	}
}

func TestRunReturnsImmediatelyWhenDisabled(t *testing.T) {
	p := &fakePruner{}
	statecleanup.Run(context.Background(), 0, time.Hour, p, nil) // interval<=0
	statecleanup.Run(context.Background(), time.Hour, 0, p, nil) // ttl<=0
	if p.count() != 0 {
		t.Fatalf("sweeps = %d, want 0 when disabled", p.count())
	}
}
