package statecleanup_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/statecleanup"
)

type fakePruner struct {
	mu          sync.Mutex
	cutoffs     []time.Time
	metaCutoffs []time.Time
}

func (f *fakePruner) PruneItemsBefore(_ context.Context, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cutoffs = append(f.cutoffs, cutoff)
	return 1, nil
}

func (f *fakePruner) PruneFeedMetaBefore(_ context.Context, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metaCutoffs = append(f.metaCutoffs, cutoff)
	return 1, nil
}

func (f *fakePruner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cutoffs)
}

func (f *fakePruner) metaCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.metaCutoffs)
}

func TestRunSweepsImmediatelyThenOnTick(t *testing.T) {
	p := &fakePruner{}
	var results int
	var totalRemoved int64
	var rmu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		statecleanup.Run(ctx, 20*time.Millisecond, time.Hour, p, func(removed int64, err error) {
			rmu.Lock()
			results++
			totalRemoved += removed
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
	// feed_meta must also be pruned each sweep with the same cutoff.
	if p.metaCount() < 2 {
		t.Fatalf("meta sweeps = %d, want >= 2", p.metaCount())
	}
	rmu.Lock()
	defer rmu.Unlock()
	if results < 2 {
		t.Fatalf("onResult calls = %d, want >= 2", results)
	}
	// Each sweep reports nItems+nMeta = 1+1 = 2; with >= 2 sweeps, total >= 4.
	if totalRemoved < 4 {
		t.Fatalf("totalRemoved = %d, want >= 4 (2 per sweep × >= 2 sweeps)", totalRemoved)
	}

	// Verify that items and feed_meta were pruned with the same cutoff on each sweep.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cutoffs[0] != p.metaCutoffs[0] {
		t.Fatalf("item and feed_meta pruned with different cutoffs: %v vs %v", p.cutoffs[0], p.metaCutoffs[0])
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
