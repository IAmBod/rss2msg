package feedsource

import (
	"context"
	"sync"

	"github.com/iambod/rss2msg/internal/config"
)

// Aggregator merges the feeds from an ordered list of sources into one desired
// set. Order is precedence: earlier sources win on URL collision. A source that
// errors keeps its last successful contribution (last-known-good). An empty
// merged set is a valid result, not an error.
type Aggregator struct {
	sources []Source
	out     chan struct{}

	mu       sync.Mutex
	lastGood map[string][]config.FeedConfig // source name -> last successful feeds
}

// NewAggregator builds an Aggregator over sources in precedence order and starts
// forwarding each source's Changes onto the aggregator's own channel. The
// per-source forward goroutines run for the lifetime of the process (they exit
// when a source closes its Changes channel); this is intentional for a
// long-lived daemon.
func NewAggregator(sources ...Source) *Aggregator {
	a := &Aggregator{
		sources:  sources,
		out:      make(chan struct{}, 1),
		lastGood: make(map[string][]config.FeedConfig),
	}
	for _, s := range sources {
		go a.forward(s.Changes())
	}
	return a
}

func (a *Aggregator) forward(ch <-chan struct{}) {
	for range ch {
		a.Trigger()
	}
}

// Trigger asks the consumer to re-read Desired. Non-blocking and coalescing:
// if a signal is already pending it is dropped (a single reconcile reads the
// latest state anyway). Used to fan in source signals and to drive SIGHUP.
func (a *Aggregator) Trigger() {
	select {
	case a.out <- struct{}{}:
	default:
	}
}

// Changes signals when the desired set may have changed.
func (a *Aggregator) Changes() <-chan struct{} { return a.out }

// Desired fetches every source concurrently, then merges their results in source
// order into the deduped feed list. The mutex is held across the whole call to
// serialise reconciles (the daemon runs a single reconcile loop); the fan-out
// parallelises the individual Feeds calls within one reconcile, so startup and
// reload latency is the slowest source rather than the sum of all sources.
func (a *Aggregator) Desired(ctx context.Context) ([]config.FeedConfig, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Fetch concurrently. Each goroutine writes its own slot, so no result needs
	// locking; precedence is restored by merging in source order below.
	type fetched struct {
		feeds []config.FeedConfig
		err   error
	}
	results := make([]fetched, len(a.sources))
	var wg sync.WaitGroup
	for i, s := range a.sources {
		wg.Add(1)
		go func(i int, s Source) {
			defer wg.Done()
			feeds, err := s.Feeds(ctx)
			results[i] = fetched{feeds: feeds, err: err}
		}(i, s)
	}
	wg.Wait()

	seen := make(map[string]struct{})
	var merged []config.FeedConfig
	for i, s := range a.sources {
		feeds := results[i].feeds
		if results[i].err != nil {
			feeds = a.lastGood[s.Name()] // keep last-known-good; nil if never succeeded
		} else {
			a.lastGood[s.Name()] = feeds
		}
		for _, fc := range feeds {
			if _, dup := seen[fc.URL]; dup {
				continue // dedup by URL; earlier source already won
			}
			seen[fc.URL] = struct{}{}
			merged = append(merged, fc)
		}
	}
	return merged, nil
}
