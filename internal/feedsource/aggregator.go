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
// forwarding each source's Changes onto the aggregator's own channel.
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

// Desired reads every source in order and returns the merged, deduped feed list.
func (a *Aggregator) Desired(ctx context.Context) ([]config.FeedConfig, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	seen := make(map[string]struct{})
	var merged []config.FeedConfig
	for _, s := range a.sources {
		feeds, err := s.Feeds(ctx)
		if err != nil {
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
