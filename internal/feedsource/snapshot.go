package feedsource

import (
	"context"
	"fmt"

	"github.com/iambod/rss2msg/internal/config"
)

// Snapshot reads every source once, in precedence order, and returns the merged,
// deduped desired feed list — earlier sources win on URL collision, matching
// Aggregator.Desired. Unlike Aggregator it starts no goroutines and keeps no
// last-known-good state, so it is the right primitive for the one-shot execution
// modes (run-once, lambda), where the feed list is resolved exactly once per run.
//
// A source error aborts the snapshot rather than falling back to stale data: a
// scheduled job should fail loudly (and be retried) instead of silently polling a
// partial feed set. An empty merged result is a valid, non-error outcome.
func Snapshot(ctx context.Context, sources ...Source) ([]config.FeedConfig, error) {
	seen := make(map[string]struct{})
	var merged []config.FeedConfig
	for _, s := range sources {
		feeds, err := s.Feeds(ctx)
		if err != nil {
			return nil, fmt.Errorf("feed source %q: %w", s.Name(), err)
		}
		for _, fc := range feeds {
			if _, dup := seen[fc.URL]; dup {
				continue // earlier source already won this URL
			}
			seen[fc.URL] = struct{}{}
			merged = append(merged, fc)
		}
	}
	return merged, nil
}
