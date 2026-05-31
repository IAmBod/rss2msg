// Package feed implements an RSS/Atom syndication sink: it keeps a rolling
// window of recent changes and serves them as RSS 2.0 and Atom 1.0 over HTTP.
package feed

import (
	"context"

	"github.com/iambod/rss2msg/internal/model"
)

// Store is the rolling window of recent changes the sink renders into a feed.
// Implementations dedup by (FeedURL, ItemID); an update overwrites in place and
// becomes the most-recent entry. Recent returns up to n changes, newest first.
type Store interface {
	Write(ctx context.Context, c model.Change) error
	Recent(ctx context.Context, n int) ([]model.Change, error)
	Ping(ctx context.Context) error
	Close() error
}

func validIdentifier(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
