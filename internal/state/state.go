package state

import (
	"context"
	"time"
)

type ItemState struct {
	ContentHash string
	LastSeenAt  time.Time
}

type FeedMeta struct {
	ETag         string
	LastModified time.Time
}

// Store tracks per-item seen-state (for new/updated detection) and per-feed
// HTTP cache validators (ETag, Last-Modified).
type Store interface {
	GetItem(ctx context.Context, feedURL, itemID string) (state ItemState, found bool, err error)
	UpsertItem(ctx context.Context, feedURL, itemID, hash string, seenAt time.Time) error

	GetFeedMeta(ctx context.Context, feedURL string) (meta FeedMeta, found bool, err error)
	UpsertFeedMeta(ctx context.Context, feedURL string, meta FeedMeta) error

	// Ping verifies the store is reachable. Used by validate-config.
	Ping(ctx context.Context) error
	Close() error
}
