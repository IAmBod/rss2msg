package feedsource

import (
	"context"

	"github.com/iambod/rss2msg/internal/config"
)

// Compile-time assertion that *Static satisfies Source.
var _ Source = (*Static)(nil)

// Static is a source backed by a fixed feed list (e.g. the config feeds: block).
// Its contents never change at runtime, so Changes never fires.
type Static struct {
	name  string
	feeds []config.FeedConfig
	never chan struct{}
}

// NewStatic returns a Static source. The feeds slice is used as-is.
func NewStatic(name string, feeds []config.FeedConfig) *Static {
	return &Static{name: name, feeds: feeds, never: make(chan struct{})}
}

func (s *Static) Name() string { return s.name }

func (s *Static) Feeds(context.Context) ([]config.FeedConfig, error) {
	return s.feeds, nil
}

func (s *Static) Changes() <-chan struct{} { return s.never }
