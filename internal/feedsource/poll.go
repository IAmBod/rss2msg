package feedsource

import (
	"context"
	"sync"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

// compile-time assertion that Poll satisfies Source.
var _ Source = (*Poll)(nil)

// FetchFunc retrieves the current feed list from a backing store.
type FetchFunc func(ctx context.Context) ([]config.FeedConfig, error)

// Poll is a source that signals Changes on a fixed interval, delegating reads to
// a FetchFunc. The aggregator's per-source last-known-good handles fetch errors,
// so Poll just forwards whatever FetchFunc returns.
type Poll struct {
	name  string
	fetch FetchFunc
	out   chan struct{}
	stop  chan struct{}
	once  sync.Once
}

// NewPoll returns a Poll source that ticks every interval. A non-positive
// interval defaults to 1s to avoid a busy loop.
func NewPoll(name string, interval time.Duration, fetch FetchFunc) *Poll {
	if interval <= 0 {
		interval = time.Second
	}
	p := &Poll{name: name, fetch: fetch, out: make(chan struct{}, 1), stop: make(chan struct{})}
	go p.loop(interval)
	return p
}

func (p *Poll) Name() string { return p.name }

func (p *Poll) Feeds(ctx context.Context) ([]config.FeedConfig, error) { return p.fetch(ctx) }

func (p *Poll) Changes() <-chan struct{} { return p.out }

func (p *Poll) Close() { p.once.Do(func() { close(p.stop) }) }

func (p *Poll) loop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			select {
			case p.out <- struct{}{}:
			default:
			}
		}
	}
}
