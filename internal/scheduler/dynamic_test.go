package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/model"
)

// dynCountingPipeline records ticks per feed URL via a shared counter pointer,
// so multiple factory calls for the same "logical" feed share the same counter.
type dynCountingPipeline struct {
	url   string
	calls *int32
}

func (c dynCountingPipeline) FeedURL() string { return c.url }
func (c dynCountingPipeline) RunOnce(ctx context.Context, feedURL string, at time.Time) ([]model.Change, error) {
	atomic.AddInt32(c.calls, 1)
	return nil, nil
}

// manualProvider lets the test push desired sets.
type manualProvider struct {
	mu  sync.Mutex
	cur []config.FeedConfig
	ch  chan struct{}
}

func newManualProvider() *manualProvider { return &manualProvider{ch: make(chan struct{}, 1)} }
func (m *manualProvider) Desired(context.Context) ([]config.FeedConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cur, nil
}
func (m *manualProvider) Changes() <-chan struct{} { return m.ch }
func (m *manualProvider) set(feeds []config.FeedConfig) {
	m.mu.Lock()
	m.cur = feeds
	m.mu.Unlock()
	m.ch <- struct{}{}
}

func TestServeDynamicAddsAndRemovesFeeds(t *testing.T) {
	var c1, c2 int32
	factory := func(fc config.FeedConfig) (FeedPipeline, error) {
		switch fc.URL {
		case "https://e/1":
			return dynCountingPipeline{url: fc.URL, calls: &c1}, nil
		default:
			return dynCountingPipeline{url: fc.URL, calls: &c2}, nil
		}
	}
	prov := newManualProvider()
	prov.cur = []config.FeedConfig{{URL: "https://e/1", Interval: 20 * time.Millisecond}}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		_ = ServeDynamic(ctx, DynamicConfig{Provider: prov, Factory: factory, DrainTimeout: 200 * time.Millisecond})
		close(done)
	}()

	time.Sleep(80 * time.Millisecond)
	if atomic.LoadInt32(&c1) < 1 {
		t.Fatal("feed 1 never ticked")
	}

	// Add feed 2, remove feed 1.
	prov.set([]config.FeedConfig{{URL: "https://e/2", Interval: 20 * time.Millisecond}})
	time.Sleep(80 * time.Millisecond)
	stopped := atomic.LoadInt32(&c1)
	time.Sleep(80 * time.Millisecond)
	if atomic.LoadInt32(&c1) != stopped {
		t.Fatal("feed 1 kept ticking after removal")
	}
	if atomic.LoadInt32(&c2) < 1 {
		t.Fatal("feed 2 never ticked after add")
	}

	cancel()
	<-done
}

func TestServeDynamicEmptySetDrainsAll(t *testing.T) {
	var calls int32
	factory := func(fc config.FeedConfig) (FeedPipeline, error) {
		return dynCountingPipeline{url: fc.URL, calls: &calls}, nil
	}
	prov := newManualProvider()
	prov.cur = []config.FeedConfig{{URL: "https://e/1", Interval: 20 * time.Millisecond}}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = ServeDynamic(ctx, DynamicConfig{Provider: prov, Factory: factory, DrainTimeout: 200 * time.Millisecond})
	}()

	time.Sleep(60 * time.Millisecond)
	prov.set(nil) // drain everything
	time.Sleep(60 * time.Millisecond)
	stopped := atomic.LoadInt32(&calls)
	time.Sleep(60 * time.Millisecond)
	if atomic.LoadInt32(&calls) != stopped {
		t.Fatal("feeds kept ticking after empty reconcile")
	}
}

// A factory error for ANY feed in the desired set must abort the ENTIRE reconcile:
// no feed from that set is started, and the previously-running set is untouched.
func TestServeDynamicFactoryErrorAbortsWholeReconcile(t *testing.T) {
	var c1, c2 int32
	factory := func(fc config.FeedConfig) (FeedPipeline, error) {
		switch fc.URL {
		case "https://e/1":
			return dynCountingPipeline{url: fc.URL, calls: &c1}, nil
		case "https://e/2":
			return dynCountingPipeline{url: fc.URL, calls: &c2}, nil
		default: // "https://bad"
			return nil, errors.New("unknown sink")
		}
	}
	prov := newManualProvider()
	prov.cur = []config.FeedConfig{{URL: "https://e/1", Interval: 20 * time.Millisecond}}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = ServeDynamic(ctx, DynamicConfig{Provider: prov, Factory: factory, DrainTimeout: 200 * time.Millisecond})
	}()

	time.Sleep(60 * time.Millisecond)
	if atomic.LoadInt32(&c1) < 1 {
		t.Fatal("feed 1 never ticked")
	}

	// Desired set adds a good new feed (e/2) AND a feed whose factory fails (bad).
	// Atomic reconcile => neither is applied; e/2 must NOT start.
	prov.set([]config.FeedConfig{
		{URL: "https://e/1", Interval: 20 * time.Millisecond},
		{URL: "https://e/2", Interval: 20 * time.Millisecond},
		{URL: "https://bad", Interval: 20 * time.Millisecond},
	})
	time.Sleep(80 * time.Millisecond)
	if atomic.LoadInt32(&c2) != 0 {
		t.Fatal("atomic reconcile: good new feed must NOT start when another feed in the set fails")
	}
	// feed 1 (previously running) keeps going.
	before := atomic.LoadInt32(&c1)
	time.Sleep(60 * time.Millisecond)
	if atomic.LoadInt32(&c1) <= before {
		t.Fatal("feed 1 should keep running after an aborted reconcile")
	}
	cancel()
}

func TestServeDynamicRejectsNonPositiveInterval(t *testing.T) {
	var calls int32
	factory := func(fc config.FeedConfig) (FeedPipeline, error) {
		return dynCountingPipeline{url: fc.URL, calls: &calls}, nil
	}
	var gotErr atomic.Bool
	prov := newManualProvider()
	prov.cur = []config.FeedConfig{{URL: "https://e/1"}} // Interval defaults to 0
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		_ = ServeDynamic(ctx, DynamicConfig{
			Provider:     prov,
			Factory:      factory,
			DrainTimeout: 100 * time.Millisecond,
			OnError:      func(error) { gotErr.Store(true) },
		})
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatal("feed with non-positive interval must not start")
	}
	if !gotErr.Load() {
		t.Fatal("expected OnError for a non-positive interval")
	}
	cancel()
	<-done
}

type errProvider struct{ ch chan struct{} }

func (e errProvider) Desired(context.Context) ([]config.FeedConfig, error) {
	return nil, errors.New("provider down")
}
func (e errProvider) Changes() <-chan struct{} { return e.ch }

func TestServeDynamicCallsOnErrorWhenDesiredFails(t *testing.T) {
	var gotErr atomic.Bool
	prov := errProvider{ch: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	factory := func(fc config.FeedConfig) (FeedPipeline, error) {
		return dynCountingPipeline{url: fc.URL, calls: new(int32)}, nil
	}
	done := make(chan struct{})
	go func() {
		_ = ServeDynamic(ctx, DynamicConfig{
			Provider:     prov,
			Factory:      factory,
			DrainTimeout: 100 * time.Millisecond,
			OnError:      func(error) { gotErr.Store(true) },
		})
		close(done)
	}()
	// The immediate reconcile at startup should already have hit the Desired error.
	time.Sleep(50 * time.Millisecond)
	if !gotErr.Load() {
		t.Fatal("OnError was not called on Desired() failure")
	}
	cancel()
	<-done
}
