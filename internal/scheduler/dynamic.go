package scheduler

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

// FeedProvider yields the desired feed set and signals when it may have changed.
// feedsource.Aggregator satisfies this structurally.
type FeedProvider interface {
	Desired(ctx context.Context) ([]config.FeedConfig, error)
	Changes() <-chan struct{}
}

// PipelineFactory builds a FeedPipeline for one feed.
type PipelineFactory func(fc config.FeedConfig) (FeedPipeline, error)

// DynamicConfig configures ServeDynamic.
type DynamicConfig struct {
	Provider     FeedProvider
	Factory      PipelineFactory
	DrainTimeout time.Duration
	// OnReconcile, if set, is called after each applied reconcile with the
	// counts of feeds added/removed/changed. Optional.
	OnReconcile func(added, removed, changed int)
	// OnError, if set, is called when a reconcile is aborted (e.g. a feed's
	// pipeline failed to build). The running set is left unchanged. Optional.
	OnError func(err error)
	// OnPollOverrun, if set, is called when a single poll takes longer than the
	// feed's interval, so the effective polling rate is below what's configured.
	// Optional.
	OnPollOverrun func(feedURL string, took, interval time.Duration)
	// OnPollComplete, if set, is called after every poll of a running feed with
	// the change count and poll error. Optional.
	OnPollComplete func(feedURL string, changeCount int, err error, when time.Time)
}

type runningFeed struct {
	cfg    config.FeedConfig
	cancel context.CancelFunc
	done   chan struct{}
}

// ServeDynamic runs the daemon with a reconcilable feed set. It reconciles once
// immediately, then again on every Provider.Changes signal, until ctx is
// cancelled, at which point all feeds drain (bounded by DrainTimeout).
func ServeDynamic(ctx context.Context, cfg DynamicConfig) error {
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	running := make(map[string]*runningFeed)

	reconcile := func() {
		desired, err := cfg.Provider.Desired(ctx)
		if err != nil {
			if cfg.OnError != nil {
				cfg.OnError(err)
			}
			return // provider keeps last-known-good; nothing applied
		}
		byURL := make(map[string]config.FeedConfig, len(desired))
		for _, fc := range desired {
			byURL[fc.URL] = fc
		}

		// Pre-build pipelines for new and changed feeds. If ANY fails, abort the
		// whole reconcile and leave the running set untouched (atomic reload).
		toStart := make(map[string]FeedPipeline)
		for url, fc := range byURL {
			if rf, ok := running[url]; ok && reflect.DeepEqual(rf.cfg, fc) {
				continue // unchanged: leave running, no rebuild
			}
			if fc.Interval <= 0 {
				if cfg.OnError != nil {
					cfg.OnError(fmt.Errorf("feed %s: interval is required (minimum 1s)", fc.URL))
				}
				return // abort: nothing applied
			}
			p, err := cfg.Factory(fc)
			if err != nil {
				if cfg.OnError != nil {
					cfg.OnError(err)
				}
				return // abort: nothing applied
			}
			toStart[url] = p
		}

		var added, removed, changed int
		// Stop removed.
		for url, rf := range running {
			if _, keep := byURL[url]; !keep {
				rf.cancel()
				<-rf.done
				delete(running, url)
				removed++
			}
		}
		// Apply new / restart changed (all pipelines already built, can't fail).
		for url, p := range toStart {
			if rf, ok := running[url]; ok {
				rf.cancel() // changed: stop then restart (resets ticker)
				<-rf.done
				changed++
			} else {
				added++
			}
			running[url] = startFeed(ctx, byURL[url], p, cfg.OnPollOverrun, cfg.OnPollComplete)
		}
		if cfg.OnReconcile != nil {
			cfg.OnReconcile(added, removed, changed)
		}
	}

	reconcile()
	for {
		select {
		case <-ctx.Done():
			drainAll(running, cfg.DrainTimeout)
			return nil
		case <-cfg.Provider.Changes():
			reconcile()
		}
	}
}

// startFeed launches a pre-built pipeline's loop and returns its handle.
func startFeed(parent context.Context, fc config.FeedConfig, p FeedPipeline,
	onPollOverrun func(feedURL string, took, interval time.Duration),
	onPollComplete func(feedURL string, changeCount int, err error, when time.Time),
) *runningFeed {
	fctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	var onOverrun func(took time.Duration)
	if onPollOverrun != nil {
		onOverrun = func(took time.Duration) { onPollOverrun(fc.URL, took, fc.Interval) }
	}
	var onComplete func(int, error, time.Time)
	if onPollComplete != nil {
		onComplete = func(n int, err error, when time.Time) { onPollComplete(fc.URL, n, err, when) }
	}
	go func() {
		defer close(done)
		// Per-tick RunOnce errors are logged inside the pipeline; the dynamic
		// scheduler does not aggregate them (unlike static Serve).
		runFeedLoop(fctx, p, fc.Interval, func(error) {}, onOverrun, onComplete)
	}()
	return &runningFeed{cfg: fc, cancel: cancel, done: done}
}

// drainAll cancels every running feed and waits for them all to finish,
// bounded by a single overall timeout.
func drainAll(running map[string]*runningFeed, timeout time.Duration) {
	for _, rf := range running {
		rf.cancel()
	}
	allDone := make(chan struct{})
	go func() {
		for _, rf := range running {
			<-rf.done
		}
		close(allDone)
	}()
	select {
	case <-allDone:
	case <-time.After(timeout):
	}
}
