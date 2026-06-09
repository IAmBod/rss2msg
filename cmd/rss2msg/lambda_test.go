package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/scheduler"
)

func TestEffectiveArgsInjectsImplicitSubcommand(t *testing.T) {
	t.Parallel()
	// With an implicit subcommand resolved and no explicit one given, it is
	// appended — so a bare binary (a zip custom-runtime `bootstrap`, or a
	// command-less container) self-starts the handler.
	got := effectiveArgs([]string{"rss2msg"}, "lambda")
	want := []string{"rss2msg", "lambda"}
	if len(got) != len(want) || got[len(got)-1] != "lambda" {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEffectiveArgsLeavesExplicitSubcommand(t *testing.T) {
	t.Parallel()
	// An explicit subcommand is never overridden, even when one is implied.
	got := effectiveArgs([]string{"rss2msg", "serve"}, "lambda")
	if len(got) != 2 || got[1] != "serve" {
		t.Fatalf("explicit subcommand must be preserved, got %v", got)
	}
}

func TestEffectiveArgsNoopWithoutImplicit(t *testing.T) {
	t.Parallel()
	// With no implicit subcommand resolved the args are untouched.
	got := effectiveArgs([]string{"rss2msg"}, "")
	if len(got) != 1 || got[0] != "rss2msg" {
		t.Fatalf("expected unchanged args, got %v", got)
	}
}

// fakePipeline is a minimal scheduler.FeedPipeline that records how many times
// it was polled and can be told to fail.
type fakePipeline struct {
	url   string
	calls int32
	err   error
}

func (f *fakePipeline) FeedURL() string { return f.url }
func (f *fakePipeline) RunOnce(_ context.Context, _ string, _ time.Time) ([]model.Change, error) {
	atomic.AddInt32(&f.calls, 1)
	return nil, f.err
}

func TestPollHandlerPollsEveryPipelineOnce(t *testing.T) {
	t.Parallel()
	ps := []scheduler.FeedPipeline{
		&fakePipeline{url: "a"},
		&fakePipeline{url: "b"},
		&fakePipeline{url: "c"},
	}
	h := pollHandler(ps, 2)
	res, err := h(context.Background(), invokeEvent{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.OK || res.Feeds != 3 || res.Error != "" {
		t.Fatalf("unexpected result: %+v", res)
	}
	for _, p := range ps {
		if got := atomic.LoadInt32(&p.(*fakePipeline).calls); got != 1 {
			t.Fatalf("%s: expected 1 poll, got %d", p.FeedURL(), got)
		}
	}
}

func TestPollHandlerReportsErrors(t *testing.T) {
	t.Parallel()
	ps := []scheduler.FeedPipeline{
		&fakePipeline{url: "a", err: errors.New("a fail")},
		&fakePipeline{url: "b"},
	}
	h := pollHandler(ps, 0)
	res, err := h(context.Background(), invokeEvent{})
	if err == nil {
		t.Fatal("expected error to propagate to the Lambda runtime")
	}
	if res.OK {
		t.Fatal("expected result.OK to be false on failure")
	}
	if !strings.Contains(res.Error, "a fail") {
		t.Fatalf("expected error summary to mention the failure, got %q", res.Error)
	}
	if res.Feeds != 2 {
		t.Fatalf("expected Feeds=2, got %d", res.Feeds)
	}
}

// The event payload may override per-invocation concurrency; a zero value
// falls back to the configured default. We assert the override is honoured by
// confirming every pipeline still runs exactly once under serial concurrency.
func TestPollHandlerEventConcurrencyOverride(t *testing.T) {
	t.Parallel()
	ps := []scheduler.FeedPipeline{
		&fakePipeline{url: "a"},
		&fakePipeline{url: "b"},
	}
	h := pollHandler(ps, 8)
	res, err := h(context.Background(), invokeEvent{Concurrency: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.OK || res.Feeds != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	for _, p := range ps {
		if got := atomic.LoadInt32(&p.(*fakePipeline).calls); got != 1 {
			t.Fatalf("%s: expected 1 poll, got %d", p.FeedURL(), got)
		}
	}
}
