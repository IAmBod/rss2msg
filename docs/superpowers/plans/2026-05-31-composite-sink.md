# Composite Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `driver: composite` sink that fans every change out to a list of named child sinks, transparently — the composite has no retry or dead-letter of its own; each child behaves exactly as a sink referenced directly by a feed.

**Architecture:** A new `internal/sink/composite` package implements `sink.Publisher`. It is built as a *shell* (child names only) and added to the registry, then a **link pass** in `cmd/rss2msg/wire.go` resolves each child via a shared `wrapSink` helper and attaches the wrapped branches. `wrapSink` wraps a composite **pass-through** (`retry.Config{MaxAttempts: 1}`, no DLQ) so it is never retried as a unit, while every other sink keeps today's `WithRetry(primary, dlq, cfg.Retry)` behavior. Config validation rejects empty/unknown/self/duplicate children, cycles, and a `dead_letter` set on a composite.

**Tech Stack:** Go 1.25, zerolog, OpenTelemetry metrics, `task` (taskfile.yaml), `go test -race`.

**Source of truth:** GitHub issue [#45](https://github.com/IAmBod/rss2msg/issues/45). No separate spec file — the issue body is the spec.

---

## File Structure

- `internal/sink/composite/composite.go` — the composite `Publisher` (fan-out, per-child outcome aggregation, logging). **New.**
- `internal/sink/composite/telemetry.go` — `composite_sink.*` OTEL counters, nil-meter-safe. **New.**
- `internal/sink/composite/composite_unit_test.go` — unit tests with fake children. **New.**
- `internal/config/config.go` — `CompositeSinkConfig` + `Composite` field on `SinkConfig`. **Modify.**
- `internal/config/validate.go` — register `composite`, dead_letter guard, `case "composite"`, cycle detection. **Modify.**
- `internal/config/validate_test.go` — composite validation cases. **Modify.**
- `cmd/rss2msg/wire.go` — extract `wrapSink`, `case "composite"` in `buildPublisher`, `linkComposites` + call in `wireAll`. **Modify.**
- `cmd/rss2msg/wire_test.go` — `wrapSink` + `linkComposites` tests. **Modify.**
- `docs/how-to/sinks/composite.md` — driver page. **New.**
- `docs/how-to/choose-a-sink.md` — driver row + driver list. **Modify.**
- `config.example.yaml` — `default`-as-composite fan-out example. **Modify.**

Run after every task: `task test` (`go test -race ./...`) and `task vet`.

---

## Task 1: Config struct + driver registration

**Files:**
- Modify: `internal/config/config.go` (`SinkConfig` ~line 114)
- Modify: `internal/config/validate.go` (`knownSinkDrivers` ~line 29)
- Test: `internal/config/validate_test.go`

- [ ] **Step 1: Write the failing test**

In `internal/config/validate_test.go`:

```go
func TestValidateAcceptsComposite(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name:      "fanout",
		Driver:    "composite",
		Composite: CompositeSinkConfig{Children: []string{"pg-main", "dlq-main"}},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestValidateAcceptsComposite -v`
Expected: FAIL — compile error (`CompositeSinkConfig` undefined) or `sinks[3].driver "composite" is not supported`.

- [ ] **Step 3: Write minimal implementation**

In `internal/config/config.go`, add the field to `SinkConfig` (after the `Feed` field):

```go
	Feed       FeedSinkConfig         `mapstructure:"feed"`
	Composite  CompositeSinkConfig    `mapstructure:"composite"`
	Extra      map[string]interface{} `mapstructure:",remain"`
```

And add the new type near the other sink config types:

```go
// CompositeSinkConfig configures a composite sink: a transparent fan-out to a
// list of other declared sinks by name. The composite has no retry or
// dead_letter of its own; each child keeps its own.
type CompositeSinkConfig struct {
	Children []string `mapstructure:"children"`
}
```

In `internal/config/validate.go`, add to `knownSinkDrivers`:

```go
	"feed":     {},
	"composite": {},
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestValidateAcceptsComposite -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git commit -- internal/config/config.go internal/config/validate.go internal/config/validate_test.go \
  -m "feat(config): register composite sink driver and config struct"
```

---

## Task 2: Composite validation rules

**Files:**
- Modify: `internal/config/validate.go` (sink loop ~lines 197-341)
- Test: `internal/config/validate_test.go`

- [ ] **Step 1: Write the failing tests**

In `internal/config/validate_test.go`:

```go
func compositeCfg(children []string) Config {
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "fanout", Driver: "composite",
		Composite: CompositeSinkConfig{Children: children},
	})
	return c
}

func TestValidateRejectsCompositeEmptyChildren(t *testing.T) {
	t.Parallel()
	_, err := Validate(compositeCfg(nil))
	if err == nil || !strings.Contains(err.Error(), "children") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeUnknownChild(t *testing.T) {
	t.Parallel()
	_, err := Validate(compositeCfg([]string{"nope"}))
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeSelfReference(t *testing.T) {
	t.Parallel()
	_, err := Validate(compositeCfg([]string{"fanout"}))
	if err == nil || !strings.Contains(err.Error(), "its own sink") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeDuplicateChild(t *testing.T) {
	t.Parallel()
	_, err := Validate(compositeCfg([]string{"pg-main", "pg-main"}))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeWithDeadLetter(t *testing.T) {
	t.Parallel()
	c := compositeCfg([]string{"pg-main"})
	c.Sinks[len(c.Sinks)-1].DeadLetter = "dlq-main"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "dead_letter") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeCycle(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks,
		SinkConfig{Name: "a", Driver: "composite", Composite: CompositeSinkConfig{Children: []string{"b"}}},
		SinkConfig{Name: "b", Driver: "composite", Composite: CompositeSinkConfig{Children: []string{"a"}}},
	)
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cyclic") {
		t.Fatalf("got %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/ -run TestValidateRejectsComposite -v`
Expected: FAIL — all currently pass validation (no `case "composite"` rules), so each assertion fails because `err == nil`.

- [ ] **Step 3: Write minimal implementation**

In `internal/config/validate.go`, just before the `for i, s := range c.Sinks {` loop that contains the driver switch (the second loop, ~line 201, where `sinkDrivers` is built), add a composite-children map:

```go
	// Build a map from sink name to driver for dead-letter driver checks.
	sinkDrivers := make(map[string]string, len(c.Sinks))
	compositeChildren := make(map[string][]string)
	for _, s := range c.Sinks {
		sinkDrivers[s.Name] = s.Driver
		if s.Driver == "composite" {
			compositeChildren[s.Name] = s.Composite.Children
		}
	}
```

Inside that loop, immediately after the existing `if s.DeadLetter != "" { ... }` block, add the composite dead_letter guard:

```go
		if s.Driver == "composite" && s.DeadLetter != "" {
			return *warnings, fmt.Errorf("sinks[%d] (composite %q): dead_letter is not allowed on a composite; configure dead_letter on each child instead", i, s.Name)
		}
```

Add a new `case "composite":` to the driver `switch s.Driver {` (alongside `case "feed":`):

```go
		case "composite":
			if len(s.Composite.Children) == 0 {
				return *warnings, fmt.Errorf("sinks[%d] (composite %q): composite.children is required", i, s.Name)
			}
			seen := make(map[string]struct{}, len(s.Composite.Children))
			for _, child := range s.Composite.Children {
				if child == s.Name {
					return *warnings, fmt.Errorf("sinks[%d] (composite %q): composite.children must not reference its own sink", i, s.Name)
				}
				if _, ok := names[child]; !ok {
					return *warnings, fmt.Errorf("sinks[%d] (composite %q): composite.children references unknown sink %q", i, s.Name, child)
				}
				if _, dup := seen[child]; dup {
					return *warnings, fmt.Errorf("sinks[%d] (composite %q): composite.children has a duplicate entry %q", i, s.Name, child)
				}
				seen[child] = struct{}{}
			}
			if path := compositeCycle(s.Name, compositeChildren); path != "" {
				return *warnings, fmt.Errorf("sinks[%d] (composite %q): cyclic child reference (%s)", i, s.Name, path)
			}
```

Add the cycle-detection helper at the bottom of `validate.go` (near `ResolveFeedSinks`):

```go
// compositeCycle returns a "a -> b -> a" path string if the composite graph
// rooted at start contains a cycle. It only walks into children that are
// themselves composites (present as keys in childrenOf).
func compositeCycle(start string, childrenOf map[string][]string) string {
	onStack := make(map[string]bool)
	var path []string
	var dfs func(n string) bool
	dfs = func(n string) bool {
		onStack[n] = true
		path = append(path, n)
		for _, c := range childrenOf[n] {
			if _, isComposite := childrenOf[c]; !isComposite {
				continue
			}
			if onStack[c] {
				path = append(path, c)
				return true
			}
			if dfs(c) {
				return true
			}
		}
		onStack[n] = false
		path = path[:len(path)-1]
		return false
	}
	if dfs(start) {
		return strings.Join(path, " -> ")
	}
	return ""
}
```

(`strings` is already imported in `validate.go`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/ -run 'TestValidate.*Composite' -v`
Expected: PASS (including Task 1's `TestValidateAcceptsComposite`).

- [ ] **Step 5: Commit**

```bash
git commit -- internal/config/validate.go internal/config/validate_test.go \
  -m "feat(config): validate composite children, dead_letter guard, cycle detection"
```

---

## Task 3: Composite sink package

**Files:**
- Create: `internal/sink/composite/composite.go`
- Create: `internal/sink/composite/telemetry.go`
- Test: `internal/sink/composite/composite_unit_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/sink/composite/composite_unit_test.go`:

```go
package composite

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/retry"
	"github.com/iambod/rss2msg/internal/sink"
)

// fakeSink records the changes it receives and optionally fails.
type fakeSink struct {
	name     string
	failWith error
	mu       sync.Mutex
	received []model.Change
	closed   int
}

func (f *fakeSink) Name() string { return f.name }
func (f *fakeSink) Close() error { f.mu.Lock(); defer f.mu.Unlock(); f.closed++; return nil }
func (f *fakeSink) Publish(_ context.Context, c model.Change) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, c)
	return f.failWith
}
func (f *fakeSink) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.received) }

func branch(name string, primary, dlq sink.Publisher) Branch {
	return Branch{Name: name, Wrapped: sink.WithRetry(primary, dlq, retry.Config{MaxAttempts: 1})}
}

func sampleChange() model.Change {
	return model.Change{
		SchemaVersion: model.SchemaVersion, FeedURL: "https://e/feed", ItemID: "i1",
		Kind: model.ChangeNew, ContentHash: "deadbeef",
		DetectedAt: time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC),
	}
}

func TestNewRejectsMissingName(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{}); err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestPublishFansOutToAllChildren(t *testing.T) {
	t.Parallel()
	a, b := &fakeSink{name: "a"}, &fakeSink{name: "b"}
	p, err := New(Options{Name: "fanout"})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBranches([]Branch{branch("a", a, nil), branch("b", b, nil)})
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if a.count() != 1 || b.count() != 1 {
		t.Fatalf("each child should receive once: a=%d b=%d", a.count(), b.count())
	}
}

func TestPublishReportsDroppedChildAndStillDeliversOthers(t *testing.T) {
	t.Parallel()
	good := &fakeSink{name: "good"}
	bad := &fakeSink{name: "bad", failWith: errors.New("boom")} // no DLQ -> dropped
	p, _ := New(Options{Name: "fanout"})
	p.SetBranches([]Branch{branch("good", good, nil), branch("bad", bad, nil)})
	err := p.Publish(context.Background(), sampleChange())
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("error must name dropped child, got %v", err)
	}
	if good.count() != 1 {
		t.Fatalf("healthy child should still receive: good=%d", good.count())
	}
}

func TestPublishChildCapturedByDLQReturnsNil(t *testing.T) {
	t.Parallel()
	bad := &fakeSink{name: "bad", failWith: errors.New("boom")}
	dlq := &fakeSink{name: "dlq"}
	p, _ := New(Options{Name: "fanout"})
	p.SetBranches([]Branch{branch("bad", bad, dlq)})
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("child captured by DLQ must not fail the composite, got %v", err)
	}
	if dlq.count() != 1 {
		t.Fatalf("DLQ should receive the envelope: dlq=%d", dlq.count())
	}
	if dlq.received[0].DLQFromSink != "bad" {
		t.Fatalf("envelope should be annotated, got %q", dlq.received[0].DLQFromSink)
	}
}

func TestCloseDoesNotCloseChildren(t *testing.T) {
	t.Parallel()
	a := &fakeSink{name: "a"}
	p, _ := New(Options{Name: "fanout"})
	p.SetBranches([]Branch{branch("a", a, nil)})
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if a.closed != 0 {
		t.Fatalf("composite must not close children, closed=%d", a.closed)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/composite/ -v`
Expected: FAIL — compile error (`composite` package / `New` / `Branch` / `Options` undefined).

- [ ] **Step 3: Write minimal implementation**

Create `internal/sink/composite/composite.go`:

```go
// Package composite implements a sink.Publisher that fans every Change out to a
// list of child sinks. A composite is a transparent fan-out: it adds no retry
// or dead-letter of its own. Each child is wrapped exactly as a sink referenced
// directly by a feed (its own retry budget and dead_letter), so a child reached
// through a composite behaves identically to one referenced directly.
//
// Children are top-level registered sinks owned by the sink Registry, so Close
// is a no-op here to avoid double-closing them.
package composite

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink"
)

// Branch is a single child of a composite: a named sink already wrapped with
// its own retry + dead-letter (or wrapped pass-through, when the child is
// itself a composite).
type Branch struct {
	Name    string
	Wrapped *sink.RetryingPublisher
}

// Options configures a composite Publisher.
type Options struct {
	Name     string         // sink name (required)
	Children []string       // child sink names, for diagnostics
	Logger   zerolog.Logger // structured logging; zero value is fine
	Meter    metric.Meter   // optional; nil => no metrics
}

// Publisher fans a Change out to its child branches.
type Publisher struct {
	name     string
	children []string
	branches []Branch
	logger   zerolog.Logger
	instr    *instruments
}

// New constructs a composite shell. Branches are attached later via SetBranches
// once every sink has been built and registered (see the wiring link pass).
func New(o Options) (*Publisher, error) {
	if o.Name == "" {
		return nil, fmt.Errorf("composite sink: name is required")
	}
	p := &Publisher{name: o.Name, children: o.Children, logger: o.Logger}
	if o.Meter != nil {
		instr, err := newInstruments(o.Meter)
		if err != nil {
			return nil, fmt.Errorf("composite sink %q: instruments: %w", o.Name, err)
		}
		p.instr = instr
	}
	return p, nil
}

// SetBranches attaches the resolved, wrapped child branches. Called once during
// wiring; the branch slice is read-only afterwards, so Publish is safe for
// concurrent use across feeds.
func (p *Publisher) SetBranches(b []Branch) { p.branches = b }

func (p *Publisher) Name() string { return p.name }

// Close is a no-op: children are registered sinks closed by the Registry.
func (p *Publisher) Close() error { return nil }

// Publish delivers change to every child sequentially. It returns nil when each
// child either succeeded or was captured by its own dead-letter, and an error
// naming the children that were dropped (failed with no dead-letter) otherwise.
func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	if p.instr != nil {
		p.instr.publishes.Add(ctx, 1, metric.WithAttributes(attribute.String("sink.name", p.name)))
	}
	var dropped []string
	for _, b := range p.branches {
		r := b.Wrapped.Deliver(ctx, change)
		outcome := "success"
		switch r.State {
		case sink.BranchSuccess:
			p.logger.Debug().Str("sink", p.name).Str("child", b.Name).Str("item_id", change.ItemID).Msg("composite child published")
		case sink.BranchDLQ:
			outcome = "dlq"
			p.logger.Warn().Err(r.Err).Str("sink", p.name).Str("child", b.Name).Str("item_id", change.ItemID).Int("attempts", r.Attempts).Msg("composite child captured by DLQ")
		case sink.BranchDropped:
			outcome = "dropped"
			dropped = append(dropped, b.Name)
			p.logger.Error().Err(r.Err).Str("sink", p.name).Str("child", b.Name).Str("item_id", change.ItemID).Int("attempts", r.Attempts).Msg("composite child dropped")
		}
		if p.instr != nil {
			p.instr.children.Add(ctx, 1, metric.WithAttributes(
				attribute.String("sink.name", p.name),
				attribute.String("child", b.Name),
				attribute.String("outcome", outcome),
			))
		}
	}
	if len(dropped) > 0 {
		return fmt.Errorf("composite sink %q: %d child sink(s) dropped: %s", p.name, len(dropped), strings.Join(dropped, ", "))
	}
	return nil
}
```

Create `internal/sink/composite/telemetry.go`:

```go
package composite

import "go.opentelemetry.io/otel/metric"

type instruments struct {
	publishes metric.Int64Counter // one per Publish call
	children  metric.Int64Counter // per child delivery, attr outcome=success|dlq|dropped
}

func newInstruments(m metric.Meter) (*instruments, error) {
	pubs, err := m.Int64Counter("composite_sink.publishes")
	if err != nil {
		return nil, err
	}
	ch, err := m.Int64Counter("composite_sink.child_deliveries")
	if err != nil {
		return nil, err
	}
	return &instruments{publishes: pubs, children: ch}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sink/composite/ -race -v`
Expected: PASS (all five tests).

- [ ] **Step 5: Add a telemetry test**

Append to `composite_unit_test.go`:

```go
func TestPublishRecordsMetrics(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	p, err := New(Options{Name: "fanout", Meter: mp.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBranches([]Branch{branch("a", &fakeSink{name: "a"}, nil)})
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatal(err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected metrics recorded")
	}
}
```

Add the imports to the test file's import block:

```go
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
```

Run: `go test ./internal/sink/composite/ -race -run TestPublishRecordsMetrics -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git commit -- internal/sink/composite/ \
  -m "feat(sink): composite fan-out publisher with per-child outcomes and telemetry"
```

---

## Task 4: Wiring — wrapSink, buildPublisher case, link pass

**Files:**
- Modify: `cmd/rss2msg/wire.go`
- Test: `cmd/rss2msg/wire_test.go`

- [ ] **Step 1: Write the failing tests**

In `cmd/rss2msg/wire_test.go` add (and extend the import block with `"github.com/iambod/rss2msg/internal/sink"`, `sinkstdout "github.com/iambod/rss2msg/internal/sink/stdout"`, `compositesink "github.com/iambod/rss2msg/internal/sink/composite"`, `"github.com/iambod/rss2msg/internal/model"`, `"context"` already present):

```go
func TestWrapSinkComposite_IsPassThrough(t *testing.T) {
	reg := sink.NewRegistry()
	comp, _ := compositesink.New(compositesink.Options{Name: "fanout"})
	_ = reg.Add(comp)
	cfg := config.Config{
		Retry: config.RetryConfig{MaxAttempts: 5},
		Sinks: []config.SinkConfig{{Name: "fanout", Driver: "composite"}},
	}
	w, err := wrapSink(reg, cfg, "fanout")
	if err != nil {
		t.Fatal(err)
	}
	// A composite with no branches always succeeds; a single Deliver attempt.
	r := w.Deliver(context.Background(), model.Change{ItemID: "x"})
	if r.State != sink.BranchSuccess || r.Attempts != 1 {
		t.Fatalf("composite must be wrapped pass-through: state=%v attempts=%d", r.State, r.Attempts)
	}
}

func TestLinkComposites_FansOut(t *testing.T) {
	reg := sink.NewRegistry()
	a, _ := sinkstdout.New(sinkstdout.Options{Name: "a"})
	b, _ := sinkstdout.New(sinkstdout.Options{Name: "b"})
	comp, _ := compositesink.New(compositesink.Options{Name: "default"})
	_ = reg.Add(a)
	_ = reg.Add(b)
	_ = reg.Add(comp)
	cfg := config.Config{
		Retry: config.RetryConfig{MaxAttempts: 1},
		Sinks: []config.SinkConfig{
			{Name: "a", Driver: "stdout", Stdout: config.StdoutSinkConfig{Target: "stderr"}},
			{Name: "b", Driver: "stdout", Stdout: config.StdoutSinkConfig{Target: "stderr"}},
			{Name: "default", Driver: "composite", Composite: config.CompositeSinkConfig{Children: []string{"a", "b"}}},
		},
	}
	if err := linkComposites(reg, cfg); err != nil {
		t.Fatal(err)
	}
	// After linking, the composite must publish to both children without error.
	if err := comp.Publish(context.Background(), model.Change{ItemID: "x"}); err != nil {
		t.Fatalf("composite publish: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/rss2msg/ -run 'TestWrapSinkComposite|TestLinkComposites' -v`
Expected: FAIL — `wrapSink` and `linkComposites` undefined (compile error).

- [ ] **Step 3: Write minimal implementation**

In `cmd/rss2msg/wire.go`, add the composite import:

```go
	feedsink "github.com/iambod/rss2msg/internal/sink/feed"
	compositesink "github.com/iambod/rss2msg/internal/sink/composite"
```

Add the `wrapSink` helper and `linkComposites` (place near `findSink`):

```go
// wrapSink resolves a sink by name and wraps it for delivery. A composite owns
// its per-child retry/DLQ internally and must never be retried as a unit (that
// would re-send to children that already succeeded), so it is wrapped
// pass-through: one attempt, no DLQ. Every other driver gets the global retry
// budget plus its configured dead_letter.
func wrapSink(reg *sink.Registry, cfg config.Config, name string) (*sink.RetryingPublisher, error) {
	primary, ok := reg.Get(name)
	if !ok {
		return nil, fmt.Errorf("unknown sink %q", name)
	}
	scCfg := findSink(cfg.Sinks, name)
	if scCfg.Driver == "composite" {
		return sink.WithRetry(primary, nil, retry.Config{MaxAttempts: 1}), nil
	}
	var dlq sink.Publisher
	if scCfg.DeadLetter != "" {
		dlq, _ = reg.Get(scCfg.DeadLetter)
	}
	return sink.WithRetry(primary, dlq, retry.Config{
		MaxAttempts: cfg.Retry.MaxAttempts,
		BaseDelay:   cfg.Retry.BaseDelay,
		MaxDelay:    cfg.Retry.MaxDelay,
	}), nil
}

// linkComposites resolves each composite sink's children from the registry and
// attaches the wrapped branches. Called after every sink is built and added,
// so child pointers (including nested composites) are already present.
func linkComposites(reg *sink.Registry, cfg config.Config) error {
	for _, sc := range cfg.Sinks {
		if sc.Driver != "composite" {
			continue
		}
		p, _ := reg.Get(sc.Name)
		comp, ok := p.(*compositesink.Publisher)
		if !ok {
			return fmt.Errorf("sink %q: expected composite publisher", sc.Name)
		}
		branches := make([]compositesink.Branch, 0, len(sc.Composite.Children))
		for _, child := range sc.Composite.Children {
			wrapped, err := wrapSink(reg, cfg, child)
			if err != nil {
				return fmt.Errorf("composite %q: child %q: %w", sc.Name, child, err)
			}
			branches = append(branches, compositesink.Branch{Name: child, Wrapped: wrapped})
		}
		comp.SetBranches(branches)
	}
	return nil
}
```

Add the `case "composite":` to `buildPublisher` (before `default:`):

```go
	case "composite":
		return compositesink.New(compositesink.Options{
			Name:     sc.Name,
			Children: sc.Composite.Children,
			Logger:   tel.Logger,
			Meter:    tel.Meter,
		})
```

In `wireAll`, immediately after the sink-build loop (after the closing `}` of `for _, sc := range cfg.Sinks { ... }`, ~line 106) add the link pass:

```go
	if err := linkComposites(reg, cfg); err != nil {
		_ = reg.Close()
		_ = st.Close()
		return nil, err
	}
```

Refactor `newPipelineFactory` to use `wrapSink` (replace the inline `primary, ok := ...; scCfg := ...; WithRetry(...)` block in the `for _, name := range names` loop):

```go
		for _, name := range names {
			wrapped, err := wrapSink(w.registry, cfg, name)
			if err != nil {
				return nil, fmt.Errorf("feed %s: %w", fc.URL, err)
			}
			branches = append(branches, sinkBranch{name: name, wrapped: wrapped})
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/rss2msg/ -run 'TestWrapSinkComposite|TestLinkComposites|TestBuildPublisher' -v`
Expected: PASS. Then `task test` to confirm the `newPipelineFactory` refactor didn't regress existing behavior.

- [ ] **Step 5: Commit**

```bash
git commit -- cmd/rss2msg/wire.go cmd/rss2msg/wire_test.go \
  -m "feat(cmd): wire composite sink via shared wrapSink helper and link pass"
```

---

## Task 5: Docs + config example

**Files:**
- Create: `docs/how-to/sinks/composite.md`
- Modify: `docs/how-to/choose-a-sink.md`
- Modify: `config.example.yaml`

Docs rule (project memory): move content into place, ground every claim against the code/issue, use portable relative links, and verify links resolve.

- [ ] **Step 1: Read an existing sink page for structure**

Run: `cat docs/how-to/sinks/stdout.md` and note its frontmatter (`title`, `type: how-to`, `tags`, `summary`, `updated`) and section layout.

- [ ] **Step 2: Create `docs/how-to/sinks/composite.md`**

```markdown
---
title: Composite Sink
type: how-to
tags: [rss2msg/docs, sinks]
summary: Fan one change out to several child sinks under a single name, with each child keeping its own retry and dead-letter.
updated: 2026-05-31
---

# Composite Sink

`driver: composite` is a transparent fan-out: it delivers every change to a
list of other declared sinks (its `children`). Use it to define a fan-out group
once and reference it by a single name — for example as the implicit `default`
sink, or nested inside another composite.

The composite has **no retry or dead-letter of its own**. Each child is
delivered exactly as if a feed referenced it directly, so a child keeps its own
retry budget and its own `dead_letter`.

## Config

```yaml
sinks:
  - name: default
    driver: composite
    composite:
      children: [pretty, archive]

  - name: pretty
    driver: stdout
    stdout: { target: stdout, format: pretty }

  - name: archive
    driver: sqs
    sqs: { queue_url: https://sqs.eu-west-1.amazonaws.com/123/changes }
    dead_letter: archive-dlq
```

| field      | required | notes |
| ---------- | -------- | ----- |
| `children` | yes      | Names of other declared sinks. Each must exist; no self-reference, duplicates, or cycles. A child may itself be a composite (nesting). |

A composite may **not** set its own `dead_letter`; configure dead-lettering on
each child instead.

## Delivery

- The change is published to every child sequentially.
- The feed's state is committed once every child has either succeeded or been
  captured by its own dead-letter. If a child fails with no dead-letter, the
  change is left uncommitted and retried on the next poll.
- No overlap dedup: if the same sink is reachable by more than one path in a
  composite tree, it receives the change once per path.

## Telemetry

- `composite_sink.publishes` — one per change published to the composite.
- `composite_sink.child_deliveries` — per child, attributes `sink.name`,
  `child`, and `outcome` (`success` / `dlq` / `dropped`).

## Related

- [Choose a Sink](../choose-a-sink.md) — common fields and the driver table.
- [Operational Notes](../../explanation/operations.md) — at-least-once delivery and DLQ behavior.
```

- [ ] **Step 3: Update `docs/how-to/choose-a-sink.md`**

Add `composite` to the `driver` row note (the common-fields table):

```markdown
| `driver`      | yes      | One of `postgres`, `kafka`, `rabbitmq`, `sqs`, `sns`, `stdout`, `http`, `feed`, `composite`. |
```

Add a row to the Drivers table:

```markdown
| composite | fan one change out to several child sinks under one name | [composite](sinks/composite.md) |
```

- [ ] **Step 4: Add an example to `config.example.yaml`**

In the `sinks:` block, add a composite `default` that fans out, with a short comment. Place it as the first sink so the `default`-fallback story is clear:

```yaml
  # A composite sink fans every change out to its children. Used here as
  # `default`, so feeds with no explicit `sinks:` list publish to all of them.
  - name: default
    driver: composite
    composite:
      children: [pretty, archive]
```

Ensure `pretty` and `archive` sinks exist in the example (add minimal `stdout`
and `sqs`/`postgres` entries if not already present), so the file stays
internally valid.

- [ ] **Step 5: Verify links and config**

Run:
```bash
# every relative link target in the new page must exist
grep -oE '\]\(([^)]+\.md)\)' docs/how-to/sinks/composite.md
ls docs/how-to/choose-a-sink.md docs/explanation/operations.md
# the example config must validate
go run ./cmd/rss2msg validate --config config.example.yaml 2>&1 | tail -5 || true
go build ./...
```
Expected: the linked files exist; the example config validates (or, if `validate` is not a subcommand, `go build` succeeds and a manual reading confirms the YAML matches the schema in `internal/config/config.go`).

- [ ] **Step 6: Commit**

```bash
git commit -- docs/how-to/sinks/composite.md docs/how-to/choose-a-sink.md config.example.yaml \
  -m "docs(sinks): document composite sink + config example"
```

---

## Final verification

- [ ] `task test` — full `-race` suite green.
- [ ] `task vet` — clean.
- [ ] `task build` — binary builds.
- [ ] Manual smoke: a `config.yaml` with `default` as a composite over a `stdout` child plus one more sink; run `./rss2msg run` against a test feed and confirm each child receives every change; point one child at an unreachable target and confirm the others keep delivering and feed state stays uncommitted for the failing item.
- [ ] Re-read issue [#45](https://github.com/IAmBod/rss2msg/issues/45) acceptance criteria; tick each.

---

## Self-Review notes

- **Spec coverage:** every acceptance-criteria bullet in #45 maps to a task — driver accepted (T1), nesting + validation rejects empty/unknown/self/dup/cycle/dead_letter (T2), per-child retry/DLQ + transparent composite (T3/T4), pass-through wrap so the composite is never retried as a unit (T4 `wrapSink`), telemetry (T3), docs + example (T5).
- **Type consistency:** `composite.New`/`Options`/`Branch`/`SetBranches`/`Publisher`, `wrapSink(reg, cfg, name)`, `linkComposites(reg, cfg)`, `CompositeSinkConfig{Children}`, and `compositeCycle(start, childrenOf)` are used identically across tasks.
- **No link checker target exists** in `taskfile.yaml`; Task 5 verifies links with `grep`/`ls` instead of an automated checker.
