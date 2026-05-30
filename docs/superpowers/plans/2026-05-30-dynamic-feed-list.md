# Dynamic Feed List Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the `serve` daemon change its set of feeds at runtime — add, remove, and re-tune feeds — without a restart, by reconciling a desired feed list produced by an ordered list of pluggable feed sources.

**Architecture:** A new `internal/feedsource` package defines a `Source` interface; an `Aggregator` merges the feeds yielded by an ordered list of sources (dedup by `url`, earlier source wins on collision, last-known-good per source on failure, empty set accepted). A reconciling scheduler (`ServeDynamic`) diffs the desired set against the running per-feed goroutines and starts/stops/restarts only the feeds that changed. The `serve` command wires the aggregator + sources + a SIGHUP trigger into `ServeDynamic`, using a pipeline factory extracted from `wireAll`.

**Tech Stack:** Go 1.25, `github.com/fsnotify/fsnotify` (already an indirect dep), existing `config`, `scheduler`, `feed`, `sink`, `state`, `coord` packages.

**Scope of THIS plan:** core engine + canonical schema + **static** and **file** sources + a generic **poll** helper + **SIGHUP** trigger + reconcile + serve wiring + validation + docs. This produces working software: dynamic reconcile from the static `feeds:` block plus a watched file, covering the SIGHUP, file-watch, and poll trigger types.

**Deferred to follow-up plans (one per source, same `Source` interface + `FeedSpec` schema):** HTTP, Postgres, SQLite, Redis, object storage (S3), environment. Each is a thin adapter that produces `[]config.FeedConfig` and signals `Changes()`; the poll helper from Task 6 covers their trigger. They are intentionally NOT in this plan.

**Reference:** GitHub issue #37 (the spec). Re-read it before starting.

---

## File Structure

- `internal/feedsource/source.go` — `Source` interface, `FeedSpec` wire/JSON shape, `ToFeedConfig`.
- `internal/feedsource/static.go` — static source (wraps a fixed `[]config.FeedConfig`).
- `internal/feedsource/aggregator.go` — ordered merge, dedup-by-url, precedence, last-known-good, `Changes()` fan-in + `Trigger()`.
- `internal/feedsource/file.go` — file source (fsnotify watch + debounce + parse).
- `internal/feedsource/poll.go` — generic poll-driven source helper (for file refresh fallback + future pull sources).
- `internal/scheduler/dynamic.go` — `ServeDynamic` reconciling loop + per-feed registry.
- `internal/config/config.go` — add `FeedSources []FeedSourceConfig` and the type.
- `internal/config/validate.go` — validate `feed_sources`.
- `cmd/rss2msg/wire.go` — extract a `pipelineFactory` from `wireAll`.
- `cmd/rss2msg/main.go` — wire aggregator + sources + SIGHUP into the `serve` command.
- Tests alongside each (`*_test.go`).

**Key types (must stay consistent across tasks):**

```go
// internal/feedsource/source.go
type Source interface {
    Name() string
    Feeds(ctx context.Context) ([]config.FeedConfig, error)
    Changes() <-chan struct{}
}

// internal/scheduler/dynamic.go
type FeedProvider interface {
    Desired(ctx context.Context) ([]config.FeedConfig, error)
    Changes() <-chan struct{}
}
type PipelineFactory func(fc config.FeedConfig) (FeedPipeline, error)
```

`feedsource.Aggregator` implements `scheduler.FeedProvider` structurally (`Desired` + `Changes`), so `scheduler` does not import `feedsource` — no import cycle.

---

## Task 1: Config — `feed_sources` section

**Files:**
- Modify: `internal/config/config.go` (add type + field on `Config`)
- Test: `internal/config/load_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/config/load_test.go`:

```go
func TestLoadParsesFeedSources(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
feed_sources:
  - type: file
    name: control-plane
    path: /etc/rss2msg/feeds.json
    interval: 30s
  - type: static
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.FeedSources) != 2 {
		t.Fatalf("want 2 feed sources, got %d", len(cfg.FeedSources))
	}
	if cfg.FeedSources[0].Type != "file" || cfg.FeedSources[0].Name != "control-plane" {
		t.Fatalf("source[0] = %+v", cfg.FeedSources[0])
	}
	if cfg.FeedSources[0].Interval != 30*time.Second {
		t.Fatalf("interval = %v", cfg.FeedSources[0].Interval)
	}
	if cfg.FeedSources[1].Type != "static" {
		t.Fatalf("source[1].type = %q", cfg.FeedSources[1].Type)
	}
}
```

Ensure `load_test.go` imports `path/filepath`, `os`, `time` (add any missing).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestLoadParsesFeedSources -v`
Expected: FAIL — `cfg.FeedSources` undefined (compile error).

- [ ] **Step 3: Add the type and field**

In `internal/config/config.go`, add the field to `Config` (after `Feeds`):

```go
	Feeds       []FeedConfig       `mapstructure:"feeds"`
	FeedSources []FeedSourceConfig `mapstructure:"feed_sources"`
```

And add the type near `FeedConfig`:

```go
// FeedSourceConfig is one entry in the ordered feed_sources list. Order is
// precedence: earlier entries win on URL collision. The static feeds: block is
// represented by an entry with Type "static".
type FeedSourceConfig struct {
	Type     string        `mapstructure:"type"` // static|file|http|postgres|sqlite|redis|s3|env
	Name     string        `mapstructure:"name"` // optional; defaults to "<type>[index]"
	Path     string        `mapstructure:"path"` // file source
	Interval time.Duration `mapstructure:"interval"`
}
```

(Per-type fields like `url`, `dsn`, `query`, bucket/key, etc. are added by the follow-up source plans. `Path` and `Interval` are all this plan needs.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestLoadParsesFeedSources -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/load_test.go
git commit -m "feat(config): add feed_sources section"
```

---

## Task 2: `feedsource` package — Source interface + canonical FeedSpec

**Files:**
- Create: `internal/feedsource/source.go`
- Test: `internal/feedsource/source_test.go`

The canonical payload schema every external source decodes into is `FeedSpec`. It carries `url` plus optional `interval`, `sinks`, `http` (omitted sinks → resolved to the `default` sink downstream).

- [ ] **Step 1: Write the failing test**

```go
package feedsource

import (
	"testing"
	"time"
)

func TestFeedSpecToFeedConfig(t *testing.T) {
	spec := FeedSpec{
		URL:      "https://example.com/feed.xml",
		Interval: "5m",
		Sinks:    []string{"out"},
		HTTP:     &FeedSpecHTTP{Timeout: "10s", Headers: map[string]string{"X-Token": "abc"}},
	}
	fc, err := spec.ToFeedConfig()
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if fc.URL != "https://example.com/feed.xml" {
		t.Fatalf("url = %q", fc.URL)
	}
	if fc.Interval != 5*time.Minute {
		t.Fatalf("interval = %v", fc.Interval)
	}
	if len(fc.Sinks) != 1 || fc.Sinks[0] != "out" {
		t.Fatalf("sinks = %v", fc.Sinks)
	}
	if fc.HTTP.Timeout != 10*time.Second || fc.HTTP.Headers["X-Token"] != "abc" {
		t.Fatalf("http = %+v", fc.HTTP)
	}
}

func TestFeedSpecRejectsEmptyURL(t *testing.T) {
	if _, err := (FeedSpec{}).ToFeedConfig(); err == nil {
		t.Fatal("expected error for empty url")
	}
}

func TestFeedSpecRejectsBadDuration(t *testing.T) {
	if _, err := (FeedSpec{URL: "u", Interval: "nope"}).ToFeedConfig(); err == nil {
		t.Fatal("expected error for bad interval")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/feedsource/ -v`
Expected: FAIL — package/types don't exist.

- [ ] **Step 3: Write the implementation**

`internal/feedsource/source.go`:

```go
// Package feedsource produces the desired feed list for the serve daemon from
// an ordered list of pluggable sources, merged by URL with earlier sources
// winning on collision.
package feedsource

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

// Source is one configured feed-source instance. Multiple instances of the
// same type may exist, each with its own config. Implementations must be safe
// for concurrent calls to Feeds and Changes.
type Source interface {
	// Name uniquely identifies this instance (used in logs/metrics).
	Name() string
	// Feeds returns this instance's current desired feed list.
	Feeds(ctx context.Context) ([]config.FeedConfig, error)
	// Changes signals that the caller should re-read Feeds. A nil/never-firing
	// channel is valid for sources whose contents cannot change at runtime.
	Changes() <-chan struct{}
}

// FeedSpec is the canonical wire/serialized shape a source yields per feed.
// Every external source (file, http, db, ...) decodes into FeedSpec, then
// ToFeedConfig converts to the internal config.FeedConfig.
type FeedSpec struct {
	URL      string        `json:"url" yaml:"url"`
	Interval string        `json:"interval,omitempty" yaml:"interval,omitempty"`
	Sinks    []string      `json:"sinks,omitempty" yaml:"sinks,omitempty"`
	HTTP     *FeedSpecHTTP `json:"http,omitempty" yaml:"http,omitempty"`
}

// FeedSpecHTTP mirrors config.FeedHTTPConfig in serialized form.
type FeedSpecHTTP struct {
	Timeout string            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Headers map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
}

// ToFeedConfig converts a FeedSpec to a config.FeedConfig, parsing durations.
// An empty URL is an error; sinks are left empty here (resolved to "default"
// downstream by config.ResolveFeedSinks).
func (s FeedSpec) ToFeedConfig() (config.FeedConfig, error) {
	if strings.TrimSpace(s.URL) == "" {
		return config.FeedConfig{}, fmt.Errorf("feed spec: url is required")
	}
	fc := config.FeedConfig{URL: s.URL, Sinks: s.Sinks}
	if s.Interval != "" {
		d, err := time.ParseDuration(s.Interval)
		if err != nil {
			return config.FeedConfig{}, fmt.Errorf("feed spec %s: interval: %w", s.URL, err)
		}
		fc.Interval = d
	}
	if s.HTTP != nil {
		fc.HTTP.Headers = s.HTTP.Headers
		if s.HTTP.Timeout != "" {
			d, err := time.ParseDuration(s.HTTP.Timeout)
			if err != nil {
				return config.FeedConfig{}, fmt.Errorf("feed spec %s: http.timeout: %w", s.URL, err)
			}
			fc.HTTP.Timeout = d
		}
	}
	return fc, nil
}

// SpecsToConfigs converts a slice of specs, failing on the first invalid one.
func SpecsToConfigs(specs []FeedSpec) ([]config.FeedConfig, error) {
	out := make([]config.FeedConfig, 0, len(specs))
	for _, s := range specs {
		fc, err := s.ToFeedConfig()
		if err != nil {
			return nil, err
		}
		out = append(out, fc)
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/feedsource/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/source.go internal/feedsource/source_test.go
git commit -m "feat(feedsource): Source interface and canonical FeedSpec schema"
```

---

## Task 3: Static source

**Files:**
- Create: `internal/feedsource/static.go`
- Test: `internal/feedsource/static_test.go`

- [ ] **Step 1: Write the failing test**

```go
package feedsource

import (
	"context"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

func TestStaticSourceReturnsFixedFeeds(t *testing.T) {
	feeds := []config.FeedConfig{{URL: "https://e/1", Interval: time.Minute}}
	s := NewStatic("static", feeds)

	got, err := s.Feeds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].URL != "https://e/1" {
		t.Fatalf("feeds = %+v", got)
	}
	if s.Name() != "static" {
		t.Fatalf("name = %q", s.Name())
	}
	// Changes never fires for a static source.
	select {
	case <-s.Changes():
		t.Fatal("static source should not signal changes")
	case <-time.After(20 * time.Millisecond):
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/feedsource/ -run TestStaticSource -v`
Expected: FAIL — `NewStatic` undefined.

- [ ] **Step 3: Write the implementation**

`internal/feedsource/static.go`:

```go
package feedsource

import (
	"context"

	"github.com/iambod/rss2msg/internal/config"
)

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/feedsource/ -run TestStaticSource -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/static.go internal/feedsource/static_test.go
git commit -m "feat(feedsource): static source"
```

---

## Task 4: Aggregator — ordered merge, dedup-by-url, precedence, last-known-good

**Files:**
- Create: `internal/feedsource/aggregator.go`
- Test: `internal/feedsource/aggregator_test.go`

- [ ] **Step 1: Write the failing test**

```go
package feedsource

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

// fakeSource is a test Source with controllable feeds/error and a manual signal.
type fakeSource struct {
	name string
	ch   chan struct{}
	fn   func() ([]config.FeedConfig, error)
}

func newFake(name string, fn func() ([]config.FeedConfig, error)) *fakeSource {
	return &fakeSource{name: name, ch: make(chan struct{}, 1), fn: fn}
}
func (f *fakeSource) Name() string                                        { return f.name }
func (f *fakeSource) Feeds(context.Context) ([]config.FeedConfig, error)  { return f.fn() }
func (f *fakeSource) Changes() <-chan struct{}                            { return f.ch }
func (f *fakeSource) signal()                                             { f.ch <- struct{}{} }

func feed(url string, interval time.Duration) config.FeedConfig {
	return config.FeedConfig{URL: url, Interval: interval}
}

func TestAggregatorMergesAndDedupsByURL(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://x", time.Minute), feed("https://x", time.Minute)}, nil
	})
	b := newFake("b", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://y", time.Minute)}, nil
	})
	agg := NewAggregator(a, b)
	got, err := agg.Desired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 deduped feeds, got %d: %+v", len(got), got)
	}
}

func TestAggregatorEarlierSourceWinsOnCollision(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://x", 1*time.Minute)}, nil
	})
	b := newFake("b", func() ([]config.FeedConfig, error) {
		return []config.FeedConfig{feed("https://x", 9*time.Minute)}, nil
	})
	agg := NewAggregator(a, b) // a has precedence
	got, _ := agg.Desired(context.Background())
	if len(got) != 1 || got[0].Interval != 1*time.Minute {
		t.Fatalf("want winner from a (1m), got %+v", got)
	}
}

func TestAggregatorFailingSourceKeepsLastKnownGood(t *testing.T) {
	fail := false
	a := newFake("a", func() ([]config.FeedConfig, error) {
		if fail {
			return nil, errors.New("boom")
		}
		return []config.FeedConfig{feed("https://x", time.Minute)}, nil
	})
	agg := NewAggregator(a)

	if _, err := agg.Desired(context.Background()); err != nil { // primes last-known-good
		t.Fatal(err)
	}
	fail = true
	got, err := agg.Desired(context.Background())
	if err != nil {
		t.Fatalf("aggregator should not surface source error: %v", err)
	}
	if len(got) != 1 || got[0].URL != "https://x" {
		t.Fatalf("want last-known-good retained, got %+v", got)
	}
}

func TestAggregatorEmptySetAccepted(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) { return nil, nil })
	agg := NewAggregator(a)
	got, err := agg.Desired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty set, got %+v", got)
	}
}

func TestAggregatorChangesFanIn(t *testing.T) {
	a := newFake("a", func() ([]config.FeedConfig, error) { return nil, nil })
	agg := NewAggregator(a)
	a.signal()
	select {
	case <-agg.Changes():
	case <-time.After(time.Second):
		t.Fatal("expected aggregator to forward source signal")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/feedsource/ -run TestAggregator -v`
Expected: FAIL — `NewAggregator` undefined.

- [ ] **Step 3: Write the implementation**

`internal/feedsource/aggregator.go`:

```go
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

	mu        sync.Mutex
	lastGood  map[string][]config.FeedConfig // source name -> last successful feeds
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/feedsource/ -run TestAggregator -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/aggregator.go internal/feedsource/aggregator_test.go
git commit -m "feat(feedsource): aggregator with precedence merge and last-known-good"
```

---

## Task 5: File source

**Files:**
- Create: `internal/feedsource/file.go`
- Test: `internal/feedsource/file_test.go`

The file holds a JSON array of `FeedSpec`. The source watches the file's **directory** (to survive editor atomic-rename writes), debounces, and signals `Changes()`.

- [ ] **Step 1: Write the failing test**

```go
package feedsource

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFeeds(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFileSourceReadsFeeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feeds.json")
	writeFeeds(t, path, `[{"url":"https://e/1","interval":"5m"}]`)

	s, err := NewFile("file", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.Feeds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].URL != "https://e/1" || got[0].Interval != 5*time.Minute {
		t.Fatalf("feeds = %+v", got)
	}
}

func TestFileSourceSignalsOnChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feeds.json")
	writeFeeds(t, path, `[{"url":"https://e/1"}]`)

	s, err := NewFile("file", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	writeFeeds(t, path, `[{"url":"https://e/2"}]`)
	select {
	case <-s.Changes():
	case <-time.After(2 * time.Second):
		t.Fatal("expected a change signal after rewrite")
	}
}

func TestFileSourceMissingFileIsEmptyNotError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "absent.json")
	s, err := NewFile("file", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.Feeds(context.Background())
	if err != nil {
		t.Fatalf("missing file should be empty, not error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/feedsource/ -run TestFileSource -v`
Expected: FAIL — `NewFile` undefined.

- [ ] **Step 3: Write the implementation**

`internal/feedsource/file.go`:

```go
package feedsource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/iambod/rss2msg/internal/config"
)

// File is a source backed by a JSON file containing an array of FeedSpec. It
// watches the file's directory so editor atomic-rename writes are detected, and
// debounces rapid events before signaling Changes.
type File struct {
	name    string
	path    string
	watcher *fsnotify.Watcher
	out     chan struct{}
	done    chan struct{}
}

const fileDebounce = 150 * time.Millisecond

// NewFile creates a File source watching path. It does not require the file to
// exist yet (a missing file reads as an empty feed list).
func NewFile(name, path string) (*File, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("file source %s: watcher: %w", name, err)
	}
	// Watch the parent dir: atomic saves replace the file via rename, which a
	// watch on the file itself would miss.
	if err := w.Add(filepath.Dir(path)); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("file source %s: watch dir: %w", name, err)
	}
	f := &File{
		name:    name,
		path:    path,
		watcher: w,
		out:     make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go f.loop()
	return f, nil
}

func (f *File) Name() string { return f.name }

func (f *File) Changes() <-chan struct{} { return f.out }

func (f *File) Feeds(context.Context) ([]config.FeedConfig, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("file source %s: read: %w", f.name, err)
	}
	var specs []FeedSpec
	if err := json.Unmarshal(data, &specs); err != nil {
		return nil, fmt.Errorf("file source %s: parse: %w", f.name, err)
	}
	return SpecsToConfigs(specs)
}

func (f *File) Close() error {
	close(f.done)
	return f.watcher.Close()
}

func (f *File) loop() {
	var timer *time.Timer
	emit := func() {
		select {
		case f.out <- struct{}{}:
		default:
		}
	}
	for {
		select {
		case <-f.done:
			return
		case ev, ok := <-f.watcher.Events:
			if !ok {
				return
			}
			// Only react to events touching our file (dir watch sees siblings).
			if filepath.Clean(ev.Name) != filepath.Clean(f.path) {
				continue
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(fileDebounce, emit)
		case _, ok := <-f.watcher.Errors:
			if !ok {
				return
			}
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/feedsource/ -run TestFileSource -v`
Expected: PASS

- [ ] **Step 5: Promote fsnotify to a direct dependency and commit**

Run: `go mod tidy`
Then:

```bash
git add internal/feedsource/file.go internal/feedsource/file_test.go go.mod go.sum
git commit -m "feat(feedsource): file source with fsnotify watch and debounce"
```

---

## Task 6: Generic poll-driven source helper

This gives pull-style sources (and a polling fallback) a `Changes()` that ticks on an interval. The follow-up HTTP/DB/Redis source plans wrap their fetch in `NewPoll`.

**Files:**
- Create: `internal/feedsource/poll.go`
- Test: `internal/feedsource/poll_test.go`

- [ ] **Step 1: Write the failing test**

```go
package feedsource

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

func TestPollSourceTicksAndFetches(t *testing.T) {
	var calls int32
	p := NewPoll("poll", 20*time.Millisecond, func(context.Context) ([]config.FeedConfig, error) {
		atomic.AddInt32(&calls, 1)
		return []config.FeedConfig{feed("https://e/1", time.Minute)}, nil
	})
	t.Cleanup(p.Close)

	got, err := p.Feeds(context.Background())
	if err != nil || len(got) != 1 {
		t.Fatalf("feeds=%+v err=%v", got, err)
	}

	signals := 0
	deadline := time.After(200 * time.Millisecond)
	for signals < 2 {
		select {
		case <-p.Changes():
			signals++
		case <-deadline:
			t.Fatalf("want >=2 tick signals, got %d", signals)
		}
	}
	if atomic.LoadInt32(&calls) < 1 {
		t.Fatal("fetch never called")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/feedsource/ -run TestPollSource -v`
Expected: FAIL — `NewPoll` undefined.

- [ ] **Step 3: Write the implementation**

`internal/feedsource/poll.go`:

```go
package feedsource

import (
	"context"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

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
}

// NewPoll returns a Poll source that ticks every interval (minimum 1s).
func NewPoll(name string, interval time.Duration, fetch FetchFunc) *Poll {
	if interval < time.Second {
		interval = time.Second
	}
	p := &Poll{name: name, fetch: fetch, out: make(chan struct{}, 1), stop: make(chan struct{})}
	go p.loop(interval)
	return p
}

func (p *Poll) Name() string { return p.name }

func (p *Poll) Feeds(ctx context.Context) ([]config.FeedConfig, error) { return p.fetch(ctx) }

func (p *Poll) Changes() <-chan struct{} { return p.out }

func (p *Poll) Close() { close(p.stop) }

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/feedsource/ -run TestPollSource -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/poll.go internal/feedsource/poll_test.go
git commit -m "feat(feedsource): generic poll-driven source helper"
```

---

## Task 7: Reconciling scheduler `ServeDynamic`

**Files:**
- Create: `internal/scheduler/dynamic.go`
- Test: `internal/scheduler/dynamic_test.go`

`ServeDynamic` owns a `map[feedURL]*runningFeed`. On each provider signal it recomputes the desired set and diffs by URL: start added, stop removed (cancel → drain), restart changed (any `FeedConfig` field differs → cancel + start fresh, which also resets the ticker). It reuses the existing `runFeedLoop` from `serve.go`.

- [ ] **Step 1: Write the failing test**

```go
package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/model"
)

// countingPipeline records ticks per feed URL.
type countingPipeline struct {
	url   string
	calls *int32
}

func (c countingPipeline) FeedURL() string { return c.url }
func (c countingPipeline) RunOnce(ctx context.Context, feedURL string, at time.Time) ([]model.Change, error) {
	atomic.AddInt32(c.calls, 1)
	return nil, nil
}

// manualProvider lets the test push desired sets.
type manualProvider struct {
	mu   sync.Mutex
	cur  []config.FeedConfig
	ch   chan struct{}
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
			return countingPipeline{url: fc.URL, calls: &c1}, nil
		default:
			return countingPipeline{url: fc.URL, calls: &c2}, nil
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
		return countingPipeline{url: fc.URL, calls: &calls}, nil
	}
	prov := newManualProvider()
	prov.cur = []config.FeedConfig{{URL: "https://e/1", Interval: 20 * time.Millisecond}}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ServeDynamic(ctx, DynamicConfig{Provider: prov, Factory: factory, DrainTimeout: 200 * time.Millisecond}) }()

	time.Sleep(60 * time.Millisecond)
	prov.set(nil) // drain everything
	time.Sleep(60 * time.Millisecond)
	stopped := atomic.LoadInt32(&calls)
	time.Sleep(60 * time.Millisecond)
	if atomic.LoadInt32(&calls) != stopped {
		t.Fatal("feeds kept ticking after empty reconcile")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/scheduler/ -run TestServeDynamic -v`
Expected: FAIL — `ServeDynamic`/`DynamicConfig` undefined.

- [ ] **Step 3: Write the implementation**

`internal/scheduler/dynamic.go`:

```go
package scheduler

import (
	"context"
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

// PipelineFactory builds a FeedPipeline for one feed. Returning an error skips
// that feed for this reconcile (logged by the factory); other feeds proceed.
type PipelineFactory func(fc config.FeedConfig) (FeedPipeline, error)

// DynamicConfig configures ServeDynamic.
type DynamicConfig struct {
	Provider     FeedProvider
	Factory      PipelineFactory
	DrainTimeout time.Duration
	// OnReconcile, if set, is called after each reconcile with the counts of
	// feeds added/removed/changed. Optional; used for logging/metrics.
	OnReconcile func(added, removed, changed int)
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
			return // provider keeps last-known-good; nothing to do
		}
		byURL := make(map[string]config.FeedConfig, len(desired))
		for _, fc := range desired {
			byURL[fc.URL] = fc
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
		// Add new / restart changed.
		for url, fc := range byURL {
			rf, ok := running[url]
			if ok && reflect.DeepEqual(rf.cfg, fc) {
				continue // unchanged: leave running (ticker untouched)
			}
			if ok {
				rf.cancel() // changed: stop then restart (resets ticker)
				<-rf.done
				changed++
			} else {
				added++
			}
			if nf := startFeed(ctx, fc, cfg.Factory); nf != nil {
				running[url] = nf
			} else {
				delete(running, url) // factory failed; don't leave a stale entry
			}
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

// startFeed builds a pipeline and launches its loop. Returns nil if the factory
// fails (the feed is skipped this round).
func startFeed(parent context.Context, fc config.FeedConfig, factory PipelineFactory) *runningFeed {
	p, err := factory(fc)
	if err != nil {
		return nil
	}
	fctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runFeedLoop(fctx, p, fc.Interval, func(error) {})
	}()
	return &runningFeed{cfg: fc, cancel: cancel, done: done}
}

// drainAll cancels and waits for every running feed, bounded by timeout.
func drainAll(running map[string]*runningFeed, timeout time.Duration) {
	for _, rf := range running {
		rf.cancel()
	}
	deadline := time.After(timeout)
	for _, rf := range running {
		select {
		case <-rf.done:
		case <-deadline:
			return
		}
	}
}
```

Note: `runFeedLoop` already exists in `serve.go` and ticks once immediately on start, so added feeds poll right away. The `collect` callback is a no-op here because per-feed errors are logged inside the pipeline; surface them via `OnReconcile`/pipeline logging instead of a joined error.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/scheduler/ -run TestServeDynamic -v`
Expected: PASS

- [ ] **Step 5: Run the whole scheduler package**

Run: `go test ./internal/scheduler/ -v`
Expected: PASS (existing `TestServeRunsEachFeedOnSchedule` still green).

- [ ] **Step 6: Commit**

```bash
git add internal/scheduler/dynamic.go internal/scheduler/dynamic_test.go
git commit -m "feat(scheduler): ServeDynamic reconciling feed loop"
```

---

## Task 8: Extract a pipeline factory in `wire.go`

Currently `wireAll` inlines per-feed pipeline construction. Extract a `pipelineFactory` closure so both boot wiring and `ServeDynamic` build pipelines the same way.

**Files:**
- Modify: `cmd/rss2msg/wire.go`
- Modify: `cmd/rss2msg/main.go` (compile only; full wiring in Task 9)

- [ ] **Step 1: Add the factory builder to `wire.go`**

Add to `cmd/rss2msg/wire.go`:

```go
// newPipelineFactory returns a factory that builds a *pipeline for any feed,
// sharing the wired fetcher/detector/store/coord/instruments. Used both at boot
// and by ServeDynamic to construct pipelines for feeds added at runtime.
func (w *wired) newPipelineFactory(cfg config.Config, tel *telemetry.Telemetry, fetcher *feed.Fetcher, det *feed.Detector, instr telemetry.Instruments) scheduler.PipelineFactory {
	return func(fc config.FeedConfig) (scheduler.FeedPipeline, error) {
		names := config.ResolveFeedSinks(fc)
		branches := make([]sinkBranch, 0, len(names))
		for _, name := range names {
			primary, ok := w.registry.Get(name)
			if !ok {
				return nil, fmt.Errorf("feed %s: unknown sink %q", fc.URL, name)
			}
			scCfg := findSink(cfg.Sinks, name)
			var dlq sink.Publisher
			if scCfg.DeadLetter != "" {
				dlq, _ = w.registry.Get(scCfg.DeadLetter)
			}
			wrapped := sink.WithRetry(primary, dlq, retry.Config{
				MaxAttempts: cfg.Retry.MaxAttempts,
				BaseDelay:   cfg.Retry.BaseDelay,
				MaxDelay:    cfg.Retry.MaxDelay,
			})
			branches = append(branches, sinkBranch{name: name, wrapped: wrapped})
		}
		return &pipeline{
			cfg:     fc,
			sinks:   branches,
			fetcher: fetcher,
			detect:  det,
			store:   w.store,
			log:     tel.Logger,
			tracer:  tel.Tracer,
			instr:   instr,
			coord:   w.coord,
		}, nil
	}
}
```

Add `"github.com/iambod/rss2msg/internal/scheduler"` to `wire.go` imports.

- [ ] **Step 2: Refactor `wireAll`'s feed loop to use the factory**

In `wireAll`, replace the `for _, fc := range cfg.Feeds { ... }` block (lines ~88-120) with:

```go
	factory := w.newPipelineFactory(cfg, tel, fetcher, det, instr)
	for _, fc := range cfg.Feeds {
		p, err := factory(fc)
		if err != nil {
			w.Close()
			return nil, err
		}
		w.pipelines = append(w.pipelines, p.(*pipeline))
	}
	w.factory = factory
	return w, nil
```

Add a `factory scheduler.PipelineFactory` field to the `wired` struct so Task 9 can reach it:

```go
type wired struct {
	store     state.Store
	registry  *sink.Registry
	coord     coord.Coordinator
	pipelines []*pipeline
	factory   scheduler.PipelineFactory
}
```

- [ ] **Step 3: Build to verify it compiles**

Run: `go build ./...`
Expected: success (no behavior change yet).

- [ ] **Step 4: Run existing tests**

Run: `go test ./cmd/... ./internal/... -count=1`
Expected: PASS (refactor is behavior-preserving).

- [ ] **Step 5: Commit**

```bash
git add cmd/rss2msg/wire.go
git commit -m "refactor(wire): extract pipeline factory shared by boot and reconcile"
```

---

## Task 9: Wire the `serve` command to sources + aggregator + SIGHUP

**Files:**
- Modify: `cmd/rss2msg/main.go`
- Create: `cmd/rss2msg/sources.go` (builds sources from config)

- [ ] **Step 1: Build sources from config**

Create `cmd/rss2msg/sources.go`:

```go
package main

import (
	"fmt"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/feedsource"
)

// buildSources constructs the ordered source list from config. If no
// feed_sources are configured, the static feeds: block is the sole source
// (preserving today's behavior). When feed_sources IS configured, a "static"
// entry injects the feeds: block at its position; otherwise the static block is
// not included.
func buildSources(cfg config.Config) ([]feedsource.Source, func(), error) {
	staticName := "static"
	if len(cfg.FeedSources) == 0 {
		return []feedsource.Source{feedsource.NewStatic(staticName, cfg.Feeds)}, func() {}, nil
	}

	var sources []feedsource.Source
	var closers []func()
	for i, sc := range cfg.FeedSources {
		name := sc.Name
		if name == "" {
			name = fmt.Sprintf("%s[%d]", sc.Type, i)
		}
		switch sc.Type {
		case "static":
			sources = append(sources, feedsource.NewStatic(name, cfg.Feeds))
		case "file":
			f, err := feedsource.NewFile(name, sc.Path)
			if err != nil {
				closeAll(closers)
				return nil, nil, err
			}
			closers = append(closers, func() { _ = f.Close() })
			sources = append(sources, f)
		default:
			closeAll(closers)
			return nil, nil, fmt.Errorf("feed_sources[%d]: unsupported type %q", i, sc.Type)
		}
	}
	return sources, func() { closeAll(closers) }, nil
}

func closeAll(fns []func()) {
	for _, fn := range fns {
		fn()
	}
}
```

(Follow-up plans add `case "http"`, `"postgres"`, `"sqlite"`, `"redis"`, `"s3"`, `"env"` here, each wrapping `feedsource.NewPoll`.)

- [ ] **Step 2: Rewrite the serve command to use ServeDynamic + SIGHUP**

In `cmd/rss2msg/main.go`, replace the body of `newServeCmd`'s `RunE` (lines ~39-61) with:

```go
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg, tel, w, err := bootstrap(ctx, opts)
			if err != nil {
				return err
			}
			defer func() {
				_ = tel.Shutdown(context.Background())
				w.Close()
			}()

			sources, closeSources, err := buildSources(cfg)
			if err != nil {
				return err
			}
			defer closeSources()

			agg := feedsource.NewAggregator(sources...)

			// SIGHUP forces a re-read of all sources.
			hup := make(chan os.Signal, 1)
			signal.Notify(hup, syscall.SIGHUP)
			defer signal.Stop(hup)
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-hup:
						tel.Logger.Info().Msg("SIGHUP: reloading feed sources")
						agg.Trigger()
					}
				}
			}()

			return scheduler.ServeDynamic(ctx, scheduler.DynamicConfig{
				Provider:     agg,
				Factory:      w.factory,
				DrainTimeout: cfg.Runtime.ShutdownDrainTimeout,
				OnReconcile: func(added, removed, changed int) {
					if added+removed+changed > 0 {
						tel.Logger.Info().
							Int("added", added).Int("removed", removed).Int("changed", changed).
							Msg("feeds reconciled")
					}
				},
			})
		},
```

Add imports to `main.go`: `"os/signal"`, `"syscall"`, `"github.com/iambod/rss2msg/internal/feedsource"`. (`os`, `context`, `scheduler` are already imported.)

- [ ] **Step 3: Build**

Run: `go build ./...`
Expected: success.

- [ ] **Step 4: Smoke-test serve with the static block**

Run:
```bash
go build -o bin/rss2msg ./cmd/rss2msg
RSS2MSG_LOG__LEVEL=debug ./bin/rss2msg serve --config config.yaml & sleep 2; kill %1
```
Expected: starts, ticks feeds from the static block, shuts down cleanly. (Confirms no regression vs the old static path.)

- [ ] **Step 5: Manual reconcile check (file source)**

Create a throwaway config `/tmp/dyn.yaml` with a `sinks:` entry named `default` (driver `stdout`), an empty `feeds: []`, and:
```yaml
feed_sources:
  - type: file
    name: cp
    path: /tmp/feeds.json
state:
  driver: sqlite
  sqlite:
    path: /tmp/dyn.db
```
Then:
```bash
echo '[{"url":"https://hnrss.org/frontpage","interval":"30s"}]' > /tmp/feeds.json
./bin/rss2msg serve --config /tmp/dyn.yaml &
sleep 3
echo '[]' > /tmp/feeds.json   # drain
sleep 3
kill %1
```
Expected: logs show "feeds reconciled added=1" then "removed=1" without a restart.

- [ ] **Step 6: Commit**

```bash
git add cmd/rss2msg/main.go cmd/rss2msg/sources.go
git commit -m "feat(serve): dynamic feed reconcile from sources with SIGHUP reload"
```

---

## Task 10: Validate `feed_sources`

**Files:**
- Modify: `internal/config/validate.go`
- Test: `internal/config/validate_test.go`

The existing feed validation requires at least one feed. With dynamic sources, the desired set can legitimately start empty, so relax that to: require **either** a non-empty `feeds:` block **or** at least one `feed_sources` entry. Validate each source's required fields.

- [ ] **Step 1: Write the failing test**

```go
func TestValidateAllowsEmptyFeedsWithSources(t *testing.T) {
	cfg := config.Defaults()
	cfg.State = config.StateConfig{Driver: "sqlite", SQLite: config.SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []config.SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.Feeds = nil
	cfg.FeedSources = []config.FeedSourceConfig{{Type: "file", Path: "/tmp/feeds.json"}}
	if err := config.Validate(cfg); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
}

func TestValidateRejectsFileSourceWithoutPath(t *testing.T) {
	cfg := config.Defaults()
	cfg.State = config.StateConfig{Driver: "sqlite", SQLite: config.SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []config.SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.FeedSources = []config.FeedSourceConfig{{Type: "file"}}
	if err := config.Validate(cfg); err == nil {
		t.Fatal("expected error for file source without path")
	}
}

func TestValidateRejectsNoFeedsAndNoSources(t *testing.T) {
	cfg := config.Defaults()
	cfg.State = config.StateConfig{Driver: "sqlite", SQLite: config.SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []config.SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.Feeds = nil
	cfg.FeedSources = nil
	if err := config.Validate(cfg); err == nil {
		t.Fatal("expected error when neither feeds nor feed_sources is set")
	}
}
```

(Confirm the exact `Validate` signature and existing helpers in `validate.go` before writing; adjust the state/sink setup to whatever makes the rest of `Validate` pass in current tests.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestValidate -v`
Expected: FAIL — empty-feeds case currently errors; file-source validation absent.

- [ ] **Step 3: Update validation**

In `internal/config/validate.go`, change the feeds guard from:

```go
	if len(c.Feeds) == 0 {
		return fmt.Errorf("at least one feed must be declared")
	}
```
to:
```go
	if len(c.Feeds) == 0 && len(c.FeedSources) == 0 {
		return fmt.Errorf("at least one feed or feed_source must be declared")
	}
```

Then, after the existing `for i, f := range c.Feeds { ... }` loop, add source validation:

```go
	staticSeen := false
	for i, s := range c.FeedSources {
		switch s.Type {
		case "static":
			staticSeen = true
		case "file":
			if strings.TrimSpace(s.Path) == "" {
				return fmt.Errorf("feed_sources[%d] (file): path is required", i)
			}
		case "":
			return fmt.Errorf("feed_sources[%d]: type is required", i)
		default:
			return fmt.Errorf("feed_sources[%d]: unsupported type %q", i, s.Type)
		}
		if s.Interval != 0 && s.Interval < time.Second {
			return fmt.Errorf("feed_sources[%d].interval %v is below the 1s minimum", i, s.Interval)
		}
	}
	_ = staticSeen
```

(As later source types land, extend the `switch`. `staticSeen` is a hook for a future "static entry references empty feeds:" warning; left unused for now.)

- [ ] **Step 4: Run tests**

Run: `go test ./internal/config/ -v`
Expected: PASS (new tests green; pre-existing validation tests still green — check none relied on the old "at least one feed" message).

- [ ] **Step 5: Commit**

```bash
git add internal/config/validate.go internal/config/validate_test.go
git commit -m "feat(config): validate feed_sources and allow feeds-via-sources"
```

---

## Task 11: Documentation

**Files:**
- Modify: `README.md` (feeds/config section)
- Modify: `config.example.yaml`

- [ ] **Step 1: Document dynamic feeds in README**

Add a "Dynamic feeds" subsection near the existing feeds docs covering:
- `feed_sources:` is an ordered list; **order is precedence** (earlier wins on URL collision); feeds dedup by `url`.
- The `static` type injects the `feeds:` block at its position; with no `feed_sources`, the `feeds:` block is the sole source (unchanged behavior).
- Per-source `interval` for poll sources; the `file` source watches its file and reloads on change; **SIGHUP** forces a reload of all sources.
- Scope: **feeds-only** reload; the `feed_sources` set itself and all other config require a restart.
- A feed with no `sinks` resolves to the `default` sink; an unknown sink fails the whole reload atomically.
- Removed feeds keep their state (re-adding resumes); an `interval` change resets that feed's ticker.

- [ ] **Step 2: Add a commented example to `config.example.yaml`**

```yaml
# Dynamic feed sources (optional). Ordered list; earlier entries win on URL
# collision. Omit entirely to use only the static feeds: block above.
# feed_sources:
#   - type: file        # watches the file; reloads on change (and on SIGHUP)
#     name: control-plane
#     path: /etc/rss2msg/feeds.json
#   - type: static      # injects the feeds: block at this precedence position
```

- [ ] **Step 3: Verify the build and full suite**

Run: `go build ./... && go test ./... -count=1`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add README.md config.example.yaml
git commit -m "docs: document dynamic feed sources"
```

---

## Task 12: Final verification

- [ ] **Step 1: Full test suite + vet**

Run:
```bash
go vet ./...
go test ./... -count=1
```
Expected: all PASS.

- [ ] **Step 2: Confirm acceptance criteria from issue #37 covered by this plan**

Check off, in the issue or a PR description:
- Static block + file source reconcile live (added/removed/changed) — Tasks 7, 9.
- Dedup by `url`; earlier source wins — Task 4.
- Removed feed keeps state; interval change resets ticker — Task 7 (restart-on-change).
- No sinks → `default`; unknown sink fails reload — Task 8 (factory returns error → reconcile skips; at boot it still hard-fails). NOTE: for runtime reconcile, an unknown sink currently skips just that feed via the factory error. **If the issue's "fail the WHOLE reload atomically" must hold at runtime too, add a pre-flight validation pass in `reconcile` that rejects the entire desired set when any feed names an undeclared sink — see Open item below.**
- Empty merged set accepted — Tasks 4, 7.
- Failing source keeps last-known-good — Task 4.
- SIGHUP + file-watch + poll triggers — Tasks 9, 5, 6.
- `feed_sources` set not hot-reloadable — by construction (built once at boot).

- [ ] **Step 3: Open item to resolve during review**

The atomic "unknown sink fails the whole reload" rule (issue, Sink resolution) is only partially enforced: the per-feed factory error skips one feed. To enforce atomicity at runtime, add a validation step at the top of `reconcile` that resolves every desired feed's sinks against `w.registry` *before* mutating `running`, and aborts the whole reconcile (keeping the current set) if any are unknown. Decide whether to do this now or in the follow-up that adds the first external source (where unknown-sink risk is higher). Recommended: do it now, as a small Task 7 addendum.

---

## Follow-up plans (NOT in this plan)

Each adds one source `case` in `cmd/rss2msg/sources.go` wrapping `feedsource.NewPoll`, a small adapter that fetches `[]FeedSpec` and converts via `SpecsToConfigs`, plus per-type config fields on `FeedSourceConfig` and validation. One plan each:

1. **HTTP source** — GET a JSON `[]FeedSpec`; headers/auth; ETag-based change skip.
2. **Postgres source** — query rows → `FeedSpec`; reuse pgx + TLS config patterns.
3. **SQLite source** — query a local DB → `FeedSpec` (mirrors state sqlite).
4. **Redis source** — read a key/structure → `FeedSpec`.
5. **Object storage (S3)** — fetch an object → `[]FeedSpec`.
6. **Environment source** — parse env vars → `FeedSpec`.

Each is independently testable and ships working software on top of this core.
