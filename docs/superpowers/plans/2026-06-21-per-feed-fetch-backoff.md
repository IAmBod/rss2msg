# Per-feed Backoff on Fetch Failures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retry a feed fetch within the same poll tick on transient errors (5xx/429/timeout/network) using exponential backoff with full jitter, configurable globally and per-feed.

**Architecture:** Reuse the existing `internal/retry` helper (exponential backoff + full jitter) for feed fetches. Add an optional `Retryable` predicate to `retry.Config` so only transient errors retry. The fetcher returns a typed `*feed.FetchError` that a `feed.IsRetryable` classifier inspects. The per-feed pipeline wraps its fetch call in `retry.Do`; the pipeline factory computes the effective retry config by merging the global `http.retry` with the per-feed `feeds[].http.retry` override.

**Tech Stack:** Go 1.25, Viper config, zerolog + OpenTelemetry, testify, `net/http/httptest`.

## Global Constraints

- Go 1.25; Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`).
- **Staging hazard:** never `git add -A`/`.`; stage explicit pathspecs and verify with `git status` before each commit (the working copy is an Obsidian vault with auto-staging).
- TDD: write the failing test first, watch it fail, then implement.
- `task test` runs `go test -race ./...`; `task vet` runs `go vet ./...`.
- `examples/config.example.yaml` and `internal/config/example.yaml` MUST stay byte-identical (a test enforces this).
- Default fetch retry: `max_attempts=3`, `base_delay=500ms`, `max_delay=10s`. `max_attempts: 1` disables retry (= current behavior).
- Transient (retry): transport/network errors, request timeouts (`context.DeadlineExceeded`), HTTP `5xx`, HTTP `429`. Permanent (no retry): `4xx` other than `429`, parse errors, malformed-request errors, `context.Canceled`.
- Existing sink-delivery retry behavior must not change (`retry.Config` constructed without `Retryable` stays "retry all").

---

### Task 1: Optional retryable predicate in `internal/retry`

**Files:**
- Modify: `internal/retry/retry.go`
- Test: `internal/retry/retry_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `retry.Config.Retryable func(error) bool` — when non-nil and it returns `false` for an error from `fn`, `Do` stops immediately and returns that error in `Result.Err` with the attempts made so far. Nil preserves "retry every error".

- [ ] **Step 1: Write the failing tests**

Append to `internal/retry/retry_test.go` (keep the existing package clause and imports; add `errors` if not present):

```go
func TestDoStopsOnNonRetryableError(t *testing.T) {
	stop := errors.New("permanent")
	calls := 0
	res := Do(context.Background(), Config{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		Retryable:   func(error) bool { return false },
	}, func(context.Context) error {
		calls++
		return stop
	})
	if calls != 1 {
		t.Fatalf("expected fn called once, got %d", calls)
	}
	if res.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", res.Attempts)
	}
	if !errors.Is(res.Err, stop) {
		t.Fatalf("expected stop error, got %v", res.Err)
	}
}

func TestDoRetriesWhenPredicateAllows(t *testing.T) {
	calls := 0
	res := Do(context.Background(), Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		Retryable:   func(error) bool { return true },
	}, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if res.Err != nil {
		t.Fatalf("expected success, got %v", res.Err)
	}
}

func TestDoNilPredicateRetriesAll(t *testing.T) {
	calls := 0
	res := Do(context.Background(), Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
	}, func(context.Context) error {
		calls++
		return errors.New("boom")
	})
	if calls != 3 {
		t.Fatalf("expected 3 calls (nil predicate retries all), got %d", calls)
	}
	if res.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", res.Attempts)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd .worktrees/feed-fetch-backoff && go test ./internal/retry/ -run 'TestDoStopsOnNonRetryableError|TestDoRetriesWhenPredicateAllows|TestDoNilPredicateRetriesAll' -v`
Expected: compile failure — `Config has no field Retryable` (or test failures once it compiles).

- [ ] **Step 3: Add the `Retryable` field**

In `internal/retry/retry.go`, extend `Config`:

```go
type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	// Retryable reports whether an error returned by fn is worth retrying.
	// Nil means "retry any error" (the behavior used by sink delivery).
	Retryable func(error) bool
}
```

- [ ] **Step 4: Honor the predicate in `Do`**

In `Do`, replace the block that records `lastErr` and checks `attempt == cfg.MaxAttempts`. The current loop body is:

```go
		err := fn(ctx)
		if err == nil {
			return Result{Attempts: attempt, Err: nil}
		}
		lastErr = err
		if attempt == cfg.MaxAttempts {
			break
		}
```

Change it to:

```go
		err := fn(ctx)
		if err == nil {
			return Result{Attempts: attempt, Err: nil}
		}
		lastErr = err
		if cfg.Retryable != nil && !cfg.Retryable(err) {
			return Result{Attempts: attempt, Err: err}
		}
		if attempt == cfg.MaxAttempts {
			break
		}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd .worktrees/feed-fetch-backoff && go test ./internal/retry/ -v`
Expected: PASS (including the pre-existing tests).

- [ ] **Step 6: Commit**

```bash
cd .worktrees/feed-fetch-backoff
git add internal/retry/retry.go internal/retry/retry_test.go
git status   # verify only these two files are staged
git commit -m "feat(retry): optional Retryable predicate to stop on terminal errors"
```

---

### Task 2: Typed fetch errors + `IsRetryable` classifier in `internal/feed`

**Files:**
- Create: `internal/feed/errors.go`
- Create: `internal/feed/errors_test.go`
- Modify: `internal/feed/fetcher.go` (the four error returns)
- Test: `internal/feed/fetcher_test.go` (add typed-error assertions)

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `feed.FetchError{Op string, Status int, Err error}` with `Error() string` and `Unwrap() error`. `Op` ∈ `"request" | "transport" | "parse" | "status"`; `Status` is the HTTP status when `Op == "status"`.
  - `feed.IsRetryable(err error) bool` — `transport` → true unless `errors.Is(err, context.Canceled)`; `status` → `Status >= 500 || Status == 429`; everything else → false.

- [ ] **Step 1: Write the failing classifier test**

Create `internal/feed/errors_test.go`:

```go
package feed

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"transport", &FetchError{Op: "transport", Err: errors.New("conn refused")}, true},
		{"transport-timeout", &FetchError{Op: "transport", Err: context.DeadlineExceeded}, true},
		{"transport-canceled", &FetchError{Op: "transport", Err: context.Canceled}, false},
		{"status-500", &FetchError{Op: "status", Status: 500, Err: errors.New("unexpected status 500")}, true},
		{"status-503", &FetchError{Op: "status", Status: 503, Err: errors.New("unexpected status 503")}, true},
		{"status-429", &FetchError{Op: "status", Status: 429, Err: errors.New("unexpected status 429")}, true},
		{"status-404", &FetchError{Op: "status", Status: 404, Err: errors.New("unexpected status 404")}, false},
		{"status-401", &FetchError{Op: "status", Status: 401, Err: errors.New("unexpected status 401")}, false},
		{"parse", &FetchError{Op: "parse", Err: errors.New("bad xml")}, false},
		{"request", &FetchError{Op: "request", Err: errors.New("bad url")}, false},
		{"untyped", errors.New("???"), false},
		{"wrapped-transport", fmt.Errorf("poll: %w", &FetchError{Op: "transport", Err: errors.New("x")}), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsRetryable(c.err); got != c.want {
				t.Fatalf("IsRetryable(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd .worktrees/feed-fetch-backoff && go test ./internal/feed/ -run TestIsRetryable -v`
Expected: compile failure — `undefined: FetchError` / `undefined: IsRetryable`.

- [ ] **Step 3: Implement the typed error and classifier**

Create `internal/feed/errors.go`:

```go
package feed

import (
	"context"
	"errors"
)

// FetchError categorizes a feed-fetch failure so callers can decide whether a
// retry is worthwhile without matching on error strings.
type FetchError struct {
	Op     string // "request" | "transport" | "parse" | "status"
	Status int    // HTTP status code when Op == "status"
	Err    error
}

func (e *FetchError) Error() string { return e.Err.Error() }
func (e *FetchError) Unwrap() error { return e.Err }

// IsRetryable reports whether err represents a transient fetch failure that is
// worth retrying: transport/network errors (except context cancellation) and
// HTTP 5xx / 429 responses. Parse errors, bad requests, and 4xx are permanent.
func IsRetryable(err error) bool {
	var fe *FetchError
	if !errors.As(err, &fe) {
		return false
	}
	switch fe.Op {
	case "transport":
		return !errors.Is(err, context.Canceled)
	case "status":
		return fe.Status >= 500 || fe.Status == 429
	default: // "parse", "request"
		return false
	}
}
```

- [ ] **Step 4: Run the classifier test to verify it passes**

Run: `cd .worktrees/feed-fetch-backoff && go test ./internal/feed/ -run TestIsRetryable -v`
Expected: PASS.

- [ ] **Step 5: Wire the typed errors into the fetcher**

In `internal/feed/fetcher.go`, replace the four error returns (the `Error()` strings are preserved so existing logs read the same):

Line ~57 (build request):
```go
		return FetchResult{}, &FetchError{Op: "request", Err: fmt.Errorf("build request: %w", err)}
```
Line ~88 (http get):
```go
		return FetchResult{}, &FetchError{Op: "transport", Err: fmt.Errorf("http get: %w", err)}
```
Line ~109 (parse feed):
```go
			return res, &FetchError{Op: "parse", Err: fmt.Errorf("parse feed: %w", err)}
```
Line ~114 (unexpected status):
```go
		return res, &FetchError{Op: "status", Status: resp.StatusCode, Err: fmt.Errorf("unexpected status %d", resp.StatusCode)}
```

(`fmt` is already imported in `fetcher.go`.)

- [ ] **Step 6: Add fetcher typed-error assertions**

Append to `internal/feed/fetcher_test.go` (reuse the package's existing imports; add `errors`, `net/http`, `net/http/httptest` if not already present):

```go
func TestFetchReturnsTypedStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	_, err := NewFetcher(Options{}).Fetch(context.Background(), FetchRequest{URL: srv.URL})
	var fe *FetchError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FetchError, got %T: %v", err, err)
	}
	if fe.Op != "status" || fe.Status != http.StatusServiceUnavailable {
		t.Fatalf("got Op=%q Status=%d", fe.Op, fe.Status)
	}
	if !IsRetryable(err) {
		t.Fatalf("503 should be retryable")
	}
}

func TestFetchReturnsTypedParseError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not xml at all"))
	}))
	defer srv.Close()

	_, err := NewFetcher(Options{}).Fetch(context.Background(), FetchRequest{URL: srv.URL})
	var fe *FetchError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FetchError, got %T: %v", err, err)
	}
	if fe.Op != "parse" {
		t.Fatalf("got Op=%q, want parse", fe.Op)
	}
	if IsRetryable(err) {
		t.Fatalf("parse errors must not be retryable")
	}
}
```

- [ ] **Step 7: Run the feed tests to verify they pass**

Run: `cd .worktrees/feed-fetch-backoff && go test ./internal/feed/ -v`
Expected: PASS (all pre-existing tests still pass).

- [ ] **Step 8: Commit**

```bash
cd .worktrees/feed-fetch-backoff
git add internal/feed/errors.go internal/feed/errors_test.go internal/feed/fetcher.go internal/feed/fetcher_test.go
git status
git commit -m "feat(feed): typed FetchError and IsRetryable classifier"
```

---

### Task 3: Config — `http.retry` (global) and `feeds[].http.retry` (per-feed)

**Files:**
- Modify: `internal/config/config.go` (`HTTPConfig`, `FeedHTTPConfig`, `Defaults()`)
- Modify: `internal/config/load.go` (`applyDefaults`)
- Modify: `internal/config/validate.go` (new `validateRetry` + call sites)
- Test: `internal/config/validate_test.go`, `internal/config/load_test.go` (or the existing defaults test)

**Interfaces:**
- Consumes: existing `config.RetryConfig{MaxAttempts int, BaseDelay, MaxDelay time.Duration}`.
- Produces:
  - `config.HTTPConfig.Retry RetryConfig` (`mapstructure:"retry"`).
  - `config.FeedHTTPConfig.Retry RetryConfig` (`mapstructure:"retry"`).
  - Defaults: `http.retry.max_attempts=3`, `http.retry.base_delay=500ms`, `http.retry.max_delay=10s`.

- [ ] **Step 1: Write failing config tests**

Append to `internal/config/validate_test.go` (match the file's existing test style/imports — it already imports `config`/`testing`; add `time` if needed):

```go
func TestValidateRejectsNegativeFetchRetry(t *testing.T) {
	c := minimalValidConfig(t)
	c.HTTP.Retry.MaxAttempts = -1
	if _, err := Validate(c); err == nil {
		t.Fatalf("expected error for negative http.retry.max_attempts")
	}
}

func TestValidateRejectsMaxDelayBelowBaseDelay(t *testing.T) {
	c := minimalValidConfig(t)
	c.HTTP.Retry.BaseDelay = 2 * time.Second
	c.HTTP.Retry.MaxDelay = time.Second
	if _, err := Validate(c); err == nil {
		t.Fatalf("expected error for http.retry.max_delay < base_delay")
	}
}

func TestValidateRejectsNegativePerFeedRetry(t *testing.T) {
	c := minimalValidConfig(t)
	c.Feeds[0].HTTP.Retry.MaxAttempts = -2
	if _, err := Validate(c); err == nil {
		t.Fatalf("expected error for negative feeds[0].http.retry.max_attempts")
	}
}

func TestValidateAllowsZeroPerFeedRetry(t *testing.T) {
	c := minimalValidConfig(t)
	c.Feeds[0].HTTP.Retry = RetryConfig{} // all zero == inherit
	if _, err := Validate(c); err != nil {
		t.Fatalf("zero per-feed retry should inherit, got error: %v", err)
	}
}
```

> **Helper note:** `minimalValidConfig(t)` must return a `Config` that already passes `Validate` (at least one sink and one feed referencing it). If a similar helper already exists in `internal/config` tests, reuse it and delete this scaffold. Otherwise add it once near the top of `validate_test.go`:
>
> ```go
> func minimalValidConfig(t *testing.T) Config {
> 	t.Helper()
> 	c := Defaults()
> 	c.Sinks = []SinkConfig{{Name: "default", Type: "stdout"}}
> 	c.Feeds = []FeedConfig{{URL: "https://e/feed.xml", Interval: time.Minute, Sinks: []string{"default"}}}
> 	return c
> }
> ```
>
> Before writing it, grep the test file for an existing constructor (e.g. `grep -n "func.*Config {" internal/config/validate_test.go`) and prefer it. Confirm `stdout` is a valid sink type for validation; if not, use whatever minimal sink the existing passing tests use.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd .worktrees/feed-fetch-backoff && go test ./internal/config/ -run 'FetchRetry|MaxDelayBelow|PerFeedRetry' -v`
Expected: compile failure — `HTTPConfig has no field Retry` (and/or `FeedHTTPConfig has no field Retry`).

- [ ] **Step 3: Add the config fields**

In `internal/config/config.go`:

```go
type HTTPConfig struct {
	UserAgent string        `mapstructure:"user_agent"`
	Timeout   time.Duration `mapstructure:"timeout"`
	Retry     RetryConfig   `mapstructure:"retry"`
}
```

```go
type FeedHTTPConfig struct {
	Timeout time.Duration     `mapstructure:"timeout"`
	Headers map[string]string `mapstructure:"headers"`
	Retry   RetryConfig       `mapstructure:"retry"`
}
```

- [ ] **Step 4: Add the global default**

In `Defaults()` (`internal/config/config.go`), set the HTTP retry default:

```go
		HTTP: HTTPConfig{
			UserAgent: "rss2msg/0.1",
			Timeout:   30 * time.Second,
			Retry:     RetryConfig{MaxAttempts: 3, BaseDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second},
		},
```

In `applyDefaults` (`internal/config/load.go`), register the keys so `RSS2MSG_HTTP__RETRY__*` env overrides bind:

```go
	v.SetDefault("http.retry.max_attempts", d.HTTP.Retry.MaxAttempts)
	v.SetDefault("http.retry.base_delay", d.HTTP.Retry.BaseDelay)
	v.SetDefault("http.retry.max_delay", d.HTTP.Retry.MaxDelay)
```

- [ ] **Step 5: Add validation**

In `internal/config/validate.go`, add a helper (near the other validate helpers):

```go
func validateRetry(label string, r RetryConfig) error {
	if r.MaxAttempts < 0 {
		return fmt.Errorf("%s.max_attempts must not be negative", label)
	}
	if r.BaseDelay < 0 {
		return fmt.Errorf("%s.base_delay must not be negative", label)
	}
	if r.MaxDelay < 0 {
		return fmt.Errorf("%s.max_delay must not be negative", label)
	}
	if r.MaxDelay != 0 && r.BaseDelay != 0 && r.MaxDelay < r.BaseDelay {
		return fmt.Errorf("%s.max_delay %v is below base_delay %v", label, r.MaxDelay, r.BaseDelay)
	}
	return nil
}
```

> **Note:** confirm whether `validate.go` refers to config types as bare names (`RetryConfig`) because it is in `package config`, or via an alias. Match the file's existing convention (the surrounding code uses bare `c.Feeds`, `s.HTTP`, etc., so it is `package config` — use bare `RetryConfig`).

Call it for the global block. Add this near the start of `Validate` where other top-level blocks are checked (search for where `c.HTTP` / `c.Retry` is referenced; if neither is validated yet, add it just before the `for i, f := range c.Feeds` loop):

```go
	if err := validateRetry("http.retry", c.HTTP.Retry); err != nil {
		return *warnings, err
	}
```

Inside the existing `for i, f := range c.Feeds { ... }` loop (after the header-reserved-key check at lines ~857-862), add the per-feed check:

```go
		if err := validateRetry(fmt.Sprintf("feeds[%d].http.retry", i), f.HTTP.Retry); err != nil {
			return *warnings, err
		}
```

- [ ] **Step 6: Run the config tests to verify they pass**

Run: `cd .worktrees/feed-fetch-backoff && go test ./internal/config/ -v`
Expected: PASS (including the example-drift test, which is unaffected so far).

- [ ] **Step 7: Commit**

```bash
cd .worktrees/feed-fetch-backoff
git add internal/config/config.go internal/config/load.go internal/config/validate.go internal/config/validate_test.go
git status
git commit -m "feat(config): http.retry global + per-feed fetch retry config"
```

---

### Task 4: Pipeline retry loop + factory merge

**Files:**
- Modify: `cmd/rss2msg/pipeline.go` (add `fetchRetry` field; wrap fetch in `retry.Do`)
- Modify: `cmd/rss2msg/wire.go` (compute effective config in `newPipelineFactory`; add `effectiveFetchRetry` helper)
- Test: `cmd/rss2msg/pipeline_test.go` (retry behavior), `cmd/rss2msg/wire_test.go` or a new `cmd/rss2msg/retrycfg_test.go` (merge logic)

**Interfaces:**
- Consumes: `retry.Config` (with `Retryable`, from Task 1), `feed.IsRetryable` (Task 2), `config.HTTPConfig.Retry` / `config.FeedHTTPConfig.Retry` (Task 3).
- Produces:
  - `pipeline.fetchRetry retry.Config` field.
  - `effectiveFetchRetry(global, perFeed config.RetryConfig) retry.Config` in `cmd/rss2msg` — per-field "per-feed if non-zero else global", with `Retryable: feed.IsRetryable` always set.

- [ ] **Step 1: Write the failing merge test**

Create `cmd/rss2msg/retrycfg_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

func TestEffectiveFetchRetryInheritsGlobal(t *testing.T) {
	global := config.RetryConfig{MaxAttempts: 3, BaseDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second}
	got := effectiveFetchRetry(global, config.RetryConfig{}) // per-feed empty
	if got.MaxAttempts != 3 || got.BaseDelay != 500*time.Millisecond || got.MaxDelay != 10*time.Second {
		t.Fatalf("expected global inherited, got %+v", got)
	}
	if got.Retryable == nil {
		t.Fatalf("Retryable predicate must be set")
	}
}

func TestEffectiveFetchRetryPerFeedOverrides(t *testing.T) {
	global := config.RetryConfig{MaxAttempts: 3, BaseDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second}
	perFeed := config.RetryConfig{MaxAttempts: 5} // only attempts overridden
	got := effectiveFetchRetry(global, perFeed)
	if got.MaxAttempts != 5 {
		t.Fatalf("expected per-feed max_attempts=5, got %d", got.MaxAttempts)
	}
	if got.BaseDelay != 500*time.Millisecond || got.MaxDelay != 10*time.Second {
		t.Fatalf("expected base/max inherited, got %+v", got)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd .worktrees/feed-fetch-backoff && go test ./cmd/rss2msg/ -run TestEffectiveFetchRetry -v`
Expected: compile failure — `undefined: effectiveFetchRetry`.

- [ ] **Step 3: Add the merge helper**

In `cmd/rss2msg/wire.go`, add (top-level, near `newPipelineFactory`; `retry` and `feed` are already imported in this package — verify and add imports if needed):

```go
// effectiveFetchRetry merges the global http.retry defaults with a per-feed
// override: each field uses the per-feed value when non-zero, else the global
// value. The transient-only predicate is always applied.
func effectiveFetchRetry(global, perFeed config.RetryConfig) retry.Config {
	eff := global
	if perFeed.MaxAttempts != 0 {
		eff.MaxAttempts = perFeed.MaxAttempts
	}
	if perFeed.BaseDelay != 0 {
		eff.BaseDelay = perFeed.BaseDelay
	}
	if perFeed.MaxDelay != 0 {
		eff.MaxDelay = perFeed.MaxDelay
	}
	return retry.Config{
		MaxAttempts: eff.MaxAttempts,
		BaseDelay:   eff.BaseDelay,
		MaxDelay:    eff.MaxDelay,
		Retryable:   feed.IsRetryable,
	}
}
```

> Add imports to `wire.go` if missing: `"github.com/iambod/rss2msg/internal/retry"` and `"github.com/iambod/rss2msg/internal/feed"` (feed is likely already imported).

- [ ] **Step 4: Run the merge test to verify it passes**

Run: `cd .worktrees/feed-fetch-backoff && go test ./cmd/rss2msg/ -run TestEffectiveFetchRetry -v`
Expected: PASS.

- [ ] **Step 5: Write the failing pipeline retry test**

In `cmd/rss2msg/pipeline_test.go`, add a helper server that fails `n` times then serves the feed, and two tests. (`net/http`, `net/http/httptest`, `sync/atomic` may need importing — check the existing import block.)

```go
// serveRSSFlaky returns 503 for the first failTimes requests, then the body.
func serveRSSFlaky(t *testing.T, body string, failTimes int32) string {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= failTimes {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestRunOnceRetriesTransientThenSucceeds(t *testing.T) {
	url := serveRSSFlaky(t, rssOneItem, 2) // fail twice, succeed on 3rd
	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, url, cd, st, branch("s", snk, nil))
	p.fetchRetry = retry.Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		Retryable:   feed.IsRetryable,
	}

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Equal(t, 1, snk.count())
}

func TestRunOnceDoesNotRetryPermanent(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	p := newTestPipeline(t, srv.URL, cd, st)
	p.fetchRetry = retry.Config{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		Retryable:   feed.IsRetryable,
	}

	_, err := p.RunOnce(context.Background(), srv.URL, time.Now())
	require.Error(t, err)
	require.Equal(t, int32(1), calls.Load(), "404 must not be retried")
}
```

- [ ] **Step 6: Make `newTestPipeline` set a default `fetchRetry`**

So existing pipeline tests keep one-attempt behavior, add `fetchRetry: fastRetry` to the struct literal in `newTestPipeline` (`cmd/rss2msg/pipeline_test.go` ~line 175):

```go
	return &pipeline{
		cfg:        config.FeedConfig{URL: feedURL},
		sinks:      sinks,
		fetcher:    feed.NewFetcher(feed.Options{UserAgent: "rss2msg/test", Timeout: 5 * time.Second}),
		detect:     feed.NewDetector(),
		store:      st,
		log:        zerolog.Nop(),
		tracer:     tracenoop.NewTracerProvider().Tracer("test"),
		instr:      noopInstruments(t),
		coord:      cd,
		fetchRetry: fastRetry,
	}
```

- [ ] **Step 7: Run the new pipeline tests — expect failure**

Run: `cd .worktrees/feed-fetch-backoff && go test ./cmd/rss2msg/ -run 'TestRunOnceRetriesTransientThenSucceeds|TestRunOnceDoesNotRetryPermanent' -v`
Expected: compile failure — `pipeline has no field fetchRetry`.

- [ ] **Step 8: Add the `fetchRetry` field and wrap the fetch**

In `cmd/rss2msg/pipeline.go`, add the field to the struct:

```go
type pipeline struct {
	cfg        config.FeedConfig
	sinks      []sinkBranch
	fetcher    *feed.Fetcher
	detect     *feed.Detector
	store      state.Store
	log        zerolog.Logger
	tracer     trace.Tracer
	instr      telemetry.Instruments
	coord      coord.Coordinator
	fetchRetry retry.Config
}
```

Add the import `"github.com/iambod/rss2msg/internal/retry"` to `pipeline.go`.

Replace the fetch block (current lines ~74-91):

```go
	fetchCtx, fetchSpan := p.tracer.Start(ctx, "feed.fetch")
	fetchStart := time.Now()
	res, err := p.fetcher.Fetch(fetchCtx, feed.FetchRequest{
		URL:          feedURL,
		Headers:      p.cfg.HTTP.Headers,
		Timeout:      p.cfg.HTTP.Timeout,
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
	})
	p.instr.FeedFetchDuration.Record(ctx, float64(time.Since(fetchStart).Milliseconds()),
		metric.WithAttributes(attribute.String("feed_url", feedURL)))
	p.instr.FeedFetches.Add(ctx, 1,
		metric.WithAttributes(attribute.String("feed_url", feedURL), attribute.Int("http.status", res.Status)))
	fetchSpan.End()
	if err != nil {
		log.Error().Err(err).Msg("fetch")
		return nil, err
	}
```

with:

```go
	fetchCtx, fetchSpan := p.tracer.Start(ctx, "feed.fetch")
	fetchStart := time.Now()
	var res feed.FetchResult
	rr := retry.Do(fetchCtx, p.fetchRetry, func(ctx context.Context) error {
		var ferr error
		res, ferr = p.fetcher.Fetch(ctx, feed.FetchRequest{
			URL:          feedURL,
			Headers:      p.cfg.HTTP.Headers,
			Timeout:      p.cfg.HTTP.Timeout,
			ETag:         meta.ETag,
			LastModified: meta.LastModified,
		})
		// Count every HTTP attempt so the status distribution reflects retried 5xx.
		p.instr.FeedFetches.Add(ctx, 1,
			metric.WithAttributes(attribute.String("feed_url", feedURL), attribute.Int("http.status", res.Status)))
		return ferr
	})
	p.instr.FeedFetchDuration.Record(ctx, float64(time.Since(fetchStart).Milliseconds()),
		metric.WithAttributes(attribute.String("feed_url", feedURL)))
	fetchSpan.SetAttributes(attribute.Int("fetch.attempts", rr.Attempts))
	fetchSpan.End()
	if rr.Err != nil {
		log.Error().Err(rr.Err).Int("attempts", rr.Attempts).Msg("fetch")
		return nil, rr.Err
	}
```

- [ ] **Step 9: Wire the effective config into the factory**

In `cmd/rss2msg/wire.go`, `newPipelineFactory`'s returned `&pipeline{...}` (line ~80), add the field:

```go
		return &pipeline{
			cfg:        fc,
			sinks:      branches,
			fetcher:    fetcher,
			detect:     det,
			store:      w.store,
			log:        tel.Logger,
			tracer:     tel.Tracer,
			instr:      instr,
			coord:      w.coord,
			fetchRetry: effectiveFetchRetry(cfg.HTTP.Retry, fc.HTTP.Retry),
		}, nil
```

- [ ] **Step 10: Run the cmd tests to verify they pass**

Run: `cd .worktrees/feed-fetch-backoff && go test ./cmd/rss2msg/ -v`
Expected: PASS (new retry tests + all pre-existing pipeline/wire tests).

- [ ] **Step 11: Build and vet**

Run: `cd .worktrees/feed-fetch-backoff && go build ./... && go vet ./...`
Expected: no output (success).

- [ ] **Step 12: Commit**

```bash
cd .worktrees/feed-fetch-backoff
git add cmd/rss2msg/pipeline.go cmd/rss2msg/wire.go cmd/rss2msg/pipeline_test.go cmd/rss2msg/retrycfg_test.go
git status
git commit -m "feat(pipeline): in-tick fetch retry on transient errors"
```

---

### Task 5: Example config + docs

**Files:**
- Modify: `examples/config.example.yaml`
- Modify: `internal/config/example.yaml` (must end byte-identical to the above)
- Modify: `docs/reference/configuration.md`

**Interfaces:** none (docs/config only).

- [ ] **Step 1: Add the global `http.retry` block to the example**

In `examples/config.example.yaml`, change the `http:` block (lines ~56-58) to:

```yaml
http:
  user_agent: "rss2msg/0.1 (+https://example.com)"
  timeout: 30s
  retry:                 # in-tick retry of a feed fetch on transient errors
    max_attempts: 3      # total tries including the first; 1 disables retry
    base_delay: 500ms    # initial backoff (exponential, full jitter)
    max_delay: 10s       # cap on the backoff delay
```

(The top-level `retry:` block that follows stays as-is — that is the *sink* retry policy.)

- [ ] **Step 2: Add a per-feed override example**

In the `feeds:` section of `examples/config.example.yaml` (the existing entry around lines 293-302 with `http.timeout`/`headers`), add a `retry:` under that feed's `http:`:

```yaml
    http:
      timeout: 10s
      headers:
        Authorization: "Bearer ${OTHER_FEED_TOKEN}"
      retry:             # per-feed override; omitted fields inherit http.retry above
        max_attempts: 5
```

> Open the file and place this so it stays valid YAML under the existing feed entry; match the existing indentation exactly.

- [ ] **Step 3: Mirror into the internal copy**

Run (copies the canonical example into the embedded one so they are byte-identical):

```bash
cd .worktrees/feed-fetch-backoff
cp examples/config.example.yaml internal/config/example.yaml
```

- [ ] **Step 4: Verify the drift test passes**

Run: `cd .worktrees/feed-fetch-backoff && go test ./internal/config/ -run Example -v`
Expected: PASS (the byte-identical guard).

> If the embedded file has a different header/path comment than the canonical one, the copy may break another assertion. If so, instead hand-edit `internal/config/example.yaml` to make the same two additions and re-run; the only requirement is the two files are byte-identical.

- [ ] **Step 5: Document the new options**

In `docs/reference/configuration.md`, update the `## http` section (lines ~245-253). Replace its table with one that includes the retry sub-block and clarify the distinction from the sink `retry`:

```markdown
## `http`

Global HTTP defaults for feed fetching. Each feed can override these under
`feeds[].http`.

| field                  | default       | notes |
| ---------------------- | ------------- | ----- |
| `user_agent`           | `rss2msg/0.1` | Sent as the `User-Agent` header. Override to identify your deployment. |
| `timeout`              | `30s`         | Per-request timeout. |
| `retry.max_attempts`   | `3`           | Total fetch tries within one poll tick (including the first). `1` disables retry. |
| `retry.base_delay`     | `500ms`       | Initial backoff between fetch attempts (exponential, full jitter). |
| `retry.max_delay`      | `10s`         | Cap on the fetch backoff delay. |

`http.retry` retries a **feed fetch** within the same poll tick on transient
errors only — network/connection failures, request timeouts, HTTP `5xx`, and
`429`. Permanent failures (HTTP `4xx` other than `429`, and feed parse errors)
are not retried. Each feed can override any field under `feeds[].http.retry`;
omitted fields inherit the global value (`0` means "inherit"). This is distinct
from the [`retry`](#retry) block below, which governs **sink delivery**.
```

(Leave the existing `## retry` sink section unchanged.)

- [ ] **Step 6: Run the docs link checker**

Run: `cd .worktrees/feed-fetch-backoff && bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`.

- [ ] **Step 7: Commit**

```bash
cd .worktrees/feed-fetch-backoff
git add examples/config.example.yaml internal/config/example.yaml docs/reference/configuration.md
git status
git commit -m "docs: document http.retry feed-fetch backoff (global + per-feed)"
```

---

### Task 6: Full gate + PR

**Files:** none (verification + PR).

- [ ] **Step 1: Full test + vet**

Run: `cd .worktrees/feed-fetch-backoff && task test && task vet`
Expected: all pass. (No sink/state/coordinator backend changed, so `task test-integration` is not required — note this explicitly in the PR.)

- [ ] **Step 2: Lint (if available)**

Run: `cd .worktrees/feed-fetch-backoff && task lint`
Expected: clean. If `golangci-lint` v2 is not installed locally, note it in the PR so CI is the gate.

- [ ] **Step 3: Push and open the PR**

```bash
cd .worktrees/feed-fetch-backoff
git push -u origin feat/feed-fetch-backoff
gh pr create --title "feat: per-feed backoff (in-tick retry) on fetch failures" \
  --body "Implements the spec in docs/superpowers/specs/2026-06-21-per-feed-fetch-backoff-design.md. In-tick exponential-backoff retry of feed fetches on transient errors (5xx/429/timeout/network); permanent errors (4xx/parse) are not retried. Configurable via http.retry globally and per-feed (feeds[].http.retry). Skipped task test-integration: no sink/state/coordinator backend changed."
```

---

## Self-Review

**Spec coverage:**
- Behavior (in-tick retry, transient-only, 304 = success) → Tasks 2 (classifier) + 4 (loop). ✔
- Config `http.retry` global + per-feed, merge rule, defaults, validation → Task 3 + Task 4 (`effectiveFetchRetry`). ✔
- `retry.Retryable` predicate, backward-compatible for sinks → Task 1 (incl. nil-predicate regression test). ✔
- Typed `FetchError` + `IsRetryable` → Task 2. ✔
- Pipeline telemetry (per-attempt `FeedFetches`, whole-loop `FeedFetchDuration`, `fetch.attempts` span attr) → Task 4 Step 8. ✔
- Example yaml (both files byte-identical) + docs + link check → Task 5. ✔
- Out of scope (no rescheduling, no persisted counts, sinks unchanged) → honored; Task 1 keeps sink behavior. ✔

**Placeholder scan:** No TBD/TODO; every code step shows code. The only conditional guidance (`minimalValidConfig` helper reuse, import checks, embedded-yaml header) is explicit fallback instruction, not a placeholder.

**Type consistency:** `effectiveFetchRetry(global, perFeed config.RetryConfig) retry.Config`, `pipeline.fetchRetry retry.Config`, `feed.FetchError{Op, Status, Err}`, `feed.IsRetryable(error) bool`, `retry.Config.Retryable func(error) bool` — names and signatures consistent across Tasks 1–4.
