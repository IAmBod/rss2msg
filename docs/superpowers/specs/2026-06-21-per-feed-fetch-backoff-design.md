# Per-feed backoff on fetch failures — design

- **Status:** approved (design)
- **Date:** 2026-06-21
- **Branch / worktree:** `feat/feed-fetch-backoff` in `.worktrees/feed-fetch-backoff`

## Problem

Each feed runs its own goroutine with a fixed-interval ticker
(`internal/scheduler/serve.go:92` `runFeedLoop`). When a feed fetch fails the
pipeline logs an ERROR and returns immediately
(`cmd/rss2msg/pipeline.go:88-91`); the feed is not retried until the next
scheduled tick (e.g. 5m later). A transient blip (a brief 5xx, a dropped
connection, a slow response) therefore costs a full poll interval of missed
items even though a retry seconds later would have succeeded.

There is already an exponential-backoff-with-full-jitter helper
(`internal/retry/retry.go`), but it is wired only into sink delivery
(`sink.RetryingPublisher`), not into feed fetching.

## Behavior

When a feed fetch fails with a **transient** error, retry the fetch **within
the same poll tick** using exponential backoff with full jitter, up to
`max_attempts`. The feed's poll interval is unchanged — this is in-tick retry,
not adaptive rescheduling. On success, or on a non-retryable error, stop
immediately. `304 Not Modified` is a success and is never retried.

Classification:

- **Transient (retry):** connection errors, DNS failures, request timeouts
  (`context.DeadlineExceeded` from the per-attempt HTTP timeout), HTTP `5xx`,
  HTTP `429`.
- **Permanent (no retry):** HTTP `4xx` other than `429`, feed parse errors,
  malformed-request / bad-URL errors, and `context.Canceled` (process
  shutdown).

## Configuration (config-first, per-feed with global default)

Retry lives under the existing HTTP config blocks, next to `timeout` /
`headers`. The existing `RetryConfig` shape (`max_attempts`, `base_delay`,
`max_delay`) is reused.

```yaml
http:                    # global defaults applied to every feed
  timeout: 30s
  retry:
    max_attempts: 3      # 1 = disabled (current behavior)
    base_delay: 500ms
    max_delay: 10s

feeds:
  - url: https://flaky.example/rss.xml
    http:
      retry:             # per-feed override; omitted fields inherit global
        max_attempts: 5
  - url: https://stable.example/atom.xml
    http:
      retry:
        max_attempts: 1  # opt this feed out of retry
```

- Add `Retry RetryConfig` to **both** `HTTPConfig` (global) and
  `FeedHTTPConfig` (per-feed) in `internal/config/config.go`.
- **Merge rule (computed in the pipeline factory):** for each field, the
  effective value is the per-feed value if non-zero, otherwise the global
  value. A feed that omits `http.retry` inherits the global default
  (3 attempts). `max_attempts: 1` disables retry for that feed. This mirrors
  how per-feed `http.timeout` already overrides only when `> 0`.
- Defaults registered in `applyDefaults` (`internal/config/load.go`) and
  `Defaults()` (`internal/config/config.go`):
  `http.retry.max_attempts=3`, `http.retry.base_delay=500ms`,
  `http.retry.max_delay=10s`.
- Validation in `internal/config/validate.go` for both the global `http.retry`
  and each `feeds[].http.retry`: `max_attempts`, `base_delay`, and `max_delay`
  must not be negative, and `max_delay >= base_delay` when both are non-zero.
  A field of `0` means "inherit / default" (the global default fills the global
  block via Viper; a per-feed `0` inherits the global value), so `0` is valid —
  only negatives and `max_delay < base_delay` are rejected.

## Components

### 1. `internal/retry` — optional retryable predicate

Add an optional field to `retry.Config`:

```go
// Retryable reports whether an error from fn is worth retrying. Nil means
// "retry any error" (the existing behavior used by sink delivery).
Retryable func(error) bool
```

In `Do`, after `fn` returns a non-nil error, break out of the loop early when
`cfg.Retryable != nil && !cfg.Retryable(err)` (treating the error as terminal,
returned in `Result.Err` with the attempts made so far). Backward compatible:
sinks construct `retry.Config` without this field, so it stays `nil` and
behavior is unchanged.

### 2. `internal/feed` — typed fetch errors + classifier

Wrap the fetcher's failure returns (`internal/feed/fetcher.go:57,88,109,114`)
in a typed error so callers can classify without string matching:

```go
type FetchError struct {
    Op     string // "request" | "transport" | "parse" | "status"
    Status int    // HTTP status when Op == "status"
    Err    error
}
func (e *FetchError) Error() string { ... }
func (e *FetchError) Unwrap() error { return e.Err }
```

- `build request: %w`        -> `Op: "request"`
- `http get: %w`             -> `Op: "transport"`
- `parse feed: %w`           -> `Op: "parse"`
- `unexpected status %d`     -> `Op: "status", Status: code`

Add the classifier used as the retry predicate:

```go
func IsRetryable(err error) bool
```

- `transport` -> true, except `errors.Is(err, context.Canceled)` -> false
- `status`    -> `Status >= 500 || Status == 429`
- `parse`, `request` -> false
- unknown / untyped error -> false (conservative)

### 3. `cmd/rss2msg/pipeline.go` — wrap the fetch in `retry.Do`

Replace the single `p.fetcher.Fetch(...)` call (lines 76–91) with a `retry.Do`
loop using the per-feed effective config and `Retryable: feed.IsRetryable`. The
`pipeline` struct gains a `fetchRetry retry.Config` field, set by the factory.

Telemetry inside the loop:

- `FeedFetches` counter increments **per attempt** (keyed by `http.status`), so
  the status distribution reflects retried 5xx responses.
- `FeedFetchDuration` records the **whole** retry loop (the poll's fetch budget).
- The `feed.fetch` span gets a `fetch.attempts` attribute.

On exhaustion, log ERROR with the attempt count (as today) and return the
error, so the existing `OnPollComplete` reporting path is unchanged.

### 4. `cmd/rss2msg/wire.go` — compute the effective config in the factory

`newPipelineFactory` (`cmd/rss2msg/wire.go:69`) already receives both the global
`cfg config.Config` and the per-feed `fc config.FeedConfig`. It merges
`cfg.HTTP.Retry` with `fc.HTTP.Retry` per the merge rule and sets
`pipeline.fetchRetry`.

## Testing (TDD)

- `internal/retry`: predicate stops early on a non-retryable error; a nil
  predicate still retries all errors (regression guard for sink delivery).
- `internal/feed`: table test for `IsRetryable` across
  5xx / 429 / 4xx / parse / transport / canceled; fetcher returns a correctly
  typed `*FetchError` for each failure mode (driven by an `httptest` server
  returning 500, 404, and a malformed body).
- `cmd/rss2msg/pipeline_test.go`: a fake fetcher that fails N times then
  succeeds completes within `max_attempts`; a 404 triggers exactly one attempt;
  `FeedFetches` is counted per attempt; a cancelled context aborts mid-retry.
- Merge logic: unit test that per-feed values override and omitted fields
  inherit the global default.
- Config: validation rejects `max_attempts: 0` and `max_delay < base_delay`.

## Docs & examples

- Update **both** `examples/config.example.yaml` and
  `internal/config/example.yaml` — they must stay byte-identical (a test
  enforces this).
- Document the new `http.retry` block (global and per-feed) in the relevant
  `docs/` page; run `bash scripts/check-doc-links.sh`.

## Out of scope

- No adaptive rescheduling or circuit-breaking across ticks (a failing feed
  keeps its configured interval between ticks).
- No persisted error counts; the state store is untouched.
- No change to sink-delivery retry behavior.
