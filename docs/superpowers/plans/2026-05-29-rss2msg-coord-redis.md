# rss2msg — Redis Coordinator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Redis-backed implementation of the existing `coord.Coordinator` interface (alongside the v1.5 `noop` and `postgres` backends), so operators running multiple rss2msg instances can use a shared Redis instead of Postgres for poll-coordination.

**Architecture:** Classic Redis lease — `SET key token NX EX ttl` to acquire, a background renewal goroutine that runs a CAS-checked `PEXPIRE` Lua script every `lock_ttl/3`, and a CAS-checked `DEL` Lua script on release. Implementation lives in `internal/coord/redis` and is selected via `coordination.driver=redis` in config. The pipeline and `openCoordinator` wiring in `cmd/rss2msg` are coord-driver-agnostic, so this is purely additive.

**Tech Stack:** Go 1.22, `github.com/redis/go-redis/v9`, `github.com/google/uuid`, `github.com/testcontainers/testcontainers-go/modules/redis`.

**Module path:** `github.com/iambod/rss2msg`.

**Source spec:** `docs/superpowers/specs/2026-05-28-rss2msg-coord-redis-design.md`.

---

## File Structure

```
internal/coord/redis/
  redis.go                       // Coordinator, Options, New, scripts, renewal loop (Task 2 + 3)
  redis_unit_test.go             // unit tests, no Redis dep (Task 2)
  redis_test.go                  // //go:build integration, testcontainers redis (Task 3)

internal/config/
  config.go                      // CoordinationConfig + CoordinationRedisConfig (Task 1)
  validate.go                    // knownCoordinationDrivers gains "redis"; redis validation (Task 1)
  validate_test.go               // new test cases for redis driver (Task 1)

cmd/rss2msg/
  wire.go                        // openCoordinator gains case "redis" (Task 4)

config.example.yaml              // commented-out redis block (Task 4)
README.md                        // bullet mentioning the redis driver (Task 4)
```

---

## Task 1: Config types + defaults + validation for the `redis` driver

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/validate.go`
- Modify: `internal/config/validate_test.go`

- [ ] **Step 1.1: Add `CoordinationRedisConfig` and embed it in `CoordinationConfig`**

In `internal/config/config.go`, change the existing `CoordinationConfig` block (around lines 55–62) to:

```go
type CoordinationConfig struct {
	Driver   string                  `mapstructure:"driver"`
	Postgres CoordinationPGConfig    `mapstructure:"postgres"`
	Redis    CoordinationRedisConfig `mapstructure:"redis"`
}

type CoordinationPGConfig struct {
	DSN string `mapstructure:"dsn"`
}

type CoordinationRedisConfig struct {
	URL             string        `mapstructure:"url"`
	LockTTL         time.Duration `mapstructure:"lock_ttl"`
	RenewalInterval time.Duration `mapstructure:"renewal_interval"`
}
```

No changes to `Defaults()` — the empty `CoordinationRedisConfig{}` is correct, and the redis backend itself applies the `30s` / `LockTTL/3` defaults inside `New`.

- [ ] **Step 1.2: Write failing validation tests**

In `internal/config/validate_test.go`, append (do not replace existing tests):

```go
func TestValidateAcceptsCoordinationRedis(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "redis://localhost:6379/0"
	if err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsRedisWithoutURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = ""
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "coordination.redis.url") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisWithUnparseableURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "not a url"
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "coordination.redis.url") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisLockTTLBelowOneSecond(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "redis://localhost:6379/0"
	c.Coordination.Redis.LockTTL = 500 * time.Millisecond
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "lock_ttl") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisRenewalAtOrAboveTTL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "redis://localhost:6379/0"
	c.Coordination.Redis.LockTTL = 5 * time.Second
	c.Coordination.Redis.RenewalInterval = 5 * time.Second
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "renewal_interval") {
		t.Fatalf("got %v", err)
	}
}
```

Run:

```bash
go test ./internal/config/...
```

Expected: FAIL — `TestValidateRejectsRedisWithoutURL` (and the others depending on the existing code path) report `coordination.driver "redis" is not supported`, because `knownCoordinationDrivers` does not yet include `"redis"`.

- [ ] **Step 1.3: Add `"redis"` to the driver allowlist and enforce redis-specific rules**

In `internal/config/validate.go`:

1. Add `"redis"` to `knownCoordinationDrivers`:

   ```go
   var knownCoordinationDrivers = map[string]struct{}{
   	"":         {},
   	"noop":     {},
   	"postgres": {},
   	"redis":    {},
   }
   ```

2. Add a redis branch immediately after the existing `if c.Coordination.Driver == "postgres" { ... }` block, and add an `net/url` import:

   ```go
   if c.Coordination.Driver == "redis" {
   	url := strings.TrimSpace(c.Coordination.Redis.URL)
   	if url == "" {
   		return fmt.Errorf("coordination.redis.url is required when coordination.driver=redis")
   	}
   	if _, err := redisparseURL(url); err != nil {
   		return fmt.Errorf("coordination.redis.url %q is not parseable: %w", url, err)
   	}
   	if ttl := c.Coordination.Redis.LockTTL; ttl != 0 && ttl < time.Second {
   		return fmt.Errorf("coordination.redis.lock_ttl %v is below the 1s minimum", ttl)
   	}
   	if ri := c.Coordination.Redis.RenewalInterval; ri != 0 {
   		ttl := c.Coordination.Redis.LockTTL
   		if ttl == 0 {
   			ttl = 30 * time.Second
   		}
   		if ri >= ttl {
   			return fmt.Errorf("coordination.redis.renewal_interval %v must be less than lock_ttl %v", ri, ttl)
   		}
   	}
   }
   ```

3. To keep `internal/config` free of a hard dependency on the `redis` client, parse the URL ourselves with `net/url`. Add this helper at the bottom of `validate.go`:

   ```go
   // redisparseURL is a lightweight syntactic check that mirrors the subset of
   // redis.ParseURL we care about at config-validate time: scheme must be
   // redis or rediss, host must be non-empty, optional /<db> path must be a
   // non-negative integer. The actual TLS / auth handling is done by
   // redis.ParseURL inside the coord/redis package at startup.
   func redisparseURL(raw string) (*url.URL, error) {
   	u, err := url.Parse(raw)
   	if err != nil {
   		return nil, err
   	}
   	if u.Scheme != "redis" && u.Scheme != "rediss" {
   		return nil, fmt.Errorf("scheme must be redis or rediss, got %q", u.Scheme)
   	}
   	if u.Host == "" {
   		return nil, fmt.Errorf("host is required")
   	}
   	if p := strings.TrimPrefix(u.Path, "/"); p != "" {
   		if _, err := strconv.Atoi(p); err != nil {
   			return nil, fmt.Errorf("db index %q must be an integer", p)
   		}
   	}
   	return u, nil
   }
   ```

4. Add the new imports at the top of `validate.go`:

   ```go
   import (
   	"fmt"
   	"net/textproto"
   	"net/url"
   	"strconv"
   	"strings"
   	"time"
   )
   ```

- [ ] **Step 1.4: Run config tests to verify they pass**

Run:

```bash
go test ./internal/config/...
```

Expected: PASS — all existing tests plus the five new redis-driver tests.

- [ ] **Step 1.5: Commit**

```bash
git add internal/config
git commit -m "feat(config): coordination redis driver types + validation"
```

---

## Task 2: `internal/coord/redis` — package skeleton, key derivation, unit tests

**Files:**
- Create: `internal/coord/redis/redis.go`
- Create: `internal/coord/redis/redis_unit_test.go`

- [ ] **Step 2.1: Add the go-redis and uuid dependencies**

Run:

```bash
go get github.com/redis/go-redis/v9@latest github.com/google/uuid@latest
```

Expected: `go.mod` / `go.sum` updated with both modules. No code changes yet.

- [ ] **Step 2.2: Write the failing unit test**

Create `internal/coord/redis/redis_unit_test.go`:

```go
package redis

import (
	"strings"
	"testing"
)

func TestLockKeyIsDeterministicAndHumanReadable(t *testing.T) {
	t.Parallel()
	a := lockKey("https://example.com/feed.xml")
	b := lockKey("https://example.com/feed.xml")
	if a != b {
		t.Fatalf("lockKey not deterministic: %s vs %s", a, b)
	}
	if !strings.HasPrefix(a, "rss2msg:coord:") {
		t.Fatalf("expected rss2msg:coord: prefix, got %q", a)
	}
	hex := strings.TrimPrefix(a, "rss2msg:coord:")
	if len(hex) != 64 || strings.ToLower(hex) != hex {
		t.Fatalf("expected 64-char lowercase hex suffix, got %q", hex)
	}
}

func TestLockKeyDiffersForDifferentFeeds(t *testing.T) {
	t.Parallel()
	if lockKey("https://e/1") == lockKey("https://e/2") {
		t.Fatalf("expected distinct keys for distinct feed URLs")
	}
}

func TestNewTokenIsUnique(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		tok := newToken()
		if tok == "" {
			t.Fatalf("empty token")
		}
		if _, dup := seen[tok]; dup {
			t.Fatalf("token collision: %q", tok)
		}
		seen[tok] = struct{}{}
	}
}

func TestResolveDefaults(t *testing.T) {
	t.Parallel()
	o := Options{URL: "redis://localhost:6379/0"}
	r := o.resolved()
	if r.LockTTL.String() != "30s" {
		t.Fatalf("expected 30s default LockTTL, got %v", r.LockTTL)
	}
	if r.RenewalInterval.String() != "10s" {
		t.Fatalf("expected 10s default RenewalInterval (LockTTL/3), got %v", r.RenewalInterval)
	}
}

func TestResolveExplicitRenewalIntervalRespected(t *testing.T) {
	t.Parallel()
	o := Options{URL: "redis://x", LockTTL: 60 * 1_000_000_000, RenewalInterval: 5 * 1_000_000_000}
	r := o.resolved()
	if r.RenewalInterval.String() != "5s" {
		t.Fatalf("expected explicit 5s, got %v", r.RenewalInterval)
	}
}
```

Run:

```bash
go test ./internal/coord/redis/...
```

Expected: FAIL — `undefined: lockKey`, `undefined: newToken`, `undefined: Options`.

- [ ] **Step 2.3: Implement the package skeleton**

Create `internal/coord/redis/redis.go`:

```go
// Package redis provides a Coordinator backed by a Redis lease:
//   SET key token NX EX <lock_ttl>
// with a background renewal goroutine that CAS-extends the lease every
// LockTTL/3, and a CAS-checked DEL on release. The key derivation mirrors
// the Postgres backend's hash domain (sha256(feed_url)) but is rendered as
// hex for human-readable debugging via `KEYS rss2msg:coord:*`.
package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

// Options configures the Redis-backed Coordinator. Zero LockTTL means "30s";
// zero RenewalInterval means "LockTTL / 3".
type Options struct {
	URL             string        // required
	LockTTL         time.Duration // 0 -> 30s
	RenewalInterval time.Duration // 0 -> LockTTL / 3
}

type resolvedOptions struct {
	URL             string
	LockTTL         time.Duration
	RenewalInterval time.Duration
}

func (o Options) resolved() resolvedOptions {
	r := resolvedOptions{
		URL:             o.URL,
		LockTTL:         o.LockTTL,
		RenewalInterval: o.RenewalInterval,
	}
	if r.LockTTL <= 0 {
		r.LockTTL = 30 * time.Second
	}
	if r.RenewalInterval <= 0 {
		r.RenewalInterval = r.LockTTL / 3
	}
	return r
}

// lockKey is the Redis key for feedURL.
func lockKey(feedURL string) string {
	sum := sha256.Sum256([]byte(feedURL))
	return "rss2msg:coord:" + hex.EncodeToString(sum[:])
}

// newToken returns a fresh per-acquisition owner token.
func newToken() string {
	return uuid.NewString()
}
```

- [ ] **Step 2.4: Run unit tests to verify they pass**

Run:

```bash
go test ./internal/coord/redis/...
```

Expected: PASS — all five unit tests.

- [ ] **Step 2.5: Commit**

```bash
go mod tidy
git add go.mod go.sum internal/coord/redis
git commit -m "feat(coord/redis): package skeleton with key derivation, options, and token helpers"
```

---

## Task 3: `internal/coord/redis` — full `Coordinator` with renewal + integration tests

**Files:**
- Modify: `internal/coord/redis/redis.go`
- Create: `internal/coord/redis/redis_test.go`

This task implements `New`, `TryAcquire`, `ReleaseFunc`, the renewal goroutine, and `Close`, then exercises the full surface against a `redis:7-alpine` testcontainer.

- [ ] **Step 3.1: Add the testcontainers redis module dependency**

Run:

```bash
go get github.com/testcontainers/testcontainers-go/modules/redis@latest
```

Expected: `go.mod` / `go.sum` updated.

- [ ] **Step 3.2: Implement the full `Coordinator`**

Replace the contents of `internal/coord/redis/redis.go` with the following (this is a full file replace — `lockKey`, `newToken`, `Options`, `resolved` are preserved verbatim):

```go
// Package redis provides a Coordinator backed by a Redis lease:
//   SET key token NX EX <lock_ttl>
// with a background renewal goroutine that CAS-extends the lease every
// LockTTL/3, and a CAS-checked DEL on release. The key derivation mirrors
// the Postgres backend's hash domain (sha256(feed_url)) but is rendered as
// hex for human-readable debugging via `KEYS rss2msg:coord:*`.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/coord"
)

// Options configures the Redis-backed Coordinator. Zero LockTTL means "30s";
// zero RenewalInterval means "LockTTL / 3".
type Options struct {
	URL             string        // required
	LockTTL         time.Duration // 0 -> 30s
	RenewalInterval time.Duration // 0 -> LockTTL / 3
}

type resolvedOptions struct {
	URL             string
	LockTTL         time.Duration
	RenewalInterval time.Duration
}

func (o Options) resolved() resolvedOptions {
	r := resolvedOptions{
		URL:             o.URL,
		LockTTL:         o.LockTTL,
		RenewalInterval: o.RenewalInterval,
	}
	if r.LockTTL <= 0 {
		r.LockTTL = 30 * time.Second
	}
	if r.RenewalInterval <= 0 {
		r.RenewalInterval = r.LockTTL / 3
	}
	return r
}

// renewScript: PEXPIRE key only if its value still matches our token.
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end
`)

// releaseScript: DEL key only if its value still matches our token.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

type lease struct {
	key    string
	token  string
	cancel context.CancelFunc
	done   chan struct{}
}

type Coordinator struct {
	client *redis.Client
	opts   resolvedOptions

	mu      sync.Mutex
	held    map[*lease]struct{} // nil after Close
	closing bool
}

// New parses opts.URL, dials Redis, and returns a ready Coordinator.
func New(ctx context.Context, opts Options) (*Coordinator, error) {
	ro := opts.resolved()
	if ro.URL == "" {
		return nil, fmt.Errorf("coord/redis: url is required")
	}
	cfg, err := redis.ParseURL(ro.URL)
	if err != nil {
		return nil, fmt.Errorf("coord/redis: parse url: %w", err)
	}
	client := redis.NewClient(cfg)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("coord/redis: ping: %w", err)
	}
	return &Coordinator{
		client: client,
		opts:   ro,
		held:   make(map[*lease]struct{}),
	}, nil
}

// Close cancels every renewal goroutine, best-effort CAS-deletes every
// still-held lease, and closes the underlying client.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	if c.held == nil {
		c.mu.Unlock()
		return nil
	}
	c.closing = true
	leases := make([]*lease, 0, len(c.held))
	for l := range c.held {
		leases = append(leases, l)
	}
	c.held = nil
	c.mu.Unlock()

	for _, l := range leases {
		l.cancel()
		<-l.done
		c.casDelete(l)
	}
	return c.client.Close()
}

func (c *Coordinator) TryAcquire(ctx context.Context, feedURL string) (coord.ReleaseFunc, bool, error) {
	key := lockKey(feedURL)
	token := newToken()

	ok, err := c.client.SetNX(ctx, key, token, c.opts.LockTTL).Result()
	if err != nil {
		return nil, false, fmt.Errorf("coord/redis: SET NX EX: %w", err)
	}
	if !ok {
		return nil, false, nil
	}

	renewalCtx, cancel := context.WithCancel(context.Background())
	l := &lease{
		key:    key,
		token:  token,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	c.mu.Lock()
	if c.held == nil || c.closing {
		c.mu.Unlock()
		cancel()
		close(l.done)
		c.casDelete(l) // best-effort
		return nil, false, nil
	}
	c.held[l] = struct{}{}
	c.mu.Unlock()

	go c.renewLoop(renewalCtx, l, feedURL)

	release := func(_ context.Context) error {
		c.mu.Lock()
		if c.held == nil {
			c.mu.Unlock()
			return nil
		}
		if _, ok := c.held[l]; !ok {
			c.mu.Unlock()
			return nil
		}
		delete(c.held, l)
		c.mu.Unlock()

		l.cancel()
		<-l.done
		c.casDelete(l)
		return nil
	}
	return release, true, nil
}

// renewLoop CAS-extends the lease every opts.RenewalInterval until ctx is
// canceled or Redis tells us we no longer own the key. Lock-loss events are
// logged at warn; the goroutine exits and the eventual release becomes a
// no-op (casDelete will also see CAS=0).
func (c *Coordinator) renewLoop(ctx context.Context, l *lease, feedURL string) {
	defer close(l.done)
	t := time.NewTicker(c.opts.RenewalInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			renewCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			res, err := renewScript.Run(renewCtx, c.client,
				[]string{l.key}, l.token, c.opts.LockTTL.Milliseconds()).Result()
			cancel()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Warn().
					Str("coord_driver", "redis").
					Str("feed_url", feedURL).
					Str("event", "renew_error").
					Err(err).
					Msg("coord/redis: renew failed; exiting renewal loop")
				return
			}
			n, _ := res.(int64)
			if n == 0 {
				log.Warn().
					Str("coord_driver", "redis").
					Str("feed_url", feedURL).
					Str("event", "lock_lost").
					Msg("coord/redis: lease lost (CAS mismatch); exiting renewal loop")
				return
			}
		}
	}
}

// casDelete runs the release Lua script on a fresh 5s background ctx. A
// return of 0 (TTL already expired, or another instance now holds the key,
// or the renewal goroutine already noted the loss) is not an error.
func (c *Coordinator) casDelete(l *lease) {
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := releaseScript.Run(delCtx, c.client, []string{l.key}, l.token).Err(); err != nil {
		log.Warn().
			Str("coord_driver", "redis").
			Str("event", "release_error").
			Err(err).
			Msg("coord/redis: CAS delete failed")
	}
}

// lockKey is the Redis key for feedURL.
func lockKey(feedURL string) string {
	sum := sha256.Sum256([]byte(feedURL))
	return "rss2msg:coord:" + hex.EncodeToString(sum[:])
}

// newToken returns a fresh per-acquisition owner token.
func newToken() string {
	return uuid.NewString()
}
```

- [ ] **Step 3.3: Run unit tests to verify the skeleton still passes**

Run:

```bash
go test ./internal/coord/redis/...
```

Expected: PASS — the five tests from Task 2 still pass against the expanded file.

- [ ] **Step 3.4: Write the failing integration tests**

Create `internal/coord/redis/redis_test.go`:

```go
//go:build integration

package redis_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	coordredis "github.com/iambod/rss2msg/internal/coord/redis"
)

func lockKeyFor(feedURL string) string {
	sum := sha256.Sum256([]byte(feedURL))
	return "rss2msg:coord:" + hex.EncodeToString(sum[:])
}

func newRedisURL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	rC, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rC.Terminate(ctx) })
	url, err := rC.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return url
}

func newCoord(t *testing.T, url string, ttl, renew time.Duration) *coordredis.Coordinator {
	t.Helper()
	c, err := coordredis.New(context.Background(), coordredis.Options{
		URL:             url,
		LockTTL:         ttl,
		RenewalInterval: renew,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestTwoCoordinatorsRaceForSameFeed(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 30*time.Second, 10*time.Second)
	b := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	relA, okA, err := a.TryAcquire(ctx, "https://e/feed-x")
	if err != nil || !okA {
		t.Fatalf("A acquire: ok=%v err=%v", okA, err)
	}
	defer relA(ctx)

	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-x")
	if err != nil {
		t.Fatalf("B acquire err: %v", err)
	}
	if okB {
		_ = relB(ctx)
		t.Fatalf("B should not have acquired while A holds")
	}
}

func TestReleaseAllowsAcquireAgain(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	rel, ok, err := a.TryAcquire(ctx, "https://e/feed-y")
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	if err := rel(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	rel2, ok, err := a.TryAcquire(ctx, "https://e/feed-y")
	if err != nil || !ok {
		t.Fatalf("re-acquire: ok=%v err=%v", ok, err)
	}
	_ = rel2(ctx)
}

func TestHeldLeaseSurvivesPastInitialTTL(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 1*time.Second, 300*time.Millisecond)
	b := newCoord(t, url, 1*time.Second, 300*time.Millisecond)
	ctx := context.Background()

	relA, ok, err := a.TryAcquire(ctx, "https://e/feed-z")
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}

	// Sleep past the initial TTL; renewal should keep the lease alive.
	time.Sleep(2 * time.Second)

	rB, okB, err := b.TryAcquire(ctx, "https://e/feed-z")
	if err != nil {
		t.Fatalf("B acquire err: %v", err)
	}
	if okB {
		_ = rB(ctx)
		t.Fatalf("B should still be locked out after renewal")
	}

	if err := relA(ctx); err != nil {
		t.Fatalf("A release: %v", err)
	}

	// Give a moment, then B should succeed.
	rB2, okB2, err := b.TryAcquire(ctx, "https://e/feed-z")
	if err != nil || !okB2 {
		t.Fatalf("B acquire after A release: ok=%v err=%v", okB2, err)
	}
	_ = rB2(ctx)
}

func TestLockLostExternallyMakesReleaseNoOp(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 1*time.Second, 300*time.Millisecond)
	b := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	relA, ok, err := a.TryAcquire(ctx, "https://e/feed-lost")
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}

	// Simulate external eviction: bypass the coordinator and DEL the key.
	cfg, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	side := goredis.NewClient(cfg)
	defer side.Close()
	if n, err := side.Del(ctx, lockKeyFor("https://e/feed-lost")).Result(); err != nil || n != 1 {
		t.Fatalf("DEL setup: n=%d err=%v", n, err)
	}

	// B now acquires (CAS-free SET NX succeeds because key is gone).
	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-lost")
	if err != nil || !okB {
		t.Fatalf("B acquire after eviction: ok=%v err=%v", okB, err)
	}

	// Sleep two renewal intervals so A's renewal goroutine has noticed the
	// CAS-zero and exited.
	time.Sleep(700 * time.Millisecond)

	// A's release must be a no-op and must not delete B's lease.
	if err := relA(ctx); err != nil {
		t.Fatalf("A release should be nil, got %v", err)
	}
	if err := relB(ctx); err != nil {
		t.Fatalf("B release: %v", err)
	}
}

func TestCoordinatorCloseReleasesHeldLeases(t *testing.T) {
	url := newRedisURL(t)
	a, err := coordredis.New(context.Background(), coordredis.Options{
		URL:             url,
		LockTTL:         30 * time.Second,
		RenewalInterval: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	b := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	_, ok, err := a.TryAcquire(ctx, "https://e/feed-close")
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("A close: %v", err)
	}

	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-close")
	if err != nil || !okB {
		t.Fatalf("B acquire after A.Close: ok=%v err=%v", okB, err)
	}
	_ = relB(ctx)
}

func TestCanceledReleaseCtxStillFreesLease(t *testing.T) {
	url := newRedisURL(t)
	a := newCoord(t, url, 30*time.Second, 10*time.Second)
	b := newCoord(t, url, 30*time.Second, 10*time.Second)
	ctx := context.Background()

	rel, ok, err := a.TryAcquire(ctx, "https://e/feed-cancel")
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}

	doomed, cancel := context.WithCancel(ctx)
	cancel()
	if err := rel(doomed); err != nil {
		t.Fatalf("release on canceled ctx: %v", err)
	}

	relB, okB, err := b.TryAcquire(ctx, "https://e/feed-cancel")
	if err != nil || !okB {
		t.Fatalf("B acquire after canceled release: ok=%v err=%v", okB, err)
	}
	_ = relB(ctx)
}

```

- [ ] **Step 3.5: Run integration tests to verify they pass**

Run:

```bash
go mod tidy
go test -race -tags=integration ./internal/coord/redis/...
```

Expected: PASS — all six integration tests. Requires a working Docker daemon for testcontainers; the runtime is ~15–25 seconds per test due to container boot. (The `make test-integration` target already passes the build tag, so `make test-integration` works too.)

- [ ] **Step 3.6: Commit**

```bash
git add internal/coord/redis go.mod go.sum
git commit -m "feat(coord/redis): SET NX EX-backed Coordinator with renewal and CAS release"
```

---

## Task 4: Wire `openCoordinator`, update example config, document

**Files:**
- Modify: `cmd/rss2msg/wire.go`
- Modify: `config.example.yaml`
- Modify: `README.md`

- [ ] **Step 4.1: Add the `redis` case to `openCoordinator`**

In `cmd/rss2msg/wire.go`:

1. Add the import alongside the existing `coordpg` import:

   ```go
   coordredis "github.com/iambod/rss2msg/internal/coord/redis"
   ```

2. Add a `case "redis"` branch inside the `switch driver` block in `openCoordinator` (immediately after the `case "postgres"` branch):

   ```go
   case "redis":
   	return coordredis.New(ctx, coordredis.Options{
   		URL:             cc.Redis.URL,
   		LockTTL:         cc.Redis.LockTTL,
   		RenewalInterval: cc.Redis.RenewalInterval,
   	})
   ```

   The full `switch` now looks like:

   ```go
   switch driver {
   case "noop":
   	return coordnoop.New(), nil
   case "postgres":
   	dsn := cc.Postgres.DSN
   	if dsn == "" {
   		dsn = sc.Postgres.DSN
   	}
   	if dsn == "" {
   		return nil, fmt.Errorf("coordination postgres: no dsn (and no state.postgres.dsn fallback)")
   	}
   	return coordpg.New(ctx, dsn, feedCount)
   case "redis":
   	return coordredis.New(ctx, coordredis.Options{
   		URL:             cc.Redis.URL,
   		LockTTL:         cc.Redis.LockTTL,
   		RenewalInterval: cc.Redis.RenewalInterval,
   	})
   default:
   	return nil, fmt.Errorf("unsupported coordination driver %q", driver)
   }
   ```

- [ ] **Step 4.2: Build and run the unit/non-integration test suite**

Run:

```bash
go build ./...
go test -race ./...
```

Expected: build and tests pass. (The default test run skips `//go:build integration` files, so no Redis is needed here.)

- [ ] **Step 4.3: Update `config.example.yaml`**

Replace the existing `coordination:` block at the top of `config.example.yaml` (currently around lines 27–30) with:

```yaml
coordination:
  driver: postgres   # noop | postgres | redis ; default noop
  postgres:
    dsn: ${POSTGRES_DSN}  # optional; falls back to state.postgres.dsn
  # redis:
  #   url: ${REDIS_URL}      # e.g. redis://localhost:6379/0 or rediss://...
  #   lock_ttl: 30s          # optional, default 30s
  #   renewal_interval: 10s  # optional, default = lock_ttl / 3
```

- [ ] **Step 4.4: Update `README.md`**

Find the bullet starting `- **Running multiple instances.**` in `README.md` (around line 45). After the existing sentence ending `…release their locks automatically because Postgres advisory locks die with the session.`, append a new sentence in the same bullet:

```
Alternatively, set `coordination.driver=redis` and `coordination.redis.url`
to share a Redis instance across deployments; the redis backend uses a
lease with a background renewal goroutine (see `config.example.yaml` for
TTL and renewal-interval knobs).
```

- [ ] **Step 4.5: Smoke-test `validate-config` with each driver**

Run:

```bash
go build ./...
./rss2msg --help >/dev/null
```

Then, in a scratch directory, write `cfg-noop.yaml`, `cfg-pg.yaml`, and `cfg-redis.yaml`, each a minimal config with one feed and one sink, varying only the `coordination` block. For each:

```bash
./rss2msg --config cfg-noop.yaml  validate-config
./rss2msg --config cfg-pg.yaml    validate-config   # uses your local Postgres DSN
./rss2msg --config cfg-redis.yaml validate-config   # uses redis://localhost:6379/0
```

Expected: exit code 0 for the configs whose backing service is reachable; clear, single-line error and exit code 1 for unreachable ones. (`validate-config` is allowed to dial — it does for Postgres today and will for Redis via `New`'s `Ping`.)

If Redis is not running locally, this step's redis check is optional; the unit + integration tests already validate the success path.

- [ ] **Step 4.6: Commit**

```bash
git add cmd/rss2msg/wire.go config.example.yaml README.md
git commit -m "feat(cmd): wire redis coordinator and document configuration"
```

---

## Out of scope (carried over from the design)

- Redlock (multi-Redis-node consensus).
- Sentinel / Cluster topologies beyond what `redis.ParseURL` natively supports.
- Configurable renewal jitter.
- Migration of lease state between coordinator backends.