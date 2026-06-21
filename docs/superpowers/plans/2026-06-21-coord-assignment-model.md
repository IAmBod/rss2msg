# Coordinator assignment/partition model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a membership + rendezvous-hash (HRW) assignment layer over the existing coordinator so each feed is scheduled by exactly one owner instance, removing the N×M wakeup/lock amplification, while `assignment.enabled:false` and the memory coordinator behave identically to today.

**Architecture:** A pure HRW function (`internal/assign`) decides `owner(feed)` from a live member set. A new `coord.Membership` interface (one impl per backend, sharing the coordinator's client via an optional `coord.MembershipProvider`) registers this instance under a TTL lease and enumerates the live set. An `OwnerProvider` wraps the existing `feedsource.Aggregator`, filters `Desired()` to owned feeds, and signals `Changes()` on membership change — so the unchanged `ServeDynamic` reconcile loop starts/stops only owned tickers. The per-tick `TryAcquire` guard in `pipeline.go` is retained unchanged as a rebalance backstop.

**Tech Stack:** Go 1.25, Cobra/Viper config, zerolog + OpenTelemetry, `cespare/xxhash/v2` for HRW, testcontainers for integration tests (Redis/Postgres/Cosmos) + LocalStack (DynamoDB).

## Global Constraints

- Go 1.25; run everything through `task` (`task test`, `task vet`, `task lint`, `task test-integration`, `task tidy`).
- **TDD:** failing test first, then minimal implementation.
- **Conventional Commits** (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
- **Staging hazard:** this repo is an Obsidian vault with auto-staging — **never `git add -A`/`git add .`**; stage explicit pathspecs and verify `git status` before committing.
- **Config key is `coordination`** (not `coordinator`); the new block is `coordination.assignment.*`.
- **Both example configs must stay byte-identical:** `examples/config.example.yaml` and `internal/config/example.yaml` (an existing test enforces this).
- **No existing users** — prefer the right design over backward-compat shims, but `assignment.enabled:false` MUST be a no-op identical to today.
- Module path is `github.com/iambod/rss2msg`.
- Run `scripts/check-doc-links.sh` (must print `OK: all relative doc links resolve`) after touching `docs/` or the README.
- Integration tests use the `//go:build integration` tag and need Docker; run via `task test-integration`.

---

### Task 1: `internal/assign` — rendezvous (HRW) assignment

**Files:**
- Create: `internal/assign/assign.go`
- Test: `internal/assign/assign_test.go`
- Modify: `go.mod` / `go.sum` (promote `github.com/cespare/xxhash/v2` to a direct dependency via `task tidy`)

**Interfaces:**
- Produces:
  - `func Owner(feedURL string, members []string) (owner string, ok bool)`
  - `func Owned(self string, feeds, members []string) []string`

- [ ] **Step 1: Write the failing test**

Create `internal/assign/assign_test.go`:

```go
package assign

import (
	"fmt"
	"testing"
)

func TestOwnerEmptyMembers(t *testing.T) {
	if _, ok := Owner("https://e/feed", nil); ok {
		t.Fatal("expected ok=false for empty members")
	}
}

func TestOwnerDeterministicAndOrderIndependent(t *testing.T) {
	a := []string{"m1", "m2", "m3"}
	b := []string{"m3", "m1", "m2"}
	o1, ok1 := Owner("https://e/feed-7", a)
	o2, ok2 := Owner("https://e/feed-7", b)
	if !ok1 || !ok2 || o1 != o2 {
		t.Fatalf("owner not order-independent: %q vs %q", o1, o2)
	}
}

func TestOwnedSelfNotMember(t *testing.T) {
	got := Owned("ghost", []string{"https://e/a"}, []string{"m1", "m2"})
	if got != nil {
		t.Fatalf("expected nil when self not in members, got %v", got)
	}
}

func TestOwnedSingleMemberOwnsAll(t *testing.T) {
	feeds := []string{"https://e/a", "https://e/b", "https://e/c"}
	got := Owned("m1", feeds, []string{"m1"})
	if len(got) != len(feeds) {
		t.Fatalf("single member should own all %d feeds, got %d", len(feeds), len(got))
	}
}

func TestOwnedPartitionsCompletelyAndDisjointly(t *testing.T) {
	members := []string{"m1", "m2", "m3"}
	feeds := make([]string, 300)
	for i := range feeds {
		feeds[i] = fmt.Sprintf("https://e/feed-%d", i)
	}
	seen := map[string]int{}
	for _, m := range members {
		for _, f := range Owned(m, feeds, members) {
			seen[f]++
		}
	}
	if len(seen) != len(feeds) {
		t.Fatalf("expected every feed owned exactly once; covered %d/%d", len(seen), len(feeds))
	}
	for f, n := range seen {
		if n != 1 {
			t.Fatalf("feed %q owned by %d members, want 1", f, n)
		}
	}
}

func TestDistributionRoughlyEven(t *testing.T) {
	members := make([]string, 10)
	for i := range members {
		members[i] = fmt.Sprintf("m%d", i)
	}
	const M = 10000
	feeds := make([]string, M)
	for i := range feeds {
		feeds[i] = fmt.Sprintf("https://e/feed-%d", i)
	}
	counts := map[string]int{}
	for _, m := range members {
		counts[m] = len(Owned(m, feeds, members))
	}
	exp := M / len(members)
	for m, c := range counts {
		if c < exp*8/10 || c > exp*12/10 {
			t.Fatalf("member %s owns %d feeds, want within ±20%% of %d", m, c, exp)
		}
	}
}

func TestMinimalChurnOnRemoval(t *testing.T) {
	before := []string{"m1", "m2", "m3", "m4"}
	after := []string{"m1", "m2", "m4"} // removed m3
	const M = 5000
	feeds := make([]string, M)
	for i := range feeds {
		feeds[i] = fmt.Sprintf("https://e/feed-%d", i)
	}
	moved := 0
	for _, f := range feeds {
		o1, _ := Owner(f, before)
		o2, _ := Owner(f, after)
		if o1 != o2 {
			moved++
			if o1 != "m3" {
				t.Fatalf("feed %q moved (%s->%s) but its old owner was not the removed member", f, o1, o2)
			}
		}
	}
	// Only m3's former feeds (~M/4) should move; allow generous slack.
	if moved > M/3 {
		t.Fatalf("removal moved %d feeds, want ~%d (only the removed member's share)", moved, M/4)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/assign/...`
Expected: FAIL — `internal/assign/assign.go` does not exist (build error: undefined `Owner`/`Owned`).

- [ ] **Step 3: Write minimal implementation**

Create `internal/assign/assign.go`:

```go
// Package assign computes feed ownership across a fleet of instances using
// rendezvous (highest-random-weight) hashing. It is pure and does no I/O: each
// instance feeds it the same live member set and gets the same assignment, so
// owners agree without coordinating. Adding or removing one member moves only
// ~1/|members| of feeds (minimal churn); every other feed keeps its owner.
package assign

import "github.com/cespare/xxhash/v2"

// score is the rendezvous weight of (member, feedURL). The member with the
// highest score owns the feed; ties break toward the lexically larger member ID
// so the result is deterministic regardless of slice order.
func score(member, feedURL string) uint64 {
	d := xxhash.New()
	_, _ = d.WriteString(member)
	_, _ = d.Write([]byte{0})
	_, _ = d.WriteString(feedURL)
	return d.Sum64()
}

// Owner returns the member that owns feedURL under HRW hashing. ok is false when
// members is empty.
func Owner(feedURL string, members []string) (string, bool) {
	var best string
	var bestScore uint64
	found := false
	for _, m := range members {
		s := score(m, feedURL)
		if !found || s > bestScore || (s == bestScore && m > best) {
			best, bestScore, found = m, s, true
		}
	}
	return best, found
}

// Owned returns the subset of feeds owned by self given the live member set.
// Returns nil if members is empty or self is not among members.
func Owned(self string, feeds, members []string) []string {
	inSet := false
	for _, m := range members {
		if m == self {
			inSet = true
			break
		}
	}
	if !inSet {
		return nil
	}
	var owned []string
	for _, f := range feeds {
		if o, ok := Owner(f, members); ok && o == self {
			owned = append(owned, f)
		}
	}
	return owned
}
```

- [ ] **Step 4: Promote the dependency and run tests**

Run: `task tidy && go test ./internal/assign/...`
Expected: PASS (all tests). `task tidy` moves `cespare/xxhash/v2` out of the `// indirect` block in `go.mod`.

- [ ] **Step 5: Commit**

```bash
git add internal/assign/assign.go internal/assign/assign_test.go go.mod go.sum
git status
git commit -m "feat(assign): rendezvous-hash feed ownership function"
```

---

### Task 2: `coord.Membership` interface + shared member-ID + memory backend

**Files:**
- Modify: `internal/coord/coord.go` (add `Membership`, `MembershipProvider`, `NewMemberID`)
- Create: `internal/coord/memberid.go` (member-ID helper)
- Create: `internal/coord/memory/membership.go`
- Test: `internal/coord/memory/membership_test.go`
- Test: `internal/coord/memberid_test.go`

**Interfaces:**
- Produces:
  - `type Membership interface { Heartbeat(ctx context.Context) ([]string, error); Deregister(ctx context.Context) error; Close() error }`
  - `type MembershipProvider interface { Membership(self string) (Membership, error) }`
  - `func NewMemberID() string` — `hostname-pid-randomhex`
  - `memory.Coordinator` implements `MembershipProvider`; its membership always returns `[]string{self}`.

- [ ] **Step 1: Write the failing tests**

Create `internal/coord/memberid_test.go`:

```go
package coord

import (
	"strings"
	"testing"
)

func TestNewMemberIDUniqueAndShaped(t *testing.T) {
	a := NewMemberID()
	b := NewMemberID()
	if a == "" || b == "" {
		t.Fatal("member id must be non-empty")
	}
	if a == b {
		t.Fatal("two member ids should differ (random suffix)")
	}
	if strings.Count(a, "-") < 2 {
		t.Fatalf("member id %q should look like host-pid-rand", a)
	}
}
```

Create `internal/coord/memory/membership_test.go`:

```go
package memory

import (
	"context"
	"testing"

	"github.com/iambod/rss2msg/internal/coord"
)

func TestMemoryMembershipSingleMember(t *testing.T) {
	t.Parallel()
	var c coord.MembershipProvider = New()
	m, err := c.Membership("self-1")
	if err != nil {
		t.Fatalf("membership: %v", err)
	}
	got, err := m.Heartbeat(context.Background())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if len(got) != 1 || got[0] != "self-1" {
		t.Fatalf("expected single member [self-1], got %v", got)
	}
	if err := m.Deregister(context.Background()); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/coord/... 2>&1 | head -20`
Expected: FAIL — `NewMemberID`, `coord.Membership`, `coord.MembershipProvider`, and `memory` membership are undefined.

- [ ] **Step 3: Add the interfaces and member-ID helper**

Append to `internal/coord/coord.go` (keep existing `Coordinator`/`ReleaseFunc`):

```go
// Membership tracks the live set of rss2msg instances sharing a coordinator.
// Implementations register this instance under a TTL lease and return the
// currently-live member IDs (including self). Safe for concurrent use.
type Membership interface {
	// Heartbeat refreshes this instance's lease and returns the current live
	// member set, including self. Called every heartbeat_interval. On error the
	// caller keeps the last-known member set (fail-static).
	Heartbeat(ctx context.Context) ([]string, error)
	// Deregister removes this instance's member entry so peers reassign its
	// feeds promptly on graceful shutdown instead of waiting for the TTL.
	// Best-effort: callers log failures rather than treating them as fatal.
	Deregister(ctx context.Context) error
	Close() error
}

// MembershipProvider is implemented by coordinators that support the assignment
// layer. Membership returns a Membership bound to this instance's member ID,
// reusing the coordinator's existing client/connection.
type MembershipProvider interface {
	Membership(self string) (Membership, error)
}
```

Create `internal/coord/memberid.go`:

```go
package coord

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// NewMemberID returns a per-process instance identifier shaped host-pid-rand.
// crypto/rand makes two processes on the same host collision-free; the time
// fallback keeps us running if the RNG is unavailable.
func NewMemberID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d-%x", host, os.Getpid(), time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}
```

Create `internal/coord/memory/membership.go`:

```go
package memory

import (
	"context"

	"github.com/iambod/rss2msg/internal/coord"
)

// Membership implements coord.MembershipProvider for the in-process coordinator:
// the fleet is always exactly this one instance, so it owns every feed.
func (Coordinator) Membership(self string) (coord.Membership, error) {
	return staticMembership{self: self}, nil
}

type staticMembership struct{ self string }

func (m staticMembership) Heartbeat(context.Context) ([]string, error) { return []string{m.self}, nil }
func (staticMembership) Deregister(context.Context) error              { return nil }
func (staticMembership) Close() error                                  { return nil }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/coord/...`
Expected: PASS (new tests + existing memory test).

- [ ] **Step 5: Commit**

```bash
git add internal/coord/coord.go internal/coord/memberid.go internal/coord/memberid_test.go internal/coord/memory/membership.go internal/coord/memory/membership_test.go
git status
git commit -m "feat(coord): membership interface, member-id helper, memory single-member backend"
```

---

### Task 3: Config — `coordination.assignment.*` keys, defaults, validation, example sync

**Files:**
- Modify: `internal/config/config.go` (add `CoordinationAssignmentConfig`, field on `CoordinationConfig`, defaults in `Default()`)
- Modify: `internal/config/load.go` (register `SetDefault`s)
- Modify: `internal/config/validate.go` (validate the new block)
- Modify: `examples/config.example.yaml` and `internal/config/example.yaml` (byte-identical)
- Test: `internal/config/validate_test.go` (add cases)

**Interfaces:**
- Produces:
  - `config.CoordinationAssignmentConfig{ Enabled bool; Strategy string; HeartbeatInterval, MemberTTL, RebalanceGrace time.Duration }`
  - `config.CoordinationConfig.Assignment CoordinationAssignmentConfig`

- [ ] **Step 1: Write the failing validation tests**

Add to `internal/config/validate_test.go`:

```go
func TestAssignmentValidation(t *testing.T) {
	base := func() Config {
		c := Default()
		c.Coordination.Driver = "redis"
		c.Coordination.Redis.URL = "redis://localhost:6379"
		c.State.Driver = "postgres"
		c.State.Postgres.DSN = "postgres://x"
		c.Coordination.Assignment = CoordinationAssignmentConfig{
			Enabled: true, Strategy: "rendezvous",
			HeartbeatInterval: 10 * time.Second, MemberTTL: 30 * time.Second, RebalanceGrace: 5 * time.Second,
		}
		c.Feeds = []FeedConfig{{URL: "https://e/a", Interval: time.Minute}}
		c.Sinks = []SinkConfig{{Name: "s", Driver: "stdout"}}
		c.Routes = []RouteConfig{{Feed: "https://e/a", Sinks: []string{"s"}}}
		return c
	}

	t.Run("valid passes", func(t *testing.T) {
		if _, err := base().Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("member_ttl must exceed heartbeat_interval", func(t *testing.T) {
		c := base()
		c.Coordination.Assignment.MemberTTL = 10 * time.Second
		c.Coordination.Assignment.HeartbeatInterval = 10 * time.Second
		if _, err := c.Validate(); err == nil {
			t.Fatal("expected error when member_ttl <= heartbeat_interval")
		}
	})

	t.Run("unknown strategy rejected", func(t *testing.T) {
		c := base()
		c.Coordination.Assignment.Strategy = "magic"
		if _, err := c.Validate(); err == nil {
			t.Fatal("expected error for unknown strategy")
		}
	})

	t.Run("enabled with memory driver warns not errors", func(t *testing.T) {
		c := base()
		c.Coordination.Driver = "memory"
		c.Coordination.Redis = CoordinationRedisConfig{}
		c.State.Driver = "sqlite"
		c.State.Postgres = StatePostgresConfig{}
		warns, err := c.Validate()
		if err != nil {
			t.Fatalf("memory+assignment should warn, not error: %v", err)
		}
		joined := strings.Join(warns, "\n")
		if !strings.Contains(joined, "assignment") {
			t.Fatalf("expected an assignment warning, got %v", warns)
		}
	})
}
```

> Note: match the actual `Validate()` signature and the route/sink field names used elsewhere in `validate_test.go`; mirror an existing passing-config helper in that file if these field names differ.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/config/ -run TestAssignmentValidation`
Expected: FAIL — `CoordinationAssignmentConfig` undefined.

- [ ] **Step 3: Add the config struct and defaults**

In `internal/config/config.go`, add the field to `CoordinationConfig`:

```go
type CoordinationConfig struct {
	Driver     string                       `mapstructure:"driver"`
	Postgres   CoordinationPGConfig         `mapstructure:"postgres"`
	Redis      CoordinationRedisConfig      `mapstructure:"redis"`
	DynamoDB   CoordinationDynamoDBConfig   `mapstructure:"dynamodb"`
	CosmosDB   CoordinationCosmosDBConfig   `mapstructure:"cosmosdb"`
	Assignment CoordinationAssignmentConfig `mapstructure:"assignment"`
}

// CoordinationAssignmentConfig configures the membership/partition layer. When
// disabled (default) every instance schedules every feed and the per-tick
// TryAcquire lease alone decides who polls (today's behavior). When enabled,
// each instance heartbeats into the coordinator and schedules only the feeds it
// owns under rendezvous hashing of the live member set.
type CoordinationAssignmentConfig struct {
	Enabled           bool          `mapstructure:"enabled"`
	Strategy          string        `mapstructure:"strategy"`           // "rendezvous" (default)
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"` // 0 -> 10s
	MemberTTL         time.Duration `mapstructure:"member_ttl"`         // 0 -> 30s; must exceed heartbeat_interval
	RebalanceGrace    time.Duration `mapstructure:"rebalance_grace"`    // 0 -> 5s; documents the transition window covered by the guard
}
```

In `Default()` (the function returning the default `Config`, ~line 758), set:

```go
		Coordination: CoordinationConfig{
			Driver: "memory",
			Assignment: CoordinationAssignmentConfig{
				Enabled:           false,
				Strategy:          "rendezvous",
				HeartbeatInterval: 10 * time.Second,
				MemberTTL:         30 * time.Second,
				RebalanceGrace:    5 * time.Second,
			},
		},
```

- [ ] **Step 4: Register Viper defaults**

In `internal/config/load.go`, next to the other coordination defaults, add:

```go
	v.SetDefault("coordination.assignment.enabled", d.Coordination.Assignment.Enabled)
	v.SetDefault("coordination.assignment.strategy", d.Coordination.Assignment.Strategy)
	v.SetDefault("coordination.assignment.heartbeat_interval", d.Coordination.Assignment.HeartbeatInterval)
	v.SetDefault("coordination.assignment.member_ttl", d.Coordination.Assignment.MemberTTL)
	v.SetDefault("coordination.assignment.rebalance_grace", d.Coordination.Assignment.RebalanceGrace)
```

- [ ] **Step 5: Add validation**

In `internal/config/validate.go`, inside the coordination validation block, add an assignment check (place after the driver-known check, before the function returns its warnings):

```go
	if a := c.Coordination.Assignment; a.Enabled {
		if a.Strategy != "" && a.Strategy != "rendezvous" {
			return *warnings, fmt.Errorf("coordination.assignment.strategy %q is not supported (only \"rendezvous\")", a.Strategy)
		}
		if a.HeartbeatInterval < 0 || a.MemberTTL < 0 || a.RebalanceGrace < 0 {
			return *warnings, fmt.Errorf("coordination.assignment durations must be non-negative")
		}
		hb := a.HeartbeatInterval
		if hb == 0 {
			hb = 10 * time.Second
		}
		ttl := a.MemberTTL
		if ttl == 0 {
			ttl = 30 * time.Second
		}
		if ttl <= hb {
			return *warnings, fmt.Errorf("coordination.assignment.member_ttl (%s) must exceed heartbeat_interval (%s)", ttl, hb)
		}
		if c.Coordination.Driver == "" || c.Coordination.Driver == "memory" {
			*warnings = append(*warnings, "coordination.assignment.enabled has no effect with the memory coordinator (single instance); use redis/postgres/dynamodb/cosmosdb for multi-instance assignment")
		}
	}
```

> Match the existing return shape in `validate.go` (it returns `(warnings []string, err error)` or similar via the `*warnings` pointer pattern already used in that file). Confirm `time` is imported (it is).

- [ ] **Step 6: Update both example configs (byte-identical)**

Find the `coordination:` block in `examples/config.example.yaml` and add the `assignment` sub-block (preserve the file's existing comment style and indentation):

```yaml
coordination:
  driver: memory
  # assignment: membership + rendezvous-hash partitioning so each feed is polled
  # by exactly one instance. Default off (every instance polls every feed, the
  # per-tick lease decides). Requires a distributed driver to have any effect.
  assignment:
    enabled: false
    strategy: rendezvous      # only "rendezvous" is supported today
    heartbeat_interval: 10s
    member_ttl: 30s           # must exceed heartbeat_interval; dead members expire after this
    rebalance_grace: 5s
```

Copy the identical block into `internal/config/example.yaml`. Then verify byte-identity:

Run: `diff examples/config.example.yaml internal/config/example.yaml && echo IDENTICAL`
Expected: `IDENTICAL` (no diff output).

- [ ] **Step 7: Run config tests**

Run: `go test ./internal/config/...`
Expected: PASS (validation tests + the existing example-drift-guard test).

- [ ] **Step 8: Commit**

```bash
git add internal/config/config.go internal/config/load.go internal/config/validate.go internal/config/validate_test.go examples/config.example.yaml internal/config/example.yaml
git status
git commit -m "feat(config): coordination.assignment.* keys, defaults, validation"
```

---

### Task 4: Telemetry instruments

**Files:**
- Modify: `internal/telemetry/telemetry.go` (`Instruments` struct + `NewInstruments`)
- Test: `internal/telemetry/telemetry_test.go` (extend the instruments test)

**Interfaces:**
- Produces (added to `telemetry.Instruments`):
  - `MembershipSize metric.Int64Gauge`
  - `AssignedFeeds metric.Int64Gauge`
  - `RebalanceEvents metric.Int64Counter`

- [ ] **Step 1: Write the failing test**

Add to `internal/telemetry/telemetry_test.go` (mirror the existing test that constructs `NewInstruments` with a noop/manual meter):

```go
func TestInstrumentsHasAssignmentMeters(t *testing.T) {
	mp := metricnoop.NewMeterProvider() // use the same noop meter the existing test uses
	in, err := NewInstruments(mp.Meter("test"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	if in.MembershipSize == nil || in.AssignedFeeds == nil || in.RebalanceEvents == nil {
		t.Fatal("expected MembershipSize, AssignedFeeds, RebalanceEvents to be initialized")
	}
}
```

> Use whatever noop meter import the existing `telemetry_test.go` already uses; do not introduce a new one.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/telemetry/ -run TestInstrumentsHasAssignmentMeters`
Expected: FAIL — fields undefined.

- [ ] **Step 3: Add the instruments**

In `internal/telemetry/telemetry.go`, extend the `Instruments` struct:

```go
	PollOverran         metric.Int64Counter
	MembershipSize      metric.Int64Gauge
	AssignedFeeds       metric.Int64Gauge
	RebalanceEvents     metric.Int64Counter
	FeedFetchDuration   metric.Float64Histogram
```

In `NewInstruments`, after the existing counters, add (mirroring the existing `meter.Int64Counter(...)` error-handling style):

```go
	if i.MembershipSize, err = meter.Int64Gauge("coord.membership.size",
		metric.WithDescription("live coordinator members as last observed by this instance")); err != nil {
		return i, err
	}
	if i.AssignedFeeds, err = meter.Int64Gauge("coord.assignment.feeds",
		metric.WithDescription("feeds owned by this instance under the assignment layer")); err != nil {
		return i, err
	}
	if i.RebalanceEvents, err = meter.Int64Counter("coord.assignment.rebalance",
		metric.WithDescription("number of times this instance's owned feed set changed")); err != nil {
		return i, err
	}
```

> If the installed OTel metric API lacks `Int64Gauge` (added in a recent version), use `Int64ObservableGauge` with a registered callback instead, or fall back to `Int64UpDownCounter`. Check `go doc go.opentelemetry.io/otel/metric.Meter` first and pick the gauge type the version supports.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/telemetry/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/telemetry/telemetry.go internal/telemetry/telemetry_test.go
git status
git commit -m "feat(telemetry): membership size, assigned feeds, rebalance event meters"
```

---

### Task 5: `OwnerProvider` — ownership-filtering feed provider

**Files:**
- Create: `internal/scheduler/ownerprovider.go`
- Test: `internal/scheduler/ownerprovider_test.go`

**Interfaces:**
- Consumes: `scheduler.FeedProvider` (`Desired(ctx) ([]config.FeedConfig, error)`, `Changes() <-chan struct{}`); `coord.Membership`; `assign.Owned`.
- Produces:
  - `func NewOwnerProvider(inner FeedProvider, m coord.Membership, self string, heartbeat time.Duration, onRebalance func(members, owned int)) *OwnerProvider`
  - `*OwnerProvider` implements `FeedProvider`.
  - `func (*OwnerProvider) Run(ctx context.Context)` — heartbeat loop; returns when ctx is done.
  - `func (*OwnerProvider) Close(ctx context.Context) error` — deregisters + closes membership.

- [ ] **Step 1: Write the failing test**

Create `internal/scheduler/ownerprovider_test.go`:

```go
package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

type fakeInner struct {
	feeds []config.FeedConfig
	ch    chan struct{}
}

func (f *fakeInner) Desired(context.Context) ([]config.FeedConfig, error) { return f.feeds, nil }
func (f *fakeInner) Changes() <-chan struct{}                             { return f.ch }

type fakeMembership struct {
	mu      sync.Mutex
	members []string
}

func (m *fakeMembership) set(ids ...string) {
	m.mu.Lock()
	m.members = ids
	m.mu.Unlock()
}
func (m *fakeMembership) Heartbeat(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.members))
	copy(out, m.members)
	return out, nil
}
func (m *fakeMembership) Deregister(context.Context) error { return nil }
func (m *fakeMembership) Close() error                     { return nil }

func feeds(urls ...string) []config.FeedConfig {
	out := make([]config.FeedConfig, len(urls))
	for i, u := range urls {
		out[i] = config.FeedConfig{URL: u, Interval: time.Minute}
	}
	return out
}

func TestOwnerProviderFiltersToOwned(t *testing.T) {
	inner := &fakeInner{feeds: feeds("https://e/a", "https://e/b", "https://e/c"), ch: make(chan struct{}, 1)}
	mem := &fakeMembership{members: []string{"self", "peer"}}
	op := NewOwnerProvider(inner, mem, "self", 10*time.Millisecond, nil)

	// Prime the member snapshot.
	if _, err := op.Heartbeat(context.Background()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	owned, err := op.Desired(context.Background())
	if err != nil {
		t.Fatalf("desired: %v", err)
	}
	// Cross-check: the union of self's and peer's owned sets is all feeds, disjoint.
	opPeer := NewOwnerProvider(inner, mem, "peer", time.Hour, nil)
	_, _ = opPeer.Heartbeat(context.Background())
	peerOwned, _ := opPeer.Desired(context.Background())
	if len(owned)+len(peerOwned) != 3 {
		t.Fatalf("owned(%d)+peerOwned(%d) should equal 3 feeds", len(owned), len(peerOwned))
	}
}

func TestOwnerProviderSignalsOnMembershipChange(t *testing.T) {
	inner := &fakeInner{feeds: feeds("https://e/a", "https://e/b"), ch: make(chan struct{}, 1)}
	mem := &fakeMembership{members: []string{"self"}}
	op := NewOwnerProvider(inner, mem, "self", 5*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go op.Run(ctx)

	// Drain the initial signal (first heartbeat establishes the baseline).
	select {
	case <-op.Changes():
	case <-time.After(time.Second):
	}

	mem.set("self", "peer") // membership grows
	select {
	case <-op.Changes():
		// success: change propagated
	case <-time.After(2 * time.Second):
		t.Fatal("expected Changes() signal after membership changed")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/scheduler/ -run TestOwnerProvider`
Expected: FAIL — `NewOwnerProvider` undefined.

- [ ] **Step 3: Implement the provider**

Create `internal/scheduler/ownerprovider.go`:

```go
package scheduler

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/iambod/rss2msg/internal/assign"
	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/coord"
)

// OwnerProvider wraps a FeedProvider so that Desired() yields only the feeds
// this instance owns under rendezvous hashing of the live coordinator members.
// A heartbeat loop refreshes membership and signals Changes() whenever the
// member set changes, so ServeDynamic reconciles the owned ticker set.
type OwnerProvider struct {
	inner       FeedProvider
	membership  coord.Membership
	self        string
	heartbeat   time.Duration
	onRebalance func(members, owned int)

	changes chan struct{}

	mu      sync.RWMutex
	members []string
	lastKey string
}

// NewOwnerProvider builds the provider. heartbeat is the membership refresh
// period; onRebalance (optional) is called with the live member count and this
// instance's owned-feed count whenever membership changes.
func NewOwnerProvider(inner FeedProvider, m coord.Membership, self string, heartbeat time.Duration, onRebalance func(members, owned int)) *OwnerProvider {
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	return &OwnerProvider{
		inner: inner, membership: m, self: self, heartbeat: heartbeat,
		onRebalance: onRebalance,
		changes:     make(chan struct{}, 1),
		members:     []string{self}, // fail-static baseline: alone until first heartbeat
	}
}

func (o *OwnerProvider) snapshot() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]string, len(o.members))
	copy(out, o.members)
	return out
}

// Heartbeat refreshes membership once and returns the live set. It updates the
// cached snapshot and signals Changes() if the set changed. Exposed for tests
// and the priming call.
func (o *OwnerProvider) Heartbeat(ctx context.Context) ([]string, error) {
	members, err := o.membership.Heartbeat(ctx)
	if err != nil {
		return o.snapshot(), err // keep last-known set
	}
	sorted := append([]string(nil), members...)
	sort.Strings(sorted)
	key := strings.Join(sorted, ",")

	o.mu.Lock()
	changed := key != o.lastKey
	o.members = sorted
	o.lastKey = key
	o.mu.Unlock()

	if changed {
		o.signal()
		if o.onRebalance != nil {
			feeds, _ := o.inner.Desired(ctx)
			owned := assign.Owned(o.self, urls(feeds), sorted)
			o.onRebalance(len(sorted), len(owned))
		}
	}
	return sorted, nil
}

func (o *OwnerProvider) signal() {
	select {
	case o.changes <- struct{}{}:
	default: // already pending; reconcile reads the latest state anyway
	}
}

// Run drives the heartbeat loop until ctx is cancelled.
func (o *OwnerProvider) Run(ctx context.Context) {
	_, _ = o.Heartbeat(ctx) // establish baseline immediately
	t := time.NewTicker(o.heartbeat)
	defer t.Stop()
	// Forward inner provider changes onto our merged channel.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-o.inner.Changes():
				o.signal()
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = o.Heartbeat(ctx)
		}
	}
}

// Desired returns only this instance's owned feeds from the inner set.
func (o *OwnerProvider) Desired(ctx context.Context) ([]config.FeedConfig, error) {
	all, err := o.inner.Desired(ctx)
	if err != nil {
		return nil, err
	}
	owned := assign.Owned(o.self, urls(all), o.snapshot())
	ownedSet := make(map[string]struct{}, len(owned))
	for _, u := range owned {
		ownedSet[u] = struct{}{}
	}
	out := make([]config.FeedConfig, 0, len(owned))
	for _, fc := range all {
		if _, ok := ownedSet[fc.URL]; ok {
			out = append(out, fc)
		}
	}
	return out, nil
}

// Changes signals when either the inner feed set or the membership changes.
func (o *OwnerProvider) Changes() <-chan struct{} { return o.changes }

// Close deregisters this instance (best-effort) and closes the membership.
func (o *OwnerProvider) Close(ctx context.Context) error {
	derr := o.membership.Deregister(ctx)
	cerr := o.membership.Close()
	if derr != nil {
		return derr
	}
	return cerr
}

func urls(feeds []config.FeedConfig) []string {
	out := make([]string, len(feeds))
	for i, fc := range feeds {
		out[i] = fc.URL
	}
	return out
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/scheduler/ -run TestOwnerProvider -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/ownerprovider.go internal/scheduler/ownerprovider_test.go
git status
git commit -m "feat(scheduler): ownership-filtering feed provider with heartbeat loop"
```

---

### Task 6: Redis membership backend

**Files:**
- Create: `internal/coord/redis/membership.go`
- Test (unit): `internal/coord/redis/membership_unit_test.go`
- Test (integration): `internal/coord/redis/membership_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: the Redis `Coordinator`'s `client redis.UniversalClient` and `opts` (key prefix `rss2msg:coord:`).
- Produces: `func (c *Coordinator) Membership(self string) (coord.Membership, error)` returning a `*redisMembership`.

- [ ] **Step 1: Write the failing unit test**

Create `internal/coord/redis/membership_unit_test.go` (uses `miniredis` if the package already depends on it; otherwise mark these assertions for the integration test and keep only key-format checks here):

```go
package redis

import "testing"

func TestMemberKeyFormat(t *testing.T) {
	got := memberKey("host-123-abcd")
	want := "rss2msg:coord:member:host-123-abcd"
	if got != want {
		t.Fatalf("memberKey = %q, want %q", got, want)
	}
	if pfx := memberKeyPrefix(); pfx != "rss2msg:coord:member:" {
		t.Fatalf("memberKeyPrefix = %q", pfx)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/coord/redis/ -run TestMemberKeyFormat`
Expected: FAIL — `memberKey` undefined.

- [ ] **Step 3: Implement Redis membership**

Create `internal/coord/redis/membership.go`:

```go
package redis

import (
	"context"
	"strings"
	"time"

	"github.com/iambod/rss2msg/internal/coord"
)

func memberKeyPrefix() string      { return "rss2msg:coord:member:" }
func memberKey(self string) string { return memberKeyPrefix() + self }

// Membership returns a Redis-backed membership reusing this coordinator's client.
// Each member is a key with a TTL; the live set is a SCAN of the member prefix.
func (c *Coordinator) Membership(self string) (coord.Membership, error) {
	ttl := c.opts.MemberTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &redisMembership{c: c, self: self, ttl: ttl}, nil
}

type redisMembership struct {
	c    *Coordinator
	self string
	ttl  time.Duration
}

func (m *redisMembership) Heartbeat(ctx context.Context) ([]string, error) {
	if err := m.c.client.Set(ctx, memberKey(m.self), "1", m.ttl).Err(); err != nil {
		return nil, err
	}
	var ids []string
	prefix := memberKeyPrefix()
	iter := m.c.client.Scan(ctx, 0, prefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		ids = append(ids, strings.TrimPrefix(iter.Val(), prefix))
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (m *redisMembership) Deregister(ctx context.Context) error {
	return m.c.client.Del(ctx, memberKey(m.self)).Err()
}

func (m *redisMembership) Close() error { return nil } // shares the coordinator's client
```

> Add a `MemberTTL time.Duration` field to the Redis `Options` struct and map it in `redisCoordOptions` (wire.go) from `cc.Assignment.MemberTTL`. If `Options` has no place for it, store it on the `Coordinator` instead and set it in `New`. Confirm the client field name (`client`) and `opts` field names against `redis.go`.

- [ ] **Step 4: Write the integration test**

Create `internal/coord/redis/membership_test.go`:

```go
//go:build integration

package redis

import (
	"context"
	"testing"
	"time"
)

func TestRedisMembershipRegistersAndDeregisters(t *testing.T) {
	url := newRedisURL(t) // existing helper in redis_test.go
	ctx := context.Background()

	c1, err := New(ctx, Options{URL: url, MemberTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := New(ctx, Options{URL: url, MemberTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	m1, _ := c1.Membership("inst-1")
	m2, _ := c2.Membership("inst-2")

	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(got, "inst-1") || !contains(got, "inst-2") {
		t.Fatalf("expected both members live, got %v", got)
	}

	// Graceful deregister drops inst-1 immediately.
	if err := m1.Deregister(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = m2.Heartbeat(ctx)
	if contains(got, "inst-1") {
		t.Fatalf("inst-1 should be gone after deregister, got %v", got)
	}

	// TTL expiry drops a crashed member (stop heartbeating inst-2... emulate by waiting).
	time.Sleep(3 * time.Second)
	got, _ = m1.Heartbeat(ctx) // re-registers inst-1
	if contains(got, "inst-2") {
		t.Fatalf("inst-2 should have expired via TTL, got %v", got)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
```

- [ ] **Step 5: Run unit tests; run integration if Docker is available**

Run: `go test ./internal/coord/redis/ -run TestMemberKeyFormat`
Expected: PASS.

Run (Docker): `go test -tags=integration ./internal/coord/redis/ -run TestRedisMembership`
Expected: PASS. If Docker is unavailable, note it explicitly and rely on CI.

- [ ] **Step 6: Commit**

```bash
git add internal/coord/redis/membership.go internal/coord/redis/membership_unit_test.go internal/coord/redis/membership_test.go internal/coord/redis/redis.go cmd/rss2msg/wire.go
git status
git commit -m "feat(coord/redis): TTL-keyed membership backend"
```

---

### Task 7: Postgres membership backend

**Files:**
- Create: `internal/coord/postgres/membership.go`
- Test (integration): `internal/coord/postgres/membership_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: the PG `Coordinator`'s `*pgxpool.Pool`.
- Produces: `func (c *Coordinator) Membership(self string) (coord.Membership, error)`; auto-creates `coordination_members(id text primary key, last_seen timestamptz not null)`.

- [ ] **Step 1: Write the integration test (the table needs a real Postgres)**

Create `internal/coord/postgres/membership_test.go`:

```go
//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"
)

func TestPostgresMembershipLiveSetAndExpiry(t *testing.T) {
	dsn := newDSN(t) // existing helper in postgres_test.go
	ctx := context.Background()

	c, err := New(ctx, Options{DSN: dsn, MemberTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	m1, _ := c.Membership("inst-1")
	m2, _ := c.Membership("inst-2")
	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 live members, got %v", got)
	}

	if err := m1.Deregister(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = m2.Heartbeat(ctx)
	if len(got) != 1 || got[0] != "inst-2" {
		t.Fatalf("expected only inst-2 after deregister, got %v", got)
	}
}
```

- [ ] **Step 2: Run to verify failure (Docker)**

Run: `go test -tags=integration ./internal/coord/postgres/ -run TestPostgresMembership`
Expected: FAIL — `Membership` undefined (and `MemberTTL` field missing on `Options`).

- [ ] **Step 3: Implement Postgres membership**

Create `internal/coord/postgres/membership.go`:

```go
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/iambod/rss2msg/internal/coord"
)

const createMembersTable = `
CREATE TABLE IF NOT EXISTS coordination_members (
    id        text PRIMARY KEY,
    last_seen timestamptz NOT NULL
)`

// Membership returns a Postgres-backed membership reusing this coordinator's
// pool. Liveness is judged by last_seen relative to member_ttl (coordinator
// clock), so instance clock skew is irrelevant.
func (c *Coordinator) Membership(self string) (coord.Membership, error) {
	ttl := c.memberTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if _, err := c.pool.Exec(context.Background(), createMembersTable); err != nil {
		return nil, fmt.Errorf("create coordination_members: %w", err)
	}
	return &pgMembership{c: c, self: self, ttl: ttl}, nil
}

type pgMembership struct {
	c    *Coordinator
	self string
	ttl  time.Duration
}

func (m *pgMembership) Heartbeat(ctx context.Context) ([]string, error) {
	if _, err := m.c.pool.Exec(ctx,
		`INSERT INTO coordination_members (id, last_seen) VALUES ($1, now())
		 ON CONFLICT (id) DO UPDATE SET last_seen = now()`, m.self); err != nil {
		return nil, err
	}
	// Opportunistically reap rows older than the TTL, then read the live set.
	cutoff := m.ttl
	rows, err := m.c.pool.Query(ctx,
		`DELETE FROM coordination_members WHERE last_seen < now() - $1::interval;
		 SELECT id FROM coordination_members WHERE last_seen > now() - $1::interval`,
		fmt.Sprintf("%d milliseconds", cutoff.Milliseconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (m *pgMembership) Deregister(ctx context.Context) error {
	_, err := m.c.pool.Exec(ctx, `DELETE FROM coordination_members WHERE id = $1`, m.self)
	return err
}

func (m *pgMembership) Close() error { return nil } // shares the coordinator's pool
```

> Confirm the pool field name on the PG `Coordinator` (`pool *pgxpool.Pool` per the explore map). Add a `memberTTL time.Duration` field to the `Coordinator` and a `MemberTTL` to `Options`, set in `New`. If multi-statement `Query` (DELETE then SELECT) is rejected by pgx, split into two calls: `Exec` the DELETE, then `Query` the SELECT.

- [ ] **Step 4: Run to verify pass (Docker)**

Run: `go test -tags=integration ./internal/coord/postgres/ -run TestPostgresMembership`
Expected: PASS. Note explicitly if Docker is unavailable and defer to CI.

- [ ] **Step 5: Commit**

```bash
git add internal/coord/postgres/membership.go internal/coord/postgres/membership_test.go internal/coord/postgres/postgres.go cmd/rss2msg/wire.go
git status
git commit -m "feat(coord/postgres): coordination_members heartbeat table"
```

---

### Task 8: DynamoDB membership backend

**Files:**
- Create: `internal/coord/dynamodb/membership.go`
- Test (unit, fake client): `internal/coord/dynamodb/membership_unit_test.go`
- Test (integration): `internal/coord/dynamodb/membership_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: the Dynamo `Coordinator`'s `ddbAPI` client, table name, `LeaseDuration` (reused as member TTL when `MemberTTL` is unset).
- Produces: `func (c *Coordinator) Membership(self string) (coord.Membership, error)`; member items use `pk="member:<id>"`, attribute `lease_expiry` (epoch ms).

- [ ] **Step 1: Write the failing unit test (reuse the package's fake ddbAPI)**

Create `internal/coord/dynamodb/membership_unit_test.go` (mirror the fake client already used in `dynamodb_unit_test.go`):

```go
package dynamodb

import "testing"

func TestMemberPKFormat(t *testing.T) {
	if got := memberPK("h-1-ab"); got != "member:h-1-ab" {
		t.Fatalf("memberPK = %q", got)
	}
	if got := memberPKPrefix(); got != "member:" {
		t.Fatalf("memberPKPrefix = %q", got)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/coord/dynamodb/ -run TestMemberPKFormat`
Expected: FAIL — `memberPK` undefined.

- [ ] **Step 3: Implement DynamoDB membership**

Create `internal/coord/dynamodb/membership.go`:

```go
package dynamodb

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/iambod/rss2msg/internal/coord"
)

func memberPKPrefix() string    { return "member:" }
func memberPK(self string) string { return memberPKPrefix() + self }

// Membership returns a DynamoDB-backed membership reusing this coordinator's
// client and table. Members are items pk="member:<id>" with a lease_expiry; the
// live set is a Scan filtered to non-expired member items. The Scan runs once
// per heartbeat (not per feed), so its cost scales with members, not feeds.
func (c *Coordinator) Membership(self string) (coord.Membership, error) {
	ttl := c.memberTTL
	if ttl <= 0 {
		ttl = c.leaseDuration // fall back to the lock lease duration
	}
	return &dynamoMembership{c: c, self: self, ttl: ttl}, nil
}

type dynamoMembership struct {
	c    *Coordinator
	self string
	ttl  time.Duration
}

func (m *dynamoMembership) Heartbeat(ctx context.Context) ([]string, error) {
	expiry := nowFunc().Add(m.ttl).UnixMilli()
	_, err := m.c.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(m.c.table),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":           &ddbtypes.AttributeValueMemberS{Value: memberPK(m.self)},
			"lease_expiry": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(expiry, 10)},
		},
	})
	if err != nil {
		return nil, err
	}

	now := nowFunc().UnixMilli()
	var ids []string
	var startKey map[string]ddbtypes.AttributeValue
	for {
		out, err := m.c.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:        aws.String(m.c.table),
			FilterExpression: aws.String("begins_with(pk, :p) AND lease_expiry > :now"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":p":   &ddbtypes.AttributeValueMemberS{Value: memberPKPrefix()},
				":now": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(now, 10)},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, it := range out.Items {
			if s, ok := it["pk"].(*ddbtypes.AttributeValueMemberS); ok {
				ids = append(ids, strings.TrimPrefix(s.Value, memberPKPrefix()))
			}
		}
		if out.LastEvaluatedKey == nil {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return ids, nil
}

func (m *dynamoMembership) Deregister(ctx context.Context) error {
	_, err := m.c.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(m.c.table),
		Key:       map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: memberPK(m.self)}},
	})
	return err
}

func (m *dynamoMembership) Close() error { return nil }
```

> Align names with `dynamodb.go`: the client field (`client ddbAPI`), table field (`table string`), the clock (`nowFunc` or the struct's injected clock — reuse whatever TryAcquire uses so the integration test's expiry timing matches), the AttributeValue import alias (`ddbtypes`), and add `memberTTL`/`leaseDuration` fields if absent. Use the exact alias the existing file uses.

- [ ] **Step 4: Write the integration test**

Create `internal/coord/dynamodb/membership_test.go`:

```go
//go:build integration

package dynamodb

import (
	"context"
	"testing"
	"time"
)

func TestDynamoMembershipLiveSet(t *testing.T) {
	endpoint := startLocalDynamo(t) // existing LocalStack helper used by dynamodb_test.go
	ctx := context.Background()

	c, err := New(ctx, Options{Table: "coord_locks", EndpointURL: endpoint, Region: "us-east-1", MemberTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	m1, _ := c.Membership("inst-1")
	m2, _ := c.Membership("inst-2")
	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 members, got %v", got)
	}
	if err := m1.Deregister(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = m2.Heartbeat(ctx)
	if len(got) != 1 {
		t.Fatalf("expected 1 member after deregister, got %v", got)
	}
}
```

> Match the existing Dynamo integration setup exactly (table name, key schema "pk", how the LocalStack endpoint/table is created) — copy the `TestMain`/helper used by `dynamodb_test.go`.

- [ ] **Step 5: Run unit; run integration if Docker available**

Run: `go test ./internal/coord/dynamodb/ -run TestMemberPKFormat`
Expected: PASS.
Run (Docker): `go test -tags=integration ./internal/coord/dynamodb/ -run TestDynamoMembership`
Expected: PASS (or note Docker unavailable).

- [ ] **Step 6: Commit**

```bash
git add internal/coord/dynamodb/membership.go internal/coord/dynamodb/membership_unit_test.go internal/coord/dynamodb/membership_test.go internal/coord/dynamodb/dynamodb.go cmd/rss2msg/wire.go
git status
git commit -m "feat(coord/dynamodb): member-item heartbeat + scan-based live set"
```

---

### Task 9: CosmosDB membership backend

**Files:**
- Create: `internal/coord/cosmosdb/membership.go`
- Test (unit, fake container): `internal/coord/cosmosdb/membership_unit_test.go`
- Test (integration): `internal/coord/cosmosdb/membership_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: the Cosmos `Coordinator`'s `containerAPI`, lease duration.
- Produces: `func (c *Coordinator) Membership(self string) (coord.Membership, error)`; member docs `id="member:<id>"`, field `lease_expiry`.

- [ ] **Step 1: Write the failing unit test (reuse the package's fake containerAPI)**

Create `internal/coord/cosmosdb/membership_unit_test.go`:

```go
package cosmosdb

import "testing"

func TestMemberIDFormat(t *testing.T) {
	if got := memberDocID("h-1-ab"); got != "member:h-1-ab" {
		t.Fatalf("memberDocID = %q", got)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/coord/cosmosdb/ -run TestMemberIDFormat`
Expected: FAIL — `memberDocID` undefined.

- [ ] **Step 3: Implement CosmosDB membership**

Create `internal/coord/cosmosdb/membership.go`:

```go
package cosmosdb

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/iambod/rss2msg/internal/coord"
)

func memberPrefix() string          { return "member:" }
func memberDocID(self string) string { return memberPrefix() + self }

type memberDoc struct {
	ID          string `json:"id"`
	PK          string `json:"pk"`
	LeaseExpiry int64  `json:"lease_expiry"` // epoch ms
}

// Membership returns a Cosmos-backed membership reusing this coordinator's
// container. Members are documents id="member:<id>"; the live set is a query
// filtered to non-expired member docs.
func (c *Coordinator) Membership(self string) (coord.Membership, error) {
	ttl := c.memberTTL
	if ttl <= 0 {
		ttl = c.leaseDuration
	}
	return &cosmosMembership{c: c, self: self, ttl: ttl}, nil
}

type cosmosMembership struct {
	c    *Coordinator
	self string
	ttl  time.Duration
}

func (m *cosmosMembership) Heartbeat(ctx context.Context) ([]string, error) {
	id := memberDocID(m.self)
	doc := memberDoc{ID: id, PK: id, LeaseExpiry: nowFunc().Add(m.ttl).UnixMilli()}
	body, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	if err := m.c.upsertItem(ctx, id, body); err != nil { // mirror existing upsert/replace helper
		return nil, err
	}

	now := nowFunc().UnixMilli()
	ids, err := m.c.queryMemberIDs(ctx, memberPrefix(), now) // see helper below
	if err != nil {
		return nil, err
	}
	for i, v := range ids {
		ids[i] = strings.TrimPrefix(v, memberPrefix())
	}
	return ids, nil
}

func (m *cosmosMembership) Deregister(ctx context.Context) error {
	return m.c.deleteItem(ctx, memberDocID(m.self)) // mirror existing delete helper
}

func (m *cosmosMembership) Close() error { return nil }
```

> The Cosmos client wrappers vary; implement `upsertItem`, `deleteItem`, and a `queryMemberIDs(ctx, prefix, nowMs)` helper directly against the existing `containerAPI` interface in `cosmosdb.go` (it already has create/replace/read/delete + query plumbing for locks). The query is `SELECT c.id FROM c WHERE STARTSWITH(c.id, @prefix) AND c.lease_expiry > @now` with the container's partition-key cross-partition option. Reuse `nowFunc`/clock and `memberTTL`/`leaseDuration` fields (add if absent), matching the names already in the file.

- [ ] **Step 4: Write the integration test**

Create `internal/coord/cosmosdb/membership_integration_test.go`:

```go
//go:build integration

package cosmosdb

import (
	"context"
	"testing"
	"time"
)

func TestCosmosMembershipLiveSet(t *testing.T) {
	opts := startCosmosEmulator(t) // existing helper from cosmosdb_integration_test.go (returns Options w/ conn string + client opts)
	opts.MemberTTL = 2 * time.Second
	ctx := context.Background()

	c, err := New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	m1, _ := c.Membership("inst-1")
	m2, _ := c.Membership("inst-2")
	if _, err := m1.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := m2.Heartbeat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 members, got %v", got)
	}
	if err := m1.Deregister(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ = m2.Heartbeat(ctx)
	if len(got) != 1 {
		t.Fatalf("expected 1 member after deregister, got %v", got)
	}
}
```

> Reuse the emulator bootstrap (container create + self-signed TLS client options) from the existing `cosmosdb_integration_test.go`; adapt the helper name to whatever exists there.

- [ ] **Step 5: Run unit; run integration if Docker available**

Run: `go test ./internal/coord/cosmosdb/ -run TestMemberIDFormat`
Expected: PASS.
Run (Docker): `go test -tags=integration ./internal/coord/cosmosdb/ -run TestCosmosMembership`
Expected: PASS (or note Docker unavailable).

- [ ] **Step 6: Commit**

```bash
git add internal/coord/cosmosdb/membership.go internal/coord/cosmosdb/membership_unit_test.go internal/coord/cosmosdb/membership_integration_test.go internal/coord/cosmosdb/cosmosdb.go cmd/rss2msg/wire.go
git status
git commit -m "feat(coord/cosmosdb): member-doc heartbeat + query-based live set"
```

---

### Task 10: Wire assignment into the serve daemon

**Files:**
- Modify: `cmd/rss2msg/wire.go` (Options `MemberTTL` plumbing; the `redisCoordOptions` mapping; expose member-TTL on each coordinator Options)
- Modify: `cmd/rss2msg/serve.go` (build membership, wrap aggregator, run heartbeat loop, close on shutdown)
- Test: `cmd/rss2msg/serve_assignment_test.go` (unit-level: disabled path returns the raw aggregator; enabled path errors clearly when the driver lacks `MembershipProvider`)

**Interfaces:**
- Consumes: `coord.MembershipProvider`, `coord.NewMemberID`, `scheduler.NewOwnerProvider`, `config.CoordinationAssignmentConfig`.
- Produces: helper `func maybeWrapProvider(cfg config.Config, cd coord.Coordinator, inner scheduler.FeedProvider, self string, instr telemetry.Instruments) (scheduler.FeedProvider, *scheduler.OwnerProvider, error)`.

- [ ] **Step 1: Write the failing test**

Create `cmd/rss2msg/serve_assignment_test.go`:

```go
package main

import (
	"testing"

	"github.com/iambod/rss2msg/internal/config"
	coordmem "github.com/iambod/rss2msg/internal/coord/memory"
	"github.com/iambod/rss2msg/internal/scheduler"
	"github.com/iambod/rss2msg/internal/telemetry"
)

type nopProvider struct{}

func (nopProvider) Desired(ctx contextEcho) ([]config.FeedConfig, error) { return nil, nil }

func TestMaybeWrapProviderDisabledReturnsInner(t *testing.T) {
	cfg := config.Default()
	cfg.Coordination.Assignment.Enabled = false
	var inner scheduler.FeedProvider = stubProvider{}
	got, op, err := maybeWrapProvider(cfg, coordmem.New(), inner, "self", telemetry.Instruments{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if op != nil {
		t.Fatal("expected no OwnerProvider when assignment disabled")
	}
	if got != inner {
		t.Fatal("expected the raw inner provider when assignment disabled")
	}
}

func TestMaybeWrapProviderEnabledWrapsMemory(t *testing.T) {
	cfg := config.Default()
	cfg.Coordination.Assignment.Enabled = true
	got, op, err := maybeWrapProvider(cfg, coordmem.New(), stubProvider{}, "self", telemetry.Instruments{})
	if err != nil {
		t.Fatalf("memory implements MembershipProvider, expected no err: %v", err)
	}
	if op == nil || got == nil {
		t.Fatal("expected a wrapped provider when enabled")
	}
}
```

> Replace `stubProvider`/`contextEcho` with a minimal real `scheduler.FeedProvider` implementation in the test file (a struct with `Desired(context.Context)` and `Changes()`); the snippet above just shows intent. Keep it compiling against the real interface.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/rss2msg/ -run TestMaybeWrapProvider`
Expected: FAIL — `maybeWrapProvider` undefined.

- [ ] **Step 3: Implement `maybeWrapProvider`**

Add to `cmd/rss2msg/serve.go` (or a new `cmd/rss2msg/assignment.go`):

```go
// maybeWrapProvider wraps inner with an OwnerProvider when assignment is enabled,
// reusing the coordinator's client via coord.MembershipProvider. Returns the
// (possibly unchanged) provider plus the OwnerProvider handle (nil when disabled)
// so the caller can Run() its heartbeat loop and Close() it on shutdown.
func maybeWrapProvider(cfg config.Config, cd coord.Coordinator, inner scheduler.FeedProvider, self string, instr telemetry.Instruments) (scheduler.FeedProvider, *scheduler.OwnerProvider, error) {
	a := cfg.Coordination.Assignment
	if !a.Enabled {
		return inner, nil, nil
	}
	mp, ok := cd.(coord.MembershipProvider)
	if !ok {
		return nil, nil, fmt.Errorf("coordination.assignment.enabled but driver %q does not support membership", cfg.Coordination.Driver)
	}
	m, err := mp.Membership(self)
	if err != nil {
		return nil, nil, fmt.Errorf("build membership: %w", err)
	}
	onRebalance := func(members, owned int) {
		ctx := context.Background()
		instr.MembershipSize.Record(ctx, int64(members))
		instr.AssignedFeeds.Record(ctx, int64(owned))
		instr.RebalanceEvents.Add(ctx, 1)
	}
	op := scheduler.NewOwnerProvider(inner, m, self, a.HeartbeatInterval, onRebalance)
	return op, op, nil
}
```

> Guard the `instr.*` calls if the instruments can be nil in tests (they're nil in the `telemetry.Instruments{}` zero value). Either pass real noop instruments in the test or nil-check each meter inside `onRebalance`. Simplest: nil-check `instr.MembershipSize != nil` before recording.

- [ ] **Step 4: Integrate into the serve command**

In `serve.go`'s `RunE`, after `agg := feedsource.NewAggregator(...)` and before `scheduler.ServeDynamic`:

```go
		self := coord.NewMemberID()
		provider, owner, err := maybeWrapProvider(cfg, w.coord, agg, self, w.instr)
		if err != nil {
			return err
		}
		if owner != nil {
			go owner.Run(ctx)
			defer func() {
				cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = owner.Close(cctx) // deregister after ServeDynamic drains
			}()
		}
```

Then change the `ServeDynamic` call to use `provider` instead of `agg`:

```go
		return scheduler.ServeDynamic(ctx, scheduler.DynamicConfig{
			Provider: provider,
			// ...rest unchanged...
		})
```

> Ordering matters: `ServeDynamic` returns only after its tickers drain (ctx cancelled), and the deferred `owner.Close` runs after that, so the instance drains its own feeds *then* deregisters. Confirm `coord`, `time`, `context`, `fmt` are imported in `serve.go`.

- [ ] **Step 5: Plumb MemberTTL into coordinator Options**

In `wire.go`, set `MemberTTL: cc.Assignment.MemberTTL` on each coordinator's Options (redis via `redisCoordOptions`, plus postgres/dynamodb/cosmosdb in `openCoordinator`). Add the `MemberTTL time.Duration` field to each backend's `Options` struct (done in Tasks 6–9). Memory needs nothing.

- [ ] **Step 6: Run tests + build**

Run: `go test ./cmd/rss2msg/ -run TestMaybeWrapProvider && task build`
Expected: PASS and a successful build.

- [ ] **Step 7: Commit**

```bash
git add cmd/rss2msg/serve.go cmd/rss2msg/assignment.go cmd/rss2msg/serve_assignment_test.go cmd/rss2msg/wire.go
git status
git commit -m "feat(serve): wire membership/assignment provider into the daemon"
```

---

### Task 11: Documentation

**Files:**
- Create: `docs/explanation/coordination-assignment.md`
- Create: `docs/reference/coordination-assignment.md`
- Modify: the coordinator hub / scaling page that lists coordinator topics (find it: `grep -rl "coordination" docs/`), add a link to both new pages and a `## Related` backlink.
- Run: `bash scripts/check-doc-links.sh`

**Interfaces:** none (docs only).

- [ ] **Step 1: Write the explanation page**

Create `docs/explanation/coordination-assignment.md` with the standard frontmatter (`title`, `type: explanation`, `tags`, `summary`, `updated: 2026-06-21`) and content drawn **verbatim** from the design spec sections: the problem (N×M amplification), the three pieces (HRW, membership, owner-filtering provider), the always-on guard and its scaling trade-off, the rebalance walkthroughs (join/leave/feed-set change), N>M standbys, parameter relationships, and poll-duration-vs-lock-TTL. Ground every claim in the code/config it describes. End with a `## Related` footer linking the reference page and the coordinator hub.

- [ ] **Step 2: Write the reference page**

Create `docs/reference/coordination-assignment.md` (frontmatter `type: reference`) documenting each `coordination.assignment.*` key: name, type, default, meaning, and constraints (`member_ttl > heartbeat_interval`; `strategy: rendezvous` only; requires a distributed driver). Include the example YAML block. `## Related` footer to the explanation page.

- [ ] **Step 3: Link from the hub and run the checker**

Add links to both pages from the coordinator hub page found via grep. Then:

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`.

- [ ] **Step 4: Commit**

```bash
git add docs/explanation/coordination-assignment.md docs/reference/coordination-assignment.md docs/<hub-page-you-edited>.md
git status
git commit -m "docs: explain and reference the coordination assignment model"
```

---

### Task 12: Final gates + issue update + PR

**Files:** none new (verification + housekeeping).

- [ ] **Step 1: Run the full non-Docker gate**

Run: `task test && task vet && task lint`
Expected: all PASS. Fix any lint findings (CI blocks on them).

- [ ] **Step 2: Run integration tests (Docker)**

Run: `task test-integration`
Expected: PASS for the redis/postgres/dynamodb/cosmosdb membership tests. If Docker is unavailable in this environment, state that explicitly in the PR body and rely on CI's sharded integration matrix.

- [ ] **Step 3: Run the benchmark gate (hot paths must not regress)**

Run: `scripts/bench-compare.sh main`
Expected: no significant regression (assignment touches scheduling, not the change-detection/parse/fan-out hot paths, so this should be `~`).

- [ ] **Step 4: Update the issue body**

Edit issue #183's body (via `gh api` REST — `gh issue edit` fails on this repo's classic-projects link) to record the final decisions: always-on guard (criterion #2 relaxed to "N×M → M per cycle"), deregister-on-shutdown, member reuse of the coordinator client, and the rendezvous-only strategy. Keep the body the single source of truth.

- [ ] **Step 5: Push and open the PR**

```bash
git push -u origin feat/coord-assignment-model
gh pr create --title "feat(coord): membership + rendezvous assignment model (#183)" \
  --body "Implements the partition/assignment model from #183. See docs/superpowers/specs/2026-06-21-coord-assignment-model-design.md.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

- [ ] **Step 6: Clean up after merge** (later)

After the PR merges: delete the local + remote branch and `git worktree remove .worktrees/assignment-model`.

---

## Self-Review notes

- **Spec coverage:** assignment fn (T1), membership interface + memory (T2), config (T3), metrics (T4), owner provider (T5), redis/pg/dynamo/cosmos backends (T6–T9), wiring + deregister-on-shutdown (T10), docs (T11), gates/PR (T12). Acceptance criteria #1/#3/#4/#5/#6/#7/#8 covered; #2 relaxed per the design decision (documented in T11/T12).
- **Guard / pipeline.go:** intentionally unmodified (always-on guard) — no task touches it; the existing `not_owner`/`coord_error` skip path stays.
- **Type consistency:** `Membership`/`MembershipProvider`/`Owned`/`Owner`/`NewOwnerProvider`/`NewMemberID`/`maybeWrapProvider` signatures are consistent across tasks. `MemberTTL` is added to each backend's `Options` (T6–T9) and consumed in `wire.go` (T10).
- **Known follow-ups flagged inline:** exact field names (`client`/`pool`/`table`/`nowFunc`/AttributeValue alias) and the OTel gauge type must be confirmed against each file at implementation time — every such spot has a `>` note telling the implementer to mirror the sibling coordinator file.
