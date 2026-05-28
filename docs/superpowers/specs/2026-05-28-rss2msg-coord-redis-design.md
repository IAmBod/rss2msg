# rss2msg — Redis Coordinator Design

Status: approved (brainstorming)
Date: 2026-05-28
Builds on: `2026-05-28-rss2msg-v1.5-design.md`

## Purpose

Add a Redis-backed implementation of the existing `coord.Coordinator`
interface alongside the v1.5 `noop` and `postgres` backends. The pipeline
and wiring are already coord-driver-agnostic; this milestone is additive.

## Lock protocol

Classic Redis lease with explicit owner tokens, plus a background renewal
goroutine that keeps the lease alive across long polls.

### Key derivation

```
key = "rss2msg:coord:" + hex(sha256(feed_url))
```

The same hash domain that the Postgres backend uses, rendered as hex so
Redis-side debugging (`KEYS rss2msg:coord:*`) stays human-readable.

### Acquire

```
SET <key> <token> NX EX <lock_ttl_seconds>
```

- `token` is a freshly generated UUIDv4 (one per `TryAcquire` success).
- Reply `OK` → acquired; start the renewal goroutine and return a
  `ReleaseFunc` that closes over `(key, token, renewalCancel)`.
- Reply `nil` → another instance holds the lease; return `(nil, false, nil)`.
- Any other error → `(nil, false, err)`.

### Renewal

A goroutine started on successful acquire runs every `renewal_interval`:

```lua
if redis.call("GET", KEYS[1]) == ARGV[1]
  then return redis.call("PEXPIRE", KEYS[1], ARGV[2])
  else return 0
end
```

`ARGV[2]` is `lock_ttl` in milliseconds. The goroutine exits when any of:

- Its `cancel` channel is closed (release path or coordinator close).
- The CAS returns 0 (we no longer own the lease — TTL expired and another
  instance grabbed it). The goroutine logs at `warn` with the lost feed URL
  and exits. The `ReleaseFunc` returned by acquire becomes a no-op when
  invoked subsequently (the CAS in release will also return 0 — safe).
- Redis returns a non-retriable error. Same log + exit behaviour.

### Release

The `ReleaseFunc` returned by acquire:

1. Cancels the renewal goroutine and waits for it to exit (`sync.WaitGroup`
   tied to the renewal loop).
2. Runs a Lua CAS delete on a fresh `context.WithTimeout(Background, 5s)` —
   reusing the same canceled-ctx-survives convention as the Postgres
   coordinator:

   ```lua
   if redis.call("GET", KEYS[1]) == ARGV[1]
     then return redis.call("DEL", KEYS[1])
     else return 0
   end
   ```

   A return of 0 (because TTL already expired and someone else holds the
   key, or because the renewal goroutine already noted the loss) is not an
   error from the caller's perspective.

### Close

`Coordinator.Close()`:

1. Sets a closing flag and signals every tracked renewal goroutine to
   cancel.
2. For every currently-held lease, runs the CAS delete script
   best-effort on a 5s background timeout.
3. Closes the underlying `*redis.Client`.

### Concurrency invariants

- Held leases are tracked in a `held map[string]*lease` keyed by the lock
  key, guarded by a mutex. Same shape as the Postgres backend's `held`.
- Acquire records the lease entry under the mutex AFTER the SET succeeds
  AND a closing-flag check. If acquire wins the SET but loses the
  closing-flag race, it runs the CAS delete and returns `(nil, false, nil)`.
- Release removes the held entry under the mutex before running the delete
  SQL/Lua, so concurrent Close iterates a snapshot that excludes
  in-flight releases.

## Config

```yaml
coordination:
  driver: redis
  redis:
    url: redis://localhost:6379/0      # required
    lock_ttl: 30s                      # optional, default 30s
    renewal_interval: 10s              # optional, default = lock_ttl / 3
```

The `url` field accepts the standard `redis://[user[:password]@]host:port[/db]`
and `rediss://` (TLS) forms, parsed via `redis.ParseURL`. Username/password
substitution via `${ENV}` (loader hook from v1) covers secret handling.

### Typed structs

In `internal/config/config.go`:

```go
type CoordinationConfig struct {
    Driver   string                  `mapstructure:"driver"`
    Postgres CoordinationPGConfig    `mapstructure:"postgres"`
    Redis    CoordinationRedisConfig `mapstructure:"redis"`
}

type CoordinationRedisConfig struct {
    URL             string        `mapstructure:"url"`
    LockTTL         time.Duration `mapstructure:"lock_ttl"`
    RenewalInterval time.Duration `mapstructure:"renewal_interval"`
}
```

### Validation rules (additions)

- `coordination.driver` allowlist gains `"redis"`.
- `coordination.driver=redis` requires:
  - `redis.url` non-empty (post env-substitution).
  - `redis.url` parseable by `redis.ParseURL` (validated at startup; surfaces a
    clear error before connecting).
  - `redis.lock_ttl` either zero (default 30s applies) or ≥ 1s.
  - `redis.renewal_interval` either zero (derive `lock_ttl / 3`) or
    `< lock_ttl` (a renewal that fires at or after expiry is meaningless).

All other v1 / v1.5 validation rules unchanged.

## Wiring

In `cmd/rss2msg/wire.go`:

- `knownCoordinationDrivers` map in `internal/config/validate.go` gains
  `"redis"`.
- `openCoordinator` gets a `case "redis"` that constructs the backend with a
  resolved options struct:

  ```go
  case "redis":
      return coordredis.New(ctx, coordredis.Options{
          URL:             cc.Redis.URL,
          LockTTL:         cc.Redis.LockTTL,
          RenewalInterval: cc.Redis.RenewalInterval,
      })
  ```

`coordredis.New` applies the defaults (30s TTL, `LockTTL/3` renewal) and
returns the `*Coordinator`.

## Package shape

```
internal/coord/redis/
  redis.go        # Coordinator + Options + New + scripts
  redis_test.go   # //go:build integration ; uses testcontainers redis module
```

### `Options`

```go
type Options struct {
    URL             string        // required
    LockTTL         time.Duration // 0 -> 30s
    RenewalInterval time.Duration // 0 -> LockTTL/3
}
```

### Public surface

- `func New(ctx context.Context, opts Options) (*Coordinator, error)`
- `func (*Coordinator) TryAcquire(ctx context.Context, feedURL string) (coord.ReleaseFunc, bool, error)`
- `func (*Coordinator) Close() error`

Implementation detail: the renewal and release Lua scripts are kept as
`redis.NewScript` instances (`*redis.Script`) on the Coordinator struct so
the client uses `EVALSHA` after the first run.

## Telemetry

No new metrics. The existing `feed.poll.skipped{reason}` counter (from v1.5)
covers both `not_owner` (Redis SET NX returned nil) and `coord_error`
(Redis dial/Lua failure surfaced by `TryAcquire`).

Renewal-goroutine lock-loss events are logged at `warn` with
`coord_driver=redis`, `feed_url`, and an `event=lock_lost` field, so
operators can grep for them. The pipeline does NOT see lock-loss directly;
it sees it as a release that's effectively a no-op (next poll will
re-acquire).

## Testing

### Unit tests

- `internal/coord/redis_unit_test.go` (no Redis dep): key derivation
  determinism for a given URL; tokens differ between calls.

### Integration tests (`//go:build integration`)

Use `github.com/testcontainers/testcontainers-go/modules/redis` to boot a
`redis:7-alpine` container. Shared per package via a small `setup` helper.

1. **`TestTwoCoordinatorsRaceForSameFeed`** — A and B race; exactly one
   acquires.
2. **`TestReleaseAllowsAcquireAgain`** — A acquires → releases → A re-acquires.
3. **`TestHeldLeaseSurvivesPastInitialTTL`** — A acquires with a short
   `lock_ttl` (e.g. 1s) and `renewal_interval` (e.g. 300ms); wait 2s while
   holding; assert B cannot acquire; release; assert B can.
4. **`TestLockLostExternallyMakesReleaseNoOp`** — A acquires; the test
   issues a direct `DEL` on the lock key through the go-redis client
   (simulating an operator intervention or eviction); B's coordinator
   `TryAcquire` then succeeds with B's own token; the test sleeps two
   renewal intervals so A's renewal goroutine has had time to detect the
   CAS-zero result and exit; A's release returns nil (no error) and does
   not delete B's lease (asserted by B's subsequent successful release).
5. **`TestCoordinatorCloseReleasesHeldLeases`** — A acquires; `A.Close()`;
   B can acquire.
6. **`TestCanceledReleaseCtxStillFreesLease`** — release ignores its
   parameter ctx (matches the v1.5 Postgres backend convention); passing
   a pre-canceled ctx still frees the lease.

### Dependencies (new)

- `github.com/redis/go-redis/v9`
- `github.com/google/uuid` (or `crypto/rand`-based 128-bit token if we want
  to avoid adding `uuid`; choose `uuid` for readability)
- `github.com/testcontainers/testcontainers-go/modules/redis`

## Documentation

`README.md` — add a bullet under "Running multiple instances":

> The coordinator backend can also be set to `redis` for shared-Redis
> deployments. See `config.example.yaml` for the URL and TTL knobs.

`config.example.yaml` — add a commented-out `redis:` block beside the
existing `postgres:` block under `coordination:` so users can swap drivers
by uncommenting + editing.

## Out of scope

- Redlock (multi-Redis-node consensus).
- Sentinel / Cluster topologies beyond what `redis.ParseURL` natively
  supports (single instance, single Sentinel-fronted master).
- Configurable renewal jitter.
- Migration of existing lease state between coordinator backends.
