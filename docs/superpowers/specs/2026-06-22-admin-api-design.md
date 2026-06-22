# Admin API — design

Issue: [#185](https://github.com/IAmBod/rss2msg/issues/185)

## Problem

A running `serve` daemon is largely opaque at runtime. Operators can see the
Kubernetes health probes (`/healthz`, `/readyz`, `/startupz`) and the Prometheus
`/metrics` endpoint, but there is no way to ask a live instance *what it is doing*:
which feeds it knows about, when each was last polled, which instance owns a feed in
a clustered deployment, or what drivers/effective settings it booted with. There is
also no on-demand way to nudge it — re-read feed sources, force-poll a single feed,
or run a state prune — short of sending signals or waiting for the next tick.

## Goal

An **opt-in**, authenticated HTTP **Admin API** that lets an operator introspect a
running instance and trigger a small set of *safe* maintenance actions. JSON over
HTTP, versioned under `/v1`, on its own listener, **disabled by default**, and
**requiring auth** when enabled. It does **not** become a control plane: feeds and
config stay YAML/config-first; the API never mutates the desired feed set or the
config at runtime.

## Scope decisions

Settled during brainstorming:

- **Surface:** introspection (read) endpoints **plus** safe control actions.
- **Listener:** its own `admin:` config block + dedicated listener (not mounted on
  the health probe port, which is typically exposed unauthenticated).
- **Auth:** reuse the existing feed-sink auth model (bearer / basic / API-key),
  **required by default** when the API is enabled.
- **Actions for v1:** reconcile feeds, prune state, **and** per-feed poll-now.
  *(Drain toggle was considered and dropped — marginal for a non-serving poller and
  currently irreversible.)*
- **Auth packaging:** extract the feed-sink auth core into a shared
  `internal/httpauth` package; the feed sink delegates to it, the admin server reuses
  it. One source of truth.
- **Delivery:** a single branch/PR (`feat/admin-api`).

## Config

New top-level section, sibling to `health` / `heartbeat`:

```yaml
admin:
  enabled: false            # default off
  listen: ":8090"           # dedicated listener; required when enabled
  auth:                     # same shape as a feed-sink surface auth block
    disabled: false         # must be set true to run the API without auth (opt-out)
    bearer_tokens:
      - name: ops
        token: "${ADMIN_TOKEN}"
    basic_users:
      - username: admin
        password: "${ADMIN_PASSWORD}"
    api_keys:
      - name: ci
        key: "${ADMIN_API_KEY}"
    api_key_header: X-API-Key   # default when omitted
```

- `internal/config/config.go`: add `Admin AdminConfig` to the top-level `Config`.
  Define `AdminConfig{ Enabled bool; Listen string; Auth FeedAuthConfig }` with
  `mapstructure` tags, **reusing the existing `FeedAuthConfig` type** (the same type
  the feed sink uses) so the auth shape and its env/`${VAR}` handling are identical.
- `internal/config/load.go`: register Viper defaults `admin.enabled` (`false`) and
  `admin.listen` (`":8090"`) in `applyDefaults()`.
- `internal/config/validate.go`: `validateAdmin()` — when `enabled`:
  - `listen` is required (non-empty);
  - **auth is required**: at least one credential across `bearer_tokens` /
    `basic_users` / `api_keys` must be present **unless** `auth.disabled: true` is set
    explicitly (secure-by-default — you must opt out of auth on purpose);
  - reuse the feed-sink auth validation for credential well-formedness (non-empty
    names/secrets, etc.);
  - **warn** (not fail) if `admin.listen` equals `health.listen` or
    `telemetry.prometheus.listen` — same collision-warning pattern health already
    uses, since two servers cannot bind the same address.

Env overrides follow the existing scheme; `admin.enabled` / `admin.listen` have
registered defaults so `RSS2MSG_ADMIN__ENABLED` / `RSS2MSG_ADMIN__LISTEN` bind.
(Nested auth lists, like the feed sink's, must be set in the file — slices don't bind
from env.)

## Shared auth package (`internal/httpauth`)

Extract the credential-checking core currently in `internal/sink/feed/auth.go`
(bearer / basic / API-key, constant-time compare via `subtle.ConstantTimeCompare`,
low-cardinality failure reasons `no_credentials` / `bad_token`) into a new
`internal/httpauth` package:

```go
package httpauth

type BasicCred   struct{ Username, Password string }
type NamedSecret struct{ Name, Secret string }

type Auth struct {
    BasicUsers   []BasicCred
    BearerTokens []NamedSecret
    APIKeys      []NamedSecret
    APIKeyHeader string // empty => X-API-Key
}

// Authenticate reports the matched credential name and whether any method passed.
func (a *Auth) Authenticate(r *http.Request) (name string, ok bool)
// FailReason returns a low-cardinality reason for metrics ("no_credentials" | "bad_token").
func (a *Auth) FailReason(r *http.Request) string
// WriteChallenge advertises Basic/Bearer per configured methods (sets WWW-Authenticate).
func (a *Auth) WriteChallenge(w http.ResponseWriter)
// Empty reports whether no credentials are configured (used by callers to decide "public").
func (a *Auth) Empty() bool
```

- `internal/sink/feed/auth.go` keeps its **surface-specific** wrapper (per-surface
  config resolution + the OTel auth-success/failure metrics with the `surface`
  attribute) but **delegates** the actual credential check to `httpauth.Auth`. Its
  `SurfaceAuth` becomes a thin adapter over `httpauth.Auth`.
- Before refactoring, add **characterization tests** that pin the current feed-sink
  auth behavior (each method accept/reject, constant-time path, challenge header,
  fail reasons) so the extraction is provably behavior-preserving.
- Provide a single converter `FeedAuthConfig -> httpauth.Auth` (reused by both the
  feed-sink wiring and the admin wiring) so config→auth mapping lives in one place.

## Admin server (`internal/admin`)

A small, isolated unit modeled on `internal/health`:

```go
package admin

// Deps are the read/act capabilities the API needs, injected as narrow
// interfaces so the package is decoupled and unit-testable with fakes.
type Deps struct {
    Build      BuildInfo                 // version/commit/date + instance id
    StartedAt  time.Time                 // for uptime
    Feeds      FeedLister                // Desired() []config.FeedConfig
    State      StateInspector            // GetFeedMeta + Prune* + Ping
    Members    MembershipInspector       // optional (nil for non-clustered): self + member set
    Health     HealthInspector           // optional: dependency pings, started/draining flags
    Reconcile  func()                    // Aggregator.Trigger()
    PollNow    func(feedURL string) bool // best-effort poke; false if not poke-able
    Self       string                    // instance id used for ownership computation
    Assignment bool                      // assignment mode on/off (gates "owned" field)
}

func New(cfg config.AdminConfig, auth *httpauth.Auth, deps Deps, log zerolog.Logger) *Server
func (s *Server) Start() error
func (s *Server) Shutdown(ctx context.Context) error
```

- `net/http.ServeMux`; every route wrapped by an auth middleware that runs
  `auth.Authenticate`, records the same OTel success/failure metrics with
  `surface="admin"`, and on failure writes `401` + challenge. (Auth is mandatory at
  the config layer unless `auth.disabled`, in which case the middleware is a
  pass-through.)
- All responses JSON. Errors: `{"error": "..."}` with the appropriate status code.
- The narrow interfaces (`FeedLister`, `StateInspector`, `MembershipInspector`,
  `HealthInspector`) are satisfied structurally by the existing
  `feedsource.Aggregator`, `state.Store`, `coord.MembershipProvider`, and
  `health.Server` — no changes to those types beyond what poll-now needs.

### Endpoints (all under `/v1`, all behind auth)

**Read**

| Method · Route | Returns |
| --- | --- |
| `GET /v1/status` | `instance_id`, `version`/`commit`/`date`, `uptime_seconds`, `started`, `draining`, `coordinator_driver`, `state_driver`, `assignment_enabled`, `sink_count`, `feed_count`, `member_count`. Non-secret effective settings only. |
| `GET /v1/feeds` | array of `{url, interval_seconds, owned, last_polled, etag, last_modified}`. Feed list from `Feeds.Desired()`; per-feed metadata joined from `State.GetFeedMeta` (absent meta → null fields). `owned` is computed via `assign.Owned(self, feeds, members)` when assignment is on, else always `true`. |
| `GET /v1/feeds/{id}` | one feed (see below for `{id}`); `404` if not in the desired set. |
| `GET /v1/members` | `{self, members: [...], ownership: {feedURL: member}}`. Members from `MembershipInspector` when available; single-self response for memory/non-clustered backends. `ownership` computed with `assign.Owned`. |
| `GET /v1/health` | machine-readable dependency pings (state, coordinator) + `started`/`draining` — a JSON sibling of `/readyz`. Reuses the same `health.Check` functions. |

**Safe actions**

| Method · Route | Effect |
| --- | --- |
| `POST /v1/feeds/reconcile` | calls `Deps.Reconcile()` (== `Aggregator.Trigger()`, same as SIGHUP). Async; returns `202 {"status":"reconcile triggered"}`. |
| `POST /v1/feeds/{id}/poll` | validates `{id}` is in the desired set (`404` if not); calls `Deps.PollNow(url)`. Returns `202 {"status":"poll triggered"}`. Best-effort: if the feed isn't currently running on this instance (e.g. owned by another in assignment mode), the poke is dropped — body notes `"running": false`. |
| `POST /v1/state/prune` | body `{"items_older_than":"720h","feed_meta_older_than":"720h"}` (each optional; default to the configured `state.item_ttl`). Calls `State.PruneItemsBefore` / `PruneFeedMetaBefore` with `now - duration`. Returns `200 {"items_removed":N,"feed_meta_removed":M}`. |

**`{id}` encoding:** feed URLs are the natural key but contain `/` and `:`. Accept the
**percent-encoded full feed URL** as the path segment (operators copy a value from
`GET /v1/feeds`); the handler URL-decodes and matches exactly against the desired
set. (Avoids inventing a separate ID scheme; matches how feeds are keyed everywhere
else.)

## Per-feed poll-now: scheduler plumbing

`POST /v1/feeds/{id}/poll` needs the dynamic scheduler to poll one feed off-cycle.
Minimal, opt-in additions to `internal/scheduler/dynamic.go` / `serve.go`:

- `DynamicConfig` gains `PollNow <-chan string` (optional; `nil` ⇒ feature off).
- `runningFeed` gains an unexported buffered `poke chan struct{}` (cap 1, so repeated
  pokes coalesce instead of queueing).
- `startFeed` creates the `poke` channel and passes its receive end into
  `runFeedLoop`, which adds `case <-poke: runTick(...)` to its `select` alongside the
  ticker and `ctx.Done()`.
- `ServeDynamic`'s main loop adds `case url := <-cfg.PollNow:` → look up
  `running[url]`; if present, non-blocking send to its `poke` (`select { case rf.poke
  <- struct{}{}: default: }`); if absent, drop (feed not running here).
- `cmd/rss2msg/serve.go` creates a `pollNow := make(chan string, 16)`, passes the
  receive end to `ServeDynamic` and a send-closure (`func(url string) bool`) to the
  admin `Deps.PollNow`. The closure does a non-blocking send and reports whether it
  was accepted.

This is additive and behavior-preserving when `PollNow` is nil (existing scheduler
tests unaffected). The poke triggers exactly the same `runTick` path as a normal tick
(same metrics/`OnPollComplete` callbacks), so there is no second code path for polls.

## Wiring (`cmd/rss2msg/serve.go` / `wire.go`)

- After the health server starts and before/around `scheduler.ServeDynamic`,
  construct the admin server when `cfg.Admin.Enabled`:
  - build `httpauth.Auth` from `cfg.Admin.Auth` via the shared converter;
  - assemble `admin.Deps` from the already-wired aggregator (`Feeds`, `Reconcile`),
    state store (`State`), coordinator membership (`Members`, when it implements
    `MembershipProvider`), health checks (`Health`), the poll-now closure, instance
    id (`cfg.Telemetry.InstanceID`), and build vars from `cmd/rss2msg/version.go`
    (plumb the `version`/`commit`/`date` package vars into `Deps.Build`);
  - `Start()` it in a goroutine and `Shutdown(ctx)` it in the same drain path as the
    health server.
- Build vars (`version`, `commit`, `date`) currently live unexported in package
  `main`; pass them into the serve command (e.g. via the existing `opts`/wire path)
  so the admin server can report them. No new global state.

## Docs / examples

- `docs/reference/admin-api.md` — endpoint reference (routes, request/response JSON,
  auth, status codes), standard frontmatter + `## Related` footer.
- `docs/how-to/operate-the-admin-api.md` — enable it, configure auth, curl examples,
  security guidance (bind privately / put behind auth; do not expose with
  `auth.disabled`).
- `docs/reference/configuration.md` — document the `admin:` block.
- Add the `admin:` block to **both** `internal/config/example.yaml` and
  `examples/config.example.yaml` (must stay byte-identical — drift-guard test).
- Cross-link from `docs/index.md` / relevant how-to hubs; run
  `bash scripts/check-doc-links.sh` (must print `OK: all relative doc links
  resolve`).

## Testing (TDD)

- `internal/config`: defaults (`admin.enabled=false`, `admin.listen=":8090"`);
  validation (enabled w/o listen → error; enabled w/o any credential and not
  `auth.disabled` → error; enabled + `auth.disabled` → ok; enabled + credentials →
  ok; listen collision with health/prometheus → warning).
- `internal/httpauth`: unit tests for each method (accept/reject), constant-time
  path, challenge header, fail reasons, `Empty()`.
- `internal/sink/feed`: characterization tests added **before** the extraction;
  green both before and after, proving no behavior change.
- `internal/admin`: `httptest` handler tests per endpoint — authed vs unauthed
  (`401` + challenge), `/v1/status` shape, `/v1/feeds` join with fake state
  (present/absent meta), `/v1/feeds/{id}` `404`, `/v1/members` self-only vs clustered
  (fake membership) + ownership map, `/v1/health` pass/fail, reconcile invokes the
  closure (`202`), poll-now `404` for unknown feed and `202` + `running` flag via a
  fake `PollNow`, prune calls `Prune*` with the right cutoff and returns counts,
  default-duration fallback to `item_ttl`.
- `internal/scheduler`: `PollNow` triggers an extra `runTick` for the targeted feed;
  unknown/not-running URL is a no-op; coalescing (rapid pokes don't queue); nil
  `PollNow` leaves existing behavior identical.
- `cmd/rss2msg`: a serve-level test that, with `admin.enabled`, the listener comes up
  and a `GET /v1/status` with valid credentials returns `200` (mirrors the existing
  health serve test).
- Run `task test`, `task vet`, `task lint`. Run `task test-integration` (the change
  touches the state store via prune and the scheduler) — or state explicitly if
  skipped for lack of Docker.

## Out of scope

- Runtime mutation of the feed set or config (add/remove/edit feeds, edit config,
  rotate credentials) — stays config-first / YAML-driven.
- Full effective-config dump endpoint — deferred; would require robust secret
  redaction. `/v1/status` exposes only curated non-secret settings instead.
- Drain toggle via API.
- A web UI — JSON API only.
- mTLS for the admin listener (the chosen auth is bearer/basic/api-key; mTLS can be
  layered later by a reverse proxy if needed).

## Acceptance criteria

- Default config starts no admin listener and changes nothing.
- With `admin.enabled: true` + a configured credential, the listener binds on
  `admin.listen`; unauthenticated requests get `401` with a challenge; authenticated
  requests reach the endpoints.
- `enabled: true` with no credentials and `auth.disabled` unset fails validation.
- `GET /v1/status`, `/v1/feeds`, `/v1/feeds/{id}`, `/v1/members`, `/v1/health` return
  correct JSON for both single-instance and clustered (assignment) deployments.
- `POST /v1/feeds/reconcile` re-reads feed sources; `POST /v1/feeds/{id}/poll`
  forces an off-cycle poll of a running feed and `404`s an unknown one;
  `POST /v1/state/prune` removes aged rows and reports counts.
- Feed-sink auth behavior is unchanged after the `internal/httpauth` extraction
  (characterization tests green before and after).
- `task test`, `task vet`, `task lint` pass; the two example YAML files remain
  byte-identical; `check-doc-links.sh` passes.
