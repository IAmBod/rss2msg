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
- **Auth:** reuse the existing feed-sink auth model (bearer / basic / API-key) for
  application auth, controlled by `admin.auth.enabled` which is **`true` by default**
  (you must deliberately turn it off). Additionally support **mTLS** (client-cert auth)
  as an independent transport-layer option via `admin.tls`. Validation refuses to start
  an admin API that is neither application-authenticated nor mTLS-protected unless the
  operator explicitly opts into an open API.
- **Actions for v1:** reconcile feeds, prune state, **and** per-feed poll-now.
  *(Drain toggle was considered and dropped — marginal for a non-serving poller and
  currently irreversible.)*
- **Auth packaging:** extract the feed-sink auth core into a shared
  `internal/httpauth` package; the feed sink delegates to it, the admin server reuses
  it. One source of truth.
- **Dashboard forward-compat:** v1 stays a focused read/action API but takes two
  near-free decisions so a future web dashboard isn't blocked by an API redesign:
  (1) list endpoints return an **envelope** (`{"feeds":[...], "total":N}`) rather than
  a bare array, so pagination/filtering can be added without a breaking shape change;
  (2) a reserved **CORS config hook** (`admin.cors.allowed_origins`, empty = off) so
  enabling a browser SPA is a config change, not a code change. The richer
  dashboard-backend pieces (per-feed live poll-status registry, recent-activity/events
  feed, `/v1/sinks` view, JSON metrics summary) are **deferred to a follow-up** — see
  *Out of scope*.
- **Delivery:** a single branch/PR (`feat/admin-api`).

## Config

New top-level section, sibling to `health` / `heartbeat`:

```yaml
admin:
  enabled: false            # default off
  listen: ":8090"           # dedicated listener; required when enabled
  tls:                      # optional transport security + mTLS client-cert auth
    enabled: false
    cert_file: ""           # server cert (PEM); required when tls.enabled
    key_file: ""            # server key  (PEM); required when tls.enabled
    client_ca_file: ""      # when set => require & verify client certs (mTLS)
  auth:                     # token / basic / api-key application auth
    enabled: true           # default ON; set false to deliberately disable token auth
    bearer_tokens:
      - name: ops
        token: "${ADMIN_TOKEN}"
    basic_users:
      - name: admin
        username: admin
        password: "${ADMIN_PASSWORD}"
    api_keys:
      - name: ci
        key: "${ADMIN_API_KEY}"
    api_key_header: X-API-Key   # default when omitted
  cors:
    allowed_origins: []         # empty => CORS disabled (no browser SPA). e.g. ["https://ops.example.com"]
```

- `internal/config/config.go`: add `Admin AdminConfig` to the top-level `Config`.
  Define `AdminConfig{ Enabled bool; Listen string; TLS AdminTLSConfig; Auth
  AdminAuthConfig; CORS AdminCORSConfig }` with `mapstructure` tags.
  - `AdminAuthConfig` has an **`Enabled bool` (default `true`)** plus the credential
    lists, reusing the existing element types `FeedBasicAuthConfig` /
    `FeedBearerCred` / `FeedAPIKeyCred` and `api_key_header`. *(It does **not** reuse
    `FeedAuthConfig` directly, because that type carries the feed-sink's `disabled`
    flag; admin uses the clearer `enabled`/default-on semantic. The credential element
    types and the `httpauth` builder are still shared — only the on/off field
    differs.)*
  - `AdminTLSConfig{ Enabled bool; CertFile, KeyFile, ClientCAFile string }` — when
    `ClientCAFile` is set the server requires and verifies client certs
    (`tls.RequireAndVerifyClientCert`), i.e. mTLS. Mirrors the existing sink/server TLS
    config shape.
  - `AdminCORSConfig{ AllowedOrigins []string \`mapstructure:"allowed_origins"\` }` —
    empty slice means CORS is off (the default).
- `internal/config/load.go`: register Viper defaults `admin.enabled` (`false`),
  `admin.listen` (`":8090"`), and **`admin.auth.enabled` (`true`)** in
  `applyDefaults()`.
- `internal/config/validate.go`: `validateAdmin()` — when `admin.enabled`:
  - `listen` is required (non-empty);
  - **the API must not be left unauthenticated by accident.** It is considered
    authenticated if **either** mTLS is on (`tls.enabled` + `tls.client_ca_file` set)
    **or** application auth is on (`auth.enabled` *and* ≥1 credential across
    `bearer_tokens` / `basic_users` / `api_keys`). If `auth.enabled` is true (the
    default) but **no** credential is configured → **error** ("admin.auth enabled but
    no credentials"). If `auth.enabled: false` **and** mTLS is not configured → the API
    is fully open; allow it (it is now an explicit opt-out) but emit a loud
    **warning**;
  - reuse the credential well-formedness checks (non-empty names/secrets);
  - TLS: when `tls.enabled`, `cert_file` and `key_file` are required; `client_ca_file`
    set while `tls.enabled` is false → **error** (mTLS needs TLS);
  - **warn** (not fail) if `admin.listen` equals `health.listen` or
    `telemetry.prometheus.listen` — same collision-warning pattern health already
    uses, since two servers cannot bind the same address;
  - each `cors.allowed_origins` entry, when present, must be a valid origin
    (`scheme://host[:port]`, or the literal `*`); a `*` wildcard combined with auth
    is permitted but **warns**, since credentialed wildcard CORS is a smell.

Env overrides follow the existing scheme; `admin.enabled` / `admin.listen` /
`admin.auth.enabled` have registered defaults so `RSS2MSG_ADMIN__ENABLED` /
`RSS2MSG_ADMIN__LISTEN` / `RSS2MSG_ADMIN__AUTH__ENABLED` bind. (Nested credential
lists and TLS file paths must be set in the file — slices don't bind from env, and
file paths are best kept in config.)

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
- Provide a shared builder in `httpauth` that constructs an `httpauth.Auth` from the
  credential element slices (`[]FeedBasicAuthConfig` / `[]FeedBearerCred` /
  `[]FeedAPIKeyCred` + `api_key_header`). Both the feed-sink converter
  (`FeedAuthConfig -> httpauth.Auth`) and the admin converter
  (`AdminAuthConfig -> httpauth.Auth`) call it, so the config→auth mapping lives in one
  place. The admin converter additionally honors `AdminAuthConfig.Enabled`: when
  `false` it yields an empty `Auth` (middleware becomes a pass-through).

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
  `surface="admin"`, and on failure writes `401` + challenge. When application auth is
  off (`auth.enabled: false` → the passed `httpauth.Auth` is empty), the middleware is
  a pass-through. Validation guarantees that this only happens when mTLS is configured
  or the operator explicitly opted into an open API.
- **TLS / mTLS** (`Start()`): when `cfg.TLS.Enabled`, serve over TLS using
  `cert_file`/`key_file`; when `cfg.TLS.ClientCAFile` is set, build a `tls.Config` with
  `ClientCAs` = that CA pool and `ClientAuth: tls.RequireAndVerifyClientCert` — every
  request must present a client cert signed by that CA (mTLS). mTLS and the
  bearer/basic/api-key auth are independent layers and may be combined (defence in
  depth) or used alone. Reuses the existing server-TLS helper pattern used by other
  listeners in the repo. Plain HTTP remains the default when `tls.enabled` is false.
- **CORS middleware** (only active when `cors.allowed_origins` is non-empty): echoes
  an allowed `Origin` into `Access-Control-Allow-Origin`, sets
  `Access-Control-Allow-Credentials: true`, advertises the methods/headers the API
  uses (incl. `Authorization`/`X-API-Key`), and short-circuits `OPTIONS` preflight
  with `204`. When the list is empty the middleware is omitted entirely (no CORS
  headers, identical to today). A disallowed origin simply gets no CORS headers.
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
| `GET /v1/feeds` | envelope `{"feeds":[{url, interval_seconds, owned, etag, last_modified}, ...], "total":N}` (envelope, not a bare array, so pagination/filtering can be added non-breakingly later). Feed list from `Feeds.Desired()`; per-feed `etag`/`last_modified` joined from `State.GetFeedMeta` (absent meta → null fields). `owned` is computed via `assign.Owner(url, members) == self` when assignment is on, else always `true`. *(Note: a `last_polled` timestamp is intentionally absent — `state.FeedMeta` exposes only `{ETag, LastModified}`, the feed's own HTTP cache validators, not when we last polled. Last-poll time is part of the deferred poll-status registry.)* |
| `GET /v1/feeds/{id}` | one feed (see below for `{id}`); `404` if not in the desired set. |
| `GET /v1/members` | `{self, members: [...], ownership: {feedURL: member}}`. Members from `MembershipInspector` (an `OwnerProvider.Self()`/`Members()` snapshot — side-effect-free, not a coordinator heartbeat) when assignment is on; single-self response (`members:[self]`, every feed owned by self) for memory/non-clustered backends. `ownership` computed per feed with `assign.Owner(url, members)`. |
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
  - build `httpauth.Auth` from `cfg.Admin.Auth` via the shared converter (honoring
    `auth.enabled`), and pass `cfg.Admin.TLS` through for the listener;
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
- `docs/how-to/operate-the-admin-api.md` — enable it, configure auth (token/basic/
  api-key) and/or **mTLS**, curl examples, security guidance (bind privately; keep
  `auth.enabled: true` or use mTLS; only set `auth.enabled: false` deliberately). Link
  to the existing TLS how-to (`secure-connections-tls.md`) for cert setup.
- `docs/reference/configuration.md` — document the `admin:` block (incl. `tls`/mTLS
  and `cors`).
- Add the `admin:` block to **both** `internal/config/example.yaml` and
  `examples/config.example.yaml` (must stay byte-identical — drift-guard test).
- Cross-link from `docs/index.md` / relevant how-to hubs; run
  `bash scripts/check-doc-links.sh` (must print `OK: all relative doc links
  resolve`).

## Testing (TDD)

- `internal/config`: defaults (`admin.enabled=false`, `admin.listen=":8090"`,
  `admin.auth.enabled=true`, `cors.allowed_origins` empty); validation
  (enabled w/o listen → error; `auth.enabled` (default) + no credential → error;
  `auth.enabled:false` + no mTLS → ok **with warning**; `auth.enabled:false` + mTLS →
  ok, no warning; enabled + credentials → ok; `tls.enabled` w/o cert/key → error;
  `client_ca_file` set while `tls.enabled:false` → error; listen collision with
  health/prometheus → warning; malformed CORS origin → error; `*` origin → warning).
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
  default-duration fallback to `item_ttl`. Plus: `/v1/feeds` returns the
  `{feeds,total}` envelope; CORS middleware echoes an allowed origin + handles
  `OPTIONS` preflight, omits headers for a disallowed origin, and is absent entirely
  when `allowed_origins` is empty. Auth middleware is a pass-through when the built
  `httpauth.Auth` is empty (`auth.enabled:false`).
- mTLS (`internal/admin` or `cmd/rss2msg` serve-level, using `httptest.NewTLSServer`
  / a `tls.Config` with a test CA + client cert): a request with a valid client cert
  succeeds; a request with no / an untrusted client cert is rejected at the TLS layer;
  a server configured without `client_ca_file` does not require a client cert.
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
- A web UI / dashboard itself — v1 is a JSON API only. The two cheap forward-compat
  hooks (list envelope + CORS config) are *in* scope; building a dashboard is not.
- **Deferred dashboard-backend data (follow-up issue), intentionally not in v1:**
  - per-feed **live poll-status registry** (last poll time/result/error message, last
    change count, consecutive-failure streak, next scheduled run) fed by the
    scheduler's `OnPollComplete`/`OnPollOverrun` hooks — the one piece the current data
    model can't reconstruct later;
  - a **recent-activity / events** endpoint (changes & errors stream);
  - a **`/v1/sinks`** health/delivery view;
  - a **JSON metrics summary** (dashboards can scrape Prometheus `/metrics` meanwhile);
  - **pagination/filtering** on `/v1/feeds` (the envelope shape reserves room for it).

## Acceptance criteria

- Default config starts no admin listener and changes nothing.
- With `admin.enabled: true` + a configured credential, the listener binds on
  `admin.listen`; unauthenticated requests get `401` with a challenge; authenticated
  requests reach the endpoints.
- `admin.enabled: true` with `auth.enabled: true` (default) and no credentials fails
  validation; `auth.enabled: false` without mTLS validates but warns.
- With `admin.tls.client_ca_file` set, the listener requires a valid client cert
  (mTLS): a trusted client cert is accepted, an absent/untrusted one is rejected at the
  TLS handshake; mTLS may be used alone or alongside token/basic/api-key auth.
- `GET /v1/status`, `/v1/feeds`, `/v1/feeds/{id}`, `/v1/members`, `/v1/health` return
  correct JSON for both single-instance and clustered (assignment) deployments;
  `/v1/feeds` uses the `{feeds,total}` envelope.
- With `admin.cors.allowed_origins` set, browser preflight (`OPTIONS`) and
  cross-origin requests from a listed origin succeed; with it empty, no CORS headers
  are emitted (behavior identical to having no CORS).
- `POST /v1/feeds/reconcile` re-reads feed sources; `POST /v1/feeds/{id}/poll`
  forces an off-cycle poll of a running feed and `404`s an unknown one;
  `POST /v1/state/prune` removes aged rows and reports counts.
- Feed-sink auth behavior is unchanged after the `internal/httpauth` extraction
  (characterization tests green before and after).
- `task test`, `task vet`, `task lint` pass; the two example YAML files remain
  byte-identical; `check-doc-links.sh` passes.
