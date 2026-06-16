# Feed Sink Auth — PR-A (Token Methods) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the feed sink's single-credential, all-surfaces auth with multiple labeled credentials (basic / bearer / API-key), an API-key header, per-surface rules (default + override + public opt-out), and auth success/failure metrics. **mTLS is out of scope — it ships in PR-B.**

**Architecture:** The config layer resolves each surface's *effective* auth (surface override fully replaces the sink default; a `disabled` or method-less block becomes public) and hands the feed package a `*SurfaceAuth` per surface (`nil` = public). The feed package only enforces a given `SurfaceAuth` — it never resolves defaults. Token methods are OR'd (any one valid credential authenticates). Spec: [`docs/superpowers/specs/2026-06-16-feed-sink-auth-design.md`](../specs/2026-06-16-feed-sink-auth-design.md). Issue: [#131](https://github.com/IAmBod/rss2msg/issues/131).

**Tech Stack:** Go 1.25, `crypto/subtle` for constant-time compares, `net/http`, OpenTelemetry metrics (`go.opentelemetry.io/otel/metric` + `attribute`), Viper/mapstructure config, `task test` / `task vet` / `task lint`.

---

## Worktree

Per `AGENTS.md`, do **not** work on `main`. Before Task 1, create an isolated worktree:

```bash
git worktree add .worktrees/feed-sink-auth-pr-a -b feat/feed-sink-auth-pr-a
cd .worktrees/feed-sink-auth-pr-a
```

All paths below are relative to the repo root inside that worktree.

## Build-greenness note (read before starting)

This is a coordinated cross-package refactor. The `internal/sink/feed`, `internal/config`, and `cmd/rss2msg` packages are coupled through the auth types. To keep tasks bite-sized:

- **Task 1** rewrites the feed package and is verified with the **package-scoped** command `go test ./internal/sink/feed/...`. After Task 1 the *whole-repo* build (`go build ./...`) is **temporarily red** because `cmd/rss2msg/wire.go` still references the removed `feedsink.AuthConfig`. This is expected.
- **Task 2** rewrites config + validation, verified with `go test ./internal/config/...`. Repo build still red.
- **Task 3** updates wiring and restores a **green whole-repo build** (`go build ./...`, `task test`, `task vet`, `task lint`).

Commit at the end of each task (the working tree must be saved). When reviewing, treat Tasks 1–3 as a set: full-repo green is asserted at Task 3.

## File map

| File | Change | Responsibility |
| --- | --- | --- |
| `internal/sink/feed/auth.go` | Rewrite | `SurfaceAuth`/`BasicCred`/`NamedSecret` types, `authenticate()`, `writeAuthChallenge()`, `authFailReason()`, `ctEqual()` |
| `internal/sink/feed/server.go` | Modify | `handlerConfig.rssAuth/atomAuth`, `authFor`/`surfaceName`, metric recording, `setCacheHeaders(w, a)` |
| `internal/sink/feed/telemetry.go` | Modify | Add `authSuccess` / `authFailure` counters |
| `internal/sink/feed/mcp.go` | Modify | `mcpAuthMiddleware` takes `*SurfaceAuth` + `*instruments`, records mcp auth metrics |
| `internal/sink/feed/feed.go` | Modify | `Options` gets `RSSAuth/AtomAuth/MCPAuth *SurfaceAuth` (drop `Auth`); `New()` wires them |
| `internal/sink/feed/auth_test.go` | Rewrite | Tests for the new model |
| `internal/sink/feed/feed_test.go` | Modify | Update `TestMCP_RouteMountedAndAuthGated` to per-surface auth |
| `internal/config/config.go` | Modify | New `FeedAuthConfig` shape, credential structs, `FeedSurfaceConfig.Auth`, `EffectiveAuth`, `HasMethods` |
| `internal/config/validate.go` | Modify | Replace the basic-XOR-bearer rule with per-block validation |
| `internal/config/config_test.go` | Modify | Update the feed-auth load assertion to the new shape |
| `internal/config/validate_test.go` | Modify | Replace `TestValidate_FeedAuthExactlyOne`; add new auth validation tests |
| `cmd/rss2msg/wire.go` | Modify | `toFeedSurfaceAuth` helper + per-surface wiring |
| `docs/how-to/sinks/feed.md` | Modify | Document the new auth schema |

---

## Task 1: Feed-package auth model (types, evaluation, handler, metrics, MCP)

**Files:**
- Modify: `internal/sink/feed/auth.go` (full rewrite)
- Modify: `internal/sink/feed/server.go:15-25` (handlerConfig), `:49-87` (ServeHTTP), `:152-162` (setCacheHeaders)
- Modify: `internal/sink/feed/telemetry.go`
- Modify: `internal/sink/feed/mcp.go:15-28`
- Modify: `internal/sink/feed/feed.go:42` (Options), `:143-148` (newHandler), `:162-174` (mcp mount)
- Test: `internal/sink/feed/auth_test.go` (rewrite), `internal/sink/feed/feed_test.go:96-133` (update)

- [ ] **Step 1: Rewrite `internal/sink/feed/auth.go`**

```go
package feed

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

const defaultAPIKeyHeader = "X-API-Key"

// SurfaceAuth is the resolved auth requirement for one surface. A nil
// *SurfaceAuth means the surface is public. The config layer resolves
// default+override and collapses "disabled" / "no methods" to nil before
// constructing this, so a non-nil SurfaceAuth always carries >=1 method.
type SurfaceAuth struct {
	BasicUsers   []BasicCred
	BearerTokens []NamedSecret
	APIKeys      []NamedSecret
	APIKeyHeader string // header to read API keys from; empty => X-API-Key
}

// BasicCred is one accepted HTTP Basic credential with an optional name.
type BasicCred struct {
	Name     string
	Username string
	Password string
}

// NamedSecret is one accepted opaque secret (bearer token or API key) with an
// optional name for observability.
type NamedSecret struct {
	Name   string
	Secret string
}

// authenticate reports whether r satisfies a (nil => public, always passes) and
// returns the matched credential's name (may be "" for an unnamed credential or
// the public case). Token methods are OR'd: any one valid credential passes.
func authenticate(a *SurfaceAuth, r *http.Request) (name string, ok bool) {
	if a == nil {
		return "", true
	}
	if got := r.Header.Get("Authorization"); strings.HasPrefix(got, "Bearer ") {
		tok := got[len("Bearer "):]
		for _, c := range a.BearerTokens {
			if ctEqual(tok, c.Secret) {
				return c.Name, true
			}
		}
	}
	if u, pw, has := r.BasicAuth(); has {
		for _, c := range a.BasicUsers {
			// Evaluate both comparisons unconditionally (no && short-circuit) so
			// timing doesn't reveal whether the username alone matched.
			userOK := ctEqual(u, c.Username)
			passOK := ctEqual(pw, c.Password)
			if userOK && passOK {
				return c.Name, true
			}
		}
	}
	if key := r.Header.Get(a.apiKeyHeader()); key != "" {
		for _, c := range a.APIKeys {
			if ctEqual(key, c.Secret) {
				return c.Name, true
			}
		}
	}
	return "", false
}

func (a *SurfaceAuth) apiKeyHeader() string {
	if a.APIKeyHeader == "" {
		return defaultAPIKeyHeader
	}
	return a.APIKeyHeader
}

// authFailReason classifies a failed authentication for the failure metric.
// Low-cardinality: "no_credentials" when the request presented nothing,
// "bad_token" otherwise. (PR-B adds "no_client_cert" for mTLS.)
func authFailReason(a *SurfaceAuth, r *http.Request) string {
	if a == nil {
		return ""
	}
	if r.Header.Get("Authorization") == "" && r.Header.Get(a.apiKeyHeader()) == "" {
		return "no_credentials"
	}
	return "bad_token"
}

// writeAuthChallenge writes a 401, advertising Basic when basic auth is among
// the accepted methods (otherwise Bearer).
func writeAuthChallenge(a *SurfaceAuth, w http.ResponseWriter) {
	if a != nil && len(a.BasicUsers) > 0 {
		w.Header().Set("WWW-Authenticate", `Basic realm="rss2msg"`)
	} else {
		w.Header().Set("WWW-Authenticate", `Bearer realm="rss2msg"`)
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

func ctEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
```

(The old `authOK`/`writeUnauthorized` handler methods are removed — ServeHTTP inlines the logic in Step 2.)

- [ ] **Step 2: Update `internal/sink/feed/server.go`**

Change `handlerConfig` (lines ~15-25): replace `auth *AuthConfig` with the two per-surface fields:

```go
type handlerConfig struct {
	store           Store
	meta            FeedMeta
	maxItems        int
	rssPath         string
	atomPath        string
	renderCacheTTL  time.Duration
	cacheControlTTL time.Duration
	rssAuth         *SurfaceAuth
	atomAuth        *SurfaceAuth
	startedAt       time.Time
}
```

Add imports `"go.opentelemetry.io/otel/attribute"` and `"go.opentelemetry.io/otel/metric"` to the import block (alongside the existing `context`).

Replace the auth gate in `ServeHTTP` (the `if !h.authOK(r) { ... }` block, lines ~60-63) with:

```go
	a := h.authFor(path)
	name, ok := authenticate(a, r)
	if !ok {
		h.recordAuthFailure(r.Context(), h.surfaceName(path), authFailReason(a, r))
		writeAuthChallenge(a, w)
		return
	}
	if a != nil {
		h.recordAuthSuccess(r.Context(), h.surfaceName(path), name)
	}
```

Change `setCacheHeaders` to take the effective auth, and its call site in `ServeHTTP` from `h.setCacheHeaders(w)` to `h.setCacheHeaders(w, a)`:

```go
func (h *handler) setCacheHeaders(w http.ResponseWriter, a *SurfaceAuth) {
	scope := "public"
	if a != nil {
		scope = "private"
	}
	if h.cfg.cacheControlTTL > 0 {
		w.Header().Set("Cache-Control", scope+", max-age="+strconv.Itoa(int(h.cfg.cacheControlTTL.Seconds())))
	} else {
		w.Header().Set("Cache-Control", scope+", no-cache")
	}
}
```

Add these helpers at the end of `server.go`:

```go
func (h *handler) authFor(path string) *SurfaceAuth {
	if path == h.cfg.atomPath {
		return h.cfg.atomAuth
	}
	return h.cfg.rssAuth
}

func (h *handler) surfaceName(path string) string {
	if path == h.cfg.atomPath {
		return "atom"
	}
	return "rss"
}

func (h *handler) recordAuthSuccess(ctx context.Context, surface, cred string) {
	if h.instr == nil {
		return
	}
	h.instr.authSuccess.Add(ctx, 1, metric.WithAttributes(
		attribute.String("surface", surface),
		attribute.String("credential", cred),
	))
}

func (h *handler) recordAuthFailure(ctx context.Context, surface, reason string) {
	if h.instr == nil {
		return
	}
	h.instr.authFailure.Add(ctx, 1, metric.WithAttributes(
		attribute.String("surface", surface),
		attribute.String("reason", reason),
	))
}
```

- [ ] **Step 3: Update `internal/sink/feed/telemetry.go`**

```go
package feed

import "go.opentelemetry.io/otel/metric"

type instruments struct {
	requests    metric.Int64Counter
	notMod      metric.Int64Counter
	mcpRequests metric.Int64Counter
	authSuccess metric.Int64Counter
	authFailure metric.Int64Counter
}

func newInstruments(m metric.Meter) (*instruments, error) {
	reqs, err := m.Int64Counter("feed_sink.requests")
	if err != nil {
		return nil, err
	}
	nm, err := m.Int64Counter("feed_sink.not_modified")
	if err != nil {
		return nil, err
	}
	mcpReqs, err := m.Int64Counter("feed_sink.mcp_requests")
	if err != nil {
		return nil, err
	}
	authOK, err := m.Int64Counter("feed_sink.auth_success")
	if err != nil {
		return nil, err
	}
	authBad, err := m.Int64Counter("feed_sink.auth_failure")
	if err != nil {
		return nil, err
	}
	return &instruments{requests: reqs, notMod: nm, mcpRequests: mcpReqs, authSuccess: authOK, authFailure: authBad}, nil
}
```

- [ ] **Step 4: Update `mcpAuthMiddleware` in `internal/sink/feed/mcp.go`**

Add `"go.opentelemetry.io/otel/attribute"` to the imports, then replace the function (lines ~15-28):

```go
// mcpAuthMiddleware gates the MCP route with the surface's auth (same evaluation
// as RSS/Atom) and records auth + request metrics when a meter is configured.
func mcpAuthMiddleware(a *SurfaceAuth, instr *instruments, count metric.Int64Counter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, ok := authenticate(a, r)
		if !ok {
			if instr != nil {
				instr.authFailure.Add(r.Context(), 1, metric.WithAttributes(
					attribute.String("surface", "mcp"),
					attribute.String("reason", authFailReason(a, r)),
				))
			}
			writeAuthChallenge(a, w)
			return
		}
		if a != nil && instr != nil {
			instr.authSuccess.Add(r.Context(), 1, metric.WithAttributes(
				attribute.String("surface", "mcp"),
				attribute.String("credential", name),
			))
		}
		if count != nil {
			count.Add(r.Context(), 1)
		}
		next.ServeHTTP(w, r)
	})
}
```

- [ ] **Step 5: Update `Options` and `New()` in `internal/sink/feed/feed.go`**

In `Options` (line ~42) replace `Auth *AuthConfig` with:

```go
	RSSAuth  *SurfaceAuth
	AtomAuth *SurfaceAuth
	MCPAuth  *SurfaceAuth
```

In `New()`, update the `newHandler(handlerConfig{...})` call (lines ~143-148) — replace `auth: o.Auth,` with:

```go
		rssAuth: o.RSSAuth, atomAuth: o.AtomAuth,
```

Update the MCP mount (line ~172) from `mcpAuthMiddleware(o.Auth, mcpCount, sh)` to:

```go
		mux.Handle(mcpPath, mcpAuthMiddleware(o.MCPAuth, h.instr, mcpCount, sh))
```

- [ ] **Step 6: Rewrite `internal/sink/feed/auth_test.go`** (table-driven over the new model)

```go
package feed

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAuthenticate_Methods(t *testing.T) {
	a := &SurfaceAuth{
		BasicUsers:   []BasicCred{{Name: "alice", Username: "alice", Password: "pw"}},
		BearerTokens: []NamedSecret{{Name: "ci", Secret: "tok"}},
		APIKeys:      []NamedSecret{{Name: "partner", Secret: "key"}},
	}
	tests := []struct {
		name     string
		set      func(*http.Request)
		wantOK   bool
		wantName string
	}{
		{"no creds", func(*http.Request) {}, false, ""},
		{"good bearer", func(r *http.Request) { r.Header.Set("Authorization", "Bearer tok") }, true, "ci"},
		{"bad bearer", func(r *http.Request) { r.Header.Set("Authorization", "Bearer nope") }, false, ""},
		{"good basic", func(r *http.Request) { r.SetBasicAuth("alice", "pw") }, true, "alice"},
		{"bad basic", func(r *http.Request) { r.SetBasicAuth("alice", "wrong") }, false, ""},
		{"good api key", func(r *http.Request) { r.Header.Set("X-API-Key", "key") }, true, "partner"},
		{"bad api key", func(r *http.Request) { r.Header.Set("X-API-Key", "nope") }, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/rss", nil)
			tc.set(r)
			name, ok := authenticate(a, r)
			if ok != tc.wantOK || name != tc.wantName {
				t.Fatalf("authenticate = (%q,%v), want (%q,%v)", name, ok, tc.wantName, tc.wantOK)
			}
		})
	}
}

func TestAuthenticate_NilIsPublic(t *testing.T) {
	if name, ok := authenticate(nil, httptest.NewRequest(http.MethodGet, "/rss", nil)); !ok || name != "" {
		t.Fatalf("nil auth must be public, got (%q,%v)", name, ok)
	}
}

func TestAuthenticate_CustomAPIKeyHeader(t *testing.T) {
	a := &SurfaceAuth{APIKeys: []NamedSecret{{Name: "p", Secret: "key"}}, APIKeyHeader: "X-Feed-Key"}
	r := httptest.NewRequest(http.MethodGet, "/rss", nil)
	r.Header.Set("X-Feed-Key", "key")
	if _, ok := authenticate(a, r); !ok {
		t.Fatal("custom api key header must authenticate")
	}
	r2 := httptest.NewRequest(http.MethodGet, "/rss", nil)
	r2.Header.Set("X-API-Key", "key") // default header ignored when a custom one is set
	if _, ok := authenticate(a, r2); ok {
		t.Fatal("default header must not authenticate when a custom header is configured")
	}
}

func TestAuthenticate_MultipleBearerTokens(t *testing.T) {
	a := &SurfaceAuth{BearerTokens: []NamedSecret{{Name: "a", Secret: "t1"}, {Name: "b", Secret: "t2"}}}
	for tok, want := range map[string]string{"t1": "a", "t2": "b"} {
		r := httptest.NewRequest(http.MethodGet, "/rss", nil)
		r.Header.Set("Authorization", "Bearer "+tok)
		if name, ok := authenticate(a, r); !ok || name != want {
			t.Fatalf("token %q: got (%q,%v), want (%q,true)", tok, name, ok, want)
		}
	}
}

func TestServeHTTP_BasicRequiredChallengeAndPrivate(t *testing.T) {
	h := newTestHandler(t)
	h.cfg.rssAuth = &SurfaceAuth{BasicUsers: []BasicCred{{Username: "u", Password: "p"}}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 got %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic") {
		t.Fatal("missing Basic WWW-Authenticate")
	}
	req := httptest.NewRequest(http.MethodGet, "/rss", nil)
	req.SetBasicAuth("u", "p")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != 200 {
		t.Fatalf("want 200 with creds got %d", rec2.Code)
	}
	if !strings.Contains(rec2.Header().Get("Cache-Control"), "private") {
		t.Fatalf("auth must force Cache-Control private, got %q", rec2.Header().Get("Cache-Control"))
	}
}

func TestServeHTTP_PerSurfaceOverride(t *testing.T) {
	h := newTestHandler(t)
	// rss public, atom requires a bearer token.
	h.cfg.rssAuth = nil
	h.cfg.atomAuth = &SurfaceAuth{BearerTokens: []NamedSecret{{Secret: "tok"}}}

	rss := httptest.NewRecorder()
	h.ServeHTTP(rss, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if rss.Code != 200 {
		t.Fatalf("public rss: want 200 got %d", rss.Code)
	}
	atom := httptest.NewRecorder()
	h.ServeHTTP(atom, httptest.NewRequest(http.MethodGet, "/atom", nil))
	if atom.Code != http.StatusUnauthorized {
		t.Fatalf("protected atom: want 401 got %d", atom.Code)
	}
}

func TestServeHTTP_BearerChallengeWhenNoBasic(t *testing.T) {
	h := newTestHandler(t)
	h.cfg.rssAuth = &SurfaceAuth{BearerTokens: []NamedSecret{{Secret: "tok"}}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
		t.Fatalf("want Bearer challenge, got %q", rec.Header().Get("WWW-Authenticate"))
	}
}

func TestCacheControl_PublicNoCacheWhenTTLZero(t *testing.T) {
	h := newTestHandler(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if got := rec.Header().Get("Cache-Control"); got != "public, no-cache" {
		t.Fatalf("want 'public, no-cache' got %q", got)
	}
}

func TestCacheControl_PublicMaxAgeWhenTTLSet(t *testing.T) {
	h := newTestHandler(t)
	h.cfg.cacheControlTTL = 300 * time.Second
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if got := rec.Header().Get("Cache-Control"); got != "public, max-age=300" {
		t.Fatalf("want 'public, max-age=300' got %q", got)
	}
}
```

- [ ] **Step 7: Update `TestMCP_RouteMountedAndAuthGated` in `internal/sink/feed/feed_test.go`**

Replace the line `Auth: &AuthConfig{BearerToken: "secret"},` (line ~104) with:

```go
		RSS:     Surface{Enabled: true},
		MCP:     Surface{Enabled: true, Path: "/mcp"},
		RSSAuth: &SurfaceAuth{BearerTokens: []NamedSecret{{Secret: "secret"}}},
		MCPAuth: &SurfaceAuth{BearerTokens: []NamedSecret{{Secret: "secret"}}},
```

(Remove the now-duplicate `RSS:` / `MCP:` lines that preceded it so the struct literal has each field once. The rest of the test — asserting 401 on unauthenticated `/mcp` and 200 on `/rss` with `Authorization: Bearer secret` — is unchanged and still valid.)

- [ ] **Step 8: Run the feed-package tests**

Run: `go test ./internal/sink/feed/...`
Expected: PASS. (`go build ./...` will fail at `cmd/rss2msg/wire.go` — expected per the greenness note; do not run it yet.)

- [ ] **Step 9: Commit**

```bash
git add internal/sink/feed/auth.go internal/sink/feed/server.go internal/sink/feed/telemetry.go internal/sink/feed/mcp.go internal/sink/feed/feed.go internal/sink/feed/auth_test.go internal/sink/feed/feed_test.go
git status   # verify ONLY the files above are staged (vault auto-staging hazard)
git commit -m "feat(feed-sink): per-surface multi-credential auth model (#131)"
```

---

## Task 2: Config schema + validation

**Files:**
- Modify: `internal/config/config.go:359-361` (surface fields are value-typed `FeedSurfaceConfig` — add `Auth` to the struct at :376-379), `:410-418` (FeedAuthConfig + credential structs), add `EffectiveAuth`/`HasMethods`
- Modify: `internal/config/validate.go:699-705` (replace) + new `validateFeedAuth` helper
- Test: `internal/config/config_test.go:80-108`, `internal/config/validate_test.go:1191-1198` (replace) + new tests

- [ ] **Step 1: Replace the auth structs in `internal/config/config.go`**

Replace `FeedAuthConfig` + `FeedBasicAuthConfig` (lines ~410-418) with:

```go
// FeedAuthConfig is the auth requirement for the feed sink. The top-level block
// is the default for every surface; a surface's own block fully replaces it
// (replace-not-merge). An empty default, or a surface block with disabled: true,
// means that surface is public.
type FeedAuthConfig struct {
	Disabled     bool                  `mapstructure:"disabled"`
	BasicUsers   []FeedBasicAuthConfig `mapstructure:"basic_users"`
	BearerTokens []FeedBearerCred      `mapstructure:"bearer_tokens"`
	APIKeys      []FeedAPIKeyCred      `mapstructure:"api_keys"`
	APIKeyHeader string                `mapstructure:"api_key_header"`
}

// HasMethods reports whether any credential method is configured.
func (a FeedAuthConfig) HasMethods() bool {
	return len(a.BasicUsers) > 0 || len(a.BearerTokens) > 0 || len(a.APIKeys) > 0
}

type FeedBasicAuthConfig struct {
	Name     string `mapstructure:"name"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type FeedBearerCred struct {
	Name  string `mapstructure:"name"`
	Token string `mapstructure:"token"`
}

type FeedAPIKeyCred struct {
	Name string `mapstructure:"name"`
	Key  string `mapstructure:"key"`
}
```

- [ ] **Step 2: Add `Auth` to `FeedSurfaceConfig` and an `EffectiveAuth` resolver**

Add the `Auth` field to `FeedSurfaceConfig` (lines ~376-379):

```go
type FeedSurfaceConfig struct {
	Enabled *bool           `mapstructure:"enabled"`
	Path    string          `mapstructure:"path"`
	Auth    *FeedAuthConfig `mapstructure:"auth"` // nil => inherit the sink default
}
```

Add this method just below `FeedSinkConfig` (after line ~369):

```go
// EffectiveAuth resolves the auth block for a surface: the surface's own block
// fully replaces the sink default when present, else the default applies.
func (f FeedSinkConfig) EffectiveAuth(s FeedSurfaceConfig) FeedAuthConfig {
	if s.Auth != nil {
		return *s.Auth
	}
	return f.Auth
}
```

- [ ] **Step 3: Replace the auth validation in `internal/config/validate.go`**

Replace the `hasBasic := ...` block (lines ~699-705) with:

```go
				// Validate the top-level default and each per-surface override.
				// The default may be empty (=> public); an *override* that is
				// present must either be disabled or define a method.
				if err := validateFeedAuth(i, s.Name, "auth", f.Auth, false); err != nil {
					return *warnings, err
				}
				for _, ov := range []struct {
					label string
					a     *FeedAuthConfig
				}{{"rss.auth", f.RSS.Auth}, {"atom.auth", f.Atom.Auth}, {"mcp.auth", f.MCP.Auth}} {
					if ov.a == nil {
						continue
					}
					if err := validateFeedAuth(i, s.Name, ov.label, *ov.a, true); err != nil {
						return *warnings, err
					}
				}
```

- [ ] **Step 4: Add the `validateFeedAuth` helper to `internal/config/validate.go`**

Add at the end of the file (it uses `fmt` and `strings`, both already imported by `validate.go`):

```go
// validateFeedAuth checks one feed-sink auth block. isOverride marks a
// per-surface block (which, when present, must define a method or be disabled);
// the top-level default may legitimately be empty (public).
func validateFeedAuth(i int, name, label string, a FeedAuthConfig, isOverride bool) error {
	if a.Disabled && a.HasMethods() {
		return fmt.Errorf("sinks[%d] (feed %q): %s.disabled cannot be combined with credentials", i, name, label)
	}
	if isOverride && !a.Disabled && !a.HasMethods() {
		return fmt.Errorf("sinks[%d] (feed %q): %s defines no credentials and is not disabled (set disabled: true for a public surface)", i, name, label)
	}
	var basicNames, bearerNames, apiNames []string
	for _, b := range a.BasicUsers {
		if b.Username == "" || b.Password == "" {
			return fmt.Errorf("sinks[%d] (feed %q): %s.basic_users entries need both username and password", i, name, label)
		}
		basicNames = append(basicNames, b.Name)
	}
	for _, t := range a.BearerTokens {
		if t.Token == "" {
			return fmt.Errorf("sinks[%d] (feed %q): %s.bearer_tokens entries need a token", i, name, label)
		}
		bearerNames = append(bearerNames, t.Name)
	}
	for _, k := range a.APIKeys {
		if k.Key == "" {
			return fmt.Errorf("sinks[%d] (feed %q): %s.api_keys entries need a key", i, name, label)
		}
		apiNames = append(apiNames, k.Name)
	}
	for _, set := range []struct {
		kind  string
		names []string
	}{{"basic_users", basicNames}, {"bearer_tokens", bearerNames}, {"api_keys", apiNames}} {
		if dup := firstDupFeedName(set.names); dup != "" {
			return fmt.Errorf("sinks[%d] (feed %q): %s.%s has duplicate name %q", i, name, label, set.kind, dup)
		}
	}
	if h := a.APIKeyHeader; h != "" && strings.ContainsAny(h, " \t:") {
		return fmt.Errorf("sinks[%d] (feed %q): %s.api_key_header %q is not a valid header name", i, name, label, h)
	}
	return nil
}

// firstDupFeedName returns the first duplicated non-empty name, or "".
func firstDupFeedName(names []string) string {
	seen := map[string]bool{}
	for _, n := range names {
		if n == "" {
			continue // unnamed credentials never collide
		}
		if seen[n] {
			return n
		}
		seen[n] = true
	}
	return ""
}
```

- [ ] **Step 5: Update the load assertion in `internal/config/config_test.go`**

In the feed-sink load test, replace the YAML `auth:` block (lines ~80-81):

```yaml
      auth:
        basic_users:
          - { name: ops, username: u, password: p }
```

and the assertion (lines ~106-107):

```go
	if len(s.Feed.Auth.BasicUsers) != 1 || s.Feed.Auth.BasicUsers[0].Username != "u" {
		t.Fatalf("feed.auth.basic_users[0].username = %+v, want username u", s.Feed.Auth.BasicUsers)
	}
```

- [ ] **Step 6: Replace `TestValidate_FeedAuthExactlyOne` and add new tests in `internal/config/validate_test.go`**

Delete `TestValidate_FeedAuthExactlyOne` (lines ~1191-1198) and add:

```go
func TestValidate_FeedAuthMultipleCredentialsAllowed(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Auth = FeedAuthConfig{
		BasicUsers:   []FeedBasicAuthConfig{{Name: "a", Username: "u", Password: "p"}},
		BearerTokens: []FeedBearerCred{{Name: "ci", Token: "t1"}, {Name: "mob", Token: "t2"}},
		APIKeys:      []FeedAPIKeyCred{{Name: "partner", Key: "k"}},
	}
	if _, err := Validate(c); err != nil {
		t.Fatalf("multiple credentials must be valid, got %v", err)
	}
}

func TestValidate_FeedAuthBasicNeedsBoth(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Auth = FeedAuthConfig{BasicUsers: []FeedBasicAuthConfig{{Username: "u"}}}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for basic user without password")
	}
}

func TestValidate_FeedAuthBearerNeedsToken(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Auth = FeedAuthConfig{BearerTokens: []FeedBearerCred{{Name: "x"}}}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for bearer entry without token")
	}
}

func TestValidate_FeedAuthDuplicateNames(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Auth = FeedAuthConfig{BearerTokens: []FeedBearerCred{{Name: "dup", Token: "t1"}, {Name: "dup", Token: "t2"}}}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for duplicate credential names")
	}
}

func TestValidate_FeedAuthOverrideEmptyRejected(t *testing.T) {
	c := feedSinkBase()
	on := true
	empty := FeedAuthConfig{}
	c.Sinks[0].Feed.RSS = FeedSurfaceConfig{Enabled: &on, Auth: &empty}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for a present-but-empty surface auth override")
	}
}

func TestValidate_FeedAuthOverrideDisabledIsPublic(t *testing.T) {
	c := feedSinkBase()
	on := true
	pub := FeedAuthConfig{Disabled: true}
	c.Sinks[0].Feed.Auth = FeedAuthConfig{BearerTokens: []FeedBearerCred{{Name: "ci", Token: "t"}}}
	c.Sinks[0].Feed.RSS = FeedSurfaceConfig{Enabled: &on, Auth: &pub}
	if _, err := Validate(c); err != nil {
		t.Fatalf("disabled override must be valid (public surface), got %v", err)
	}
}

func TestValidate_FeedAuthDisabledWithMethodsRejected(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Auth = FeedAuthConfig{Disabled: true, BearerTokens: []FeedBearerCred{{Token: "t"}}}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for disabled combined with credentials")
	}
}

func TestValidate_FeedAuthInvalidAPIKeyHeader(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Auth = FeedAuthConfig{APIKeys: []FeedAPIKeyCred{{Key: "k"}}, APIKeyHeader: "bad header"}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for invalid api_key_header")
	}
}
```

- [ ] **Step 7: Run the config-package tests**

Run: `go test ./internal/config/...`
Expected: PASS. (Whole-repo build still red at `wire.go` — fixed in Task 3.)

- [ ] **Step 8: Commit**

```bash
git add internal/config/config.go internal/config/validate.go internal/config/config_test.go internal/config/validate_test.go
git status   # verify only the four files above are staged
git commit -m "feat(config): per-surface feed-sink auth schema + validation (#131)"
```

---

## Task 3: Wiring + whole-repo green

**Files:**
- Modify: `cmd/rss2msg/wire.go:588-594` (replace) and the `feedsink.New(...)` call (lines ~603-619)

- [ ] **Step 1: Replace the auth wiring in `cmd/rss2msg/wire.go`**

Delete the `var auth *feedsink.AuthConfig { ... }` block (lines ~588-594). In the `feedsink.Options{...}` literal, replace `Auth: auth,` (on the `TLSCertFile ... HTTP3 ... Auth: auth,` line ~615) with:

```go
				RSSAuth:  toFeedSurfaceAuth(f.EffectiveAuth(f.RSS)),
				AtomAuth: toFeedSurfaceAuth(f.EffectiveAuth(f.Atom)),
				MCPAuth:  toFeedSurfaceAuth(f.EffectiveAuth(f.MCP)),
```

(Keep `TLSCertFile: f.TLS.CertFile, TLSKeyFile: f.TLS.KeyFile, HTTP3: f.HTTP3,` on that line — only the `Auth: auth,` part is replaced.)

- [ ] **Step 2: Add the `toFeedSurfaceAuth` helper to `cmd/rss2msg/wire.go`**

Add near the other feed wiring (the `config` package is already imported):

```go
// toFeedSurfaceAuth converts a resolved feed auth block into the sink's
// SurfaceAuth, returning nil for a public surface (disabled or no methods).
func toFeedSurfaceAuth(a config.FeedAuthConfig) *feedsink.SurfaceAuth {
	if a.Disabled || !a.HasMethods() {
		return nil
	}
	sa := &feedsink.SurfaceAuth{APIKeyHeader: a.APIKeyHeader}
	for _, b := range a.BasicUsers {
		sa.BasicUsers = append(sa.BasicUsers, feedsink.BasicCred{Name: b.Name, Username: b.Username, Password: b.Password})
	}
	for _, t := range a.BearerTokens {
		sa.BearerTokens = append(sa.BearerTokens, feedsink.NamedSecret{Name: t.Name, Secret: t.Token})
	}
	for _, k := range a.APIKeys {
		sa.APIKeys = append(sa.APIKeys, feedsink.NamedSecret{Name: k.Name, Secret: k.Key})
	}
	return sa
}
```

- [ ] **Step 3: Verify the whole repo builds and all tests pass**

Run: `go build ./...`
Expected: success (no errors).

Run: `task test`
Expected: PASS (all packages, `-race`).

Run: `task vet`
Expected: no findings.

Run: `task lint`
Expected: no findings. (If golangci-lint v2 is unavailable locally, note it explicitly; CI will run it.)

- [ ] **Step 4: Commit**

```bash
git add cmd/rss2msg/wire.go
git status   # verify only wire.go is staged
git commit -m "feat(feed-sink): wire per-surface auth from config (#131)"
```

---

## Task 4: Documentation

**Files:**
- Modify: `docs/how-to/sinks/feed.md` (auth section)

- [ ] **Step 1: Update the auth section of `docs/how-to/sinks/feed.md`**

Find the existing auth subsection (it documents `auth.basic` / `auth.bearer_token`) and replace it with the new model. Document, grounded in the code just written (do not invent options):

- Top-level `auth:` is the default for all surfaces; a surface's own `auth:` block fully replaces it; `auth: {disabled: true}` makes a surface public.
- `basic_users` (each `name`/`username`/`password`), `bearer_tokens` (each `name`/`token`), `api_keys` (each `name`/`key`), `api_key_header` (default `X-API-Key`).
- Any one valid credential authenticates (OR). `name` is optional and surfaces in the `feed_sink.auth_success` metric (`credential` attribute) and `feed_sink.auth_failure` (`reason` attribute).
- A `401` with `WWW-Authenticate` is returned on failure; authenticated responses set `Cache-Control: private`.
- Add a one-line forward pointer: mTLS client-cert auth is tracked in PR-B of issue #131 and is not yet available.

Use this YAML block as the documented example:

```yaml
sinks:
  - name: myfeed
    driver: feed
    feed:
      listen: "0.0.0.0:8443"
      auth:                              # default for all surfaces
        basic_users:
          - {name: alice, username: alice, password: s3cret}
        bearer_tokens:
          - {name: ci-bot, token: tok_a}
        api_keys:
          - {name: partner-x, key: key_1}
        api_key_header: X-API-Key        # default when omitted
      rss:  {enabled: true, auth: {disabled: true}}    # public
      atom: {enabled: true}                            # inherits the default
      mcp:  {enabled: true, auth: {bearer_tokens: [{name: mcp, token: t_mcp}]}}
```

> **Note on `examples/config.example.yaml`:** the feed sink is not currently present in `examples/config.example.yaml` / `internal/config/example.yaml`, so PR-A does **not** edit them. If you choose to add a feed-sink stanza, you MUST edit **both** files identically — a drift-guard test asserts they are byte-identical.

- [ ] **Step 2: Run the doc link checker**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`

- [ ] **Step 3: Commit**

```bash
git add docs/how-to/sinks/feed.md
git status   # verify only the doc is staged
git commit -m "docs(feed-sink): document per-surface multi-credential auth (#131)"
```

---

## Task 5: Final verification & PR

- [ ] **Step 1: Full gate from a clean tree**

Run: `task test && task vet && task lint`
Expected: all pass. (`task test-integration` is not required — PR-A touches no sink store, coordinator, or state backend; the feed sink's auth is exercised by unit tests. State this explicitly in the PR body.)

- [ ] **Step 2: Open the PR**

```bash
git push -u origin feat/feed-sink-auth-pr-a
gh pr create --title "feat(feed-sink): per-surface multi-credential auth (PR-A of #131)" \
  --body "$(cat <<'EOF'
PR-A of #131 — feed sink auth hardening (token methods; mTLS is PR-B).

- Multiple labeled credentials: `basic_users` / `bearer_tokens` / `api_keys`, each with an optional `name`.
- Configurable API-key header (default `X-API-Key`).
- Per-surface auth: top-level default, full per-surface override, `disabled: true` for public surfaces.
- Any one valid credential authenticates (OR). 401 + `WWW-Authenticate` on failure.
- Metrics: `feed_sink.auth_success{surface,credential}`, `feed_sink.auth_failure{surface,reason}`.

Skipped `task test-integration`: PR-A touches no store/coordinator/state backend (auth is covered by unit tests).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review

**Spec coverage:**
- Multiple labeled credentials → Task 1 (types) + Task 2 (config) + Task 3 (wiring). ✓
- API-key header (configurable, default `X-API-Key`) → `authenticate`/`apiKeyHeader` (Task 1), `APIKeyHeader` config (Task 2). ✓
- Per-surface override + `disabled` (replace-not-merge) → `EffectiveAuth` (Task 2), `authFor`/`rssAuth`/`atomAuth` (Task 1), `MCPAuth` (Task 1/3). ✓
- OR semantics across token types → `authenticate` (Task 1), tested. ✓
- 401 + `WWW-Authenticate` (Basic when basic configured, else Bearer) → `writeAuthChallenge` (Task 1), tested. ✓
- `Cache-Control: private` when authenticated → `setCacheHeaders(w, a)` (Task 1), tested. ✓
- Observability (`feed_sink.auth_success{surface,credential}` / `auth_failure{surface,reason}`) → telemetry + recorders (Task 1). ✓
- Validation (basic needs both; bearer/key non-empty; dup names; empty override; disabled+methods; bad header) → `validateFeedAuth` (Task 2), each with a test. ✓
- Docs + example-drift note → Task 4. ✓
- **mTLS deliberately excluded** (PR-B) — the design's PR-A scope. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code; every run step shows the command and expected result.

**Type consistency:** `SurfaceAuth`, `BasicCred{Name,Username,Password}`, `NamedSecret{Name,Secret}`, `authenticate(*SurfaceAuth, *http.Request) (string,bool)`, `writeAuthChallenge(*SurfaceAuth, http.ResponseWriter)`, `authFailReason(*SurfaceAuth,*http.Request) string`, `handlerConfig.rssAuth/atomAuth`, `Options.RSSAuth/AtomAuth/MCPAuth`, `mcpAuthMiddleware(*SurfaceAuth,*instruments,metric.Int64Counter,http.Handler)`, `instruments.authSuccess/authFailure`, config `FeedAuthConfig{Disabled,BasicUsers,BearerTokens,APIKeys,APIKeyHeader}` with `HasMethods()`/`EffectiveAuth()`, `FeedBasicAuthConfig{Name,Username,Password}`, `FeedBearerCred{Name,Token}`, `FeedAPIKeyCred{Name,Key}`, `validateFeedAuth(int,string,string,FeedAuthConfig,bool)`, `toFeedSurfaceAuth(config.FeedAuthConfig) *feedsink.SurfaceAuth` — names are used consistently across all tasks.
