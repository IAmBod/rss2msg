# HTTP Feed Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `type: http` feed source that fetches the desired feed list (a JSON object `{"feeds":[...]}`) from an HTTP endpoint on an interval, with header-based auth, mTLS, and conditional GET (ETag/If-Modified-Since).

**Architecture:** A new `internal/feedsource/http.go` mirrors the existing `postgres` source: it owns an `*http.Client` and composes the `Poll` helper for the interval ticker. Each tick issues a conditional GET, decodes the `feeds` array into `[]FeedSpec`, and converts via the existing `SpecsToConfigs`. Auth is expressed through the `headers` map (bearer/basic/API-key) plus an optional `tls` block (custom CA + client cert). A `2xx` response missing the `feeds` key is warn-logged and treated as an error so the aggregator keeps last-known-good.

**Tech Stack:** Go 1.25, `net/http`, `crypto/tls`, `encoding/json`, zerolog, Viper/mapstructure config, `net/http/httptest` for tests.

Spec: `docs/superpowers/specs/2026-06-15-http-feed-source-design.md`. Issue [#161](https://github.com/IAmBod/rss2msg/issues/161).

**Conventions for every task:**
- Work in the existing worktree `.worktrees/http-feed-source` on branch `feat/http-feed-source`.
- Stage with **explicit pathspecs only** — never `git add -A`/`.` (the repo lives in an Obsidian vault with auto-staging).
- Run `task test` (= `go test -race ./...`) and `task vet` after each implementation task.
- All commit messages end with the trailer:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## File structure

| File | Responsibility | Change |
| --- | --- | --- |
| `internal/config/config.go` | `HTTPFeedSourceConfig`, `FeedSourceHTTPTLSConfig`, `FeedSourceConfig.HTTP` field | Modify |
| `internal/config/load_test.go` | YAML→struct decode test for the `http` block | Modify |
| `internal/config/validate.go` | `case "http"` validation (url required, cert/key both-or-neither, reserved cache headers) | Modify |
| `internal/config/validate_test.go` | validation tests for the `http` source | Modify |
| `internal/feedsource/http.go` | the `HTTP` source: client, conditional-GET fetch, TLS builder | Create |
| `internal/feedsource/http_test.go` | unit tests via `httptest` | Create |
| `cmd/rss2msg/sources.go` | wire `case "http"` in `buildSources` | Modify |
| `cmd/rss2msg/sources_test.go` | `buildSources` test for the http type | Modify |
| `docs/how-to/load-feeds-dynamically.md` | `### type: http` doc section + status table | Modify |
| `docs/reference/configuration.md` | new fields reference | Modify |
| `internal/config/example.yaml` | commented `http` feed-source example | Modify |

---

## Task 1: Config structs

**Files:**
- Modify: `internal/config/config.go` (near `FeedSourcePGTLSConfig`, ~line 619)
- Test: `internal/config/load_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/config/load_test.go`:

```go
func TestLoadParsesHTTPFeedSource(t *testing.T) {
	t.Parallel()
	path := writeTempConfig(t, `
feed_sources:
  - type: http
    name: control-plane
    interval: 30s
    http:
      url: https://cp.example/feeds
      timeout: 10s
      headers:
        Authorization: "Bearer tok"
      tls:
        ca_file: /etc/ssl/ca.pem
        insecure_skip_verify: true
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.FeedSources) != 1 {
		t.Fatalf("want 1 feed source, got %d", len(cfg.FeedSources))
	}
	s := cfg.FeedSources[0]
	if s.Type != "http" || s.Name != "control-plane" {
		t.Fatalf("source = %+v", s)
	}
	if s.Interval != 30*time.Second {
		t.Fatalf("interval = %v", s.Interval)
	}
	if s.HTTP.URL != "https://cp.example/feeds" {
		t.Fatalf("url = %q", s.HTTP.URL)
	}
	if s.HTTP.Timeout != 10*time.Second {
		t.Fatalf("timeout = %v", s.HTTP.Timeout)
	}
	if s.HTTP.Headers["Authorization"] != "Bearer tok" {
		t.Fatalf("headers = %+v", s.HTTP.Headers)
	}
	if s.HTTP.TLS.CAFile != "/etc/ssl/ca.pem" || !s.HTTP.TLS.InsecureSkipVerify {
		t.Fatalf("tls = %+v", s.HTTP.TLS)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestLoadParsesHTTPFeedSource`
Expected: compile failure — `s.HTTP undefined (type FeedSourceConfig has no field or method HTTP)`.

- [ ] **Step 3: Add the structs and field**

In `internal/config/config.go`, add the `HTTP` field to `FeedSourceConfig` (alongside `Postgres`):

```go
type FeedSourceConfig struct {
	Type     string                   `mapstructure:"type"` // "static", "file", "postgres", and "http" are implemented; sqlite|redis|s3|env are added by later plans
	Name     string                   `mapstructure:"name"` // optional; defaults to "<type>[index]"
	Path     string                   `mapstructure:"path"` // file source
	Interval time.Duration            `mapstructure:"interval"`
	Postgres PostgresFeedSourceConfig `mapstructure:"postgres"` // postgres source
	HTTP     HTTPFeedSourceConfig     `mapstructure:"http"`     // http source
}
```

Then add, immediately after the `FeedSourcePGTLSConfig` type:

```go
// HTTPFeedSourceConfig configures an HTTP-backed feed source. The desired feed
// list is fetched from URL on an interval as a JSON object with the feed array
// under a "feeds" key. Auth is expressed via headers (bearer/basic/API-key) and
// the optional tls block (custom CA + client cert).
type HTTPFeedSourceConfig struct {
	URL     string                  `mapstructure:"url"`     // required
	Timeout time.Duration           `mapstructure:"timeout"` // per-request; default 30s
	Headers map[string]string       `mapstructure:"headers"`
	TLS     FeedSourceHTTPTLSConfig `mapstructure:"tls"`
}

// FeedSourceHTTPTLSConfig is the client TLS surface for the http feed source.
// Same field set as FeedSourcePGTLSConfig, kept distinct for the http namespace.
type FeedSourceHTTPTLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	ServerName         string `mapstructure:"server_name"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestLoadParsesHTTPFeedSource`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/load_test.go
git commit -m "feat(config): add HTTP feed source config structs

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Validation

**Files:**
- Modify: `internal/config/validate.go` (feed_sources switch, ~line 828)
- Test: `internal/config/validate_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/config/validate_test.go`:

```go
func TestValidateAllowsHTTPSource(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.Feeds = nil
	cfg.FeedSources = []FeedSourceConfig{{
		Type: "http",
		HTTP: HTTPFeedSourceConfig{URL: "https://cp.example/feeds"},
	}}
	if _, err := Validate(cfg); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
}

func TestValidateRejectsHTTPSourceWithoutURL(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.FeedSources = []FeedSourceConfig{{Type: "http"}}
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected error for http source without url")
	}
}

func TestValidateRejectsHTTPSourceLoneCertFile(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.FeedSources = []FeedSourceConfig{{
		Type: "http",
		HTTP: HTTPFeedSourceConfig{
			URL: "https://cp.example/feeds",
			TLS: FeedSourceHTTPTLSConfig{CertFile: "/c.pem"}, // key_file missing
		},
	}}
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected error for lone cert_file")
	}
}

func TestValidateRejectsHTTPSourceReservedHeader(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.FeedSources = []FeedSourceConfig{{
		Type: "http",
		HTTP: HTTPFeedSourceConfig{
			URL:     "https://cp.example/feeds",
			Headers: map[string]string{"If-None-Match": "x"},
		},
	}}
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected error for reserved cache header")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestValidate.*HTTPSource'`
Expected: `TestValidateRejectsHTTPSourceWithoutURL` and the others FAIL — `http` currently hits the `default` "unsupported type" branch, so `TestValidateAllowsHTTPSource` errors and the rejection tests pass for the wrong reason. After this step all four should be red against the intended behavior.

- [ ] **Step 3: Add the `case "http"` branch**

In `internal/config/validate.go`, inside the `for i, s := range c.FeedSources` switch, add **before** the `case "":` branch:

```go
		case "http":
			if strings.TrimSpace(s.HTTP.URL) == "" {
				return *warnings, fmt.Errorf("feed_sources[%d] (http): url is required", i)
			}
			if (s.HTTP.TLS.CertFile == "") != (s.HTTP.TLS.KeyFile == "") {
				return *warnings, fmt.Errorf("feed_sources[%d] (http): cert_file and key_file must both be set or both empty", i)
			}
			for h := range s.HTTP.Headers {
				canon := textproto.CanonicalMIMEHeaderKey(h)
				if _, bad := reservedHeaders[canon]; bad {
					return *warnings, fmt.Errorf("feed_sources[%d] (http): headers must not set reserved cache header %q", i, h)
				}
			}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/ -run 'TestValidate.*HTTPSource'`
Expected: PASS (all four).

- [ ] **Step 5: Commit**

```bash
git add internal/config/validate.go internal/config/validate_test.go
git commit -m "feat(config): validate http feed source (url, mTLS pair, reserved headers)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: HTTP source — fetch, decode, conditional GET, missing-key handling

**Files:**
- Create: `internal/feedsource/http.go`
- Test: `internal/feedsource/http_test.go`

This task builds the source without TLS (TLS is Task 4). The struct already carries the TLS hook so Task 4 only fills `buildHTTPSourceTLS`.

- [ ] **Step 1: Write the failing tests**

Create `internal/feedsource/http_test.go`:

```go
package feedsource

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

// longInterval keeps Poll's ticker effectively dormant so tests drive fetches
// directly via Feeds().
const longInterval = time.Hour

func newTestHTTP(t *testing.T, url string, headers map[string]string) *HTTP {
	t.Helper()
	h, err := NewHTTP(HTTPOptions{Name: "test", URL: url, Headers: headers, Interval: longInterval})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func TestHTTPFetchesAndDecodes(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"feeds":[{"url":"https://a.example/rss","interval":"5m"}]}`))
	}))
	t.Cleanup(srv.Close)

	feeds, err := newTestHTTP(t, srv.URL, nil).Feeds(context.Background())
	if err != nil {
		t.Fatalf("Feeds: %v", err)
	}
	if len(feeds) != 1 || feeds[0].URL != "https://a.example/rss" || feeds[0].Interval != 5*time.Minute {
		t.Fatalf("feeds = %+v", feeds)
	}
}

func TestHTTPSendsHeaders(t *testing.T) {
	t.Parallel()
	var gotAuth, gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotKey = r.Header.Get("X-API-Key")
		_, _ = w.Write([]byte(`{"feeds":[]}`))
	}))
	t.Cleanup(srv.Close)

	h := newTestHTTP(t, srv.URL, map[string]string{"Authorization": "Bearer tok", "X-API-Key": "k"})
	if _, err := h.Feeds(context.Background()); err != nil {
		t.Fatalf("Feeds: %v", err)
	}
	if gotAuth != "Bearer tok" || gotKey != "k" {
		t.Fatalf("auth=%q key=%q", gotAuth, gotKey)
	}
}

func TestHTTPEmptyFeedsIsValid(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"feeds":[]}`))
	}))
	t.Cleanup(srv.Close)

	feeds, err := newTestHTTP(t, srv.URL, nil).Feeds(context.Background())
	if err != nil {
		t.Fatalf("Feeds: %v", err)
	}
	if len(feeds) != 0 {
		t.Fatalf("want empty, got %+v", feeds)
	}
}

func TestHTTPConditionalGET(t *testing.T) {
	t.Parallel()
	var calls, conditional int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("If-None-Match") == `"v1"` {
			conditional++
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte(`{"feeds":[{"url":"https://a.example/rss","interval":"5m"}]}`))
	}))
	t.Cleanup(srv.Close)

	h := newTestHTTP(t, srv.URL, nil)
	if _, err := h.Feeds(context.Background()); err != nil {
		t.Fatalf("first Feeds: %v", err)
	}
	feeds, err := h.Feeds(context.Background())
	if err != nil {
		t.Fatalf("second Feeds: %v", err)
	}
	if conditional != 1 {
		t.Fatalf("expected 1 conditional request, got %d (total %d)", conditional, calls)
	}
	if len(feeds) != 1 || feeds[0].URL != "https://a.example/rss" {
		t.Fatalf("304 should return cached list, got %+v", feeds)
	}
}

func TestHTTPMissingFeedsKeyErrorsAndLogs(t *testing.T) {
	// Not parallel: swaps the global zerolog logger.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"other":1}`))
	}))
	t.Cleanup(srv.Close)

	var buf bytes.Buffer
	orig := zlog.Logger
	zlog.Logger = zerolog.New(&buf)
	t.Cleanup(func() { zlog.Logger = orig })

	if _, err := newTestHTTP(t, srv.URL, nil).Feeds(context.Background()); err == nil {
		t.Fatal("expected error for missing feeds key")
	}
	out := buf.String()
	if !strings.Contains(out, "feedsource/http") || !strings.Contains(out, "missing") {
		t.Fatalf("expected warn log about missing key, got %q", out)
	}
}

func TestHTTPBareArrayErrors(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"url":"https://a.example/rss"}]`))
	}))
	t.Cleanup(srv.Close)

	if _, err := newTestHTTP(t, srv.URL, nil).Feeds(context.Background()); err == nil {
		t.Fatal("expected error for bare array payload")
	}
}

func TestHTTPNon2xxErrors(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	if _, err := newTestHTTP(t, srv.URL, nil).Feeds(context.Background()); err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestHTTPRequiresURL(t *testing.T) {
	t.Parallel()
	if _, err := NewHTTP(HTTPOptions{Name: "x", Interval: longInterval}); err == nil {
		t.Fatal("expected error for empty url")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/feedsource/ -run TestHTTP`
Expected: compile failure — `undefined: NewHTTP` / `undefined: HTTPOptions` / `undefined: HTTP`.

- [ ] **Step 3: Write the implementation**

Create `internal/feedsource/http.go`:

```go
package feedsource

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/config"
)

// Compile-time assertion that *HTTP satisfies Source.
var _ Source = (*HTTP)(nil)

const defaultHTTPTimeout = 30 * time.Second

// HTTPTLSOptions is the client TLS surface for the http feed source. Setting any
// field configures a custom *tls.Config on the transport.
type HTTPTLSOptions struct {
	CAFile, CertFile, KeyFile, ServerName string
	InsecureSkipVerify                    bool
}

// HTTPOptions configures an HTTP-backed feed source. The source fetches the
// desired feed list from URL on Interval as a JSON object whose "feeds" key
// holds an array of feed specs.
type HTTPOptions struct {
	Name     string
	URL      string // required
	Timeout  time.Duration
	Headers  map[string]string
	Interval time.Duration
	TLS      *HTTPTLSOptions
}

// HTTP is a feed source backed by an HTTP endpoint. It composes Poll for the
// interval ticker and owns the HTTP client. It keeps the last ETag/Last-Modified
// and the last decoded list so a 304 returns the cached feeds without re-parsing.
type HTTP struct {
	url     string
	headers map[string]string
	client  *http.Client
	poll    *Poll

	mu           sync.Mutex
	etag         string
	lastModified string
	cached       []config.FeedConfig
}

// feedListResponse is the wire shape the http source expects. Feeds is a pointer
// so an absent "feeds" key (nil) is distinguishable from an empty array.
type feedListResponse struct {
	Feeds *[]FeedSpec `json:"feeds"`
}

// NewHTTP builds the HTTP client (timeout + optional TLS) and returns a polling
// source. It validates options but performs no network I/O at construction.
func NewHTTP(opts HTTPOptions) (*HTTP, error) {
	if strings.TrimSpace(opts.URL) == "" {
		return nil, fmt.Errorf("http feed source %q: url is required", opts.Name)
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	client := &http.Client{Timeout: timeout}
	if opts.TLS != nil {
		tc, err := buildHTTPSourceTLS(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("http feed source %q: %w", opts.Name, err)
		}
		client.Transport = &http.Transport{TLSClientConfig: tc}
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("component", "feedsource/http").
				Str("source", opts.Name).
				Msg("http feed source: TLS verification disabled (insecure_skip_verify=true)")
		}
	}
	h := &HTTP{url: opts.URL, headers: opts.Headers, client: client}
	h.poll = NewPoll(opts.Name, opts.Interval, h.fetch)
	return h, nil
}

func (h *HTTP) Name() string { return h.poll.Name() }

func (h *HTTP) Feeds(ctx context.Context) ([]config.FeedConfig, error) { return h.fetch(ctx) }

func (h *HTTP) Changes() <-chan struct{} { return h.poll.Changes() }

// Close stops the poll ticker.
func (h *HTTP) Close() error {
	h.poll.Close()
	return nil
}

func (h *HTTP) fetch(ctx context.Context) ([]config.FeedConfig, error) {
	h.mu.Lock()
	etag, lastMod, cached := h.etag, h.lastModified, h.cached
	h.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
	if err != nil {
		return nil, fmt.Errorf("http feed source %q: build request: %w", h.poll.Name(), err)
	}
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastMod != "" {
		req.Header.Set("If-Modified-Since", lastMod)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http feed source %q: get: %w", h.poll.Name(), err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotModified:
		return cached, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("http feed source %q: read body: %w", h.poll.Name(), err)
		}
		var payload feedListResponse
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, fmt.Errorf("http feed source %q: decode: %w", h.poll.Name(), err)
		}
		if payload.Feeds == nil {
			log.Warn().
				Str("component", "feedsource/http").
				Str("source", h.poll.Name()).
				Str("url", h.url).
				Msg(`http feed source: response missing "feeds" key; keeping last-known-good`)
			return nil, fmt.Errorf("http feed source %q: response missing \"feeds\" key", h.poll.Name())
		}
		feeds, err := SpecsToConfigs(*payload.Feeds)
		if err != nil {
			return nil, fmt.Errorf("http feed source %q: %w", h.poll.Name(), err)
		}
		h.mu.Lock()
		h.etag = resp.Header.Get("ETag")
		h.lastModified = resp.Header.Get("Last-Modified")
		h.cached = feeds
		h.mu.Unlock()
		return feeds, nil
	default:
		return nil, fmt.Errorf("http feed source %q: unexpected status %d", h.poll.Name(), resp.StatusCode)
	}
}

// buildHTTPSourceTLS translates HTTPTLSOptions into a *tls.Config. Mirrors the
// postgres source's TLS builder; ServerName defaults to the request URL host
// (left empty here) unless overridden.
func buildHTTPSourceTLS(opts HTTPTLSOptions) (*tls.Config, error) {
	tc := &tls.Config{
		InsecureSkipVerify: opts.InsecureSkipVerify, //nolint:gosec // operator opt-in, logged at warn
	}
	if opts.ServerName != "" {
		tc.ServerName = opts.ServerName
	}
	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %q: no PEM certificates parsed", opts.CAFile)
		}
		tc.RootCAs = pool
	}
	if opts.CertFile != "" || opts.KeyFile != "" {
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, fmt.Errorf("cert_file and key_file must both be set or both empty")
		}
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("client cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/feedsource/ -run TestHTTP`
Expected: PASS (all `TestHTTP*` from Step 1).

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/http.go internal/feedsource/http_test.go
git commit -m "feat(feedsource): add http feed source with conditional GET

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: HTTP source — TLS round-trip tests

`buildHTTPSourceTLS` was written in Task 3; this task proves it end-to-end with `httptest` TLS servers and covers the error paths.

**Files:**
- Test: `internal/feedsource/http_test.go` (add to the existing file)

- [ ] **Step 1: Write the failing tests**

Append to `internal/feedsource/http_test.go`. Add `"encoding/pem"`, `"os"`, and `"path/filepath"` to the import block.

```go
func writeServerCAFile(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	cert := srv.Certificate() // self-signed cert httptest serves (valid for 127.0.0.1)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	return path
}

func TestHTTPTLSCustomCARoundTrip(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"feeds":[{"url":"https://a.example/rss","interval":"5m"}]}`))
	}))
	t.Cleanup(srv.Close)

	caFile := writeServerCAFile(t, srv)
	h, err := NewHTTP(HTTPOptions{
		Name:     "tls",
		URL:      srv.URL, // https://127.0.0.1:port
		Interval: longInterval,
		TLS:      &HTTPTLSOptions{CAFile: caFile},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	feeds, err := h.Feeds(context.Background())
	if err != nil {
		t.Fatalf("Feeds over TLS: %v", err)
	}
	if len(feeds) != 1 {
		t.Fatalf("feeds = %+v", feeds)
	}
}

func TestHTTPTLSUntrustedCAFails(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"feeds":[]}`))
	}))
	t.Cleanup(srv.Close)

	// No CAFile and verification on → the self-signed cert is untrusted.
	h, err := NewHTTP(HTTPOptions{Name: "tls", URL: srv.URL, Interval: longInterval, TLS: &HTTPTLSOptions{}})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if _, err := h.Feeds(context.Background()); err == nil {
		t.Fatal("expected TLS verification error")
	}
}

func TestHTTPTLSInsecureSkipVerify(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"feeds":[]}`))
	}))
	t.Cleanup(srv.Close)

	h, err := NewHTTP(HTTPOptions{Name: "tls", URL: srv.URL, Interval: longInterval, TLS: &HTTPTLSOptions{InsecureSkipVerify: true}})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if _, err := h.Feeds(context.Background()); err != nil {
		t.Fatalf("Feeds with skip-verify: %v", err)
	}
}

func TestBuildHTTPSourceTLSErrors(t *testing.T) {
	t.Parallel()
	if _, err := buildHTTPSourceTLS(HTTPTLSOptions{CertFile: "/only-cert.pem"}); err == nil {
		t.Fatal("expected error for lone cert_file")
	}
	bad := filepath.Join(t.TempDir(), "bad-ca.pem")
	if err := os.WriteFile(bad, []byte("not a pem"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := buildHTTPSourceTLS(HTTPTLSOptions{CAFile: bad}); err == nil {
		t.Fatal("expected error for unparseable CA file")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail/pass appropriately**

Run: `go test -race ./internal/feedsource/ -run 'TestHTTPTLS|TestBuildHTTPSourceTLS'`
Expected: PASS — the implementation from Task 3 already satisfies these. (If `TestHTTPTLSUntrustedCAFails` does not error, the transport is not being applied — revisit `client.Transport` wiring in `NewHTTP`.)

- [ ] **Step 3: No implementation needed**

These tests exercise existing code. If any fail, fix `internal/feedsource/http.go` accordingly before committing.

- [ ] **Step 4: Run the full package test**

Run: `go test -race ./internal/feedsource/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/http_test.go
git commit -m "test(feedsource): http source TLS round-trip and builder errors

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Wire `case "http"` into buildSources

**Files:**
- Modify: `cmd/rss2msg/sources.go` (the `switch sc.Type` in `buildSources`)
- Test: `cmd/rss2msg/sources_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `cmd/rss2msg/sources_test.go`:

```go
func TestBuildSourcesHTTP(t *testing.T) {
	cfg := config.Config{
		FeedSources: []config.FeedSourceConfig{{
			Type: "http",
			Name: "cp",
			HTTP: config.HTTPFeedSourceConfig{URL: "https://cp.example/feeds"},
		}},
	}
	sources, cleanup, err := buildSources(cfg)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	defer cleanup()
	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}
	if sources[0].Name() != "cp" {
		t.Errorf("name: got %q want %q", sources[0].Name(), "cp")
	}
}

func TestBuildSourcesHTTPMissingURL(t *testing.T) {
	cfg := config.Config{
		FeedSources: []config.FeedSourceConfig{{Type: "http"}},
	}
	if _, _, err := buildSources(cfg); err == nil {
		t.Fatal("expected error for http source without url")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/rss2msg/ -run TestBuildSourcesHTTP`
Expected: `TestBuildSourcesHTTP` FAILS — `http` hits the `default` "unsupported type" branch and `buildSources` returns an error.

- [ ] **Step 3: Add the `case "http"` branch**

In `cmd/rss2msg/sources.go`, add this case to the `switch sc.Type` (after the `postgres` case, before `default`):

```go
		case "http":
			opts := feedsource.HTTPOptions{
				Name:     name,
				URL:      sc.HTTP.URL,
				Timeout:  sc.HTTP.Timeout,
				Headers:  sc.HTTP.Headers,
				Interval: sc.Interval,
			}
			if sc.HTTP.TLS != (config.FeedSourceHTTPTLSConfig{}) {
				opts.TLS = &feedsource.HTTPTLSOptions{
					CAFile:             sc.HTTP.TLS.CAFile,
					CertFile:           sc.HTTP.TLS.CertFile,
					KeyFile:            sc.HTTP.TLS.KeyFile,
					ServerName:         sc.HTTP.TLS.ServerName,
					InsecureSkipVerify: sc.HTTP.TLS.InsecureSkipVerify,
				}
			}
			h, err := feedsource.NewHTTP(opts)
			if err != nil {
				closeAll(closers)
				return nil, nil, err
			}
			closers = append(closers, func() { _ = h.Close() })
			sources = append(sources, h)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/rss2msg/ -run TestBuildSourcesHTTP`
Expected: PASS (both).

- [ ] **Step 5: Run full suite + vet**

Run: `task test && task vet`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add cmd/rss2msg/sources.go cmd/rss2msg/sources_test.go
git commit -m "feat(cmd): wire http feed source into buildSources

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Docs and example config

**Files:**
- Modify: `docs/how-to/load-feeds-dynamically.md`
- Modify: `docs/reference/configuration.md`
- Modify: `internal/config/example.yaml`

- [ ] **Step 1: Update the source-types table**

In `docs/how-to/load-feeds-dynamically.md`, change the status table rows so `http` is implemented:

```markdown
| `postgres` | implemented | Reads the feed list from a Postgres table; polls on an interval. |
| `http`   | implemented | Fetches the feed list from an HTTP endpoint as JSON; polls on an interval with conditional GET. |
| `sqlite`, `redis`, `s3`, `env` | planned | Not yet implemented. |
```

- [ ] **Step 2: Add the `### type: http` section**

Insert after the `### type: postgres` section (before `### type: static`):

````markdown
### `type: http`

Fetches the desired feed list from an HTTP endpoint and re-fetches it every
`interval`. The response must be a JSON **object** with the feed array under a
`feeds` key (unlike the `file` source, which is a bare array):

```yaml
- type: http
  name: control-plane            # optional label for logs
  interval: 30s                  # how often to re-fetch (min 1s; defaults to 1s)
  http:
    url: https://cp.example/feeds   # required; ${ENV} expands
    timeout: 10s                    # per-request; defaults to 30s
    headers:                        # arbitrary request headers
      Authorization: "Bearer ${CP_TOKEN}"   # or "Basic <base64>", or use X-API-Key, etc.
    tls:                            # optional; same shape as the Postgres source TLS
      ca_file: ""
      cert_file: ""                 # cert_file + key_file: both or neither (mTLS)
      key_file: ""
      server_name: ""
      insecure_skip_verify: false
```

Expected response body:

```json
{
  "feeds": [
    { "url": "https://example.com/feed.xml", "interval": "5m", "sinks": ["out"] }
  ]
}
```

Each element is the same feed-spec shape the `file` source uses; both `url` and
`interval` are required for `serve` to schedule the feed. An empty list
(`{"feeds": []}`) is valid. Authenticate by setting request `headers` (bearer
token, HTTP basic, or an API-key header) and/or configure mutual TLS through the
`tls` block — see [Secure connections with TLS](./secure-connections-tls.md).

The source sends conditional-GET validators (`If-None-Match` / `If-Modified-Since`)
from the previous response's `ETag` / `Last-Modified`; a `304 Not Modified` reply
reuses the cached list. A failed fetch — unreachable host, non-2xx status, a body
that is not valid JSON, or a body missing the `feeds` key — keeps the **last
successful** feed list for this source, so a transient outage does not drop feeds.
A response missing the `feeds` key is additionally logged at warn (it usually
means the URL points at the wrong endpoint).
````

- [ ] **Step 3: Update the configuration reference**

In `docs/reference/configuration.md`, find the `feed_sources` documentation and add the `http` fields. Use the same style as the surrounding entries (locate the `postgres` feed-source block and add an `http` block beneath it):

```markdown
- `feed_sources[].http.url` — endpoint returning `{"feeds":[...]}` (required for `type: http`); `${ENV}` expands.
- `feed_sources[].http.timeout` — per-request timeout (default `30s`).
- `feed_sources[].http.headers` — request headers for auth (e.g. `Authorization`, `X-API-Key`); reserved cache headers `If-None-Match` / `If-Modified-Since` are rejected.
- `feed_sources[].http.tls` — `ca_file`, `cert_file`, `key_file`, `server_name`, `insecure_skip_verify` (client TLS / mTLS; `cert_file` and `key_file` are both-or-neither).
```

If the reference page documents feed sources in a table rather than a list, match that table's columns instead — keep the same format as the existing `postgres` rows.

- [ ] **Step 4: Update `example.yaml`**

In `internal/config/example.yaml`, update the `feed_sources` comment block:

1. Change the line `# Only "static", "file", and "postgres" types are implemented today.` to:
   `# Only "static", "file", "postgres", and "http" types are implemented today.`
2. Add this commented entry after the `postgres` example entry (and before the `static` entry):

```yaml
#   - type: http          # fetches the feed list from an HTTP endpoint; polled
#     name: control-plane
#     interval: 30s        # how often to re-fetch (min 1s)
#     http:
#       url: https://cp.example/feeds        # required; returns {"feeds":[...]}
#       timeout: 10s
#       headers:
#         Authorization: "Bearer ${CP_TOKEN}"   # or Basic <base64>, or X-API-Key
#       # tls:
#       #   ca_file: /etc/rss2msg/cp-ca.pem
#       #   cert_file: /etc/rss2msg/client.pem   # cert_file + key_file: both or neither
#       #   key_file: /etc/rss2msg/client-key.pem
```

- [ ] **Step 5: Run the doc link checker**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`.

- [ ] **Step 6: Commit**

```bash
git add docs/how-to/load-feeds-dynamically.md docs/reference/configuration.md internal/config/example.yaml
git commit -m "docs: document http feed source

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Final verification

- [ ] **Step 1: Full test + vet + lint**

Run:
```bash
task test
task vet
task lint
```
Expected: all green. (No integration suite needed — the http source is covered by in-process `httptest`; state stores / coordinators are untouched. Note this explicitly in the PR.)

- [ ] **Step 2: Confirm staged set is clean before any further commit**

Run: `git status`
Expected: working tree clean; no stray vault files. (Never `git add -A`.)

- [ ] **Step 3: Open the PR**

```bash
git push -u origin feat/http-feed-source
gh pr create --fill --base main
```
PR body should state: closes #161; summarize the `http` feed source, the headers+tls auth surface, and conditional GET; note that integration tests were not required (in-process httptest coverage). Per repo convention, ensure issue #161's body holds the final spec.

---

## Self-review notes (author)

- **Spec coverage:** config schema (Task 1) · validation incl. reserved-header guard (Task 2) · conditional GET + missing-key warn-log + error semantics (Task 3) · auth via headers (Task 3 `TestHTTPSendsHeaders`) · mTLS/custom-CA (Task 4) · wiring (Task 5) · docs/example (Task 6). All spec sections map to a task.
- **Type consistency:** `HTTPOptions`/`HTTPTLSOptions`/`feedListResponse`/`buildHTTPSourceTLS` names are used identically across Tasks 3–5; config types `HTTPFeedSourceConfig`/`FeedSourceHTTPTLSConfig` match between Tasks 1, 2, 5.
- **Decision baked in:** `feeds` key is fixed (not configurable) in v1; bare-array payloads are rejected by design (`TestHTTPBareArrayErrors`).
</content>
