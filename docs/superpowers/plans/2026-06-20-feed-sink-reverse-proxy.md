# Feed Sink Behind Reverse Proxy — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the feed sink reverse-proxy aware — derive Atom/RSS self-URLs and the real client IP from trusted forwarding headers, honoring a proxy-applied path prefix, gated by a `trusted_proxies` allowlist.

**Architecture:** A new pure helper file (`internal/sink/feed/proxy.go`) parses the `trusted_proxies` allowlist and, per request, derives the self-URL base (scheme/host/prefix) and the real client IP — but only when the request's direct peer is trusted. The HTTP handler computes the self-URL per request and injects `rel=self` into a render-cached, self-less body. Default (empty allowlist) = today's behavior, byte-for-byte.

**Tech Stack:** Go 1.25, `net`/`net/http` stdlib, `github.com/gorilla/feeds`, zerolog, Viper config, OpenTelemetry metrics. Tests with `go test -race`.

**Spec:** [docs/superpowers/specs/2026-06-20-feed-sink-reverse-proxy-design.md](../specs/2026-06-20-feed-sink-reverse-proxy-design.md) · Issue [#171](https://github.com/IAmBod/rss2msg/issues/171)

## Global Constraints

- Go 1.25; Cobra/Viper config; `mapstructure` tags on all config fields.
- Run `task test` (`go test -race ./...`) and `task vet` before any commit; `task lint` (golangci-lint v2) before the PR.
- Conventional Commits (`feat:`, `test:`, `docs:`, `refactor:`).
- **Staging hazard:** this repo is an Obsidian vault with auto-staging. NEVER `git add -A`/`git add .` — stage explicit pathspecs and verify with `git status` before each commit.
- Default behavior must not change: with `trusted_proxies` empty/unset, no forwarding header is ever read, and feed output is identical to current `main`.
- Client IP is **high-cardinality** — it goes into structured **log lines only**, never into a metric attribute.
- Forwarding headers are honored **only** when `http.Request.RemoteAddr` is inside the trusted set.
- All work happens in the existing worktree `.worktrees/feed-reverse-proxy` on branch `feat/feed-sink-reverse-proxy`. Run commands from that directory.

## File Structure

| File | Responsibility |
| --- | --- |
| `internal/sink/feed/proxy.go` *(new)* | `trustedProxies` parse/match; per-request `selfURL` derivation and `clientIP` recovery. Pure, no HTTP serving. |
| `internal/sink/feed/proxy_test.go` *(new)* | Unit tests for the above. |
| `internal/config/config.go` | Add `TrustedProxies []string` to `FeedSinkConfig`. |
| `internal/config/validate.go` | Validate each `trusted_proxies` entry (preset token or CIDR/IP). |
| `internal/config/validate_test.go` | Tests for the new validation. |
| `internal/sink/feed/render.go` | Drop `SelfRSS`/`SelfAtom` from `FeedMeta`; stop baking self-link into `ToAtom`; add per-request `injectAtomSelf` / `injectRSSSelf` helpers. |
| `internal/sink/feed/render_test.go` | Tests for the injection helpers. |
| `internal/sink/feed/server.go` | Carry `proxyConfig` + logger in `handlerConfig`; per-request derive self-URL, inject, recompute ETag over final body; cache the self-less body; log client IP on auth failure. |
| `internal/sink/feed/server_test.go` | Per-request self-URL + client-IP-log tests. |
| `internal/sink/feed/feed.go` | Add `TrustedProxies []string` to `Options`; parse it; stop setting `Meta.SelfRSS/SelfAtom`; build `proxyConfig`; pass logger to handler. |
| `internal/sink/feed/feed_test.go` | Update the existing self-link test to the per-request model; add behind-proxy test. |
| `cmd/rss2msg/wire.go` | Pass `f.TrustedProxies` into `feedsink.Options`. |
| `docs/how-to/sinks/feed.md` | Document `trusted_proxies`; rewrite "TLS vs reverse proxy". |

---

## Task 1: Config field + validation for `trusted_proxies`

**Files:**
- Modify: `internal/config/config.go` (`FeedSinkConfig`, ~line 352-369)
- Modify: `internal/config/validate.go` (feed `case "feed":`, after the absolute-URL check ~line 685-692)
- Test: `internal/config/validate_test.go`

**Interfaces:**
- Produces: `FeedSinkConfig.TrustedProxies []string` (mapstructure `trusted_proxies`). Each entry is a preset token (`private`, `all`) or a CIDR / bare IP. Invalid entries are a config error.

- [ ] **Step 1: Write the failing test**

Add to `internal/config/validate_test.go`:

```go
func TestValidate_FeedTrustedProxies(t *testing.T) {
	base := func(tp []string) Config {
		return Config{
			Feeds: []FeedConfig{{URL: "https://e.com/f.xml", Interval: time.Second}},
			Sinks: []SinkConfig{{
				Name: "f", Driver: "feed",
				Feed: FeedSinkConfig{Listen: ":0", TrustedProxies: tp},
			}},
		}
	}
	t.Run("valid mix", func(t *testing.T) {
		if _, err := Validate(base([]string{"private", "10.0.0.0/8", "127.0.0.1", "all", "::1"})); err != nil {
			t.Fatalf("want nil err, got %v", err)
		}
	})
	t.Run("invalid entry", func(t *testing.T) {
		_, err := Validate(base([]string{"not-an-ip"}))
		if err == nil {
			t.Fatal("want error for bogus trusted_proxies entry")
		}
	})
}
```

(If `Validate`'s signature differs in this file, match the existing test calls — e.g. `Validate(cfg)` returns `(warnings, error)`; look at neighbouring tests.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestValidate_FeedTrustedProxies -v`
Expected: FAIL — `TrustedProxies` field undefined (compile error).

- [ ] **Step 3: Add the config field**

In `internal/config/config.go`, inside `FeedSinkConfig` (after `HTTP3` line 366):

```go
	HTTP3           bool               `mapstructure:"http3"` // serve HTTP/3 (QUIC) alongside TCP; requires tls
	TrustedProxies  []string           `mapstructure:"trusted_proxies"` // CIDRs and/or presets (private, all); empty => forwarding headers ignored
	Auth            FeedAuthConfig     `mapstructure:"auth"`
```

- [ ] **Step 4: Add validation**

In `internal/config/validate.go`, inside `case "feed":`, immediately after the `for _, raw := range []string{f.Link, f.PublicURL}` absolute-URL block (after line 692), add:

```go
			for _, raw := range f.TrustedProxies {
				if err := validateTrustedProxyEntry(raw); err != nil {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): trusted_proxies entry %q: %w", i, s.Name, raw, err)
				}
			}
```

Then add this helper near the other feed validators in the same file:

```go
// validateTrustedProxyEntry accepts a preset token (private, all) or a CIDR /
// bare IP. Kept in lockstep with the runtime parser in internal/sink/feed.
func validateTrustedProxyEntry(raw string) error {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return fmt.Errorf("must not be empty")
	case "private", "all":
		return nil
	}
	if _, _, err := net.ParseCIDR(strings.TrimSpace(raw)); err == nil {
		return nil
	}
	if net.ParseIP(strings.TrimSpace(raw)) != nil {
		return nil
	}
	return fmt.Errorf("not a preset (private|all), CIDR, or IP")
}
```

Ensure `net` is imported in `validate.go` (add to the import block if absent).

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestValidate_FeedTrustedProxies -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/config/validate.go internal/config/validate_test.go
git status   # confirm ONLY these three files
git commit -m "feat(config): add feed sink trusted_proxies field and validation (#171)"
```

---

## Task 2: `trustedProxies` parser + membership test

**Files:**
- Create: `internal/sink/feed/proxy.go`
- Test: `internal/sink/feed/proxy_test.go`

**Interfaces:**
- Produces:
  - `type trustedProxies struct { nets []net.IPNet }`
  - `func parseTrustedProxies(entries []string) (*trustedProxies, error)` — returns `(nil, nil)` for empty input.
  - `func (t *trustedProxies) contains(ip net.IP) bool` — nil-safe (nil receiver → false).
  - `func (t *trustedProxies) trusts(remoteAddr string) bool` — splits host[:port], parses IP; nil-safe.

- [ ] **Step 1: Write the failing test**

Create `internal/sink/feed/proxy_test.go`:

```go
package feed

import "testing"

func TestParseTrustedProxies(t *testing.T) {
	if tp, err := parseTrustedProxies(nil); err != nil || tp != nil {
		t.Fatalf("empty => (nil,nil); got (%v,%v)", tp, err)
	}
	if _, err := parseTrustedProxies([]string{"bogus"}); err == nil {
		t.Fatal("want error for bogus entry")
	}
}

func TestTrustedProxies_Trusts(t *testing.T) {
	tp, err := parseTrustedProxies([]string{"private", "203.0.113.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]bool{
		"10.1.2.3:5555":    true,  // RFC1918 via "private"
		"127.0.0.1:80":     true,  // loopback via "private"
		"[::1]:443":        true,  // IPv6 loopback via "private"
		"203.0.113.9:1":    true,  // explicit CIDR
		"8.8.8.8:53":       false, // public
		"not-an-addr":      false, // unparseable
	}
	for addr, want := range cases {
		if got := tp.trusts(addr); got != want {
			t.Errorf("trusts(%q)=%v want %v", addr, got, want)
		}
	}
}

func TestTrustedProxies_NilSafe(t *testing.T) {
	var tp *trustedProxies
	if tp.trusts("10.0.0.1:1") {
		t.Fatal("nil trustedProxies must trust nothing")
	}
}

func TestTrustedProxies_All(t *testing.T) {
	tp, _ := parseTrustedProxies([]string{"all"})
	if !tp.trusts("8.8.8.8:1") || !tp.trusts("[2001:db8::1]:1") {
		t.Fatal("'all' must trust any peer")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/feed/ -run TestTrustedProxies -v`
Expected: FAIL — undefined `parseTrustedProxies` / `trustedProxies`.

- [ ] **Step 3: Implement the parser**

Create `internal/sink/feed/proxy.go`:

```go
package feed

import (
	"fmt"
	"net"
	"strings"
)

// trustedProxies is the set of upstream peers whose forwarding headers we honor.
// A nil *trustedProxies trusts nothing (the default), so all forwarding headers
// are ignored and the sink behaves as if directly exposed.
type trustedProxies struct {
	nets []net.IPNet
}

// presetCIDRs maps the convenience tokens to their CIDR expansions.
var presetCIDRs = map[string][]string{
	"private": {
		"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
		"127.0.0.0/8", "::1/128", "fc00::/7",
	},
	"all": {"0.0.0.0/0", "::/0"},
}

// parseTrustedProxies builds a trustedProxies from preset tokens (private, all),
// CIDRs, and bare IPs. Empty input returns (nil, nil) — trust nothing.
func parseTrustedProxies(entries []string) (*trustedProxies, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	var nets []net.IPNet
	add := func(cidr string) error {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			return err
		}
		nets = append(nets, *n)
		return nil
	}
	for _, raw := range entries {
		e := strings.TrimSpace(raw)
		if cidrs, ok := presetCIDRs[strings.ToLower(e)]; ok {
			for _, c := range cidrs {
				if err := add(c); err != nil {
					return nil, fmt.Errorf("preset %q: %w", e, err)
				}
			}
			continue
		}
		if strings.Contains(e, "/") {
			if err := add(e); err != nil {
				return nil, fmt.Errorf("cidr %q: %w", e, err)
			}
			continue
		}
		ip := net.ParseIP(e)
		if ip == nil {
			return nil, fmt.Errorf("entry %q: not a preset, CIDR, or IP", raw)
		}
		bits := 32
		if ip.To4() == nil {
			bits = 128
		}
		nets = append(nets, net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
	}
	return &trustedProxies{nets: nets}, nil
}

// contains reports whether ip falls in any trusted network. nil-safe.
func (t *trustedProxies) contains(ip net.IP) bool {
	if t == nil || ip == nil {
		return false
	}
	for i := range t.nets {
		if t.nets[i].Contains(ip) {
			return true
		}
	}
	return false
}

// trusts reports whether the peer at remoteAddr ("host:port" or "host") is
// trusted. nil-safe; unparseable addresses are untrusted.
func (t *trustedProxies) trusts(remoteAddr string) bool {
	if t == nil {
		return false
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	return t.contains(net.ParseIP(host))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sink/feed/ -run TestTrustedProxies -v && go test ./internal/sink/feed/ -run TestParseTrustedProxies -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sink/feed/proxy.go internal/sink/feed/proxy_test.go
git status
git commit -m "feat(feed): trusted-proxy allowlist parser and membership test (#171)"
```

---

## Task 3: Per-request self-URL derivation from forwarding headers

**Files:**
- Modify: `internal/sink/feed/proxy.go`
- Test: `internal/sink/feed/proxy_test.go`

**Interfaces:**
- Consumes: `trustedProxies` (Task 2).
- Produces:
  - `type proxyConfig struct { publicURL, link string; trusted *trustedProxies }` — `publicURL`/`link` are pre-trimmed of any trailing `/`.
  - `func (p proxyConfig) selfURL(r *http.Request, surfacePath string) string` — full absolute self-URL for a surface. Precedence: `publicURL` → trusted forwarding headers → request `Host` → `link`. Header-supplied prefix applies only in the trusted-header branch.

- [ ] **Step 1: Write the failing test**

Add to `internal/sink/feed/proxy_test.go`:

```go
import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func reqWith(remote string, h map[string]string, tls bool) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://internal:8088/atom", nil)
	r.RemoteAddr = remote
	r.Host = "internal:8088"
	for k, v := range h {
		r.Header.Set(k, v)
	}
	if tls {
		r.TLS = &tlsConnState
	}
	return r
}

func TestProxyConfig_SelfURL(t *testing.T) {
	trusted, _ := parseTrustedProxies([]string{"private"})

	t.Run("public_url wins, ignores headers", func(t *testing.T) {
		p := proxyConfig{publicURL: "https://feeds.example.com", trusted: trusted}
		r := reqWith("10.0.0.1:5000", map[string]string{
			"X-Forwarded-Host": "evil.example", "X-Forwarded-Proto": "https",
			"X-Forwarded-Prefix": "/nope",
		}, false)
		if got := p.selfURL(r, "/atom"); got != "https://feeds.example.com/atom" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("trusted headers build base with prefix", func(t *testing.T) {
		p := proxyConfig{link: "https://site.example", trusted: trusted}
		r := reqWith("10.0.0.1:5000", map[string]string{
			"X-Forwarded-Proto": "https", "X-Forwarded-Host": "feeds.example.com",
			"X-Forwarded-Prefix": "/news/",
		}, false)
		if got := p.selfURL(r, "/atom"); got != "https://feeds.example.com/news/atom" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("untrusted peer ignores headers, uses request host", func(t *testing.T) {
		p := proxyConfig{trusted: trusted}
		r := reqWith("8.8.8.8:5000", map[string]string{
			"X-Forwarded-Proto": "https", "X-Forwarded-Host": "spoof.example",
		}, false)
		if got := p.selfURL(r, "/atom"); got != "http://internal:8088/atom" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("Forwarded header wins over X-Forwarded for proto/host", func(t *testing.T) {
		p := proxyConfig{trusted: trusted}
		r := reqWith("10.0.0.1:5000", map[string]string{
			"Forwarded":         `proto=https;host=fwd.example`,
			"X-Forwarded-Proto": "http", "X-Forwarded-Host": "xfwd.example",
		}, false)
		if got := p.selfURL(r, "/rss"); got != "https://fwd.example/rss" {
			t.Fatalf("got %q", got)
		}
	})
}
```

Also add, once, at package scope in the test file (a reusable non-nil TLS state):

```go
var tlsConnState = tls.ConnectionState{} // import crypto/tls
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/feed/ -run TestProxyConfig_SelfURL -v`
Expected: FAIL — undefined `proxyConfig`.

- [ ] **Step 3: Implement derivation**

Append to `internal/sink/feed/proxy.go` (and add `"net/http"` to the import block):

```go
// proxyConfig derives per-request public URLs and client IPs. publicURL and
// link are pre-trimmed of any trailing slash. trusted is nil when no proxies
// are configured (then all forwarding headers are ignored).
type proxyConfig struct {
	publicURL string
	link      string
	trusted   *trustedProxies
}

// forwarded holds the proxy-supplied scheme/host/prefix for one request, or
// zero values when the peer is untrusted or sent nothing.
type forwarded struct {
	proto, host, prefix string
}

// selfURL returns the absolute URL of a surface for this request. Precedence:
// publicURL (static, authoritative) -> trusted forwarding headers -> request
// Host -> link. A header-supplied prefix is applied only in the headers branch.
func (p proxyConfig) selfURL(r *http.Request, surfacePath string) string {
	if p.publicURL != "" {
		return p.publicURL + surfacePath
	}
	fw := p.parseForwarded(r)
	proto, host := fw.proto, fw.host
	if host == "" {
		host = r.Host
	}
	if proto == "" {
		if r.TLS != nil {
			proto = "https"
		} else {
			proto = "http"
		}
	}
	if host != "" {
		return proto + "://" + host + fw.prefix + surfacePath
	}
	if p.link != "" {
		return p.link + surfacePath
	}
	return surfacePath
}

// parseForwarded extracts scheme/host/prefix from forwarding headers, but only
// when the direct peer is trusted. RFC 7239 Forwarded wins over X-Forwarded-*
// for proto/host; X-Forwarded-Prefix is the only prefix source.
func (p proxyConfig) parseForwarded(r *http.Request) forwarded {
	if !p.trusted.trusts(r.RemoteAddr) {
		return forwarded{}
	}
	var f forwarded
	if proto := firstValue(r.Header.Get("X-Forwarded-Proto")); proto != "" {
		f.proto = proto
	}
	if host := firstValue(r.Header.Get("X-Forwarded-Host")); host != "" {
		f.host = host
	}
	// RFC 7239 Forwarded overrides the X-Forwarded-* equivalents.
	if fwd := r.Header.Get("Forwarded"); fwd != "" {
		for _, kv := range strings.Split(firstValue(fwd), ";") {
			k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
			if !ok {
				continue
			}
			v = strings.Trim(v, `"`)
			switch strings.ToLower(k) {
			case "proto":
				f.proto = v
			case "host":
				f.host = v
			}
		}
	}
	if pfx := firstValue(r.Header.Get("X-Forwarded-Prefix")); pfx != "" {
		f.prefix = normalizePrefix(pfx)
	}
	return f
}

// firstValue returns the first comma-separated token, trimmed. Proxies append
// to forwarding headers, so the left-most value is closest to the client.
func firstValue(h string) string {
	if h == "" {
		return ""
	}
	if i := strings.IndexByte(h, ','); i >= 0 {
		h = h[:i]
	}
	return strings.TrimSpace(h)
}

// normalizePrefix ensures a leading slash and no trailing slash ("" stays "").
func normalizePrefix(p string) string {
	p = strings.TrimRight(strings.TrimSpace(p), "/")
	if p == "" {
		return ""
	}
	if p[0] != '/' {
		p = "/" + p
	}
	return p
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sink/feed/ -run TestProxyConfig_SelfURL -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sink/feed/proxy.go internal/sink/feed/proxy_test.go
git status
git commit -m "feat(feed): derive self-URL from trusted forwarding headers (#171)"
```

---

## Task 4: Real client IP recovery

**Files:**
- Modify: `internal/sink/feed/proxy.go`
- Test: `internal/sink/feed/proxy_test.go`

**Interfaces:**
- Consumes: `proxyConfig`, `trustedProxies` (Tasks 2-3).
- Produces: `func (p proxyConfig) clientIP(r *http.Request) string` — the real client IP. Walks `X-Forwarded-For` right-to-left skipping trusted hops; falls back to the direct peer IP.

- [ ] **Step 1: Write the failing test**

Add to `internal/sink/feed/proxy_test.go`:

```go
func TestProxyConfig_ClientIP(t *testing.T) {
	trusted, _ := parseTrustedProxies([]string{"private"})
	p := proxyConfig{trusted: trusted}

	t.Run("untrusted peer => peer IP, headers ignored", func(t *testing.T) {
		r := reqWith("8.8.8.8:1", map[string]string{"X-Forwarded-For": "1.2.3.4"}, false)
		if got := p.clientIP(r); got != "8.8.8.8" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("trusted peer => right-most untrusted in XFF chain", func(t *testing.T) {
		r := reqWith("10.0.0.1:1", map[string]string{
			"X-Forwarded-For": "203.0.113.7, 172.16.0.9, 10.0.0.2",
		}, false)
		if got := p.clientIP(r); got != "203.0.113.7" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("trusted peer, no XFF => peer IP", func(t *testing.T) {
		r := reqWith("10.0.0.1:1", nil, false)
		if got := p.clientIP(r); got != "10.0.0.1" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("all hops trusted => left-most", func(t *testing.T) {
		r := reqWith("10.0.0.1:1", map[string]string{"X-Forwarded-For": "10.0.0.9, 10.0.0.2"}, false)
		if got := p.clientIP(r); got != "10.0.0.9" {
			t.Fatalf("got %q", got)
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/feed/ -run TestProxyConfig_ClientIP -v`
Expected: FAIL — undefined `clientIP`.

- [ ] **Step 3: Implement client-IP recovery**

Append to `internal/sink/feed/proxy.go`:

```go
// peerIP returns the host portion of r.RemoteAddr.
func peerIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// clientIP returns the real client IP. When the direct peer is untrusted (or no
// proxies are configured) it is the peer itself. Otherwise we walk the
// X-Forwarded-For chain right-to-left, skipping trusted hops; the first
// untrusted address is the client. If every hop is trusted we return the
// left-most entry; with no chain we return the peer.
func (p proxyConfig) clientIP(r *http.Request) string {
	peer := peerIP(r)
	if !p.trusted.trusts(r.RemoteAddr) {
		return peer
	}
	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return peer
	}
	parts := strings.Split(xff, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		ip := strings.TrimSpace(parts[i])
		if ip == "" {
			continue
		}
		if !p.trusted.contains(net.ParseIP(ip)) {
			return ip
		}
	}
	return strings.TrimSpace(parts[0])
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sink/feed/ -run TestProxyConfig_ClientIP -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sink/feed/proxy.go internal/sink/feed/proxy_test.go
git status
git commit -m "feat(feed): recover real client IP from trusted X-Forwarded-For (#171)"
```

---

## Task 5: Per-request self-link injection in render layer

**Files:**
- Modify: `internal/sink/feed/render.go` (`FeedMeta` ~line 14-20, `ToAtom` ~line 105-132)
- Test: `internal/sink/feed/render_test.go`

**Interfaces:**
- Produces:
  - `injectAtomSelf(atomBody, selfURL string) string` — inserts `<link href rel="self">` into `<feed>`; no-op when `selfURL == ""`.
  - `injectRSSSelf(rssBody, selfURL string) string` — adds `xmlns:atom` to `<rss>` and an `<atom:link rel="self">` into `<channel>`; no-op when `selfURL == ""`.
  - `ToAtom(m, changes)` no longer injects `rel=self` (returns the self-less body).
  - `FeedMeta` drops `SelfRSS` and `SelfAtom` fields.

- [ ] **Step 1: Write the failing test**

Add to `internal/sink/feed/render_test.go`:

```go
func TestInjectAtomSelf(t *testing.T) {
	body, err := ToAtom(FeedMeta{Title: "t", Link: "https://x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(body, `rel="self"`) {
		t.Fatal("ToAtom must NOT bake a self link anymore")
	}
	out := injectAtomSelf(body, "https://feeds.example.com/atom")
	if !strings.Contains(out, `<link href="https://feeds.example.com/atom" rel="self">`) {
		t.Fatalf("self link not injected:\n%s", out)
	}
	if injectAtomSelf(body, "") != body {
		t.Fatal("empty selfURL must be a no-op")
	}
}

func TestInjectRSSSelf(t *testing.T) {
	body, err := ToRSS(FeedMeta{Title: "t", Link: "https://x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := injectRSSSelf(body, "https://feeds.example.com/rss")
	if !strings.Contains(out, `xmlns:atom="http://www.w3.org/2005/Atom"`) {
		t.Fatalf("atom namespace not added:\n%s", out)
	}
	if !strings.Contains(out, `<atom:link href="https://feeds.example.com/rss" rel="self" type="application/rss+xml"></atom:link>`) {
		t.Fatalf("rss self link not injected:\n%s", out)
	}
	if injectRSSSelf(body, "") != body {
		t.Fatal("empty selfURL must be a no-op")
	}
}
```

Ensure `strings` is imported in `render_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/feed/ -run 'TestInject(Atom|RSS)Self' -v`
Expected: FAIL — undefined `injectAtomSelf`/`injectRSSSelf`, and `ToAtom` still bakes the self link (the `rel="self"` assertion fails).

- [ ] **Step 3: Update `FeedMeta` and `ToAtom`, add injectors**

In `internal/sink/feed/render.go`, change `FeedMeta` to drop the self fields:

```go
// FeedMeta is the feed-level metadata sourced from config.
type FeedMeta struct {
	Title       string
	Link        string // website (rel=alternate / channel link)
	Description string
}
```

Replace `ToAtom` (the whole function) with a self-less renderer:

```go
// ToAtom renders Atom 1.0 without a rel=self link. The handler injects the
// per-request self link via injectAtomSelf so one cached body serves every host.
func ToAtom(m FeedMeta, changes []model.Change) (string, error) {
	return buildFeed(m, changes).ToAtom()
}

// injectAtomSelf inserts <link href=selfURL rel="self"> as a child of <feed>.
// gorilla's AtomFeed.Link holds a single link, so string injection is required.
// No-op when selfURL is empty or no <feed> tag is found.
func injectAtomSelf(atomBody, selfURL string) string {
	if selfURL == "" {
		return atomBody
	}
	feedTagStart := strings.Index(atomBody, "<feed")
	if feedTagStart == -1 {
		return atomBody
	}
	feedTagEnd := strings.Index(atomBody[feedTagStart:], ">")
	if feedTagEnd == -1 {
		return atomBody
	}
	insertAt := feedTagStart + feedTagEnd + 1
	link := `<link href="` + escapeXML(selfURL) + `" rel="self"></link>`
	return atomBody[:insertAt] + "\n  " + link + atomBody[insertAt:]
}

// injectRSSSelf adds the atom namespace to <rss> and an <atom:link rel="self">
// inside <channel>. No-op when selfURL is empty or the tags are not found.
func injectRSSSelf(rssBody, selfURL string) string {
	if selfURL == "" {
		return rssBody
	}
	const ns = ` xmlns:atom="http://www.w3.org/2005/Atom"`
	rssStart := strings.Index(rssBody, "<rss")
	if rssStart == -1 {
		return rssBody
	}
	rssEnd := strings.Index(rssBody[rssStart:], ">")
	if rssEnd == -1 {
		return rssBody
	}
	nsAt := rssStart + rssEnd
	rssBody = rssBody[:nsAt] + ns + rssBody[nsAt:]

	chanTag := "<channel>"
	chanAt := strings.Index(rssBody, chanTag)
	if chanAt == -1 {
		return rssBody
	}
	insertAt := chanAt + len(chanTag)
	link := `<atom:link href="` + escapeXML(selfURL) + `" rel="self" type="application/rss+xml"></atom:link>`
	return rssBody[:insertAt] + "\n    " + link + rssBody[insertAt:]
}
```

(`ToRSS` stays as-is — it already returns a self-less body.)

- [ ] **Step 4: Run render tests**

Run: `go test ./internal/sink/feed/ -run 'TestInject|TestToRSS|TestToAtom' -v`
Expected: PASS for the new injectors. **Other tests in the package will not compile yet** because `feed.go`/`server.go` still reference `Meta.SelfRSS`/`SelfAtom` — that is fixed in Tasks 6 and 8. Run the targeted render tests in isolation here; the full package build is restored in Task 8.

If `go test ./internal/sink/feed/` is required green before commit, do Tasks 5, 6, and 8 together as one unit before running the full package — but commit each task's diff separately. (The split is for review clarity; the compile barrier is internal to the package.)

- [ ] **Step 5: Commit**

```bash
git add internal/sink/feed/render.go internal/sink/feed/render_test.go
git status
git commit -m "refactor(feed): make self-link injection per-request, add RSS self-link (#171)"
```

---

## Task 6: Handler — per-request self-URL, cache self-less body, ETag over final

**Files:**
- Modify: `internal/sink/feed/server.go` (`handlerConfig`, `ServeHTTP`, `document`)
- Test: `internal/sink/feed/server_test.go`

**Interfaces:**
- Consumes: `proxyConfig.selfURL` (Task 3), `injectAtomSelf`/`injectRSSSelf` (Task 5).
- Produces: `handlerConfig` gains `proxy proxyConfig`. The render cache (keyed by `path`) stores the **self-less** body; the response body and ETag are assembled per request from the cached body + the request's self-URL.

- [ ] **Step 1: Write the failing test**

Add to `internal/sink/feed/server_test.go`:

```go
func TestHandler_SelfURLPerRequest(t *testing.T) {
	st := newMemoryStore(10)
	trusted, _ := parseTrustedProxies([]string{"private"})
	h := newHandler(handlerConfig{
		store: st, meta: FeedMeta{Title: "t", Link: "https://site"},
		maxItems: 10, rssPath: "/rss", atomPath: "/atom",
		proxy: proxyConfig{link: "https://site", trusted: trusted},
	})

	do := func(remote string, hdr map[string]string) string {
		r := httptest.NewRequest(http.MethodGet, "http://internal/atom", nil)
		r.RemoteAddr = remote
		r.Host = "internal"
		for k, v := range hdr {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Body.String()
	}

	a := do("10.0.0.1:9", map[string]string{"X-Forwarded-Proto": "https", "X-Forwarded-Host": "a.example"})
	b := do("10.0.0.1:9", map[string]string{"X-Forwarded-Proto": "https", "X-Forwarded-Host": "b.example"})
	if !strings.Contains(a, `href="https://a.example/atom" rel="self"`) {
		t.Fatalf("host a self link missing:\n%s", a)
	}
	if !strings.Contains(b, `href="https://b.example/atom" rel="self"`) {
		t.Fatalf("host b self link missing (cache leaked host a?):\n%s", b)
	}
}
```

Ensure `net/http`, `net/http/httptest`, `strings` are imported in `server_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/feed/ -run TestHandler_SelfURLPerRequest -v`
Expected: FAIL — `handlerConfig` has no `proxy` field.

- [ ] **Step 3: Add `proxy` to `handlerConfig` and thread self-URL**

In `internal/sink/feed/server.go`, add the field to `handlerConfig` (after `atomAuth`):

```go
	rssAuth         *SurfaceAuth
	atomAuth        *SurfaceAuth
	proxy           proxyConfig
	startedAt       time.Time
```

In `ServeHTTP`, replace the `doc, err := h.document(r.Context(), path)` call with a self-URL-aware one:

```go
	selfURL := h.cfg.proxy.selfURL(r, path)
	doc, err := h.document(r.Context(), path, selfURL)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
```

- [ ] **Step 4: Rework `document` to cache the self-less body**

Replace the `document` method with:

```go
// document renders (and caches) the self-less body for path, then assembles the
// per-request response: the self link is injected and the ETag is computed over
// the final body so two hosts get distinct ETags from one cached render.
func (h *handler) document(ctx context.Context, path, selfURL string) (cachedDoc, error) {
	raw, err := h.rawBody(ctx, path)
	if err != nil {
		return cachedDoc{}, err
	}
	var body string
	if path == h.cfg.rssPath {
		body = injectRSSSelf(raw.body, selfURL)
	} else {
		body = injectAtomSelf(raw.body, selfURL)
	}
	sum := sha256.Sum256([]byte(body))
	return cachedDoc{
		body:     []byte(body),
		ct:       raw.ct,
		etag:     `"` + hex.EncodeToString(sum[:]) + `"`,
		modified: raw.modified,
		at:       raw.at,
	}, nil
}

// rawBody returns the cached, self-less rendered body for path, rendering and
// caching it on miss. The cache key is the path; the body is host-independent.
func (h *handler) rawBody(ctx context.Context, path string) (cachedDoc, error) {
	if h.cfg.renderCacheTTL > 0 {
		h.mu.Lock()
		if d, ok := h.cache[path]; ok && time.Since(d.at) < h.cfg.renderCacheTTL {
			h.mu.Unlock()
			return d, nil
		}
		h.mu.Unlock()
	}
	changes, err := h.cfg.store.Recent(ctx, h.cfg.maxItems)
	if err != nil {
		return cachedDoc{}, err
	}
	var body, ct string
	if path == h.cfg.rssPath {
		ct = "application/rss+xml"
		body, err = ToRSS(h.cfg.meta, changes)
	} else {
		ct = "application/atom+xml"
		body, err = ToAtom(h.cfg.meta, changes)
	}
	if err != nil {
		return cachedDoc{}, err
	}
	doc := cachedDoc{body: []byte(body), ct: ct, modified: h.lastModified(changes), at: time.Now()}
	if h.cfg.renderCacheTTL > 0 {
		h.mu.Lock()
		h.cache[path] = doc
		h.mu.Unlock()
	}
	return doc, nil
}
```

(The cached `cachedDoc.etag` field is now unused for cache entries; it is only set on the returned response doc. Leaving the struct field is fine — `ServeHTTP` reads `doc.etag` from the response doc.)

- [ ] **Step 5: Run the test (and the package render/server tests)**

Run: `go test ./internal/sink/feed/ -run 'TestHandler_SelfURLPerRequest|TestInject' -v`
Expected: PASS. (Full-package green comes after Task 8 restores `feed.go`.)

- [ ] **Step 6: Commit**

```bash
git add internal/sink/feed/server.go internal/sink/feed/server_test.go
git status
git commit -m "feat(feed): assemble self-URL per request over cached self-less body (#171)"
```

---

## Task 7: Log client IP on auth failure

**Files:**
- Modify: `internal/sink/feed/server.go` (`handlerConfig`, `ServeHTTP` auth-failure branch, add helper)
- Test: `internal/sink/feed/server_test.go`

**Interfaces:**
- Consumes: `proxyConfig.clientIP` (Task 4).
- Produces: `handlerConfig` gains `logger zerolog.Logger`. On auth failure the handler emits one `Warn` log with `surface`, `reason`, and `client_ip`. Client IP is **not** added to any metric attribute.

- [ ] **Step 1: Write the failing test**

Add to `internal/sink/feed/server_test.go`:

```go
func TestHandler_LogsClientIPOnAuthFailure(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	trusted, _ := parseTrustedProxies([]string{"private"})
	h := newHandler(handlerConfig{
		store: newMemoryStore(10), meta: FeedMeta{Title: "t"},
		maxItems: 10, rssPath: "/rss", atomPath: "/atom",
		atomAuth: &SurfaceAuth{BearerTokens: []NamedSecret{{Name: "x", Secret: "good"}}},
		proxy:    proxyConfig{trusted: trusted},
		logger:   logger,
	})
	r := httptest.NewRequest(http.MethodGet, "http://internal/atom", nil)
	r.RemoteAddr = "10.0.0.1:5"
	r.Header.Set("X-Forwarded-For", "203.0.113.50")
	r.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
	if !strings.Contains(buf.String(), `"client_ip":"203.0.113.50"`) {
		t.Fatalf("auth-failure log missing client_ip:\n%s", buf.String())
	}
}
```

Ensure `bytes` and `github.com/rs/zerolog` are imported in `server_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/feed/ -run TestHandler_LogsClientIPOnAuthFailure -v`
Expected: FAIL — `handlerConfig` has no `logger` field.

- [ ] **Step 3: Add logger and emit on failure**

In `internal/sink/feed/server.go`, import `"github.com/rs/zerolog"` and add to `handlerConfig`:

```go
	proxy           proxyConfig
	logger          zerolog.Logger
	startedAt       time.Time
```

In `ServeHTTP`, in the auth-failure branch, add the log right before/after recording the metric:

```go
	name, ok := authenticate(a, r)
	if !ok {
		reason := authFailReason(a, r)
		h.recordAuthFailure(r.Context(), h.surfaceName(path), reason)
		h.logger.Warn().
			Str("sink_surface", h.surfaceName(path)).
			Str("reason", reason).
			Str("client_ip", h.cfg.proxy.clientIP(r)).
			Msg("feed sink auth failure")
		writeAuthChallenge(a, w)
		return
	}
```

(`zerolog.Logger`'s zero value is a no-op writer, so handlers built without a logger stay silent — matching the Publisher's existing "zero value => no logging" convention.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sink/feed/ -run TestHandler_LogsClientIPOnAuthFailure -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sink/feed/server.go internal/sink/feed/server_test.go
git status
git commit -m "feat(feed): log real client IP on auth failure (#171)"
```

---

## Task 8: Wire `Options` → handler; drop init-time self baking

**Files:**
- Modify: `internal/sink/feed/feed.go` (`Options`, `New`)
- Test: `internal/sink/feed/feed_test.go`

**Interfaces:**
- Consumes: `parseTrustedProxies` (Task 2), `proxyConfig` (Task 3), the new `handlerConfig` fields (Tasks 6-7).
- Produces: `Options` gains `TrustedProxies []string`. `New` parses it, builds `proxyConfig`, and passes `proxy` + `logger` into `newHandler`. `New` no longer sets `Meta.SelfRSS`/`SelfAtom` (those fields are gone).

- [ ] **Step 1: Write/adjust the failing test**

The existing `TestPublisher_SelfLinkUsesPublicURL` (feed_test.go ~line 213) asserts the Atom self link reflects `public_url`. It should still pass once `New` plumbs `PublicURL` into `proxyConfig` — keep it. Add a behind-proxy test:

```go
func TestPublisher_SelfLinkFromTrustedProxy(t *testing.T) {
	p, err := New(context.Background(), Options{
		Name: "f", Listen: "127.0.0.1:0",
		Meta:           FeedMeta{Title: "t", Link: "https://site"},
		Atom:           Surface{Enabled: true, Path: "/atom"},
		TrustedProxies: []string{"private"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	r := httptest.NewRequest(http.MethodGet, "http://"+p.Addr()+"/atom", nil)
	r.RemoteAddr = "10.0.0.1:5"
	r.Header.Set("X-Forwarded-Proto", "https")
	r.Header.Set("X-Forwarded-Host", "feeds.example.com")
	w := httptest.NewRecorder()
	p.server.Handler.ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), `href="https://feeds.example.com/atom" rel="self"`) {
		t.Fatalf("self link not from proxy headers:\n%s", w.Body.String())
	}
}
```

(If `TestPublisher_SelfLinkUsesPublicURL` references `Meta.SelfAtom`, update it to assert against the served HTTP body instead, like above but with `PublicURL` set and no proxy headers.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/feed/ -run TestPublisher_SelfLink -v`
Expected: FAIL — `Options` has no `TrustedProxies`; package may still reference removed `Meta.SelfRSS/SelfAtom`.

- [ ] **Step 3: Update `Options` and `New`**

In `internal/sink/feed/feed.go`, add to `Options` (after `HTTP3`):

```go
	HTTP3           bool // serve HTTP/3 over QUIC alongside TCP; requires TLS
	TrustedProxies  []string // CIDRs/presets; empty => forwarding headers ignored
```

In `New`, replace the self-baking block:

```go
	rss := surfacePath(o.RSS, "/rss")
	atom := surfacePath(o.Atom, "/atom")
	selfBase := o.PublicURL
	if selfBase == "" {
		selfBase = o.Meta.Link
	}
	selfBase = strings.TrimRight(selfBase, "/")
	o.Meta.SelfRSS = selfBase + rss
	o.Meta.SelfAtom = selfBase + atom
```

with proxy-config construction:

```go
	rss := surfacePath(o.RSS, "/rss")
	atom := surfacePath(o.Atom, "/atom")
	trusted, err := parseTrustedProxies(o.TrustedProxies)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("feed sink %q: trusted_proxies: %w", o.Name, err)
	}
	proxy := proxyConfig{
		publicURL: strings.TrimRight(o.PublicURL, "/"),
		link:      strings.TrimRight(o.Meta.Link, "/"),
		trusted:   trusted,
	}
```

Then pass them into `newHandler`:

```go
	h := newHandler(handlerConfig{
		store: store, meta: o.Meta, maxItems: o.MaxItems,
		rssPath: rss, atomPath: atom,
		renderCacheTTL: o.RenderCacheTTL, cacheControlTTL: o.CacheControlTTL,
		rssAuth: o.RSSAuth, atomAuth: o.AtomAuth,
		proxy: proxy, logger: o.Logger, startedAt: time.Now(),
	})
```

(`store, err :=` already declares `err` earlier in `New`, so use `=` not `:=` for the `parseTrustedProxies` error if `err` is in scope — verify and adjust to keep the compiler happy. `strings` is already imported.)

- [ ] **Step 4: Run the FULL package test suite**

Run: `go test -race ./internal/sink/feed/...`
Expected: PASS — the package compiles end-to-end now that `Meta.SelfRSS/SelfAtom` references are gone and `Options.TrustedProxies` exists.

- [ ] **Step 5: Commit**

```bash
git add internal/sink/feed/feed.go internal/sink/feed/feed_test.go
git status
git commit -m "feat(feed): plumb trusted_proxies into publisher, drop init-time self URLs (#171)"
```

---

## Task 9: Wire config → sink Options in `wire.go`

**Files:**
- Modify: `cmd/rss2msg/wire.go` (~line 596-615, the `feedsink.New(...)` call)

**Interfaces:**
- Consumes: `FeedSinkConfig.TrustedProxies` (Task 1), `feedsink.Options.TrustedProxies` (Task 8).

- [ ] **Step 1: Add the field to the Options literal**

In `cmd/rss2msg/wire.go`, in the `feedsink.New(ctx, feedsink.Options{...})` literal, add after `TLSCertFile: ..., HTTP3: f.HTTP3,`:

```go
			TLSCertFile: f.TLS.CertFile, TLSKeyFile: f.TLS.KeyFile, HTTP3: f.HTTP3,
			TrustedProxies: f.TrustedProxies,
			RSSAuth:        toFeedSurfaceAuth(f.EffectiveAuth(f.RSS)),
```

- [ ] **Step 2: Build and run command tests**

Run: `go build ./... && go test ./cmd/... -run Feed -v`
Expected: build succeeds; any feed-related command tests pass (or are unaffected).

- [ ] **Step 3: Commit**

```bash
git add cmd/rss2msg/wire.go
git status
git commit -m "feat(cmd): pass feed sink trusted_proxies into the publisher (#171)"
```

---

## Task 10: Documentation

**Files:**
- Modify: `docs/how-to/sinks/feed.md` (YAML example ~line 50, config table ~line 77-78, "TLS vs reverse proxy" ~line 253-262)

**Interfaces:** none (docs only).

- [ ] **Step 1: Add `trusted_proxies` to the YAML example**

In the example block, after the `http3: false` line (~line 50):

```yaml
      http3: false                    # optional; also serve HTTP/3 (QUIC) on the same port. Requires tls.
      trusted_proxies: []             # optional; CIDRs and/or presets (private, all). Empty => forwarding headers ignored.
```

- [ ] **Step 2: Add a config-table row**

After the `http3` row (~line 77):

```markdown
| `trusted_proxies`   | no       | `[]` (none)   | Upstream proxies whose `X-Forwarded-*` / `Forwarded` headers are honored, as CIDRs and/or presets (`private` = RFC1918 + loopback + ULA; `all` = any). Empty disables all header parsing. See [Behind a reverse proxy](#tls-vs-reverse-proxy). |
```

- [ ] **Step 3: Rewrite the "TLS vs reverse proxy" section**

Replace the section body (lines ~253-262) with:

```markdown
## TLS vs reverse proxy

To serve HTTPS directly from rss2msg, set `tls.cert_file` and `tls.key_file`.
Omit the `tls` block to serve plain HTTP and terminate TLS upstream (e.g. behind
a reverse proxy or load balancer).

By default the feed sink ignores all forwarding headers, so behind a proxy you
must set `public_url` to the externally-reachable base URL for the Atom/RSS
`rel=self` links to be correct (it falls back to `link` when unset).

Alternatively, list your proxies in `trusted_proxies` (CIDRs, or the presets
`private` / `all`). When a request's direct peer is in that set, rss2msg derives
the self-URL from `X-Forwarded-Proto`, `X-Forwarded-Host`, and `X-Forwarded-Prefix`
(or the RFC 7239 `Forwarded` header), and recovers the real client IP from
`X-Forwarded-For` for auth-failure logs. `public_url`, when set, always wins over
forwarding headers. Headers from an untrusted peer are never honored, so a client
hitting the listener directly cannot spoof its self-URL or client IP. The proxy
is expected to strip any `X-Forwarded-Prefix` before forwarding; rss2msg only
prepends it to self-links and does not rewrite internal routes. `trusted_proxies:
[all]` honors headers from any source — only safe when the listener is not
publicly reachable.
```

- [ ] **Step 4: Run the doc link checker**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`.

- [ ] **Step 5: Commit**

```bash
git add docs/how-to/sinks/feed.md
git status
git commit -m "docs(feed): document trusted_proxies and reverse-proxy self-URLs (#171)"
```

---

## Task 11: Full gate + PR

**Files:** none (verification).

- [ ] **Step 1: Full test + vet + lint**

Run:
```bash
task test
task vet
task lint
```
Expected: all pass. (Integration tests are not required — this change touches no store/coordinator backend behavior; the feed sink store paths are unchanged. Say so explicitly in the PR.)

- [ ] **Step 2: Confirm default behavior unchanged**

Run: `go test ./internal/sink/feed/ -run 'TestToRSS|TestToAtom|TestPublisher' -v`
Confirm that with no `trusted_proxies` and a set `public_url`/`link`, the served bodies match the prior self-link expectations.

- [ ] **Step 3: Open the PR**

```bash
git push -u origin feat/feed-sink-reverse-proxy
gh pr create --title "feat(feed): reverse-proxy support (self-URLs, prefix, client IP) (#171)" \
  --body "Implements #171. Adds opt-in \`trusted_proxies\` allowlist; derives Atom/RSS self-URLs (incl. X-Forwarded-Prefix) and real client IP from trusted forwarding headers; logs client IP on auth failure. Default (empty allowlist) is byte-identical to current behavior.

Closes #171.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review

**Spec coverage:**
- Trust model (`trusted_proxies`, presets, empty=off, peer-gated) → Tasks 1, 2, 8.
- Self-URL precedence (public_url → headers → Host → link) + prefix → Task 3.
- Render-cache restructure (self-less cached body, per-request injection, ETag over final) → Tasks 5, 6.
- Real client IP (XFF right-to-left, skip trusted) + surfacing → Tasks 4, 7.
- Config + validation + wiring → Tasks 1, 9.
- Docs (table + reverse-proxy section) → Task 10. **Note:** the feed sink is not present in `internal/config/example.yaml` / `examples/config.example.yaml` (only in `docs/how-to/sinks/feed.md`), so the example-yaml drift guard does not apply — no edits needed there.

**Deviation from spec (intentional, flagged for review):** the spec said client IP feeds "auth audit/metric attributes." Client IP is high-cardinality and must not be a metric label, so Task 7 puts it in a structured **log line** instead (Global Constraints). Metric attributes (surface, reason) are unchanged.

**Placeholder scan:** none — every code step shows complete code.

**Type consistency:** `parseTrustedProxies`, `trustedProxies.{contains,trusts}`, `proxyConfig.{selfURL,clientIP,parseForwarded}`, `injectAtomSelf`/`injectRSSSelf`, `handlerConfig.{proxy,logger}`, `Options.TrustedProxies`, `document(ctx, path, selfURL)`, `rawBody(ctx, path)` — names are used consistently across tasks.
