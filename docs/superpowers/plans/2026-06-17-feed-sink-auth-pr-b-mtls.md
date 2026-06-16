# Feed Sink Auth — PR-B (mTLS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add mutual-TLS client-certificate auth to the feed sink: a sink-wide CA pool (`feed.mtls_ca_file`) plus a per-surface `auth.mtls.require` flag that is AND-combined with PR-A's token credentials.

**Architecture:** One TLS listener per sink, so the CA pool is sink-wide; the listener uses `tls.VerifyClientCertIfGiven` whenever a CA is configured (verifies a presented cert but never demands one, so public surfaces still work on the same port). Per-surface enforcement lives in a new `authorize()` evaluator that runs the mTLS gate *before* PR-A's `authenticate()` token check (defense in depth: both must pass; an mTLS-only surface is satisfied by a valid cert alone). Builds on PR-A — all changes are **additive**, so the whole-repo build stays green after every task.

**Tech Stack:** Go 1.25, `crypto/tls` (`ClientCAs` + `VerifyClientCertIfGiven`), `crypto/x509`, OpenTelemetry metrics, Viper/mapstructure config, `task test` / `task vet` / `task lint`.

---

## Worktree & base branch

PR-B **depends on PR-A** (issue #131, PR #173). Base PR-B on whatever currently carries PR-A's code:

- **If PR-A is already merged to `main`:** branch from `main`.
  ```bash
  git worktree add .worktrees/feed-sink-auth-pr-b -b feat/feed-sink-auth-pr-b main
  ```
- **If PR-A is NOT yet merged:** branch from the PR-A branch so PR-A's types exist, and plan to rebase onto `main` once PR-A merges.
  ```bash
  git worktree add .worktrees/feed-sink-auth-pr-b -b feat/feed-sink-auth-pr-b feat/feed-sink-auth-pr-a
  ```

`cd .worktrees/feed-sink-auth-pr-b`. All paths below are relative to that worktree root.

## Greenness

Every task here is additive (new fields/functions; existing call sites updated in lockstep within the same task), so `go build ./...` and `go test ./...` stay green after each task. Verify each task with both the package-scoped test and `go build ./...`.

## File map

| File | Change | Responsibility |
| --- | --- | --- |
| `internal/sink/feed/auth.go` | Modify | `SurfaceAuth.MTLSRequire`; `authorize()` (mTLS gate AND token check); `hasTokenMethods()`; `clientCertName()` |
| `internal/sink/feed/server.go` | Modify | `ServeHTTP` calls `authorize` instead of `authenticate` |
| `internal/sink/feed/mcp.go` | Modify | `mcpAuthMiddleware` calls `authorize` |
| `internal/sink/feed/feed.go` | Modify | `Options.MTLSCAFile`; `Publisher.clientCAs`; CA pool load + listener `VerifyClientCertIfGiven`; HTTP/3 client-CA wiring; `loadCertPool` |
| `internal/sink/feed/mtls_test.go` | Create | Real-TLS mTLS integration matrix (valid/no/untrusted cert; mTLS+token AND) |
| `internal/sink/feed/auth_test.go` | Modify | Unit tests for `authorize` |
| `internal/config/config.go` | Modify | `FeedSinkConfig.MTLSCAFile`; `FeedAuthConfig.MTLS FeedMTLSConfig{Require}` |
| `internal/config/validate.go` | Modify | `validateFeedAuth` mtls cases; sink-level `mtls_ca_file`↔`tls` and `require`↔`ca_file` checks |
| `internal/config/validate_test.go` | Modify | mTLS validation tests |
| `cmd/rss2msg/wire.go` | Modify | `toFeedSurfaceAuth` sets `MTLSRequire` + nil-collapse update; `Options.MTLSCAFile` |
| `docs/how-to/sinks/feed.md` | Modify | mTLS section; drop the "PR-B not yet available" pointer |

---

## Task 1: Feed-package mTLS (evaluator + listener)

**Files:**
- Modify: `internal/sink/feed/auth.go`
- Modify: `internal/sink/feed/server.go` (ServeHTTP auth gate, ~lines 63-72)
- Modify: `internal/sink/feed/mcp.go` (mcpAuthMiddleware)
- Modify: `internal/sink/feed/feed.go` (Options, Publisher, New, enableHTTP3, + loadCertPool)
- Test: `internal/sink/feed/auth_test.go` (add authorize unit tests), `internal/sink/feed/mtls_test.go` (create)

- [ ] **Step 1: Add `MTLSRequire` to `SurfaceAuth` and update its doc comment (auth.go)**

Change the `SurfaceAuth` struct (and its doc comment) to:

```go
// SurfaceAuth is the resolved auth requirement for one surface. A nil
// *SurfaceAuth means the surface is public. The config layer collapses
// "disabled" / no-methods-and-no-mtls to nil before constructing this, so a
// non-nil SurfaceAuth always requires either >=1 token method or a client cert.
type SurfaceAuth struct {
	BasicUsers   []BasicCred
	BearerTokens []NamedSecret
	APIKeys      []NamedSecret
	APIKeyHeader string // header to read API keys from; empty => X-API-Key
	MTLSRequire  bool   // require a verified client certificate (mTLS gate)
}
```

- [ ] **Step 2: Add `authorize`, `hasTokenMethods`, and `clientCertName` (auth.go)**

Add `"crypto/tls"` is NOT needed here (we only read `r.TLS`). Add these after `authenticate`:

```go
// hasTokenMethods reports whether any token credential (basic/bearer/api-key) is
// configured for this surface.
func (a *SurfaceAuth) hasTokenMethods() bool {
	return len(a.BasicUsers) > 0 || len(a.BearerTokens) > 0 || len(a.APIKeys) > 0
}

// clientCertName returns the verified client certificate's subject CommonName,
// or "" when no client cert is present. Used as the identity for mTLS-only
// surfaces in the success metric.
func clientCertName(r *http.Request) string {
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		return r.TLS.PeerCertificates[0].Subject.CommonName
	}
	return ""
}

// authorize runs the full per-surface evaluation: the mTLS gate (when required)
// AND the token-credential check (when any are configured). A nil a is public.
// The TLS listener has already verified any presented client cert against the
// sink CA pool (VerifyClientCertIfGiven), so a non-empty PeerCertificates means
// a trusted cert. Returns the identity to record on success (matched credential
// name, or the client-cert CN for an mTLS-only surface) and, on failure, a
// low-cardinality reason for the metric.
func authorize(a *SurfaceAuth, r *http.Request) (name string, ok bool, reason string) {
	if a == nil {
		return "", true, ""
	}
	if a.MTLSRequire && (r.TLS == nil || len(r.TLS.PeerCertificates) == 0) {
		return "", false, "no_client_cert"
	}
	if a.hasTokenMethods() {
		if nm, tok := authenticate(a, r); tok {
			return nm, true, ""
		}
		return "", false, authFailReason(a, r)
	}
	// mTLS-only surface: the verified client cert is the identity.
	return clientCertName(r), true, ""
}
```

- [ ] **Step 3: Use `authorize` in `ServeHTTP` (server.go)**

Replace the auth block (currently `a := h.authFor(path)` … through the `if a != nil { h.recordAuthSuccess(...) }`) with:

```go
	a := h.authFor(path)
	name, ok, reason := authorize(a, r)
	if !ok {
		h.recordAuthFailure(r.Context(), h.surfaceName(path), reason)
		writeAuthChallenge(a, w)
		return
	}
	if a != nil {
		h.recordAuthSuccess(r.Context(), h.surfaceName(path), name)
	}
```

- [ ] **Step 4: Use `authorize` in `mcpAuthMiddleware` (mcp.go)**

Replace the body of the returned handler func with:

```go
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, ok, reason := authorize(a, r)
		if !ok {
			if instr != nil {
				instr.authFailure.Add(r.Context(), 1, metric.WithAttributes(
					attribute.String("surface", "mcp"),
					attribute.String("reason", reason),
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
```

- [ ] **Step 5: Add `Options.MTLSCAFile`, `Publisher.clientCAs`, and `loadCertPool` (feed.go)**

Add imports `"crypto/x509"` and `"os"` to feed.go's import block (`crypto/tls` and `fmt` are already imported).

In `Options`, add after `HTTP3`:
```go
	MTLSCAFile      string // PEM CA bundle to verify client certs; "" disables mTLS
```

In `Publisher`, add a field:
```go
	clientCAs *x509.CertPool // nil unless mTLS is enabled
```

Add this helper near the bottom of feed.go:
```go
// loadCertPool reads a PEM CA bundle into a cert pool.
func loadCertPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mtls ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("mtls ca %q: no certificates parsed", path)
	}
	return pool, nil
}
```

- [ ] **Step 6: Wire the CA pool into the TLS listener in `New` (feed.go)**

Right after the listener is created and BEFORE constructing `srv` (i.e. after the `ln, err := net.Listen(...)` block, before `srv := &http.Server{...}`), add:

```go
	var clientCAs *x509.CertPool
	if o.MTLSCAFile != "" {
		if o.TLSCertFile == "" || o.TLSKeyFile == "" {
			_ = ln.Close()
			_ = store.Close()
			return nil, fmt.Errorf("feed sink %q: mtls_ca_file requires tls cert_file and key_file", o.Name)
		}
		pool, err := loadCertPool(o.MTLSCAFile)
		if err != nil {
			_ = ln.Close()
			_ = store.Close()
			return nil, fmt.Errorf("feed sink %q: %w", o.Name, err)
		}
		clientCAs = pool
	}
```

Then set the server's TLS config when mTLS is enabled. Change the `srv := &http.Server{...}` literal to keep its existing fields and, immediately after it, add:

```go
	if clientCAs != nil {
		// Verify a presented client cert against the CA pool, but don't demand
		// one — public surfaces must still work on the same listener. ServeTLS
		// loads the server cert/key into a clone of this config.
		srv.TLSConfig = &tls.Config{ClientCAs: clientCAs, ClientAuth: tls.VerifyClientCertIfGiven}
	}
```

Set the field when constructing the Publisher — change the `p := &Publisher{...}` line to also set `clientCAs: clientCAs`:
```go
	p := &Publisher{name: o.Name, store: store, server: srv, ln: ln, shutdown: to.Shutdown, tlsCert: o.TLSCertFile, tlsKey: o.TLSKeyFile, logger: o.Logger, clientCAs: clientCAs}
```

- [ ] **Step 7: Apply client-CA verification to the HTTP/3 listener too (feed.go)**

In `enableHTTP3`, change the `h3 := &http3.Server{...}` construction so the TLS config carries the client-CA settings when mTLS is enabled:

```go
	tlsConf := &tls.Config{Certificates: []tls.Certificate{cert}}
	if p.clientCAs != nil {
		tlsConf.ClientCAs = p.clientCAs
		tlsConf.ClientAuth = tls.VerifyClientCertIfGiven
	}
	h3 := &http3.Server{
		Addr:      p.ln.Addr().String(),
		Handler:   h,
		TLSConfig: http3.ConfigureTLSConfig(tlsConf),
	}
```

- [ ] **Step 8: Write `authorize` unit tests (auth_test.go)**

Add `"crypto/tls"`, `"crypto/x509"`, and `"crypto/x509/pkix"` to the auth_test.go imports if not present. Add:

```go
func withClientCert(r *http.Request, cn string) *http.Request {
	r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Subject: pkix.Name{CommonName: cn}}}}
	return r
}

func TestAuthorize_NilIsPublic(t *testing.T) {
	if name, ok, reason := authorize(nil, httptest.NewRequest(http.MethodGet, "/rss", nil)); !ok || name != "" || reason != "" {
		t.Fatalf("nil => public, got (%q,%v,%q)", name, ok, reason)
	}
}

func TestAuthorize_MTLSRequiredNoCert(t *testing.T) {
	a := &SurfaceAuth{MTLSRequire: true}
	_, ok, reason := authorize(a, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if ok || reason != "no_client_cert" {
		t.Fatalf("no cert => fail/no_client_cert, got (ok=%v, reason=%q)", ok, reason)
	}
}

func TestAuthorize_MTLSOnlyWithCert(t *testing.T) {
	a := &SurfaceAuth{MTLSRequire: true}
	r := withClientCert(httptest.NewRequest(http.MethodGet, "/rss", nil), "client-a")
	name, ok, _ := authorize(a, r)
	if !ok || name != "client-a" {
		t.Fatalf("mtls-only with cert => ok, name=CN; got (%q,%v)", name, ok)
	}
}

func TestAuthorize_MTLSAndTokenBothRequired(t *testing.T) {
	a := &SurfaceAuth{MTLSRequire: true, BearerTokens: []NamedSecret{{Name: "ci", Secret: "tok"}}}

	// cert present but no/wrong token => bad_token (AND not satisfied).
	r1 := withClientCert(httptest.NewRequest(http.MethodGet, "/rss", nil), "client-a")
	if _, ok, reason := authorize(a, r1); ok || reason != "bad_token" {
		t.Fatalf("cert without token => fail/bad_token, got (ok=%v, reason=%q)", ok, reason)
	}

	// valid token but no cert => no_client_cert (gate runs first).
	r2 := httptest.NewRequest(http.MethodGet, "/rss", nil)
	r2.Header.Set("Authorization", "Bearer tok")
	if _, ok, reason := authorize(a, r2); ok || reason != "no_client_cert" {
		t.Fatalf("token without cert => fail/no_client_cert, got (ok=%v, reason=%q)", ok, reason)
	}

	// cert AND valid token => success, name = matched credential.
	r3 := withClientCert(httptest.NewRequest(http.MethodGet, "/rss", nil), "client-a")
	r3.Header.Set("Authorization", "Bearer tok")
	if name, ok, _ := authorize(a, r3); !ok || name != "ci" {
		t.Fatalf("cert+token => ok, name=ci; got (%q,%v)", name, ok)
	}
}

func TestAuthorize_TokenOnlyUnaffectedByMTLS(t *testing.T) {
	a := &SurfaceAuth{BearerTokens: []NamedSecret{{Name: "ci", Secret: "tok"}}}
	r := httptest.NewRequest(http.MethodGet, "/rss", nil)
	r.Header.Set("Authorization", "Bearer tok")
	if name, ok, _ := authorize(a, r); !ok || name != "ci" {
		t.Fatalf("token-only still works, got (%q,%v)", name, ok)
	}
}
```

- [ ] **Step 9: Run the unit tests**

Run: `go test ./internal/sink/feed/... -run 'TestAuthorize|TestAuthenticate|TestServeHTTP' -v`
Expected: PASS. Then `go build ./...` (must stay green — additive change).

- [ ] **Step 10: Write the real-TLS mTLS integration test (create `internal/sink/feed/mtls_test.go`)**

```go
package feed

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

// testCA is a tiny in-test certificate authority that can sign client certs.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key}
}

// pemFile writes the CA cert to a PEM file and returns its path.
func (ca *testCA) pemFile(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "ca.pem")
	f, _ := os.Create(path)
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw}); err != nil {
		t.Fatalf("encode ca: %v", err)
	}
	return path
}

// signClient issues a client certificate (CN) signed by this CA.
func (ca *testCA) signClient(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("client cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: tmpl}
}

// mtlsClient builds an HTTPS client that skips server verification (test server
// uses a self-signed cert) and optionally presents a client cert.
func mtlsClient(cert *tls.Certificate) *http.Client {
	cfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test
	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: 5 * time.Second}
}

// startMTLSSink starts a feed sink over TLS with the given rssAuth and a CA file
// for client-cert verification. The server cert is self-signed for 127.0.0.1.
func startMTLSSink(t *testing.T, rssAuth *SurfaceAuth, caFile string) *Publisher {
	t.Helper()
	dir := t.TempDir()
	serverCert, serverKey := writeFeedSelfSignedCert(t, dir) // from http3_test.go
	p, err := New(context.Background(), Options{
		Name: "mtls", Listen: "127.0.0.1:0",
		Meta: FeedMeta{Title: "t", Link: "https://example.com/"}, MaxItems: 10,
		RSS:         Surface{Enabled: true},
		RSSAuth:     rssAuth,
		TLSCertFile: serverCert, TLSKeyFile: serverKey,
		MTLSCAFile: caFile,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = p.Publish(context.Background(), model.Change{FeedURL: "https://f", ItemID: "1", Kind: model.ChangeNew, Title: "hello"})
	return p
}

func TestMTLS_RequiredSurface(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "test-ca")
	caFile := ca.pemFile(t, dir)
	good := ca.signClient(t, "client-a")
	other := newTestCA(t, "other-ca")
	untrusted := other.signClient(t, "rogue")

	p := startMTLSSink(t, &SurfaceAuth{MTLSRequire: true}, caFile)
	defer p.Close()
	url := "https://" + p.Addr() + "/rss"

	// 1) valid client cert => 200
	resp, err := mtlsClient(&good).Get(url)
	if err != nil {
		t.Fatalf("valid cert GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid cert: status = %d, want 200", resp.StatusCode)
	}

	// 2) no client cert => 401 (handshake succeeds, gate rejects)
	resp2, err := mtlsClient(nil).Get(url)
	if err != nil {
		t.Fatalf("no cert GET: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no cert: status = %d, want 401", resp2.StatusCode)
	}

	// 3) cert from an untrusted CA => TLS handshake fails (request errors)
	if _, err := mtlsClient(&untrusted).Get(url); err == nil {
		t.Fatal("untrusted client cert: expected TLS handshake error, got nil")
	}
}

func TestMTLS_AndBearerToken(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "test-ca")
	caFile := ca.pemFile(t, dir)
	good := ca.signClient(t, "client-a")

	p := startMTLSSink(t, &SurfaceAuth{MTLSRequire: true, BearerTokens: []NamedSecret{{Name: "ci", Secret: "tok"}}}, caFile)
	defer p.Close()
	url := "https://" + p.Addr() + "/rss"

	// cert present but no token => 401
	resp, err := mtlsClient(&good).Get(url)
	if err != nil {
		t.Fatalf("cert no token GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("cert without token: status = %d, want 401", resp.StatusCode)
	}

	// cert AND valid token => 200
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp2, err := mtlsClient(&good).Do(req)
	if err != nil {
		t.Fatalf("cert+token GET: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("cert+token: status = %d, want 200", resp2.StatusCode)
	}
}
```

- [ ] **Step 11: Run the feed-package tests + build**

Run: `go test ./internal/sink/feed/...`
Expected: PASS (including the two mTLS tests).
Run: `go vet ./internal/sink/feed/...` → clean.
Run: `go build ./...` → success.

- [ ] **Step 12: Commit** (explicit pathspecs only — repo auto-stages; never `git add -A`)

```bash
git add internal/sink/feed/auth.go internal/sink/feed/server.go internal/sink/feed/mcp.go internal/sink/feed/feed.go internal/sink/feed/auth_test.go internal/sink/feed/mtls_test.go
git status
git commit -m "feat(feed-sink): mTLS client-cert auth gate, AND-combined with tokens (#131)"
```

---

## Task 2: Config schema + validation for mTLS

**Files:**
- Modify: `internal/config/config.go` (`FeedSinkConfig`, `FeedAuthConfig`, new `FeedMTLSConfig`)
- Modify: `internal/config/validate.go` (`validateFeedAuth`, feed case)
- Test: `internal/config/validate_test.go`

- [ ] **Step 1: Add config fields (config.go)**

Add `MTLSCAFile` to `FeedSinkConfig` (place after the `HTTP3` field):
```go
	MTLSCAFile string `mapstructure:"mtls_ca_file"` // PEM CA bundle verifying client certs (sink-wide)
```

Add an `MTLS` field to `FeedAuthConfig` (after `APIKeyHeader`):
```go
	MTLS         FeedMTLSConfig        `mapstructure:"mtls"`
```

Add the new type next to `FeedAuthConfig`:
```go
// FeedMTLSConfig is the per-surface mTLS requirement. The CA pool itself is
// sink-wide (FeedSinkConfig.MTLSCAFile); this only toggles enforcement.
type FeedMTLSConfig struct {
	Require bool `mapstructure:"require"`
}
```

- [ ] **Step 2: Update `validateFeedAuth` for mTLS (validate.go)**

Replace the first two guards of `validateFeedAuth` with versions that account for `MTLS.Require`:

```go
	if a.Disabled && (a.HasMethods() || a.MTLS.Require) {
		return fmt.Errorf("sinks[%d] (feed %q): %s.disabled cannot be combined with credentials or mtls", i, name, label)
	}
	if isOverride && !a.Disabled && !a.HasMethods() && !a.MTLS.Require {
		return fmt.Errorf("sinks[%d] (feed %q): %s defines no credentials/mtls and is not disabled (set disabled: true for a public surface)", i, name, label)
	}
```

(The rest of `validateFeedAuth` is unchanged.)

- [ ] **Step 3: Add sink-level mTLS checks in the feed case (validate.go)**

Immediately AFTER the existing auth-validation override loop (the `for _, ov := range []struct{ label string; a *FeedAuthConfig }{...}` block that ends just before `sd := storeDriverOrDefault(...)`), add:

```go
			// mTLS: require needs a CA pool, and the CA pool needs a TLS listener.
			requiresMTLS := f.Auth.MTLS.Require
			for _, sa := range []*FeedAuthConfig{f.RSS.Auth, f.Atom.Auth, f.MCP.Auth} {
				if sa != nil && sa.MTLS.Require {
					requiresMTLS = true
				}
			}
			if requiresMTLS && f.MTLSCAFile == "" {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): auth.mtls.require needs feed.mtls_ca_file", i, s.Name)
			}
			if f.MTLSCAFile != "" && (f.TLS.CertFile == "" || f.TLS.KeyFile == "") {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): mtls_ca_file requires tls.cert_file and key_file", i, s.Name)
			}
```

- [ ] **Step 4: Add validation tests (validate_test.go)**

`feedSinkBase()` returns a valid `Config` with one feed sink named "out", `Listen: ":8088"`, store memory, no TLS. Add:

```go
func TestValidate_FeedMTLSRequireNeedsCA(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Auth = FeedAuthConfig{MTLS: FeedMTLSConfig{Require: true}}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error: mtls.require without mtls_ca_file")
	}
}

func TestValidate_FeedMTLSCANeedsTLS(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.MTLSCAFile = "/etc/ca.pem"
	// no tls.cert_file / key_file
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error: mtls_ca_file without tls cert/key")
	}
}

func TestValidate_FeedMTLSValid(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.TLS = FeedTLSConfig{CertFile: "/c.pem", KeyFile: "/k.pem"}
	c.Sinks[0].Feed.MTLSCAFile = "/ca.pem"
	c.Sinks[0].Feed.Auth = FeedAuthConfig{MTLS: FeedMTLSConfig{Require: true}}
	if _, err := Validate(c); err != nil {
		t.Fatalf("valid mtls config rejected: %v", err)
	}
}

func TestValidate_FeedMTLSOnlyOverrideAllowed(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.TLS = FeedTLSConfig{CertFile: "/c.pem", KeyFile: "/k.pem"}
	c.Sinks[0].Feed.MTLSCAFile = "/ca.pem"
	on := true
	mtlsOnly := FeedAuthConfig{MTLS: FeedMTLSConfig{Require: true}}
	c.Sinks[0].Feed.RSS = FeedSurfaceConfig{Enabled: &on, Auth: &mtlsOnly}
	if _, err := Validate(c); err != nil {
		t.Fatalf("mtls-only override (no token methods) must be valid, got %v", err)
	}
}

func TestValidate_FeedMTLSDisabledConflict(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.TLS = FeedTLSConfig{CertFile: "/c.pem", KeyFile: "/k.pem"}
	c.Sinks[0].Feed.MTLSCAFile = "/ca.pem"
	c.Sinks[0].Feed.Auth = FeedAuthConfig{Disabled: true, MTLS: FeedMTLSConfig{Require: true}}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error: disabled combined with mtls.require")
	}
}
```

- [ ] **Step 5: Verify**

Run: `go test ./internal/config/...` (expect PASS), `go vet ./internal/config/...` (clean), `go build ./...` (success).

- [ ] **Step 6: Commit** (explicit pathspecs only)

```bash
git add internal/config/config.go internal/config/validate.go internal/config/validate_test.go
git status
git commit -m "feat(config): feed-sink mtls_ca_file + per-surface auth.mtls.require (#131)"
```

---

## Task 3: Wiring

**Files:**
- Modify: `cmd/rss2msg/wire.go` (`toFeedSurfaceAuth`, feed `Options` literal)

- [ ] **Step 1: Update `toFeedSurfaceAuth` (wire.go)**

Replace the function with a version that keeps a surface non-public when only mTLS is required, and carries `MTLSRequire` through:

```go
// toFeedSurfaceAuth converts a resolved feed auth block into the sink's
// SurfaceAuth, returning nil for a public surface (disabled, or no token methods
// AND no mTLS requirement).
func toFeedSurfaceAuth(a config.FeedAuthConfig) *feedsink.SurfaceAuth {
	if a.Disabled || (!a.HasMethods() && !a.MTLS.Require) {
		return nil
	}
	sa := &feedsink.SurfaceAuth{APIKeyHeader: a.APIKeyHeader, MTLSRequire: a.MTLS.Require}
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

- [ ] **Step 2: Pass `MTLSCAFile` into `Options` (wire.go)**

In the `feedsink.New(ctx, feedsink.Options{...})` literal, on the line with `TLSCertFile: ..., TLSKeyFile: ..., HTTP3: f.HTTP3,` add `MTLSCAFile: f.MTLSCAFile,`:
```go
			TLSCertFile: f.TLS.CertFile, TLSKeyFile: f.TLS.KeyFile, HTTP3: f.HTTP3, MTLSCAFile: f.MTLSCAFile,
```

- [ ] **Step 3: Full verification gate**

Run each and confirm:
- `go build ./...` → success
- `task test` (or `go test -race ./...`) → PASS
- `task vet` (or `go vet ./...`) → clean
- `task lint` → no findings (if `golangci-lint` v2 is unavailable locally, say so; CI runs it)

- [ ] **Step 4: Commit** (explicit pathspec only)

```bash
git add cmd/rss2msg/wire.go
git status
git commit -m "feat(feed-sink): wire mtls_ca_file and per-surface mtls.require (#131)"
```

---

## Task 4: Documentation

**Files:**
- Modify: `docs/how-to/sinks/feed.md`

- [ ] **Step 1: Add the mTLS subsection to the Auth section**

Under the existing `## Auth` section (added in PR-A), append a `### mTLS (client certificates)` subsection. Keep the page's frontmatter and `## Related` footer; set `updated: 2026-06-17`. Remove the PR-A forward pointer line that says mTLS "is planned as PR-B of issue #131 and is not yet available." Document only what the code does:

- `feed.mtls_ca_file` is a **sink-wide** PEM CA bundle. When set, the TLS listener verifies a presented client certificate against it but does not demand one (`VerifyClientCertIfGiven`), so public surfaces still work on the same port. It requires `feed.tls.cert_file`/`key_file` (mTLS is TLS-only) and also applies to the HTTP/3 listener.
- Per surface, `auth.mtls.require: true` enforces that a verified client certificate was presented. It combines with token methods using **AND** (defense in depth): a surface with both requires a valid client cert *and* a valid token; a surface with only `mtls.require` is satisfied by a valid cert alone (the cert subject CN becomes the `credential` metric attribute).
- A missing/absent client cert on an mTLS-required surface returns **HTTP 401** (`reason=no_client_cert` on `feed_sink.auth_failure`). A client cert signed by an untrusted CA fails the TLS handshake (the connection is refused before any HTTP response).
- Validation: `auth.mtls.require` anywhere requires `feed.mtls_ca_file`; `mtls_ca_file` requires `tls.cert_file`/`key_file`; `disabled: true` cannot be combined with `mtls.require`.

Use this example (match the page's fence style):

```yaml
sinks:
  - name: myfeed
    driver: feed
    feed:
      listen: "0.0.0.0:8443"
      tls: {cert_file: /etc/rss2msg/server.pem, key_file: /etc/rss2msg/server-key.pem}
      mtls_ca_file: /etc/rss2msg/clients-ca.pem    # sink-wide client-cert CA pool
      auth:
        bearer_tokens: [{name: ci-bot, token: tok_a}]
        mtls: {require: true}                       # cert AND token (default for all surfaces)
      rss:  {enabled: true, auth: {disabled: true}}            # public
      mcp:  {enabled: true, auth: {mtls: {require: true}}}     # mTLS only (cert is the identity)
```

- [ ] **Step 2: Verify links**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`

- [ ] **Step 3: Commit** (explicit pathspec only)

```bash
git add docs/how-to/sinks/feed.md
git status
git commit -m "docs(feed-sink): document mTLS client-certificate auth (#131)"
```

---

## Task 5: Final verification & PR

- [ ] **Step 1: Full gate from a clean tree**

Run: `task test && task vet && task lint`
Expected: all pass. `task test-integration` is NOT required — mTLS is exercised by the in-process real-TLS tests in `mtls_test.go` (no store/coordinator/container surface is touched). State this in the PR body.

- [ ] **Step 2: Open the PR**

```bash
git push -u origin feat/feed-sink-auth-pr-b
gh pr create --base main --title "feat(feed-sink): mTLS client-cert auth (PR-B of #131)" \
  --body "$(cat <<'EOF'
PR-B of #131 — adds mutual-TLS client-certificate auth to the feed sink, completing the auth-hardening epic (PR-A added the token methods).

- **Sink-wide CA pool** `feed.mtls_ca_file`: the TLS listener verifies a presented client cert against it (`VerifyClientCertIfGiven`) without demanding one, so public surfaces coexist on the same port; also applied to HTTP/3.
- **Per-surface** `auth.mtls.require`: AND-combined with token methods (cert + token), or cert-only when no token methods are set (cert CN becomes the `credential` metric label).
- Missing cert on an mTLS-required surface → 401 (`reason=no_client_cert`); untrusted-CA cert → TLS handshake failure.
- Validation: `mtls.require` needs `mtls_ca_file`; `mtls_ca_file` needs `tls.cert_file`/`key_file`; `disabled` + `mtls.require` rejected.

Real-TLS integration tests cover valid cert / no cert / untrusted-CA cert and the cert-AND-token matrix. `go build`, `go test -race ./...`, `go vet`, `golangci-lint`, and the doc link checker pass. Skipped `task test-integration` (no store/coordinator/container surface touched).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review

**Spec coverage** (against `docs/superpowers/specs/2026-06-16-feed-sink-auth-design.md`, PR-B scope):
- `feed.mtls_ca_file` sink-wide CA pool → config (Task 2) + listener wiring (Task 1 Steps 6-7) + Options/wire (Tasks 1/3). ✓
- Listener uses `VerifyClientCertIfGiven` when CA set → Task 1 Steps 6-7. ✓
- Per-surface `auth.mtls.require` → config (Task 2), `SurfaceAuth.MTLSRequire` (Task 1), wire nil-collapse (Task 3). ✓
- mTLS gate runs before token check; AND semantics; mTLS-only satisfied by cert → `authorize` (Task 1 Step 2), tested unit + integration. ✓
- 401 + `reason=no_client_cert` → `authorize` + metric recording (Task 1 Steps 2-4). ✓
- Untrusted cert → handshake failure (asserted in `TestMTLS_RequiredSurface` case 3). ✓
- Validation: require↔ca_file, ca_file↔tls, disabled+require, mtls-only override allowed → Task 2 Steps 2-4. ✓
- Docs + drop PR-A forward pointer → Task 4. ✓
- example.yaml: feed sink absent from `examples/config.example.yaml` / `internal/config/example.yaml`, so untouched (drift guard intact) — same as PR-A; not restated as a task.

**Placeholder scan:** none — every code/step shows full code and exact commands.

**Type consistency:** `SurfaceAuth.MTLSRequire`, `authorize(*SurfaceAuth,*http.Request)(string,bool,string)`, `hasTokenMethods()`, `clientCertName(*http.Request)string`, `Options.MTLSCAFile`, `Publisher.clientCAs *x509.CertPool`, `loadCertPool(string)(*x509.CertPool,error)`, config `FeedSinkConfig.MTLSCAFile`, `FeedAuthConfig.MTLS FeedMTLSConfig`, `FeedMTLSConfig.Require`, `toFeedSurfaceAuth` updated nil-collapse + `MTLSRequire` — names used consistently across tasks. `authenticate` (PR-A) is retained and called by `authorize`; its direct PR-A tests remain valid.
