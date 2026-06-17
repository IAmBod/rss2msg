package http

import (
	"bytes"
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestHTTPConditionalGETLastModified(t *testing.T) {
	t.Parallel()
	const lm = "Mon, 01 Jan 2024 00:00:00 GMT"
	var conditional int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Modified-Since") == lm {
			conditional++
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Last-Modified", lm)
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
		t.Fatalf("expected 1 If-Modified-Since request, got %d", conditional)
	}
	if len(feeds) != 1 || feeds[0].URL != "https://a.example/rss" {
		t.Fatalf("304 should return cached list, got %+v", feeds)
	}
}

func TestHTTPUnsolicited304Errors(t *testing.T) {
	t.Parallel()
	// A spec-violating server returns 304 even though the first request sent no
	// validators. The source must error rather than return a silent empty list.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(srv.Close)

	if _, err := newTestHTTP(t, srv.URL, nil).Feeds(context.Background()); err == nil {
		t.Fatal("expected error for unsolicited 304 on first fetch")
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
