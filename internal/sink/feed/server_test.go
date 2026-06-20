package feed

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestHandler(t *testing.T) *handler {
	t.Helper()
	st := newMemoryStore(50)
	_ = st.Write(context.Background(), chg("f", "a", time.Unix(9000, 0).UTC()))
	return newHandler(handlerConfig{
		store: st, meta: meta(), maxItems: 50,
		rssPath: "/rss", atomPath: "/atom",
	})
}

func TestHandler_RoutesAndContentTypes(t *testing.T) {
	h := newTestHandler(t)
	for path, ct := range map[string]string{"/rss": "application/rss+xml", "/atom": "application/atom+xml"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != 200 {
			t.Fatalf("%s: want 200 got %d", path, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); got != ct {
			t.Fatalf("%s: want %s got %s", path, ct, got)
		}
		if rec.Header().Get("ETag") == "" {
			t.Fatalf("%s: missing ETag", path)
		}
	}
}

func TestHandler_404And405(t *testing.T) {
	h := newTestHandler(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != 404 {
		t.Fatalf("want 404 got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/rss", nil))
	if rec.Code != 405 {
		t.Fatalf("want 405 got %d", rec.Code)
	}
}

func TestHandler_ConditionalGET304(t *testing.T) {
	h := newTestHandler(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/rss", nil))
	etag := rec.Header().Get("ETag")
	req := httptest.NewRequest(http.MethodGet, "/rss", nil)
	req.Header.Set("If-None-Match", etag)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusNotModified {
		t.Fatalf("want 304 got %d", rec2.Code)
	}
	if rec2.Body.Len() != 0 {
		t.Fatal("304 must have no body")
	}
}

func TestHandler_RenderCacheServesWithoutStore(t *testing.T) {
	st := newMemoryStore(50)
	_ = st.Write(context.Background(), chg("f", "a", time.Unix(9000, 0).UTC()))
	h := newHandler(handlerConfig{
		store: st, meta: meta(), maxItems: 50, rssPath: "/rss", atomPath: "/atom",
		renderCacheTTL: time.Hour,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/rss", nil))
	etag1 := rec.Header().Get("ETag")
	// write a new change; with a live render cache the served doc/etag should NOT change yet
	_ = st.Write(context.Background(), chg("f", "b", time.Unix(9999, 0).UTC()))
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if rec2.Header().Get("ETag") != etag1 {
		t.Fatal("render cache should serve the cached doc (same ETag) within TTL")
	}
}

func TestHandler_SelfURLPerRequest(t *testing.T) {
	st := newMemoryStore(10)
	trusted, _ := parseTrustedProxies([]string{"private"})
	h := newHandler(handlerConfig{
		store: st, meta: FeedMeta{Title: "t", Link: "https://site"},
		maxItems: 10, rssPath: "/rss", atomPath: "/atom",
		proxy: proxyConfig{link: "https://site", trusted: trusted},
	})

	type result struct {
		body string
		etag string
	}
	do := func(remote string, hdr map[string]string) result {
		r := httptest.NewRequest(http.MethodGet, "http://internal/atom", nil)
		r.RemoteAddr = remote
		r.Host = "internal"
		for k, v := range hdr {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return result{body: w.Body.String(), etag: w.Header().Get("ETag")}
	}

	a := do("10.0.0.1:9", map[string]string{"X-Forwarded-Proto": "https", "X-Forwarded-Host": "a.example"})
	b := do("10.0.0.1:9", map[string]string{"X-Forwarded-Proto": "https", "X-Forwarded-Host": "b.example"})
	if !strings.Contains(a.body, `href="https://a.example/atom" rel="self"`) {
		t.Fatalf("host a self link missing:\n%s", a.body)
	}
	if !strings.Contains(b.body, `href="https://b.example/atom" rel="self"`) {
		t.Fatalf("host b self link missing (cache leaked host a?):\n%s", b.body)
	}
	// ETags are computed over the injected body (which embeds the host), so
	// two different hosts must produce distinct ETags.
	if a.etag == b.etag {
		t.Fatalf("ETags must differ per host: both got %q", a.etag)
	}
}

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
