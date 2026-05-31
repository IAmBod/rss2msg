package feed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
