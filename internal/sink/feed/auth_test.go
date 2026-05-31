package feed

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAuth_BasicRequired(t *testing.T) {
	h := newTestHandler(t)
	h.cfg.auth = &AuthConfig{BasicUser: "u", BasicPass: "p"}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 got %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic") {
		t.Fatal("missing WWW-Authenticate")
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

func TestAuth_BearerRequired(t *testing.T) {
	h := newTestHandler(t)
	h.cfg.auth = &AuthConfig{BearerToken: "sekret"}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/rss", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 without token got %d", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/rss", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != 200 {
		t.Fatalf("want 200 with bearer got %d", rec2.Code)
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
