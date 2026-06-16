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
