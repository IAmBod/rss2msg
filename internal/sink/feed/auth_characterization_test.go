package feed

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

func charAuth() *SurfaceAuth {
	return &SurfaceAuth{
		BasicUsers:   []BasicCred{{Name: "u1", Username: "alice", Password: "pw"}},
		BearerTokens: []NamedSecret{{Name: "b1", Secret: "tok"}},
		APIKeys:      []NamedSecret{{Name: "k1", Secret: "key"}},
		APIKeyHeader: "", // => X-API-Key
	}
}

func TestChar_NilIsPublic(t *testing.T) {
	name, ok := authenticate(nil, httptest.NewRequest(http.MethodGet, "/", nil))
	if !ok || name != "" {
		t.Fatalf("nil auth should pass public: name=%q ok=%v", name, ok)
	}
}

func TestChar_BearerBasicAPIKey(t *testing.T) {
	a := charAuth()
	cases := []struct {
		name   string
		set    func(*http.Request)
		want   string
		wantOK bool
	}{
		{"bearer ok", func(r *http.Request) { r.Header.Set("Authorization", "Bearer tok") }, "b1", true},
		{"bearer bad", func(r *http.Request) { r.Header.Set("Authorization", "Bearer nope") }, "", false},
		{"basic ok", func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:pw")))
		}, "u1", true},
		{"basic bad", func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:wrong")))
		}, "", false},
		{"apikey ok", func(r *http.Request) { r.Header.Set("X-API-Key", "key") }, "k1", true},
		{"apikey bad", func(r *http.Request) { r.Header.Set("X-API-Key", "wrong") }, "", false},
		{"no creds", func(r *http.Request) {}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			tc.set(r)
			name, ok := authenticate(a, r)
			if ok != tc.wantOK || name != tc.want {
				t.Fatalf("got (%q,%v) want (%q,%v)", name, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestChar_FailReason(t *testing.T) {
	a := charAuth()
	if got := authFailReason(a, httptest.NewRequest(http.MethodGet, "/", nil)); got != "no_credentials" {
		t.Fatalf("empty req: got %q want no_credentials", got)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer nope")
	if got := authFailReason(a, r); got != "bad_token" {
		t.Fatalf("bad bearer: got %q want bad_token", got)
	}
}

func TestChar_Challenge(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAuthChallenge(charAuth(), rec) // has basic users => Basic challenge
	if got := rec.Header().Get("WWW-Authenticate"); got != `Basic realm="rss2msg"` {
		t.Fatalf("got %q", got)
	}
	rec2 := httptest.NewRecorder()
	writeAuthChallenge(&SurfaceAuth{BearerTokens: []NamedSecret{{Secret: "t"}}}, rec2)
	if got := rec2.Header().Get("WWW-Authenticate"); got != `Bearer realm="rss2msg"` {
		t.Fatalf("got %q", got)
	}
}
