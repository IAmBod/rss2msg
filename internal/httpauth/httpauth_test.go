package httpauth

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

func sample() *Auth {
	return &Auth{
		BasicUsers:   []BasicCred{{Name: "u1", Username: "alice", Password: "pw"}},
		BearerTokens: []NamedSecret{{Name: "b1", Secret: "tok"}},
		APIKeys:      []NamedSecret{{Name: "k1", Secret: "key"}},
	}
}

func TestAuthenticate(t *testing.T) {
	a := sample()
	bearer := httptest.NewRequest(http.MethodGet, "/", nil)
	bearer.Header.Set("Authorization", "Bearer tok")
	if name, ok := a.Authenticate(bearer); !ok || name != "b1" {
		t.Fatalf("bearer: (%q,%v)", name, ok)
	}
	basic := httptest.NewRequest(http.MethodGet, "/", nil)
	basic.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:pw")))
	if name, ok := a.Authenticate(basic); !ok || name != "u1" {
		t.Fatalf("basic: (%q,%v)", name, ok)
	}
	apikey := httptest.NewRequest(http.MethodGet, "/", nil)
	apikey.Header.Set("X-API-Key", "key")
	if name, ok := a.Authenticate(apikey); !ok || name != "k1" {
		t.Fatalf("apikey: (%q,%v)", name, ok)
	}
	if _, ok := a.Authenticate(httptest.NewRequest(http.MethodGet, "/", nil)); ok {
		t.Fatal("no creds should fail")
	}
}

func TestCustomAPIKeyHeader(t *testing.T) {
	a := &Auth{APIKeys: []NamedSecret{{Name: "k", Secret: "s"}}, APIKeyHeader: "X-Token"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Token", "s")
	if _, ok := a.Authenticate(r); !ok {
		t.Fatal("custom header should authenticate")
	}
}

func TestFailReasonAndEmptyAndChallenge(t *testing.T) {
	a := sample()
	if got := a.FailReason(httptest.NewRequest(http.MethodGet, "/", nil)); got != "no_credentials" {
		t.Fatalf("got %q", got)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-API-Key", "wrong")
	if got := a.FailReason(r); got != "bad_token" {
		t.Fatalf("got %q", got)
	}
	if (&Auth{}).Empty() != true || a.Empty() != false {
		t.Fatal("Empty() wrong")
	}
	rec := httptest.NewRecorder()
	a.WriteChallenge(rec)
	if got := rec.Header().Get("WWW-Authenticate"); got != `Basic realm="rss2msg"` {
		t.Fatalf("challenge %q", got)
	}
}
