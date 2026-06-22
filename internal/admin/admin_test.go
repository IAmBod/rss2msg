package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/httpauth"
	"github.com/rs/zerolog"
)

func testServer(t *testing.T, auth *httpauth.Auth, cors []string, deps Deps) *Server {
	t.Helper()
	cfg := config.AdminConfig{Enabled: true, Listen: ":0", CORS: config.AdminCORSConfig{AllowedOrigins: cors}}
	return New(cfg, auth, deps, zerolog.Nop())
}

func baseDeps() Deps {
	return Deps{
		Build:     BuildInfo{Version: "v1.2.3", Commit: "abc", Date: "today", InstanceID: "inst-1"},
		StartedAt: time.Now().Add(-time.Minute),
		Self:      "inst-1",
	}
}

func TestStatusRequiresAuth(t *testing.T) {
	auth := &httpauth.Auth{BearerTokens: []httpauth.NamedSecret{{Name: "ops", Secret: "tok"}}}
	s := testServer(t, auth, nil, baseDeps())

	rec := httptest.NewRecorder()
	s.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no auth: got %d want 401", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("missing challenge header")
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	req.Header.Set("Authorization", "Bearer tok")
	s.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("authed: got %d want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("status not JSON: %v", err)
	}
	if body["version"] != "v1.2.3" || body["instance_id"] != "inst-1" {
		t.Fatalf("status body = %v", body)
	}
}

func TestAuthPassThroughWhenEmpty(t *testing.T) {
	s := testServer(t, &httpauth.Auth{}, nil, baseDeps()) // empty auth => open
	rec := httptest.NewRecorder()
	s.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("empty auth should pass through: got %d", rec.Code)
	}
}

func TestCORS(t *testing.T) {
	auth := &httpauth.Auth{}
	s := testServer(t, auth, []string{"https://ops.example.com"}, baseDeps())

	// preflight
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/v1/status", nil)
	req.Header.Set("Origin", "https://ops.example.com")
	s.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight got %d want 204", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://ops.example.com" {
		t.Fatalf("missing ACAO: %v", rec.Header())
	}
	// disallowed origin => no CORS header
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	s.handler().ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("disallowed origin should get no ACAO")
	}
}

var _ = context.Background
