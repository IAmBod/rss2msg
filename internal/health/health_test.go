package health

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"

	"github.com/iambod/rss2msg/internal/config"
)

func testCfg() config.HealthConfig {
	return config.HealthConfig{
		Enabled:       true,
		Listen:        "127.0.0.1:0",
		LivenessPath:  "/healthz",
		ReadinessPath: "/readyz",
		StartupPath:   "/startupz",
	}
}

func testLogger() zerolog.Logger { return zerolog.New(io.Discard) }

// do issues a GET against the in-memory handler and returns code + body.
func do(t *testing.T, s *Server, path string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	s.handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestLivenessAlwaysOK(t *testing.T) {
	s := New(testCfg(), testLogger())
	if code, body := do(t, s, "/healthz"); code != http.StatusOK || body != "ok\n" {
		t.Fatalf("liveness before start = %d %q, want 200 ok", code, body)
	}
	// Liveness must stay 200 even while draining.
	s.MarkStarted()
	s.SetDraining()
	if code, body := do(t, s, "/healthz"); code != http.StatusOK || body != "ok\n" {
		t.Fatalf("liveness while draining = %d %q, want 200 ok", code, body)
	}
}

func TestStartupFlips(t *testing.T) {
	s := New(testCfg(), testLogger())
	if code, body := do(t, s, "/startupz"); code != http.StatusServiceUnavailable || body != "starting\n" {
		t.Fatalf("startup before MarkStarted = %d %q, want 503 starting", code, body)
	}
	s.MarkStarted()
	if code, body := do(t, s, "/startupz"); code != http.StatusOK || body != "ok\n" {
		t.Fatalf("startup after MarkStarted = %d %q, want 200 ok", code, body)
	}
}

func TestReadiness(t *testing.T) {
	t.Run("before started", func(t *testing.T) {
		s := New(testCfg(), testLogger())
		if code, body := do(t, s, "/readyz"); code != http.StatusServiceUnavailable || body != "starting\n" {
			t.Fatalf("got %d %q, want 503 starting", code, body)
		}
	})

	t.Run("started no checks", func(t *testing.T) {
		s := New(testCfg(), testLogger())
		s.MarkStarted()
		if code, body := do(t, s, "/readyz"); code != http.StatusOK || body != "ok\n" {
			t.Fatalf("got %d %q, want 200 ok", code, body)
		}
	})

	t.Run("draining", func(t *testing.T) {
		s := New(testCfg(), testLogger())
		s.MarkStarted()
		s.SetDraining()
		if code, body := do(t, s, "/readyz"); code != http.StatusServiceUnavailable || body != "draining\n" {
			t.Fatalf("got %d %q, want 503 draining", code, body)
		}
	})

	t.Run("failing check", func(t *testing.T) {
		check := Check{Name: "state", Fn: func(ctx context.Context) error { return errors.New("unreachable") }}
		s := New(testCfg(), testLogger(), check)
		s.MarkStarted()
		if code, body := do(t, s, "/readyz"); code != http.StatusServiceUnavailable || body != "state: unreachable\n" {
			t.Fatalf("got %d %q, want 503 state: unreachable", code, body)
		}
	})

	t.Run("passing check", func(t *testing.T) {
		check := Check{Name: "state", Fn: func(ctx context.Context) error { return nil }}
		s := New(testCfg(), testLogger(), check)
		s.MarkStarted()
		if code, body := do(t, s, "/readyz"); code != http.StatusOK || body != "ok\n" {
			t.Fatalf("got %d %q, want 200 ok", code, body)
		}
	})
}

func TestStartDisabled(t *testing.T) {
	cfg := testCfg()
	cfg.Enabled = false
	s := New(cfg, testLogger())
	if err := s.Start(); err != nil {
		t.Fatalf("Start with Enabled=false returned %v, want nil", err)
	}
	if s.server != nil {
		t.Fatalf("server should be nil when disabled, got %#v", s.server)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown on never-started server returned %v, want nil", err)
	}
}

func TestHTTPRoundTrip(t *testing.T) {
	s := New(testCfg(), testLogger())
	s.MarkStarted()
	srv := httptest.NewServer(s.handler())
	defer srv.Close()

	for _, path := range []string{"/healthz", "/readyz", "/startupz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, resp.StatusCode)
		}
	}
}
