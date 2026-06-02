package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

func TestProbeURL_NormalizesListenAddress(t *testing.T) {
	tests := []struct {
		name   string
		listen string
		probe  string
		want   string
	}{
		{"wildcard port-only", ":8080", "readiness", "http://127.0.0.1:8080/readyz"},
		{"all interfaces", "0.0.0.0:8080", "liveness", "http://127.0.0.1:8080/healthz"},
		{"ipv6 unspecified", "[::]:8080", "startup", "http://127.0.0.1:8080/startupz"},
		{"explicit host kept", "10.0.0.5:9000", "readiness", "http://10.0.0.5:9000/readyz"},
	}
	h := config.HealthConfig{
		LivenessPath:  "/healthz",
		ReadinessPath: "/readyz",
		StartupPath:   "/startupz",
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h.Listen = tt.listen
			got, err := probeURL(h, tt.probe)
			if err != nil {
				t.Fatalf("probeURL: %v", err)
			}
			if got != tt.want {
				t.Errorf("probeURL(%q,%q) = %q, want %q", tt.listen, tt.probe, got, tt.want)
			}
		})
	}
}

func TestProbeURL_UnknownProbe(t *testing.T) {
	if _, err := probeURL(config.HealthConfig{Listen: ":8080"}, "bogus"); err == nil {
		t.Fatal("expected error for unknown probe kind")
	}
}

func TestRunHealthcheck_HealthyAndUnhealthy(t *testing.T) {
	var status int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte("body\n"))
	}))
	defer srv.Close()

	status = http.StatusOK
	if err := runHealthcheck(context.Background(), srv.URL, time.Second); err != nil {
		t.Fatalf("healthy probe should succeed, got %v", err)
	}

	status = http.StatusServiceUnavailable
	if err := runHealthcheck(context.Background(), srv.URL, time.Second); err == nil {
		t.Fatal("503 probe should fail")
	}
}

func TestRunHealthcheck_Unreachable(t *testing.T) {
	// Port 1 is reserved and not listening; the dial should fail fast.
	if err := runHealthcheck(context.Background(), "http://127.0.0.1:1/readyz", 200*time.Millisecond); err == nil {
		t.Fatal("unreachable endpoint should fail")
	}
}

func TestHealthcheckCmd_RegisteredOnRoot(t *testing.T) {
	root := newRootCmd()
	for _, c := range root.Commands() {
		if c.Name() == "healthcheck" {
			return
		}
	}
	t.Fatal("root command has no \"healthcheck\" subcommand")
}

func TestHealthcheckCmd_ProbeFlagValidation(t *testing.T) {
	if _, err := probeURL(config.HealthConfig{Listen: ":8080", ReadinessPath: "/readyz"}, "readiness"); err != nil {
		t.Fatalf("readiness should be valid: %v", err)
	}
	// Sanity: the default probe kind constant is wired to readiness.
	if !strings.HasSuffix(mustProbeURL(t, "readiness"), "/readyz") {
		t.Fatal("readiness probe should target the readiness path")
	}
}

func mustProbeURL(t *testing.T, probe string) string {
	t.Helper()
	u, err := probeURL(config.HealthConfig{
		Listen:        ":8080",
		LivenessPath:  "/healthz",
		ReadinessPath: "/readyz",
		StartupPath:   "/startupz",
	}, probe)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
