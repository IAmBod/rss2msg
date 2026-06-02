package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/spf13/cobra"

	"github.com/iambod/rss2msg/internal/config"
)

// Probe kinds the healthcheck command can target. They map onto the three
// Kubernetes-style endpoints the serve daemon exposes (see internal/health).
const (
	probeReadiness = "readiness"
	probeLiveness  = "liveness"
	probeStartup   = "startup"
)

// newHealthcheckCmd builds the `healthcheck` subcommand. It exists so the
// distroless production image — which ships no shell, curl, or wget — can still
// run a Docker `HEALTHCHECK`: the container invokes its own binary, which probes
// the in-process health endpoint over HTTP and exits 0 (healthy) or non-zero
// (unhealthy). Readiness is the default because it also verifies the state store
// and coordinator are reachable, not just that the process is alive.
func newHealthcheckCmd(opts *rootOpts) *cobra.Command {
	var (
		probe   string
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Probe the running daemon's health endpoint and exit 0/1 (for Docker HEALTHCHECK)",
		Long: "Probe the running daemon's health endpoint over HTTP and exit 0 when healthy,\n" +
			"non-zero otherwise. Designed to be the Docker HEALTHCHECK command for the\n" +
			"distroless image, which has no shell or curl. The endpoint address and paths\n" +
			"are read from the same config as the serve daemon.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(opts.configPath)
			if err != nil {
				return err
			}
			if !cfg.Health.Enabled {
				return fmt.Errorf("health endpoints are disabled in config; nothing to probe")
			}
			target, err := probeURL(cfg.Health, probe)
			if err != nil {
				return err
			}
			return runHealthcheck(cmd.Context(), target, timeout)
		},
	}
	cmd.Flags().StringVar(&probe, "probe", probeReadiness,
		fmt.Sprintf("which probe to check: %s, %s, or %s", probeReadiness, probeLiveness, probeStartup))
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Second, "overall timeout for the probe request")
	return cmd
}

// probeURL builds the HTTP URL to GET for the given probe kind. The configured
// listen address often binds a wildcard host (":8080", "0.0.0.0:8080", "[::]:8080")
// so callers from inside the container can reach it — but a client must dial a
// concrete host, so those are rewritten to 127.0.0.1. An explicit host is kept.
func probeURL(h config.HealthConfig, probe string) (string, error) {
	var path string
	switch probe {
	case probeReadiness:
		path = h.ReadinessPath
	case probeLiveness:
		path = h.LivenessPath
	case probeStartup:
		path = h.StartupPath
	default:
		return "", fmt.Errorf("unknown probe %q (want %s, %s, or %s)", probe, probeReadiness, probeLiveness, probeStartup)
	}

	host, port, err := net.SplitHostPort(h.Listen)
	if err != nil {
		return "", fmt.Errorf("parse health listen address %q: %w", h.Listen, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	u := url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: path}
	return u.String(), nil
}

// runHealthcheck GETs target and returns nil only on a 2xx response. Any
// transport error or non-2xx status is an error, which Cobra surfaces as a
// non-zero process exit — exactly what Docker's HEALTHCHECK interprets as
// unhealthy.
func runHealthcheck(ctx context.Context, target string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w", target, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("probe %s: unhealthy (HTTP %d)", target, resp.StatusCode)
	}
	return nil
}
