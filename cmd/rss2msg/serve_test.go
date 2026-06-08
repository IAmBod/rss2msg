package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This is a smoke test for the long-lived `serve` daemon. It drives the real
// cobra command through the production wiring against Docker-free backends
// (memory coordinator, SQLite state, httptest feed, stdout sink) with the
// health probe listener bound to an ephemeral port, then asserts serve boots,
// polls at least once, and shuts down cleanly on context cancellation. It
// covers newServeCmd / buildSources / scheduler.ServeDynamic / health.Start,
// which no other test exercised.
//
// The first poll is a deterministic boot signal: the dynamic scheduler ticks
// each feed once immediately on start, so we wait for the feed to be fetched
// rather than sleeping a fixed duration.

const serveFeedBody = `<?xml version="1.0"?>
<rss version="2.0"><channel><title>Sample</title>
<item><guid>a</guid><title>One</title><link>https://e/a</link><description>first</description></item>
</channel></rss>`

// writeServeConfig writes a serve-shaped config (health enabled on an ephemeral
// port, a short drain timeout) to a temp file and returns its path.
func writeServeConfig(t *testing.T, statePath, feedURL string) string {
	t.Helper()
	body := fmt.Sprintf(`log:
  level: error
  format: json
coordination:
  driver: memory
state:
  driver: sqlite
  sqlite:
    path: %s
runtime:
  shutdown_drain_timeout: 2s
health:
  enabled: true
  listen: 127.0.0.1:0
  liveness_path: /healthz
  readiness_path: /readyz
  startup_path: /startupz
sinks:
  - name: default
    driver: stdout
    stdout:
      target: stdout
      format: json
feeds:
  - url: %s
    interval: 1m
    sinks: [default]
`, statePath, feedURL)
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// silenceOutput swallows the NDJSON the stdout sink emits and the logs the
// telemetry logger writes to stderr (including the benign context-canceled
// fetch logged during shutdown drain). Both streams are bound at boot — the
// sink captures os.Stdout at construction and telemetry.Setup captures
// os.Stderr — so the redirect must be in place before serve wires up. The
// returned func restores them. A background drain keeps the pipes from
// blocking their writers.
func silenceOutput(t *testing.T) func() {
	t.Helper()
	origOut, origErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	require.NoError(t, err)
	rErr, wErr, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout, os.Stderr = wOut, wErr

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(io.Discard, rOut); done <- struct{}{} }()
	go func() { _, _ = io.Copy(io.Discard, rErr); done <- struct{}{} }()

	return func() {
		os.Stdout, os.Stderr = origOut, origErr
		_ = wOut.Close()
		_ = wErr.Close()
		<-done
		<-done
		_ = rOut.Close()
		_ = rErr.Close()
	}
}

func TestServeCommandBootsPollsAndShutsDown(t *testing.T) {
	fetched := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case fetched <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(serveFeedBody))
	}))
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "state.db")
	cfgPath := writeServeConfig(t, statePath, srv.URL)

	restore := silenceOutput(t)
	defer restore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		root := newRootCmd()
		root.SetArgs([]string{"serve", "--config", cfgPath})
		errCh <- root.ExecuteContext(ctx)
	}()

	// Wait for the daemon to boot and poll the feed once.
	select {
	case <-fetched:
	case err := <-errCh:
		t.Fatalf("serve exited before polling the feed: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not poll the feed within 20s")
	}

	// Cancelling the context must drain and return without error.
	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err, "serve must shut down cleanly on context cancel")
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not shut down within 20s of cancel")
	}
}

func TestServeCommandRejectsMissingConfig(t *testing.T) {
	root := newRootCmd()
	root.SetArgs([]string{"serve", "--config", filepath.Join(t.TempDir(), "nope.yaml")})
	require.Error(t, root.ExecuteContext(context.Background()))
}
