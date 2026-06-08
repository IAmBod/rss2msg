package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/model"
	statesqlite "github.com/iambod/rss2msg/internal/state/sqlite"
	"github.com/iambod/rss2msg/internal/telemetry"
)

// serveFeedRSS starts an httptest server that serves the given RSS body and
// returns its URL.
func serveFeedRSS(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// These tests drive the real cobra commands end-to-end through the production
// wiring (config.Load -> Validate -> telemetry.Setup -> wireAll -> the
// scheduler -> pipeline.RunOnce), using only Docker-free backends: a memory
// coordinator, a SQLite state file, an httptest feed, and a stdout sink. They
// cover bootstrap/wireAll/openStateStore/openCoordinator/buildPublisher/
// newPipelineFactory, which no unit or e2e test previously exercised.

const runOnceFeed = `<?xml version="1.0"?>
<rss version="2.0"><channel><title>Sample</title>
<item><guid>a</guid><title>One</title><link>https://e/a</link><description>first</description></item>
</channel></rss>`

// writeConfig writes a minimal, valid config to a temp file and returns its
// path. The state DB path and feed URL are substituted in.
func writeConfig(t *testing.T, statePath, feedURL string) string {
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

// captureStdout redirects os.Stdout for the duration of fn and returns whatever
// was written. The stdout sink binds os.Stdout at construction, so the redirect
// must be in place before fn wires the pipeline up.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out)
}

func newTestTelemetry(t *testing.T) *telemetry.Telemetry {
	t.Helper()
	tel, err := telemetry.Setup(context.Background(), config.Defaults(), io.Discard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	return tel
}

func TestRunOnceCommandEndToEnd(t *testing.T) {
	feed := serveFeedRSS(t, runOnceFeed)
	statePath := filepath.Join(t.TempDir(), "state.db")
	cfgPath := writeConfig(t, statePath, feed)

	out := captureStdout(t, func() {
		root := newRootCmd()
		root.SetArgs([]string{"run-once", "--config", cfgPath})
		require.NoError(t, root.ExecuteContext(context.Background()))
	})

	// The detected "new" change was published as NDJSON by the stdout sink.
	require.Contains(t, out, `"kind":"new"`)
	var change model.Change
	require.NoError(t, json.Unmarshal(bytes.TrimSpace([]byte(out)), &change),
		"stdout sink should emit exactly one NDJSON change")
	require.Equal(t, "One", change.Title)

	// State was persisted through the SQLite store the wiring opened: reopening
	// the same file shows the item committed with the published content hash.
	st, err := statesqlite.New(context.Background(), statePath)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()
	item, found, err := st.GetItem(context.Background(), feed, change.ItemID)
	require.NoError(t, err)
	require.True(t, found, "the item should be committed after a successful poll")
	require.Equal(t, change.ContentHash, item.ContentHash)
}

func TestValidateConfigCommandEndToEnd(t *testing.T) {
	feed := serveFeedRSS(t, runOnceFeed)
	statePath := filepath.Join(t.TempDir(), "state.db")
	cfgPath := writeConfig(t, statePath, feed)

	out := captureStdout(t, func() {
		root := newRootCmd()
		root.SetArgs([]string{"validate-config", "--config", cfgPath})
		require.NoError(t, root.ExecuteContext(context.Background()))
	})
	require.Contains(t, out, "config OK")
}

func TestRunOnceCommandRejectsMissingConfig(t *testing.T) {
	root := newRootCmd()
	root.SetArgs([]string{"run-once", "--config", filepath.Join(t.TempDir(), "nope.yaml")})
	require.Error(t, root.ExecuteContext(context.Background()))
}

// The following exercise wireAll's driver-dispatch error branches directly,
// since config.Validate would reject these configs before the command reached
// wireAll.

func TestOpenStateStoreUnsupportedDriver(t *testing.T) {
	_, err := openStateStore(context.Background(), config.StateConfig{Driver: "bogus"})
	require.ErrorContains(t, err, "unsupported state driver")
}

func TestOpenCoordinatorUnsupportedDriver(t *testing.T) {
	_, err := openCoordinator(context.Background(),
		config.CoordinationConfig{Driver: "bogus"}, config.StateConfig{}, 1)
	require.ErrorContains(t, err, "unsupported coordination driver")
}

func TestOpenCoordinatorDefaultsToMemory(t *testing.T) {
	cd, err := openCoordinator(context.Background(),
		config.CoordinationConfig{}, config.StateConfig{}, 1)
	require.NoError(t, err)
	require.NotNil(t, cd)
	_ = cd.Close()
}

func TestBuildPublisherUnknownDriver(t *testing.T) {
	tel := newTestTelemetry(t)
	_, err := buildPublisher(context.Background(), config.SinkConfig{Name: "x", Driver: "bogus"}, tel)
	require.Error(t, err)
}

func TestWireAllErrorsOnUnknownStateDriver(t *testing.T) {
	tel := newTestTelemetry(t)
	_, err := wireAll(context.Background(), config.Config{
		State: config.StateConfig{Driver: "bogus"},
	}, tel)
	require.ErrorContains(t, err, "unsupported state driver")
}
