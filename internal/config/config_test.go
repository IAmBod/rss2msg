package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mustLoadYAML writes yaml to a temp file and loads it as a Config.
func mustLoadYAML(t *testing.T, yaml string) Config {
	t.Helper()
	p := writeTempConfig(t, yaml)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("mustLoadYAML: %v", err)
	}
	return cfg
}

func TestRedisCoordinationModeBlocksParse(t *testing.T) {
	cfg := mustLoadYAML(t, `
coordination:
  driver: redis
  redis:
    mode: sentinel
    lock_ttl: 30s
    sentinel:
      master_name: mymaster
      addrs: [a:26379, b:26379]
      password: secret
      sentinel_password: spass
      db: 2
    cluster:
      addrs: [n1:6379, n2:6379]
      username: u
`)
	r := cfg.Coordination.Redis
	require.Equal(t, "sentinel", r.Mode)
	require.Equal(t, "mymaster", r.Sentinel.MasterName)
	require.Equal(t, []string{"a:26379", "b:26379"}, r.Sentinel.Addrs)
	require.Equal(t, "secret", r.Sentinel.Password)
	require.Equal(t, "spass", r.Sentinel.SentinelPassword)
	require.Equal(t, 2, r.Sentinel.DB)
	require.Equal(t, []string{"n1:6379", "n2:6379"}, r.Cluster.Addrs)
	require.Equal(t, "u", r.Cluster.Username)
}

func TestRedisCoordinationLegacyURLStillParses(t *testing.T) {
	cfg := mustLoadYAML(t, `
coordination:
  driver: redis
  redis:
    url: redis://localhost:6379
`)
	require.Equal(t, "", cfg.Coordination.Redis.Mode)
	require.Equal(t, "redis://localhost:6379", cfg.Coordination.Redis.URL)
}

func TestLoad_FeedSink(t *testing.T) {
	t.Parallel()
	p := writeTempConfig(t, `
state:
  driver: sqlite
  sqlite: { path: /tmp/s.db }
feeds:
  - url: https://example.com/f.xml
sinks:
  - name: out-feed
    driver: feed
    feed:
      listen: ":8088"
      public_url: "https://feeds.example.com"
      title: "changes"
      link: "https://example.com/"
      max_items: 25
      store:
        driver: postgres
        postgres: { dsn: "postgres://x", table: feed_output }
      auth:
        basic_users:
          - { name: ops, username: u, password: p }
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Sinks) != 1 {
		t.Fatalf("want 1 sink, got %d", len(cfg.Sinks))
	}
	s := cfg.Sinks[0]
	if s.Feed.Listen != ":8088" {
		t.Fatalf("feed.listen = %q, want :8088", s.Feed.Listen)
	}
	if s.Feed.PublicURL != "https://feeds.example.com" {
		t.Fatalf("feed.public_url = %q, want https://feeds.example.com", s.Feed.PublicURL)
	}
	if s.Feed.MaxItems != 25 {
		t.Fatalf("feed.max_items = %d, want 25", s.Feed.MaxItems)
	}
	if s.Feed.Store.Driver != "postgres" {
		t.Fatalf("feed.store.driver = %q, want postgres", s.Feed.Store.Driver)
	}
	if s.Feed.Store.Postgres.Table != "feed_output" {
		t.Fatalf("feed.store.postgres.table = %q, want feed_output", s.Feed.Store.Postgres.Table)
	}
	if len(s.Feed.Auth.BasicUsers) != 1 || s.Feed.Auth.BasicUsers[0].Username != "u" {
		t.Fatalf("feed.auth.basic_users[0].username = %+v, want username u", s.Feed.Auth.BasicUsers)
	}
}

func TestLoad_FeedSinkSurfaces(t *testing.T) {
	t.Parallel()
	cfg := mustLoadYAML(t, `
state:
  driver: sqlite
  sqlite: { path: /tmp/s.db }
feeds:
  - url: https://example.com/f.xml
sinks:
  - name: out-feed
    driver: feed
    feed:
      listen: ":8088"
      rss:  { enabled: true,  path: /feed.rss }
      atom: { enabled: false, path: /feed.atom }
      mcp:  { enabled: true,  path: /mcp }
`)
	f := cfg.Sinks[0].Feed
	if !f.RSS.On(false) || f.RSS.Path != "/feed.rss" {
		t.Fatalf("rss surface = %+v, want enabled with /feed.rss", f.RSS)
	}
	if f.Atom.On(true) {
		t.Fatalf("atom surface should be explicitly disabled, got %+v", f.Atom)
	}
	if !f.MCP.On(false) || f.MCP.Path != "/mcp" {
		t.Fatalf("mcp surface = %+v, want enabled with /mcp", f.MCP)
	}
}

func TestFeedSurfaceDefaults(t *testing.T) {
	t.Parallel()
	var zero FeedSurfaceConfig
	// An unset surface honors whatever default the caller passes — this is how
	// rss/atom default on while mcp defaults off, from the same zero value.
	if !zero.On(true) {
		t.Fatal("unset surface should resolve On(true) => true")
	}
	if zero.On(false) {
		t.Fatal("unset surface should resolve On(false) => false")
	}
	if zero.PathOr("/rss") != "/rss" {
		t.Fatalf("PathOr default = %q, want /rss", zero.PathOr("/rss"))
	}
	// An explicitly-disabled surface stays off even when the default is on.
	off := false
	disabled := FeedSurfaceConfig{Enabled: &off}
	if disabled.On(true) {
		t.Fatal("explicitly disabled surface should resolve On(true) => false")
	}
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	d := Defaults()
	if d.Log.Level != "info" || d.Log.Format != "json" {
		t.Fatalf("bad log defaults: %+v", d.Log)
	}
	if d.HTTP.Timeout != 30*time.Second {
		t.Fatalf("bad http timeout default: %v", d.HTTP.Timeout)
	}
	if d.Retry.MaxAttempts != 3 || d.Retry.BaseDelay != 500*time.Millisecond {
		t.Fatalf("bad retry defaults: %+v", d.Retry)
	}
	if d.Runtime.ShutdownDrainTimeout != 30*time.Second {
		t.Fatalf("bad shutdown default: %v", d.Runtime.ShutdownDrainTimeout)
	}
}
