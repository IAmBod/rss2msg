package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadMergesDefaultsAndFile(t *testing.T) {
	t.Parallel()
	p := writeTempConfig(t, `
log:
  level: debug
state:
  driver: postgres
  postgres:
    dsn: postgres://x
sinks:
  - name: default
    driver: kafka
    kafka:
      brokers: ["k:9092"]
      topic: t
feeds:
  - url: https://e/feed
    interval: 5m
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Log.Level != "debug" {
		t.Fatalf("override failed, got level=%q", cfg.Log.Level)
	}
	if cfg.HTTP.Timeout != 30*time.Second {
		t.Fatalf("default not applied, got %v", cfg.HTTP.Timeout)
	}
	if len(cfg.Sinks) != 1 || cfg.Sinks[0].Name != "default" {
		t.Fatalf("bad sinks: %+v", cfg.Sinks)
	}
}

func TestLoadEnvVarSubstitution(t *testing.T) {
	t.Setenv("MY_DSN", "postgres://from-env")
	p := writeTempConfig(t, `
state:
  driver: postgres
  postgres:
    dsn: ${MY_DSN}
sinks:
  - name: default
    driver: kafka
    kafka:
      brokers: ["k:9092"]
      topic: t
feeds:
  - url: https://e/feed
    interval: 5m
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.State.Postgres.DSN != "postgres://from-env" {
		t.Fatalf("env substitution failed, got %q", cfg.State.Postgres.DSN)
	}
}

func TestLoadEnvSubstitutionReachesNestedFields(t *testing.T) {
	// not t.Parallel() — uses t.Setenv
	t.Setenv("PG_DSN", "postgres://from-env/db")
	t.Setenv("TOKEN", "secret123")
	p := writeTempConfig(t, `
state:
  driver: postgres
  postgres:
    dsn: ${PG_DSN}
sinks:
  - name: default
    driver: postgres
    postgres:
      dsn: ${PG_DSN}
      table: t
feeds:
  - url: https://e/feed
    interval: 5m
    http:
      headers:
        Authorization: "Bearer ${TOKEN}"
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.State.Postgres.DSN != "postgres://from-env/db" {
		t.Fatalf("state.postgres.dsn = %q", cfg.State.Postgres.DSN)
	}
	if cfg.Sinks[0].Postgres.DSN != "postgres://from-env/db" {
		t.Fatalf("sinks[0].postgres.dsn = %q", cfg.Sinks[0].Postgres.DSN)
	}
	auth := cfg.Feeds[0].HTTP.Headers["Authorization"]
	// viper lowercases map keys, so try both
	if auth == "" {
		auth = cfg.Feeds[0].HTTP.Headers["authorization"]
	}
	if auth != "Bearer secret123" {
		t.Fatalf("feed header Authorization = %q (map = %+v)", auth, cfg.Feeds[0].HTTP.Headers)
	}
}

func TestLoadParsesFeedSources(t *testing.T) {
	t.Parallel()
	path := writeTempConfig(t, `
feed_sources:
  - type: file
    name: control-plane
    path: /etc/rss2msg/feeds.json
    interval: 30s
  - type: static
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.FeedSources) != 2 {
		t.Fatalf("want 2 feed sources, got %d", len(cfg.FeedSources))
	}
	if cfg.FeedSources[0].Type != "file" || cfg.FeedSources[0].Name != "control-plane" {
		t.Fatalf("source[0] = %+v", cfg.FeedSources[0])
	}
	if cfg.FeedSources[0].Interval != 30*time.Second {
		t.Fatalf("interval = %v", cfg.FeedSources[0].Interval)
	}
	if cfg.FeedSources[1].Type != "static" {
		t.Fatalf("source[1].type = %q", cfg.FeedSources[1].Type)
	}
}

func TestLoadEnvOverridesViaPrefix(t *testing.T) {
	t.Setenv("RSS2MSG_LOG__LEVEL", "warn")
	p := writeTempConfig(t, `
log:
  level: info
state:
  driver: postgres
  postgres:
    dsn: x
sinks:
  - name: default
    driver: kafka
    kafka:
      brokers: ["k:9092"]
      topic: t
feeds:
  - url: https://e/feed
    interval: 5m
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Log.Level != "warn" {
		t.Fatalf("env override failed, got %q", cfg.Log.Level)
	}
}
