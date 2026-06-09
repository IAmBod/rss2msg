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

func TestLoadKafkaSchemaRegistryExplicitFalse(t *testing.T) {
	t.Parallel()
	p := writeTempConfig(t, `
state:
  driver: sqlite
  sqlite:
    path: ":memory:"
sinks:
  - name: events
    driver: kafka
    kafka:
      brokers: ["k:9092"]
      topic: feed.changes
      schema_registry:
        url: http://sr:8081
        format: json
        auto_register: false
feeds:
  - url: https://example.com/feed.xml
    interval: 60s
    sinks: [events]
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	sr := cfg.Sinks[0].Kafka.SchemaRegistry
	if sr.AutoRegister == nil {
		t.Fatal("auto_register should be non-nil when explicitly set")
	}
	if *sr.AutoRegister {
		t.Fatal("auto_register should be false")
	}
}

func TestLoadKafkaSchemaRegistry(t *testing.T) {
	t.Parallel()
	p := writeTempConfig(t, `
state:
  driver: sqlite
  sqlite:
    path: ":memory:"
sinks:
  - name: events
    driver: kafka
    kafka:
      brokers: ["k:9092"]
      topic: feed.changes
      schema_registry:
        url: http://sr:8081
        format: json
        auto_register: true
        subject: feed.changes-value
        basic_auth:
          username: u
          password: p
feeds:
  - url: https://example.com/feed.xml
    interval: 60s
    sinks: [events]
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	sr := cfg.Sinks[0].Kafka.SchemaRegistry
	if sr.URL != "http://sr:8081" || sr.Format != "json" {
		t.Fatalf("schema_registry not parsed: %+v", sr)
	}
	if sr.AutoRegister == nil || !*sr.AutoRegister {
		t.Fatalf("auto_register not parsed: %+v", sr.AutoRegister)
	}
	if sr.BasicAuth.Username != "u" || sr.BasicAuth.Password != "p" {
		t.Fatalf("basic_auth not parsed: %+v", sr.BasicAuth)
	}
}
