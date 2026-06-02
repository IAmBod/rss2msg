package main

import (
	"testing"

	"github.com/iambod/rss2msg/internal/config"
)

func TestBuildSourcesPostgres(t *testing.T) {
	cfg := config.Config{
		FeedSources: []config.FeedSourceConfig{{
			Type:     "postgres",
			Name:     "db",
			Postgres: config.PostgresFeedSourceConfig{DSN: "postgres://u:p@localhost:5432/db"},
		}},
	}
	sources, cleanup, err := buildSources(cfg)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	defer cleanup()
	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}
	if sources[0].Name() != "db" {
		t.Errorf("name: got %q want %q", sources[0].Name(), "db")
	}
}

func TestBuildSourcesPostgresBadDSN(t *testing.T) {
	cfg := config.Config{
		FeedSources: []config.FeedSourceConfig{{
			Type:     "postgres",
			Postgres: config.PostgresFeedSourceConfig{DSN: "://not a dsn"},
		}},
	}
	if _, _, err := buildSources(cfg); err == nil {
		t.Fatal("expected error for malformed dsn")
	}
}
