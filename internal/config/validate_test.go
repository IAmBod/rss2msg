package config

import (
	"strings"
	"testing"
	"time"
)

func goodCfg() Config {
	c := Defaults()
	c.State.Driver = "postgres"
	c.State.Postgres.DSN = "postgres://x"
	c.Sinks = []SinkConfig{
		{Name: "default", Driver: "kafka", Kafka: KafkaSinkConfig{Brokers: []string{"k:9092"}, Topic: "t"}},
		{Name: "pg-main", Driver: "postgres", Postgres: PostgresSinkConfig{DSN: "postgres://x", Table: "feed_changes"}},
		{Name: "dlq-main", Driver: "postgres", Postgres: PostgresSinkConfig{DSN: "postgres://x", Table: "feed_changes_dlq"}},
	}
	c.Sinks[1].DeadLetter = "dlq-main"
	c.Feeds = []FeedConfig{
		{URL: "https://e/1", Interval: 5 * time.Minute, Sinks: []string{"pg-main"}},
		{URL: "https://e/2", Interval: 15 * time.Minute},
	}
	return c
}

func TestValidateAcceptsGoodConfig(t *testing.T) {
	t.Parallel()
	if err := Validate(goodCfg()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsUnknownSinkName(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Feeds[0].Sinks = []string{"does-not-exist"}
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "does-not-exist") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRequiresDefaultSinkWhenFeedOmitsSinks(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = c.Sinks[1:] // remove "default"
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), `"default"`) {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsSinkAsItsOwnDLQ(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks[1].DeadLetter = c.Sinks[1].Name
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "dead_letter") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsIntervalBelowOneSecond(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Feeds[0].Interval = 500 * time.Millisecond
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsReservedHeaderOverrides(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Feeds[1].HTTP.Headers = map[string]string{"If-Modified-Since": "now"}
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "If-Modified-Since") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsDuplicateSinkNames(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "pg-main", Driver: "postgres", Postgres: PostgresSinkConfig{DSN: "x", Table: "y"}})
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsUnknownStateDriver(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State.Driver = "redis"
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "state.driver") {
		t.Fatalf("got %v", err)
	}
}
