package feedsource

import (
	"testing"
	"time"
)

func TestFeedSpecToFeedConfig(t *testing.T) {
	spec := FeedSpec{
		URL:      "https://example.com/feed.xml",
		Interval: "5m",
		Sinks:    []string{"out"},
		HTTP:     &FeedSpecHTTP{Timeout: "10s", Headers: map[string]string{"X-Token": "abc"}},
	}
	fc, err := spec.ToFeedConfig()
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if fc.URL != "https://example.com/feed.xml" {
		t.Fatalf("url = %q", fc.URL)
	}
	if fc.Interval != 5*time.Minute {
		t.Fatalf("interval = %v", fc.Interval)
	}
	if len(fc.Sinks) != 1 || fc.Sinks[0] != "out" {
		t.Fatalf("sinks = %v", fc.Sinks)
	}
	if fc.HTTP.Timeout != 10*time.Second || fc.HTTP.Headers["X-Token"] != "abc" {
		t.Fatalf("http = %+v", fc.HTTP)
	}
}

func TestFeedSpecRejectsEmptyURL(t *testing.T) {
	if _, err := (FeedSpec{}).ToFeedConfig(); err == nil {
		t.Fatal("expected error for empty url")
	}
	if _, err := (FeedSpec{URL: "   "}).ToFeedConfig(); err == nil {
		t.Fatal("expected error for whitespace-only url")
	}
}

func TestFeedSpecRejectsBadDuration(t *testing.T) {
	if _, err := (FeedSpec{URL: "u", Interval: "nope"}).ToFeedConfig(); err == nil {
		t.Fatal("expected error for bad interval")
	}
}

func TestFeedSpecRejectsBadHTTPTimeout(t *testing.T) {
	spec := FeedSpec{URL: "u", HTTP: &FeedSpecHTTP{Timeout: "nope"}}
	if _, err := spec.ToFeedConfig(); err == nil {
		t.Fatal("expected error for bad http.timeout")
	}
}
