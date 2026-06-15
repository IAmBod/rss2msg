package schema

import (
	"strings"
	"testing"
)

func TestNewRejectsUnsupportedFormat(t *testing.T) {
	_, err := New(Options{URL: "http://sr:8081", Format: "avro", Topic: "t"})
	if err == nil || !strings.Contains(err.Error(), "format") {
		t.Fatalf("want unsupported-format error, got %v", err)
	}
}

func TestNewRequiresURL(t *testing.T) {
	_, err := New(Options{Format: FormatJSON, Topic: "t"})
	if err == nil || !strings.Contains(err.Error(), "url") {
		t.Fatalf("want url-required error, got %v", err)
	}
}

func TestNewRequiresTopicOrSubject(t *testing.T) {
	_, err := New(Options{URL: "http://sr:8081", Format: FormatJSON})
	if err == nil || !strings.Contains(err.Error(), "topic") {
		t.Fatalf("want topic-required error, got %v", err)
	}
}

func TestDefaultSubjectIsTopicValue(t *testing.T) {
	if got := defaultSubject("feed.changes", ""); got != "feed.changes-value" {
		t.Fatalf("default subject = %q, want feed.changes-value", got)
	}
	if got := defaultSubject("feed.changes", "custom"); got != "custom" {
		t.Fatalf("explicit subject = %q, want custom", got)
	}
}
