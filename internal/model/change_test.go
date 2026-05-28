package model

import (
	"strings"
	"testing"
	"time"
)

func TestContentHashIsStableAndNormalised(t *testing.T) {
	t.Parallel()
	a := ContentHash("Hello", "https://e/1", "  body  text  ", "alice", time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	b := ContentHash("Hello", "https://e/1", "body text", "alice", time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	if a != b {
		t.Fatalf("whitespace normalisation expected, got %s vs %s", a, b)
	}
	if len(a) != 64 || strings.ToLower(a) != a {
		t.Fatalf("expected lowercase hex sha256, got %q", a)
	}
}

func TestContentHashChangesWhenContentChanges(t *testing.T) {
	t.Parallel()
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	a := ContentHash("Hello", "https://e/1", "body one", "alice", when)
	b := ContentHash("Hello", "https://e/1", "body two", "alice", when)
	if a == b {
		t.Fatalf("hashes should differ when content differs")
	}
}

func TestIdentityKeyFallbackChain(t *testing.T) {
	t.Parallel()
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if got := IdentityKey("guid-1", "https://e/1", "Title", when); got != "guid-1" {
		t.Fatalf("expected GUID winner, got %q", got)
	}
	if got := IdentityKey("", "https://e/1", "Title", when); got != "https://e/1" {
		t.Fatalf("expected link fallback, got %q", got)
	}
	got := IdentityKey("", "", "Title", when)
	if len(got) != 64 {
		t.Fatalf("expected sha256 synthetic, got %q", got)
	}
}

func TestChangeJSONRoundTrip(t *testing.T) {
	t.Parallel()
	when := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := Change{
		SchemaVersion: 1,
		FeedURL:       "https://e/feed",
		ItemID:        "id-1",
		Kind:          ChangeNew,
		Title:         "t",
		Link:          "https://e/1",
		ContentHash:   "abc",
		DetectedAt:    when,
	}
	b, err := c.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var got Change
	if err := got.UnmarshalJSON(b); err != nil {
		t.Fatal(err)
	}
	if got.ItemID != c.ItemID || got.Kind != c.Kind {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}
