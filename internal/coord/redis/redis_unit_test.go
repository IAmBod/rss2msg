package redis

import (
	"strings"
	"testing"
	"time"
)

func TestLockKeyIsDeterministicAndHumanReadable(t *testing.T) {
	t.Parallel()
	a := lockKey("https://example.com/feed.xml")
	b := lockKey("https://example.com/feed.xml")
	if a != b {
		t.Fatalf("lockKey not deterministic: %s vs %s", a, b)
	}
	if !strings.HasPrefix(a, "rss2msg:coord:") {
		t.Fatalf("expected rss2msg:coord: prefix, got %q", a)
	}
	hex := strings.TrimPrefix(a, "rss2msg:coord:")
	if len(hex) != 64 || strings.ToLower(hex) != hex {
		t.Fatalf("expected 64-char lowercase hex suffix, got %q", hex)
	}
}

func TestLockKeyDiffersForDifferentFeeds(t *testing.T) {
	t.Parallel()
	if lockKey("https://e/1") == lockKey("https://e/2") {
		t.Fatalf("expected distinct keys for distinct feed URLs")
	}
}

func TestNewTokenIsUnique(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		tok := newToken()
		if tok == "" {
			t.Fatalf("empty token")
		}
		if _, dup := seen[tok]; dup {
			t.Fatalf("token collision: %q", tok)
		}
		seen[tok] = struct{}{}
	}
}

func TestResolveDefaults(t *testing.T) {
	t.Parallel()
	o := Options{URL: "redis://localhost:6379/0"}
	r := o.resolved()
	if r.LockTTL.String() != "30s" {
		t.Fatalf("expected 30s default LockTTL, got %v", r.LockTTL)
	}
	if r.RenewalInterval.String() != "10s" {
		t.Fatalf("expected 10s default RenewalInterval (LockTTL/3), got %v", r.RenewalInterval)
	}
}

func TestResolveExplicitRenewalIntervalRespected(t *testing.T) {
	t.Parallel()
	o := Options{URL: "redis://x", LockTTL: 60 * time.Second, RenewalInterval: 5 * time.Second}
	r := o.resolved()
	if r.RenewalInterval.String() != "5s" {
		t.Fatalf("expected explicit 5s, got %v", r.RenewalInterval)
	}
}
