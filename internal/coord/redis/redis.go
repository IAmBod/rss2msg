// Package redis provides a Coordinator backed by a Redis lease:
//   SET key token NX EX <lock_ttl>
// with a background renewal goroutine that CAS-extends the lease every
// LockTTL/3, and a CAS-checked DEL on release. The key derivation mirrors
// the Postgres backend's hash domain (sha256(feed_url)) but is rendered as
// hex for human-readable debugging via `KEYS rss2msg:coord:*`.
package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

// Options configures the Redis-backed Coordinator. Zero LockTTL means "30s";
// zero RenewalInterval means "LockTTL / 3".
type Options struct {
	URL             string        // required
	LockTTL         time.Duration // 0 -> 30s
	RenewalInterval time.Duration // 0 -> LockTTL / 3
}

type resolvedOptions struct {
	URL             string
	LockTTL         time.Duration
	RenewalInterval time.Duration
}

func (o Options) resolved() resolvedOptions {
	r := resolvedOptions{
		URL:             o.URL,
		LockTTL:         o.LockTTL,
		RenewalInterval: o.RenewalInterval,
	}
	if r.LockTTL <= 0 {
		r.LockTTL = 30 * time.Second
	}
	if r.RenewalInterval <= 0 {
		r.RenewalInterval = r.LockTTL / 3
	}
	return r
}

// lockKey is the Redis key for feedURL.
func lockKey(feedURL string) string {
	sum := sha256.Sum256([]byte(feedURL))
	return "rss2msg:coord:" + hex.EncodeToString(sum[:])
}

// newToken returns a fresh per-acquisition owner token.
func newToken() string {
	return uuid.NewString()
}
