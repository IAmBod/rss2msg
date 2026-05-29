// Package redis provides a Coordinator backed by a Redis lease:
//   SET key token NX EX <lock_ttl>
// with a background renewal goroutine that CAS-extends the lease every
// LockTTL/3, and a CAS-checked DEL on release. The key derivation mirrors
// the Postgres backend's hash domain (sha256(feed_url)) but is rendered as
// hex for human-readable debugging via `KEYS rss2msg:coord:*`.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/coord"
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

// renewScript: PEXPIRE key only if its value still matches our token.
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end
`)

// releaseScript: DEL key only if its value still matches our token.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

type lease struct {
	key    string
	token  string
	cancel context.CancelFunc
	done   chan struct{}
}

type Coordinator struct {
	client *redis.Client
	opts   resolvedOptions

	mu      sync.Mutex
	held    map[*lease]struct{} // nil after Close
	closing bool
	closed  bool // true once client.Close() has been called
}

// New parses opts.URL, dials Redis, and returns a ready Coordinator.
func New(ctx context.Context, opts Options) (*Coordinator, error) {
	ro := opts.resolved()
	if ro.URL == "" {
		return nil, fmt.Errorf("coord/redis: url is required")
	}
	cfg, err := redis.ParseURL(ro.URL)
	if err != nil {
		return nil, fmt.Errorf("coord/redis: parse url: %w", err)
	}
	client := redis.NewClient(cfg)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("coord/redis: ping: %w", err)
	}
	return &Coordinator{
		client: client,
		opts:   ro,
		held:   make(map[*lease]struct{}),
	}, nil
}

// Close cancels every renewal goroutine, best-effort CAS-deletes every
// still-held lease, and closes the underlying client.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	if c.held == nil {
		c.mu.Unlock()
		return nil
	}
	c.closing = true
	leases := make([]*lease, 0, len(c.held))
	for l := range c.held {
		leases = append(leases, l)
	}
	c.held = nil
	c.mu.Unlock()

	for _, l := range leases {
		l.cancel()
		<-l.done
		c.casDelete(l)
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.client.Close()
}

func (c *Coordinator) TryAcquire(ctx context.Context, feedURL string) (coord.ReleaseFunc, bool, error) {
	key := lockKey(feedURL)
	token := newToken()

	ok, err := c.client.SetNX(ctx, key, token, c.opts.LockTTL).Result()
	if err != nil {
		return nil, false, fmt.Errorf("coord/redis: SET NX EX: %w", err)
	}
	if !ok {
		return nil, false, nil
	}

	renewalCtx, cancel := context.WithCancel(context.Background())
	l := &lease{
		key:    key,
		token:  token,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	c.mu.Lock()
	if c.held == nil || c.closing {
		c.mu.Unlock()
		cancel()
		close(l.done)
		c.casDelete(l) // best-effort
		return nil, false, nil
	}
	c.held[l] = struct{}{}
	c.mu.Unlock()

	go c.renewLoop(renewalCtx, l, feedURL)

	release := func(_ context.Context) error {
		c.mu.Lock()
		if c.held == nil {
			c.mu.Unlock()
			return nil
		}
		if _, ok := c.held[l]; !ok {
			c.mu.Unlock()
			return nil
		}
		delete(c.held, l)
		c.mu.Unlock()

		l.cancel()
		<-l.done
		c.casDelete(l)
		return nil
	}
	return release, true, nil
}

// renewLoop CAS-extends the lease every opts.RenewalInterval until ctx is
// canceled or Redis tells us we no longer own the key. Lock-loss events are
// logged at warn; the goroutine exits and the eventual release becomes a
// no-op (casDelete will also see CAS=0).
func (c *Coordinator) renewLoop(ctx context.Context, l *lease, feedURL string) {
	defer close(l.done)
	t := time.NewTicker(c.opts.RenewalInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			renewCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			res, err := renewScript.Run(renewCtx, c.client,
				[]string{l.key}, l.token, c.opts.LockTTL.Milliseconds()).Result()
			cancel()
			if err != nil {
				log.Warn().
					Str("coord_driver", "redis").
					Str("feed_url", feedURL).
					Str("event", "renew_error").
					Err(err).
					Msg("coord/redis: renew failed; exiting renewal loop")
				return
			}
			n, _ := res.(int64)
			if n == 0 {
				log.Warn().
					Str("coord_driver", "redis").
					Str("feed_url", feedURL).
					Str("event", "lock_lost").
					Msg("coord/redis: lease lost (CAS mismatch); exiting renewal loop")
				return
			}
		}
	}
}

// casDelete runs the release Lua script on a fresh 5s background ctx. A
// return of 0 (TTL already expired, or another instance now holds the key,
// or the renewal goroutine already noted the loss) is not an error.
func (c *Coordinator) casDelete(l *lease) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := releaseScript.Run(delCtx, c.client, []string{l.key}, l.token).Err(); err != nil {
		log.Warn().
			Str("coord_driver", "redis").
			Str("event", "release_error").
			Err(err).
			Msg("coord/redis: CAS delete failed")
	}
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
