// Package redis provides a Coordinator backed by a Redis lease:
//
//	SET key token NX EX <lock_ttl>
//
// with a background renewal goroutine that CAS-extends the lease every
// LockTTL/3, and a CAS-checked DEL on release. The key derivation mirrors
// the Postgres backend's hash domain (sha256(feed_url)) but is rendered as
// hex for human-readable debugging via `KEYS rss2msg:coord:*`.
package redis

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/coord"
)

// Options configures the Redis-backed Coordinator. Zero LockTTL means "30s";
// zero RenewalInterval means "LockTTL / 3"; zero MemberTTL means "30s".
type Options struct {
	URL             string        // required for single mode
	LockTTL         time.Duration // 0 -> 30s
	RenewalInterval time.Duration // 0 -> LockTTL / 3
	MemberTTL       time.Duration // 0 -> 30s

	// TLS, if non-nil, overrides the default TLS config that redis.ParseURL
	// produces for rediss:// URLs (system roots, SNI = URL host). Must be
	// nil for plain redis:// URLs.
	TLS *TLSOptions

	Mode     string // "" or "single" | "sentinel" | "cluster"
	Sentinel SentinelOptions
	Cluster  ClusterOptions
}

// TLSOptions configures custom TLS for rediss:// connections. Zero-valued
// fields fall back to safe defaults; an entirely zero-valued TLSOptions is
// equivalent to leaving Options.TLS nil (default rediss:// TLS).
type TLSOptions struct {
	// CAFile is a PEM-encoded CA bundle to trust instead of the system roots.
	CAFile string
	// CertFile and KeyFile together enable client-certificate (mTLS) auth.
	// Either both or neither must be set.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / certificate verification hostname.
	// Defaults to the URL host.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. For
	// local/test only — logged at warn.
	InsecureSkipVerify bool
}

// SentinelOptions configures Redis Sentinel (failover) connections.
type SentinelOptions struct {
	MasterName       string
	Addrs            []string
	Username         string // data-node (master/replica) auth
	Password         string
	SentinelUsername string // sentinel-node auth
	SentinelPassword string
	DB               int
}

// ClusterOptions configures Redis Cluster connections.
type ClusterOptions struct {
	Addrs    []string
	Username string
	Password string
}

type resolvedOptions struct {
	URL             string
	LockTTL         time.Duration
	RenewalInterval time.Duration
	MemberTTL       time.Duration
}

func (o Options) resolved() resolvedOptions {
	r := resolvedOptions{
		URL:             o.URL,
		LockTTL:         o.LockTTL,
		RenewalInterval: o.RenewalInterval,
		MemberTTL:       o.MemberTTL,
	}
	if r.LockTTL <= 0 {
		r.LockTTL = 30 * time.Second
	}
	if r.RenewalInterval <= 0 {
		r.RenewalInterval = r.LockTTL / 3
	}
	if r.MemberTTL <= 0 {
		r.MemberTTL = 30 * time.Second
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
	client redis.UniversalClient
	opts   resolvedOptions

	mu      sync.Mutex
	held    map[*lease]struct{} // nil after Close
	closing bool
	closed  bool // true once client.Close() has been called
}

// buildClient constructs the topology-appropriate client WITHOUT dialing.
func buildClient(opts Options) (redis.UniversalClient, error) {
	switch opts.Mode {
	case "", "single":
		if opts.URL == "" {
			return nil, fmt.Errorf("coord/redis: url is required for single mode")
		}
		cfg, err := redis.ParseURL(opts.URL)
		if err != nil {
			return nil, fmt.Errorf("coord/redis: parse url: %w", err)
		}
		if opts.TLS != nil {
			if cfg.TLSConfig == nil {
				return nil, fmt.Errorf("coord/redis: TLS options provided but URL scheme is not rediss://")
			}
			tc, err := buildTLSConfig(*opts.TLS, cfg.TLSConfig.ServerName)
			if err != nil {
				return nil, fmt.Errorf("coord/redis: build TLS config: %w", err)
			}
			cfg.TLSConfig = tc
			warnInsecureTLS(opts.TLS)
		}
		return redis.NewClient(cfg), nil
	case "sentinel":
		tc, err := tlsConfigOrNil(opts.TLS)
		if err != nil {
			return nil, err
		}
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       opts.Sentinel.MasterName,
			SentinelAddrs:    opts.Sentinel.Addrs,
			Username:         opts.Sentinel.Username,
			Password:         opts.Sentinel.Password,
			SentinelUsername: opts.Sentinel.SentinelUsername,
			SentinelPassword: opts.Sentinel.SentinelPassword,
			DB:               opts.Sentinel.DB,
			TLSConfig:        tc,
		}), nil
	case "cluster":
		tc, err := tlsConfigOrNil(opts.TLS)
		if err != nil {
			return nil, err
		}
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     opts.Cluster.Addrs,
			Username:  opts.Cluster.Username,
			Password:  opts.Cluster.Password,
			TLSConfig: tc,
		}), nil
	default:
		return nil, fmt.Errorf("coord/redis: unsupported mode %q", opts.Mode)
	}
}

// tlsConfigOrNil builds a *tls.Config for sentinel/cluster modes (no URL to
// derive an SNI host from, so defaultServerName is empty; set TLSOptions.ServerName
// if certificate verification needs a specific host).
func tlsConfigOrNil(t *TLSOptions) (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}
	tc, err := buildTLSConfig(*t, "")
	if err != nil {
		return nil, fmt.Errorf("coord/redis: build TLS config: %w", err)
	}
	warnInsecureTLS(t)
	return tc, nil
}

func warnInsecureTLS(t *TLSOptions) {
	if t != nil && t.InsecureSkipVerify {
		log.Warn().
			Str("coord_driver", "redis").
			Msg("coord/redis: TLS verification disabled (insecure_skip_verify=true)")
	}
}

// New builds the client for opts.Mode, dials it, and returns a ready Coordinator.
func New(ctx context.Context, opts Options) (*Coordinator, error) {
	ro := opts.resolved()
	client, err := buildClient(opts)
	if err != nil {
		return nil, err
	}
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

// buildTLSConfig translates TLSOptions into a *tls.Config. defaultServerName
// is the SNI host that redis.ParseURL inferred from the URL; it's used when
// the caller didn't override it.
func buildTLSConfig(opts TLSOptions, defaultServerName string) (*tls.Config, error) {
	tc := &tls.Config{
		ServerName:         defaultServerName,
		InsecureSkipVerify: opts.InsecureSkipVerify,
	}
	if opts.ServerName != "" {
		tc.ServerName = opts.ServerName
	}
	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %q: no PEM certificates parsed", opts.CAFile)
		}
		tc.RootCAs = pool
	}
	if opts.CertFile != "" || opts.KeyFile != "" {
		// Validation upstream guarantees both-or-neither, but defend against
		// programmatic callers that bypass it.
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, fmt.Errorf("cert_file and key_file must both be set or both empty")
		}
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("client cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
