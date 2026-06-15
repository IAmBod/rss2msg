package feedsource

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/config"
)

// Compile-time assertion that *HTTP satisfies Source.
var _ Source = (*HTTP)(nil)

const (
	defaultHTTPTimeout = 30 * time.Second
	// maxBodyBytes caps the feed-list response read into memory so a
	// misconfigured or hostile endpoint cannot OOM the process.
	maxBodyBytes = 10 << 20 // 10 MiB
)

// HTTPTLSOptions is the client TLS surface for the http feed source. Setting any
// field configures a custom *tls.Config on the transport.
type HTTPTLSOptions struct {
	CAFile, CertFile, KeyFile, ServerName string
	InsecureSkipVerify                    bool
}

// HTTPOptions configures an HTTP-backed feed source. The source fetches the
// desired feed list from URL on Interval as a JSON object whose "feeds" key
// holds an array of feed specs.
type HTTPOptions struct {
	Name     string
	URL      string // required
	Timeout  time.Duration
	Headers  map[string]string
	Interval time.Duration
	TLS      *HTTPTLSOptions
}

// HTTP is a feed source backed by an HTTP endpoint. It composes Poll for the
// interval ticker and owns the HTTP client. It keeps the last ETag/Last-Modified
// and the last decoded list so a 304 returns the cached feeds without re-parsing.
type HTTP struct {
	url     string
	headers map[string]string
	client  *http.Client
	poll    *Poll

	mu           sync.Mutex
	etag         string
	lastModified string
	cached       []config.FeedConfig
}

// feedListResponse is the wire shape the http source expects. Feeds is a pointer
// so an absent "feeds" key (nil) is distinguishable from an empty array.
type feedListResponse struct {
	Feeds *[]FeedSpec `json:"feeds"`
}

// NewHTTP builds the HTTP client (timeout + optional TLS) and returns a polling
// source. It validates options but performs no network I/O at construction.
func NewHTTP(opts HTTPOptions) (*HTTP, error) {
	if strings.TrimSpace(opts.URL) == "" {
		return nil, fmt.Errorf("http feed source %q: url is required", opts.Name)
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	client := &http.Client{Timeout: timeout}
	if opts.TLS != nil {
		tc, err := buildHTTPSourceTLS(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("http feed source %q: %w", opts.Name, err)
		}
		// Clone DefaultTransport so we keep its connection-pool, keep-alive,
		// and timeout defaults while swapping in the custom TLS config.
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tc
		client.Transport = transport
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("component", "feedsource/http").
				Str("source", opts.Name).
				Msg("http feed source: TLS verification disabled (insecure_skip_verify=true)")
		}
	}
	h := &HTTP{url: opts.URL, headers: opts.Headers, client: client}
	h.poll = NewPoll(opts.Name, opts.Interval, h.fetch)
	return h, nil
}

func (h *HTTP) Name() string { return h.poll.Name() }

func (h *HTTP) Feeds(ctx context.Context) ([]config.FeedConfig, error) { return h.fetch(ctx) }

func (h *HTTP) Changes() <-chan struct{} { return h.poll.Changes() }

// Close stops the poll ticker.
func (h *HTTP) Close() error {
	h.poll.Close()
	return nil
}

func (h *HTTP) fetch(ctx context.Context) ([]config.FeedConfig, error) {
	h.mu.Lock()
	etag, lastMod, cached := h.etag, h.lastModified, h.cached
	h.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
	if err != nil {
		return nil, fmt.Errorf("http feed source %q: build request: %w", h.poll.Name(), err)
	}
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastMod != "" {
		req.Header.Set("If-Modified-Since", lastMod)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http feed source %q: get: %w", h.poll.Name(), err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotModified:
		// A 304 is only valid in response to a conditional request. If we sent
		// no validators (first fetch, or a server that never returns ETag/
		// Last-Modified), a 304 is a protocol violation and "cached" is nil —
		// surface it as an error rather than silently yielding an empty list.
		if etag == "" && lastMod == "" {
			return nil, fmt.Errorf("http feed source %q: got 304 without a prior conditional request", h.poll.Name())
		}
		return cached, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		if err != nil {
			return nil, fmt.Errorf("http feed source %q: read body: %w", h.poll.Name(), err)
		}
		if int64(len(body)) > maxBodyBytes {
			return nil, fmt.Errorf("http feed source %q: response body exceeds %d bytes", h.poll.Name(), maxBodyBytes)
		}
		var payload feedListResponse
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, fmt.Errorf("http feed source %q: decode: %w", h.poll.Name(), err)
		}
		if payload.Feeds == nil {
			log.Warn().
				Str("component", "feedsource/http").
				Str("source", h.poll.Name()).
				Str("url", h.url).
				Msg(`http feed source: response missing "feeds" key; keeping last-known-good`)
			return nil, fmt.Errorf("http feed source %q: response missing \"feeds\" key", h.poll.Name())
		}
		feeds, err := SpecsToConfigs(*payload.Feeds)
		if err != nil {
			return nil, fmt.Errorf("http feed source %q: %w", h.poll.Name(), err)
		}
		h.mu.Lock()
		h.etag = resp.Header.Get("ETag")
		h.lastModified = resp.Header.Get("Last-Modified")
		h.cached = feeds
		h.mu.Unlock()
		return feeds, nil
	default:
		return nil, fmt.Errorf("http feed source %q: unexpected status %d", h.poll.Name(), resp.StatusCode)
	}
}

// buildHTTPSourceTLS translates HTTPTLSOptions into a *tls.Config. Mirrors the
// postgres source's TLS builder; ServerName defaults to the request URL host
// (left empty here) unless overridden.
func buildHTTPSourceTLS(opts HTTPTLSOptions) (*tls.Config, error) {
	tc := &tls.Config{
		InsecureSkipVerify: opts.InsecureSkipVerify, //nolint:gosec // operator opt-in, logged at warn
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
