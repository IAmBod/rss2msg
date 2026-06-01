// Package http implements a sink.Publisher that POSTs (or PUTs) each Change
// as a JSON request body to a configured URL. Useful for webhook
// integrations (Slack incoming-webhook, custom receivers, etc.).
package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

// Options configures an HTTP webhook Publisher.
type Options struct {
	Name string // sink name (required)
	URL  string // target URL (required); must be http:// or https://

	// Method is the HTTP method to use. "POST" (default) or "PUT".
	Method string

	// Headers are static request headers applied to every request. Useful
	// for Authorization, User-Agent, custom routing keys, etc. Per-record
	// canonical headers (Content-Type, X-Feed-Url, X-Item-Id, X-Kind,
	// X-Schema-Version, X-Dlq-*) are set AFTER static headers and cannot
	// be overridden.
	Headers map[string]string

	// Timeout is the per-request timeout. 0 -> 30s.
	Timeout time.Duration

	// SuccessCodes are the HTTP status codes that count as a successful
	// publish. Empty -> {200, 201, 202, 204}.
	SuccessCodes []int

	// TLS, if non-nil, applies custom TLS (CA / client cert / verification
	// options) to the client transport. Only meaningful for https:// URLs.
	TLS *TLSOptions
}

// TLSOptions configures custom TLS for the webhook client. Same shape as the
// other sinks so operators have a consistent surface. ServerName is left empty
// by default so net/http derives it from the request URL.
type TLSOptions struct {
	// CAFile is a PEM-encoded CA bundle to trust instead of the system roots.
	CAFile string
	// CertFile and KeyFile together enable client-certificate (mTLS) auth.
	// Either both or neither must be set.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / certificate verification hostname.
	// Empty lets net/http derive it from the request URL host.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. For
	// local/test only — logged at warn.
	InsecureSkipVerify bool
}

type Publisher struct {
	name         string
	url          string
	method       string
	headers      map[string]string
	successCodes map[int]struct{}
	client       *http.Client
}

func New(opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("http sink: name is required")
	}
	if opts.URL == "" {
		return nil, fmt.Errorf("http sink %q: url is required", opts.Name)
	}
	u, err := url.Parse(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("http sink %q: parse url: %w", opts.Name, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("http sink %q: url scheme must be http or https, got %q", opts.Name, u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("http sink %q: url has no host", opts.Name)
	}

	method := opts.Method
	if method == "" {
		method = http.MethodPost
	}
	if method != http.MethodPost && method != http.MethodPut {
		return nil, fmt.Errorf("http sink %q: method must be POST or PUT, got %q", opts.Name, method)
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	codes := make(map[int]struct{})
	if len(opts.SuccessCodes) == 0 {
		for _, c := range []int{200, 201, 202, 204} {
			codes[c] = struct{}{}
		}
	} else {
		for _, c := range opts.SuccessCodes {
			if c < 100 || c > 599 {
				return nil, fmt.Errorf("http sink %q: success_code %d is out of range 100-599", opts.Name, c)
			}
			codes[c] = struct{}{}
		}
	}

	client := &http.Client{Timeout: timeout}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("http sink %q: build TLS config: %w", opts.Name, err)
		}
		client.Transport = &http.Transport{TLSClientConfig: tc}
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("sink", opts.Name).
				Str("sink_driver", "http").
				Msg("http sink: TLS verification disabled (insecure_skip_verify=true)")
		}
	}

	return &Publisher{
		name:         opts.Name,
		url:          opts.URL,
		method:       method,
		headers:      opts.Headers,
		successCodes: codes,
		client:       client,
	}, nil
}

// buildTLSConfig translates TLSOptions into a *tls.Config. ServerName is left
// empty unless overridden so net/http derives it from the request URL host.
func buildTLSConfig(opts TLSOptions) (*tls.Config, error) {
	tc := &tls.Config{
		InsecureSkipVerify: opts.InsecureSkipVerify, //nolint:gosec // opt-in, logged at warn
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

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error {
	p.client.CloseIdleConnections()
	return nil
}

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	body, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("http sink %q: marshal: %w", p.name, err)
	}

	req, err := http.NewRequestWithContext(ctx, p.method, p.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("http sink %q: build request: %w", p.name, err)
	}

	// Static headers first so the canonical per-record headers below can
	// override them — operators can supply Authorization etc., but cannot
	// clobber the metadata fields that describe what's actually in the body.
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Schema-Version", strconv.Itoa(change.SchemaVersion))
	req.Header.Set("X-Feed-Url", change.FeedURL)
	req.Header.Set("X-Item-Id", change.ItemID)
	req.Header.Set("X-Kind", string(change.Kind))
	if change.DLQFromSink != "" {
		req.Header.Set("X-Dlq-From-Sink", change.DLQFromSink)
		req.Header.Set("X-Dlq-Error", change.DLQError)
		req.Header.Set("X-Dlq-Attempts", strconv.Itoa(change.DLQAttempts))
	}
	// W3C trace context — propagator injects traceparent / tracestate into
	// the request headers when a span is active. No-op when no propagator
	// is registered.
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("http sink %q: request: %w", p.name, err)
	}
	defer resp.Body.Close()
	// Drain the body so the underlying connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	if _, ok := p.successCodes[resp.StatusCode]; !ok {
		return fmt.Errorf("http sink %q: unexpected status %d %s", p.name, resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	return nil
}
