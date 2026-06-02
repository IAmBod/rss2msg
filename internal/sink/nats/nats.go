// Package nats implements the sink.Publisher interface against a NATS server
// via nats.go. By default it does a core NATS publish to a subject and flushes
// to surface delivery errors; with JetStream=true it publishes through
// JetStream and waits for a server ack (the subject must already be bound to a
// stream — the sink never creates streams).
//
// One connection per Publisher. The nats.go client is safe for concurrent
// publishes, so no mutex is needed.
package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

// defaultPublishTimeout bounds a single publish (core flush or JetStream ack)
// when the caller's context carries no deadline.
const defaultPublishTimeout = 10 * time.Second

// Options configures a NATS Publisher.
type Options struct {
	Name    string // sink name (required)
	URL     string // one or more comma-separated NATS URLs (required)
	Subject string // subject to publish to (required)

	// Auth (optional). At most one of these groups may be set:
	//   - Token
	//   - Username + Password
	//   - CredsFile
	Token     string // token auth
	Username  string // user/password auth (both or neither)
	Password  string
	CredsFile string // path to a NATS user credentials file (JWT + NKey seed)

	// JetStream, if true, publishes through JetStream and waits for a server
	// ack. The Subject must already be bound to an existing stream; the sink
	// does not create or manage streams.
	JetStream bool

	// PublishTimeout bounds each publish when the caller's context has no
	// deadline. Defaults to 10s.
	PublishTimeout time.Duration

	// TLS, if non-nil, forces a TLS handshake (nats.Secure) using the given
	// options. NATS otherwise only upgrades to TLS when the server requires
	// it or the URL uses the tls:// scheme.
	TLS *TLSOptions
}

// TLSOptions configures TLS to the NATS server. Same shape as the other sinks
// so operators have a consistent surface. ServerName is left empty by default
// so nats.go derives it from the server URL.
type TLSOptions struct {
	// CAFile is a PEM-encoded CA bundle to trust instead of the system roots.
	CAFile string
	// CertFile and KeyFile together enable client-certificate (mTLS) auth.
	// Either both or neither must be set.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / certificate verification hostname.
	// Empty lets nats.go derive it from the server URL.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. For
	// local/test only — logged at warn.
	InsecureSkipVerify bool
}

// Publisher publishes Changes to a NATS subject.
type Publisher struct {
	name    string
	subject string
	nc      *natsgo.Conn
	js      jetstream.JetStream // non-nil only when JetStream mode is enabled
	timeout time.Duration
}

// New validates options, connects to NATS, and returns a ready Publisher.
func New(opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("nats sink: name is required")
	}
	if opts.URL == "" {
		return nil, fmt.Errorf("nats sink %q: url is required", opts.Name)
	}
	if opts.Subject == "" {
		return nil, fmt.Errorf("nats sink %q: subject is required", opts.Name)
	}

	// Auth: at most one group, and user/password must be set together.
	if (opts.Username != "") != (opts.Password != "") {
		return nil, fmt.Errorf("nats sink %q: username and password must both be set or both empty", opts.Name)
	}
	groups := 0
	if opts.Token != "" {
		groups++
	}
	if opts.Username != "" {
		groups++
	}
	if opts.CredsFile != "" {
		groups++
	}
	if groups > 1 {
		return nil, fmt.Errorf("nats sink %q: at most one of token, username/password, or creds_file may be set", opts.Name)
	}

	natsOpts := []natsgo.Option{natsgo.Name(opts.Name)}
	switch {
	case opts.Token != "":
		natsOpts = append(natsOpts, natsgo.Token(opts.Token))
	case opts.Username != "":
		natsOpts = append(natsOpts, natsgo.UserInfo(opts.Username, opts.Password))
	case opts.CredsFile != "":
		natsOpts = append(natsOpts, natsgo.UserCredentials(opts.CredsFile))
	}

	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("nats sink %q: build TLS config: %w", opts.Name, err)
		}
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("sink", opts.Name).
				Str("sink_driver", "nats").
				Msg("nats sink: TLS verification disabled (insecure_skip_verify=true)")
		}
		natsOpts = append(natsOpts, natsgo.Secure(tc))
	}

	nc, err := natsgo.Connect(opts.URL, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("nats sink %q: connect: %w", opts.Name, err)
	}

	timeout := opts.PublishTimeout
	if timeout <= 0 {
		timeout = defaultPublishTimeout
	}

	p := &Publisher{
		name:    opts.Name,
		subject: opts.Subject,
		nc:      nc,
		timeout: timeout,
	}

	if opts.JetStream {
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("nats sink %q: jetstream: %w", opts.Name, err)
		}
		p.js = js
	}

	return p, nil
}

// buildTLSConfig translates TLSOptions into a *tls.Config. ServerName is left
// empty unless overridden so nats.go derives it from the server URL.
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

// Close flushes any buffered core publishes and closes the connection.
func (p *Publisher) Close() error {
	if p.nc == nil {
		return nil
	}
	_ = p.nc.Flush()
	p.nc.Close()
	return nil
}

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	body, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("nats sink %q: marshal: %w", p.name, err)
	}

	msg := &natsgo.Msg{
		Subject: p.subject,
		Data:    body,
		Header: natsgo.Header{
			"feed_url":       []string{change.FeedURL},
			"kind":           []string{string(change.Kind)},
			"schema_version": []string{strconv.Itoa(change.SchemaVersion)},
		},
	}
	if change.DLQFromSink != "" {
		msg.Header.Set("dlq_from_sink", change.DLQFromSink)
		msg.Header.Set("dlq_error", change.DLQError)
		msg.Header.Set("dlq_attempts", strconv.Itoa(change.DLQAttempts))
	}

	// Inject W3C trace context so downstream consumers can stitch the trace.
	// Mirrors the kafka/rabbitmq/sqs/sns sinks.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		msg.Header.Set("traceparent", tp)
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		msg.Header.Set("tracestate", ts)
	}

	if p.js != nil {
		// JetStream: synchronous publish that waits for a server ack.
		pctx, cancel := p.publishContext(ctx)
		defer cancel()
		if _, err := p.js.PublishMsg(pctx, msg); err != nil {
			return fmt.Errorf("nats sink %q: jetstream publish: %w", p.name, err)
		}
		return nil
	}

	// Core NATS: publish is buffered, so flush to surface delivery errors and
	// give the publish meaningful (server-reached) semantics for the retry/DLQ
	// layer.
	if err := p.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("nats sink %q: publish: %w", p.name, err)
	}
	pctx, cancel := p.publishContext(ctx)
	defer cancel()
	if err := p.nc.FlushWithContext(pctx); err != nil {
		return fmt.Errorf("nats sink %q: flush: %w", p.name, err)
	}
	return nil
}

// publishContext returns a context bounded by the configured publish timeout
// unless the caller's context already carries an earlier deadline.
func (p *Publisher) publishContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, p.timeout)
}
