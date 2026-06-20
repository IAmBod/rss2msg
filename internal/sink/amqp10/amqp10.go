// Package amqp10 implements the sink.Publisher interface against any AMQP 1.0
// broker (RabbitMQ 4.x, Azure Service Bus, ActiveMQ, Solace, ...) via
// Azure/go-amqp. One connection + one session + one sender per Publisher;
// sends are serialised with a mutex (sessions are not concurrent-safe) and
// block until the broker settles the message.
package amqp10

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"

	"github.com/Azure/go-amqp"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

// Options configures an AMQP 1.0 Publisher.
type Options struct {
	Name     string
	URL      string // amqp:// or amqps://
	Target   string // node/queue/topic address (required)
	Username string // optional; overrides URL userinfo
	Password string // optional; overrides URL userinfo
	TLS      *TLSOptions
}

// TLSOptions configures TLS to the AMQP 1.0 broker. Same shape as the other
// sinks so operators have a consistent surface.
type TLSOptions struct {
	// CAFile is a PEM-encoded CA bundle to trust instead of the system roots.
	CAFile string
	// CertFile and KeyFile together enable client-certificate (mTLS) auth.
	// Either both or neither must be set.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / certificate verification hostname.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. For
	// local/test only — logged at warn.
	InsecureSkipVerify bool
}

// Publisher sends model.Change values to an AMQP 1.0 broker.
type Publisher struct {
	name    string
	conn    *amqp.Conn
	session *amqp.Session
	sender  *amqp.Sender
	mu      sync.Mutex // sessions/senders are not concurrent-safe
}

// New dials the broker, opens a session and a sender, and returns a ready
// Publisher. The URL userinfo is used for SASL PLAIN auth unless explicit
// username/password fields are set.
func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("amqp10 sink: name is required")
	}
	if opts.URL == "" {
		return nil, fmt.Errorf("amqp10 sink %q: url is required", opts.Name)
	}
	if opts.Target == "" {
		return nil, fmt.Errorf("amqp10 sink %q: target is required", opts.Name)
	}

	user, pass, err := resolveAuth(opts.URL, opts.Username, opts.Password)
	if err != nil {
		return nil, fmt.Errorf("amqp10 sink %q: %w", opts.Name, err)
	}

	connOpts := &amqp.ConnOptions{}
	if user != "" {
		connOpts.SASLType = amqp.SASLTypePlain(user, pass)
	}
	if opts.TLS != nil {
		tc, terr := buildTLSConfig(*opts.TLS)
		if terr != nil {
			return nil, fmt.Errorf("amqp10 sink %q: build TLS config: %w", opts.Name, terr)
		}
		if opts.TLS.InsecureSkipVerify {
			log.Warn().Str("sink", opts.Name).Str("sink_driver", "amqp10").
				Msg("amqp10 sink: TLS verification disabled (insecure_skip_verify=true)")
		}
		connOpts.TLSConfig = tc
	}

	conn, err := amqp.Dial(ctx, opts.URL, connOpts)
	if err != nil {
		return nil, fmt.Errorf("amqp10 sink %q: dial: %w", opts.Name, err)
	}
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("amqp10 sink %q: session: %w", opts.Name, err)
	}
	sender, err := session.NewSender(ctx, opts.Target, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("amqp10 sink %q: sender to %q: %w", opts.Name, opts.Target, err)
	}

	return &Publisher{name: opts.Name, conn: conn, session: session, sender: sender}, nil
}

// resolveAuth returns the SASL username/password: explicit values win, else the
// URL's userinfo, else empty (anonymous).
func resolveAuth(rawURL, user, pass string) (string, string, error) {
	if user != "" {
		return user, pass, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("parse url: %w", err)
	}
	if u.User == nil {
		return "", "", nil
	}
	pw, _ := u.User.Password()
	return u.User.Username(), pw, nil
}

// buildMessage serialises a model.Change into an AMQP 1.0 message, setting
// application properties (metadata keys identical to kafka/sqs/amqp091 sinks)
// and injecting W3C trace context.
func buildMessage(ctx context.Context, change model.Change) *amqp.Message {
	body, _ := json.Marshal(change)
	ct := "application/json"

	props := map[string]any{
		"feed_url":       change.FeedURL,
		"kind":           string(change.Kind),
		"schema_version": int32(change.SchemaVersion),
	}
	if change.DLQFromSink != "" {
		props["dlq_from_sink"] = change.DLQFromSink
		props["dlq_error"] = change.DLQError
		props["dlq_attempts"] = int32(change.DLQAttempts)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		props["traceparent"] = tp
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		props["tracestate"] = ts
	}

	return &amqp.Message{
		Properties: &amqp.MessageProperties{
			MessageID:   change.ItemID,
			ContentType: &ct,
		},
		ApplicationProperties: props,
		Data:                  [][]byte{body},
	}
}

// buildTLSConfig translates TLSOptions into a *tls.Config.
func buildTLSConfig(opts TLSOptions) (*tls.Config, error) {
	tc := &tls.Config{InsecureSkipVerify: opts.InsecureSkipVerify} //nolint:gosec // opt-in, logged at warn
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

// Name returns the configured sink name.
func (p *Publisher) Name() string { return p.name }

// Publish serialises change to JSON and sends it to the broker, blocking until
// the broker settles the message (accept-confirmed).
func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	msg := buildMessage(ctx, change)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.sender.Send(ctx, msg, nil); err != nil {
		return fmt.Errorf("amqp10 sink %q: send: %w", p.name, err)
	}
	return nil
}

// Close detaches the sender and closes the connection.
func (p *Publisher) Close() error {
	ctx := context.Background()
	if p.sender != nil {
		_ = p.sender.Close(ctx)
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
