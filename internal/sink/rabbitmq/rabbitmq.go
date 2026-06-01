// Package rabbitmq implements the sink.Publisher interface against a RabbitMQ
// broker via amqp091-go. One connection + one channel per Publisher; publishes
// are serialised with a mutex because AMQP channels are NOT safe for
// concurrent use.
package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

// Options configures a RabbitMQ Publisher.
type Options struct {
	Name string // sink name (required)
	URL  string // amqp:// or amqps:// URL (required)

	// Exchange is the destination exchange. Empty string means RabbitMQ's
	// default direct exchange (which routes by routing_key to a queue with
	// the same name).
	Exchange string

	// ExchangeType is the exchange kind to declare (when Declare=true).
	// One of: direct (default), topic, fanout, headers.
	ExchangeType string

	// RoutingKey is the static routing key used on every publish.
	RoutingKey string

	// Declare, if true, declares the exchange at startup. Use when the
	// operator is OK with the sink owning the topology; for shared brokers
	// where the exchange is pre-provisioned, leave false.
	Declare bool

	// Durable controls the durability of the declared exchange. Only
	// meaningful when Declare=true.
	Durable bool

	// Mandatory marks publishes as mandatory — the broker returns the
	// message if no queue is bound to receive it. Off by default; turning
	// it on without also handling returns (currently unhandled) effectively
	// drops unroutable messages with no warning. Set true only if you have
	// a guaranteed binding.
	Mandatory bool

	// TLS, if non-nil, dials the broker with TLS (amqp.DialTLS) using the
	// given options. The URL should use the amqps:// scheme.
	TLS *TLSOptions
}

// TLSOptions configures TLS to the RabbitMQ broker. Same shape as the other
// sinks so operators have a consistent surface. ServerName is left empty by
// default so amqp091-go fills it from the URI host.
type TLSOptions struct {
	// CAFile is a PEM-encoded CA bundle to trust instead of the system roots.
	CAFile string
	// CertFile and KeyFile together enable client-certificate (mTLS) auth.
	// Either both or neither must be set.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / certificate verification hostname.
	// Empty lets amqp091-go derive it from the URI host.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. For
	// local/test only — logged at warn.
	InsecureSkipVerify bool
}

var validExchangeTypes = map[string]struct{}{
	"direct":  {},
	"topic":   {},
	"fanout":  {},
	"headers": {},
}

type Publisher struct {
	name       string
	conn       *amqp.Connection
	ch         *amqp.Channel
	exchange   string
	routingKey string
	mandatory  bool

	mu sync.Mutex // serialises ch.PublishWithContext (channels are not thread-safe)
}

// New dials the broker, opens a channel, optionally declares the exchange,
// and returns a ready Publisher.
func New(opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("rabbitmq sink: name is required")
	}
	if opts.URL == "" {
		return nil, fmt.Errorf("rabbitmq sink %q: url is required", opts.Name)
	}
	exchangeType := opts.ExchangeType
	if exchangeType == "" {
		exchangeType = "direct"
	}
	if _, ok := validExchangeTypes[exchangeType]; !ok {
		return nil, fmt.Errorf("rabbitmq sink %q: unknown exchange_type %q", opts.Name, opts.ExchangeType)
	}
	if opts.Declare && opts.Exchange == "" {
		return nil, fmt.Errorf("rabbitmq sink %q: declare=true requires a non-empty exchange (the default exchange cannot be declared)", opts.Name)
	}

	var conn *amqp.Connection
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("rabbitmq sink %q: build TLS config: %w", opts.Name, err)
		}
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("sink", opts.Name).
				Str("sink_driver", "rabbitmq").
				Msg("rabbitmq sink: TLS verification disabled (insecure_skip_verify=true)")
		}
		conn, err = amqp.DialTLS(opts.URL, tc)
		if err != nil {
			return nil, fmt.Errorf("rabbitmq sink %q: dial: %w", opts.Name, err)
		}
	} else {
		var err error
		conn, err = amqp.Dial(opts.URL)
		if err != nil {
			return nil, fmt.Errorf("rabbitmq sink %q: dial: %w", opts.Name, err)
		}
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq sink %q: channel: %w", opts.Name, err)
	}

	if opts.Declare {
		if err := ch.ExchangeDeclare(opts.Exchange, exchangeType, opts.Durable, false, false, false, nil); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return nil, fmt.Errorf("rabbitmq sink %q: declare exchange %q: %w", opts.Name, opts.Exchange, err)
		}
	}

	return &Publisher{
		name:       opts.Name,
		conn:       conn,
		ch:         ch,
		exchange:   opts.Exchange,
		routingKey: opts.RoutingKey,
		mandatory:  opts.Mandatory,
	}, nil
}

// buildTLSConfig translates TLSOptions into a *tls.Config. ServerName is left
// empty unless overridden so amqp091-go derives it from the URI host.
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
	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	body, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("rabbitmq sink %q: marshal: %w", p.name, err)
	}

	headers := amqp.Table{
		"feed_url":       change.FeedURL,
		"kind":           string(change.Kind),
		"schema_version": int32(change.SchemaVersion),
	}
	if change.DLQFromSink != "" {
		headers["dlq_from_sink"] = change.DLQFromSink
		headers["dlq_error"] = change.DLQError
		headers["dlq_attempts"] = int32(change.DLQAttempts)
	}

	// Inject W3C trace context so downstream consumers can stitch the trace.
	// Mirrors the kafka/sqs/sns sinks.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		headers["traceparent"] = tp
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		headers["tracestate"] = ts
	}

	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    change.ItemID,
		Timestamp:    change.DetectedAt,
		Headers:      headers,
		Body:         body,
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ch.PublishWithContext(ctx, p.exchange, p.routingKey, p.mandatory, false, pub); err != nil {
		return fmt.Errorf("rabbitmq sink %q: publish: %w", p.name, err)
	}
	return nil
}
