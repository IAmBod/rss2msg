// Package rabbitmqstream implements the sink.Publisher interface against a
// RabbitMQ Stream (native stream protocol, port 5552) via
// rabbitmq-stream-go-client. One Environment + one Producer per Publisher;
// Publish serialises one in-flight message and blocks on the broker
// confirmation so a returned nil means the message was confirmed.
package rabbitmqstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	streamamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

type Options struct {
	Name           string
	URIs           []string
	URL            string
	Stream         string
	Username       string
	Password       string
	Declare        bool
	MaxAge         time.Duration
	MaxLengthBytes int64
	TLS            *TLSOptions
}

type TLSOptions struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

type Publisher struct {
	name     string
	env      *stream.Environment
	producer *stream.Producer
	confirms <-chan []*stream.ConfirmationStatus

	mu sync.Mutex // one in-flight publish + confirmation at a time
}

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("rabbitmq_stream sink: name is required")
	}
	if opts.Stream == "" {
		return nil, fmt.Errorf("rabbitmq_stream sink %q: stream is required", opts.Name)
	}
	if len(opts.URIs) == 0 && opts.URL == "" {
		return nil, fmt.Errorf("rabbitmq_stream sink %q: uris or url is required", opts.Name)
	}

	envOpts, err := buildEnvOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq_stream sink %q: %w", opts.Name, err)
	}
	env, err := stream.NewEnvironment(envOpts)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq_stream sink %q: environment: %w", opts.Name, err)
	}

	if opts.Declare {
		so := stream.NewStreamOptions()
		if opts.MaxAge > 0 {
			so = so.SetMaxAge(opts.MaxAge)
		}
		if opts.MaxLengthBytes > 0 {
			so = so.SetMaxLengthBytes(stream.ByteCapacity{}.B(opts.MaxLengthBytes))
		}
		if derr := env.DeclareStream(opts.Stream, so); derr != nil && !errors.Is(derr, stream.StreamAlreadyExists) {
			_ = env.Close()
			return nil, fmt.Errorf("rabbitmq_stream sink %q: declare stream %q: %w", opts.Name, opts.Stream, derr)
		}
	}

	producer, err := env.NewProducer(opts.Stream, stream.NewProducerOptions())
	if err != nil {
		_ = env.Close()
		return nil, fmt.Errorf("rabbitmq_stream sink %q: producer: %w", opts.Name, err)
	}

	return &Publisher{
		name:     opts.Name,
		env:      env,
		producer: producer,
		confirms: producer.NotifyPublishConfirmation(),
	}, nil
}

func buildEnvOptions(opts Options) (*stream.EnvironmentOptions, error) {
	eo := stream.NewEnvironmentOptions()
	if len(opts.URIs) > 0 {
		eo = eo.SetUris(opts.URIs)
	} else {
		eo = eo.SetUri(opts.URL)
	}
	if opts.Username != "" {
		eo = eo.SetUser(opts.Username)
	}
	if opts.Password != "" {
		eo = eo.SetPassword(opts.Password)
	}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("build TLS config: %w", err)
		}
		if opts.TLS.InsecureSkipVerify {
			log.Warn().Str("sink", opts.Name).Str("sink_driver", "rabbitmq_stream").
				Msg("rabbitmq_stream sink: TLS verification disabled (insecure_skip_verify=true)")
		}
		eo = eo.IsTLS(true).SetTLSConfig(tc)
	}
	return eo, nil
}

func buildMessage(ctx context.Context, change model.Change) message.StreamMessage {
	body, _ := json.Marshal(change)
	msg := streamamqp.NewMessage(body)

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
	msg.ApplicationProperties = props
	msg.Properties = &streamamqp.MessageProperties{
		MessageID:   change.ItemID,
		ContentType: "application/json",
	}
	return msg
}

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

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	msg := buildMessage(ctx, change)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.producer.Send(msg); err != nil {
		return fmt.Errorf("rabbitmq_stream sink %q: send: %w", p.name, err)
	}
	// One in-flight message at a time (mutex-guarded): the next confirmation
	// batch is ours.
	select {
	case batch := <-p.confirms:
		for _, st := range batch {
			if !st.IsConfirmed() {
				return fmt.Errorf("rabbitmq_stream sink %q: publish not confirmed: %w", p.name, st.GetError())
			}
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rabbitmq_stream sink %q: confirmation wait: %w", p.name, ctx.Err())
	}
}

func (p *Publisher) Close() error {
	if p.producer != nil {
		_ = p.producer.Close()
	}
	if p.env != nil {
		return p.env.Close()
	}
	return nil
}
