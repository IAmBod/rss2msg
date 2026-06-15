package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink/kafka/schema"
)

type Publisher struct {
	name    string
	client  *kgo.Client
	topic   string
	encoder schema.Encoder // nil ⇒ plain JSON
}

type Options struct {
	Name        string
	Brokers     []string
	Topic       string
	Acks        string // "all" (default) | "leader" | "none"
	Compression string // "none" | "snappy" | "lz4" | "zstd" | "gzip"

	// TLS, if non-nil, enables TLS to the brokers using the given options.
	// Kafka has no URL scheme to imply TLS, so this is the only switch.
	TLS *TLSOptions

	// Schema, if non-nil, enables Confluent Schema Registry encoding of the
	// record value. Nil keeps the plain-JSON value.
	Schema *schema.Options
}

// TLSOptions configures TLS to the Kafka brokers. Same shape as the other
// sinks so operators have a consistent surface. ServerName is left empty by
// default so franz-go applies per-broker SNI.
type TLSOptions struct {
	// CAFile is a PEM-encoded CA bundle to trust instead of the system roots.
	CAFile string
	// CertFile and KeyFile together enable client-certificate (mTLS) auth.
	// Either both or neither must be set.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / certificate verification hostname.
	// Empty leaves per-broker SNI to franz-go.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. For
	// local/test only — logged at warn.
	InsecureSkipVerify bool
}

func New(opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("kafka sink: name is required")
	}
	if len(opts.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sink %q: brokers required", opts.Name)
	}
	if opts.Topic == "" {
		return nil, fmt.Errorf("kafka sink %q: topic required", opts.Name)
	}
	kopts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.DefaultProduceTopic(opts.Topic),
	}
	switch opts.Acks {
	case "", "all":
		kopts = append(kopts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "leader":
		kopts = append(kopts, kgo.RequiredAcks(kgo.LeaderAck()))
	case "none":
		kopts = append(kopts, kgo.RequiredAcks(kgo.NoAck()))
	default:
		return nil, fmt.Errorf("kafka sink %q: unknown acks %q", opts.Name, opts.Acks)
	}
	if opts.Compression != "" {
		c, ok := compressionMap[opts.Compression]
		if !ok {
			return nil, fmt.Errorf("kafka sink %q: unknown compression %q", opts.Name, opts.Compression)
		}
		kopts = append(kopts, kgo.ProducerBatchCompression(c))
	}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("kafka sink %q: build TLS config: %w", opts.Name, err)
		}
		kopts = append(kopts, kgo.DialTLSConfig(tc))
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("sink", opts.Name).
				Str("sink_driver", "kafka").
				Msg("kafka sink: TLS verification disabled (insecure_skip_verify=true)")
		}
	}

	var enc schema.Encoder
	if opts.Schema != nil {
		var err error
		enc, err = schema.New(*opts.Schema)
		if err != nil {
			return nil, fmt.Errorf("kafka sink %q: schema: %w", opts.Name, err)
		}
	}

	client, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka sink %q: %w", opts.Name, err)
	}
	return &Publisher{name: opts.Name, client: client, topic: opts.Topic, encoder: enc}, nil
}

var compressionMap = map[string]kgo.CompressionCodec{
	"none":   kgo.NoCompression(),
	"snappy": kgo.SnappyCompression(),
	"lz4":    kgo.Lz4Compression(),
	"zstd":   kgo.ZstdCompression(),
	"gzip":   kgo.GzipCompression(),
}

// buildTLSConfig translates TLSOptions into a *tls.Config. ServerName is left
// empty unless overridden so franz-go applies per-broker SNI.
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

func (p *Publisher) Close() error { p.client.Close(); return nil }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	var (
		value []byte
		err   error
	)
	if p.encoder != nil {
		value, err = p.encoder.Encode(ctx, change)
	} else {
		value, err = json.Marshal(change)
	}
	if err != nil {
		return fmt.Errorf("kafka sink %q: encode: %w", p.name, err)
	}
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(change.ItemID),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "feed_url", Value: []byte(change.FeedURL)},
			{Key: "kind", Value: []byte(change.Kind)},
			{Key: "schema_version", Value: []byte(strconv.Itoa(change.SchemaVersion))},
		},
	}
	if change.DLQFromSink != "" {
		rec.Headers = append(rec.Headers,
			kgo.RecordHeader{Key: "dlq_from_sink", Value: []byte(change.DLQFromSink)},
			kgo.RecordHeader{Key: "dlq_error", Value: []byte(change.DLQError)},
			kgo.RecordHeader{Key: "dlq_attempts", Value: []byte(strconv.Itoa(change.DLQAttempts))},
		)
	}
	// Inject W3C trace context so downstream consumers can stitch the trace.
	// Requires a propagator to be installed (telemetry.Setup installs a
	// TraceContext+Baggage composite). When no span is active or no
	// propagator is registered, the carrier is simply empty and no
	// traceparent/tracestate headers are added.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(tp)})
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: "tracestate", Value: []byte(ts)})
	}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}
