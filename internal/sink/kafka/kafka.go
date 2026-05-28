package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iambod/rss2msg/internal/model"
)

type Publisher struct {
	name   string
	client *kgo.Client
	topic  string
}

type Options struct {
	Name        string
	Brokers     []string
	Topic       string
	Acks        string // "all" (default) | "leader" | "none"
	Compression string // "none" | "snappy" | "lz4" | "zstd" | "gzip"
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

	client, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka sink %q: %w", opts.Name, err)
	}
	return &Publisher{name: opts.Name, client: client, topic: opts.Topic}, nil
}

var compressionMap = map[string]kgo.CompressionCodec{
	"none":   kgo.NoCompression(),
	"snappy": kgo.SnappyCompression(),
	"lz4":    kgo.Lz4Compression(),
	"zstd":   kgo.ZstdCompression(),
	"gzip":   kgo.GzipCompression(),
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error { p.client.Close(); return nil }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	value, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("kafka sink %q: marshal: %w", p.name, err)
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
	return p.client.ProduceSync(ctx, rec).FirstErr()
}
