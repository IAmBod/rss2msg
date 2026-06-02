// Package dapr implements a sink that publishes changes to a Dapr pub/sub
// component via the local Dapr sidecar. A single sink transparently targets any
// broker Dapr supports (Kafka, RabbitMQ, Redis Streams, NATS, MQTT, GCP Pub/Sub,
// Azure Service Bus, AWS SNS/SQS, and more) — the broker is chosen by Dapr
// component YAML, not by rss2msg.
package dapr

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	daprclient "github.com/dapr/go-sdk/client"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

const defaultContentType = "application/json"

type Options struct {
	Name string // required: sink name

	// Address is the Dapr sidecar gRPC endpoint (host:port). Empty uses the
	// SDK default (DAPR_GRPC_ENDPOINT / DAPR_GRPC_PORT env, else localhost:50001).
	Address string

	PubsubName string // required: Dapr pub/sub component name
	Topic      string // required: topic to publish to

	// ContentType is the published payload's content type. Empty defaults to
	// application/json (the format rss2msg serializes changes in).
	ContentType string

	// Metadata is static per-publish metadata merged into every message
	// (e.g. a broker partition key). Reserved routing keys (feed_url, kind,
	// schema_version, traceparent, tracestate, dlq_*) take precedence.
	Metadata map[string]string
}

// publishFn is the seam over the Dapr client's PublishEvent so the sink can be
// unit-tested without a sidecar.
type publishFn func(ctx context.Context, pubsubName, topic string, data []byte, contentType string, metadata map[string]string) error

type Publisher struct {
	name        string
	pubsubName  string
	topic       string
	contentType string
	meta        map[string]string

	publish publishFn
	closeFn func() error
}

// newPublisher validates options and builds a Publisher without a backing
// client. New (and the white-box tests) attach the publish/close seams.
func newPublisher(opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("dapr_pubsub sink: name is required")
	}
	if opts.PubsubName == "" {
		return nil, fmt.Errorf("dapr_pubsub sink %q: pubsub_name is required", opts.Name)
	}
	if opts.Topic == "" {
		return nil, fmt.Errorf("dapr_pubsub sink %q: topic is required", opts.Name)
	}
	ct := opts.ContentType
	if ct == "" {
		ct = defaultContentType
	}
	return &Publisher{
		name:        opts.Name,
		pubsubName:  opts.PubsubName,
		topic:       opts.Topic,
		contentType: ct,
		meta:        opts.Metadata,
	}, nil
}

func New(_ context.Context, opts Options) (*Publisher, error) {
	p, err := newPublisher(opts)
	if err != nil {
		return nil, err
	}

	var c daprclient.Client
	if opts.Address != "" {
		c, err = daprclient.NewClientWithAddress(opts.Address)
	} else {
		c, err = daprclient.NewClient()
	}
	if err != nil {
		return nil, fmt.Errorf("dapr_pubsub sink %q: new client: %w", opts.Name, err)
	}

	p.publish = func(ctx context.Context, pubsubName, topic string, data []byte, contentType string, metadata map[string]string) error {
		pubOpts := []daprclient.PublishEventOption{daprclient.PublishEventWithContentType(contentType)}
		if len(metadata) > 0 {
			pubOpts = append(pubOpts, daprclient.PublishEventWithMetadata(metadata))
		}
		return c.PublishEvent(ctx, pubsubName, topic, data, pubOpts...)
	}
	p.closeFn = func() error { c.Close(); return nil }

	return p, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error {
	if p.closeFn == nil {
		return nil
	}
	return p.closeFn()
}

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	body, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("dapr_pubsub sink %q: marshal: %w", p.name, err)
	}

	// Start from any static metadata; reserved routing/trace/DLQ keys overlay
	// and win so they are always authoritative.
	meta := make(map[string]string, len(p.meta)+6)
	for k, v := range p.meta {
		meta[k] = v
	}
	meta["feed_url"] = change.FeedURL
	meta["kind"] = string(change.Kind)
	meta["schema_version"] = strconv.Itoa(change.SchemaVersion)

	// Inject W3C trace context so downstream consumers can stitch the trace.
	// With no active span or registered propagator the carrier is empty.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		meta["traceparent"] = tp
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		meta["tracestate"] = ts
	}

	if change.DLQFromSink != "" {
		meta["dlq_from_sink"] = change.DLQFromSink
		meta["dlq_error"] = change.DLQError
		meta["dlq_attempts"] = strconv.Itoa(change.DLQAttempts)
	}

	if err := p.publish(ctx, p.pubsubName, p.topic, body, p.contentType, meta); err != nil {
		return fmt.Errorf("dapr_pubsub sink %q: publish: %w", p.name, err)
	}
	return nil
}
