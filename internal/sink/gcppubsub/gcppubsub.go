package gcppubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	pubsub "cloud.google.com/go/pubsub/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/iambod/rss2msg/internal/model"
)

type Options struct {
	Name      string // required, sink name
	ProjectID string // required, GCP project that owns the topic
	TopicID   string // required, topic short name (must already exist)
	Endpoint  string // optional; Pub/Sub emulator host (connects insecure, no auth)

	// OrderingKey enables ordered delivery and selects how the per-message
	// ordering key is derived. Allowed values:
	//   "feed_url" — one key per feed: in-order per feed, parallel across feeds.
	//   "item_id"  — one key per item: maximum parallelism.
	//   "sink"     — single key across the sink: strict global ordering.
	// Empty disables ordering (the default).
	OrderingKey string
}

var validOrderingKeys = map[string]struct{}{
	"feed_url": {},
	"item_id":  {},
	"sink":     {},
}

type Publisher struct {
	name        string
	client      *pubsub.Client
	publisher   *pubsub.Publisher
	orderingKey string // "" when ordering is disabled
}

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("gcp_pubsub sink: name is required")
	}
	if opts.ProjectID == "" {
		return nil, fmt.Errorf("gcp_pubsub sink %q: project_id is required", opts.Name)
	}
	if opts.TopicID == "" {
		return nil, fmt.Errorf("gcp_pubsub sink %q: topic_id is required", opts.Name)
	}
	if opts.OrderingKey != "" {
		if _, ok := validOrderingKeys[opts.OrderingKey]; !ok {
			return nil, fmt.Errorf("gcp_pubsub sink %q: unknown ordering_key %q (want one of feed_url, item_id, sink)", opts.Name, opts.OrderingKey)
		}
	}

	var clientOpts []option.ClientOption
	if opts.Endpoint != "" {
		clientOpts = append(clientOpts,
			option.WithEndpoint(opts.Endpoint),
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
	}

	client, err := pubsub.NewClient(ctx, opts.ProjectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("gcp_pubsub sink %q: new client: %w", opts.Name, err)
	}

	publisher := client.Publisher(opts.TopicID)
	if opts.OrderingKey != "" {
		publisher.EnableMessageOrdering = true
	}

	return &Publisher{
		name:        opts.Name,
		client:      client,
		publisher:   publisher,
		orderingKey: opts.OrderingKey,
	}, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error {
	p.publisher.Stop()
	return p.client.Close()
}

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	body, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("gcp_pubsub sink %q: marshal: %w", p.name, err)
	}

	attrs := map[string]string{
		"feed_url":       change.FeedURL,
		"kind":           string(change.Kind),
		"schema_version": strconv.Itoa(change.SchemaVersion),
	}

	// Inject W3C trace context so downstream consumers can stitch the trace.
	// When no span is active or no propagator is registered, the carrier is
	// empty and no traceparent/tracestate attributes are added.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		attrs["traceparent"] = tp
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		attrs["tracestate"] = ts
	}

	if change.DLQFromSink != "" {
		attrs["dlq_from_sink"] = change.DLQFromSink
		attrs["dlq_error"] = change.DLQError
		attrs["dlq_attempts"] = strconv.Itoa(change.DLQAttempts)
	}

	msg := &pubsub.Message{Data: body, Attributes: attrs}
	key := p.orderingKeyFor(change)
	if key != "" {
		msg.OrderingKey = key
	}

	// Publish then block on the result so success/failure feeds the retry +
	// dead-letter machinery synchronously.
	if _, err := p.publisher.Publish(ctx, msg).Get(ctx); err != nil {
		// When ordering is enabled, Pub/Sub pauses the ordering key after a
		// failure; resume it so the retry wrapper's next attempt (and future
		// messages for this key) are not blocked by ErrPublishingPaused.
		if key != "" {
			p.publisher.ResumePublish(key)
		}
		return fmt.Errorf("gcp_pubsub sink %q: publish: %w", p.name, err)
	}
	return nil
}

// orderingKeyFor derives the per-message ordering key from the configured
// strategy. Returns "" when ordering is disabled.
func (p *Publisher) orderingKeyFor(change model.Change) string {
	switch p.orderingKey {
	case "feed_url":
		return change.FeedURL
	case "item_id":
		return change.ItemID
	case "sink":
		return p.name
	default: // "" — ordering disabled
		return ""
	}
}
