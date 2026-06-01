// Package azureservicebus implements the sink.Publisher interface against an
// Azure Service Bus queue or topic via the official azservicebus SDK. One
// *Client and one *Sender are created per Publisher; the SDK Sender is safe
// for concurrent use, so no mutex is needed here.
package azureservicebus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

// Options configures an Azure Service Bus Publisher. Exactly one auth field
// (ConnectionString or Namespace) and exactly one entity field (Queue or
// Topic) must be set.
type Options struct {
	Name string // sink name (required)

	// ConnectionString authenticates with a Service Bus SAS connection
	// string. Mutually exclusive with Namespace.
	ConnectionString string

	// Namespace is the fully-qualified namespace (e.g.
	// "my-bus.servicebus.windows.net"); when set, the sink authenticates with
	// DefaultAzureCredential (env / workload identity / managed identity).
	// Mutually exclusive with ConnectionString.
	Namespace string

	// Queue is the destination queue. Mutually exclusive with Topic.
	Queue string

	// Topic is the destination topic. Mutually exclusive with Queue.
	Topic string
}

type Publisher struct {
	name   string
	client *azservicebus.Client
	sender *azservicebus.Sender
}

// New validates the options, builds a Service Bus client (SAS or Azure AD),
// opens a sender for the configured entity, and returns a ready Publisher.
func New(opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("azureservicebus sink: name is required")
	}

	switch {
	case opts.ConnectionString == "" && opts.Namespace == "":
		return nil, fmt.Errorf("azureservicebus sink %q: one of connection_string or namespace is required", opts.Name)
	case opts.ConnectionString != "" && opts.Namespace != "":
		return nil, fmt.Errorf("azureservicebus sink %q: connection_string and namespace are mutually exclusive", opts.Name)
	}

	entity := opts.Queue
	switch {
	case opts.Queue == "" && opts.Topic == "":
		return nil, fmt.Errorf("azureservicebus sink %q: one of queue or topic is required", opts.Name)
	case opts.Queue != "" && opts.Topic != "":
		return nil, fmt.Errorf("azureservicebus sink %q: queue and topic are mutually exclusive", opts.Name)
	case opts.Topic != "":
		entity = opts.Topic
	}

	var (
		client *azservicebus.Client
		err    error
	)
	if opts.ConnectionString != "" {
		client, err = azservicebus.NewClientFromConnectionString(opts.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("azureservicebus sink %q: client from connection string: %w", opts.Name, err)
		}
	} else {
		cred, cerr := azidentity.NewDefaultAzureCredential(nil)
		if cerr != nil {
			return nil, fmt.Errorf("azureservicebus sink %q: default azure credential: %w", opts.Name, cerr)
		}
		client, err = azservicebus.NewClient(opts.Namespace, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("azureservicebus sink %q: client for namespace %q: %w", opts.Name, opts.Namespace, err)
		}
	}

	sender, err := client.NewSender(entity, nil)
	if err != nil {
		_ = client.Close(context.Background())
		return nil, fmt.Errorf("azureservicebus sink %q: sender for %q: %w", opts.Name, entity, err)
	}

	return &Publisher{name: opts.Name, client: client, sender: sender}, nil
}

func (p *Publisher) Name() string { return p.name }

// Close closes the sender and the underlying client. The Publisher interface's
// Close takes no context, so a background context is used.
func (p *Publisher) Close() error {
	ctx := context.Background()
	if p.sender != nil {
		_ = p.sender.Close(ctx)
	}
	if p.client != nil {
		return p.client.Close(ctx)
	}
	return nil
}

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	msg, err := buildMessage(ctx, change)
	if err != nil {
		return fmt.Errorf("azureservicebus sink %q: %w", p.name, err)
	}
	if err := p.sender.SendMessage(ctx, msg, nil); err != nil {
		return fmt.Errorf("azureservicebus sink %q: send: %w", p.name, err)
	}
	return nil
}

// buildMessage renders a Change into a Service Bus message. It is a pure
// function (no network) so the wire layout can be unit-tested. Layout mirrors
// the rabbitmq / sqs / sns sinks: JSON body, application/json content type,
// MessageID = ItemID, and metadata + trace context in ApplicationProperties.
func buildMessage(ctx context.Context, change model.Change) (*azservicebus.Message, error) {
	body, err := json.Marshal(change)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	props := map[string]any{
		"feed_url":       change.FeedURL,
		"kind":           string(change.Kind),
		"schema_version": change.SchemaVersion,
	}
	if change.DLQFromSink != "" {
		props["dlq_from_sink"] = change.DLQFromSink
		props["dlq_error"] = change.DLQError
		props["dlq_attempts"] = change.DLQAttempts
	}

	// Inject W3C trace context so downstream consumers can stitch the trace.
	// Mirrors the kafka / rabbitmq / sqs / sns sinks.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		props["traceparent"] = tp
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		props["tracestate"] = ts
	}

	msg := &azservicebus.Message{
		Body:                  body,
		ContentType:           ptr("application/json"),
		ApplicationProperties: props,
	}
	if change.ItemID != "" {
		msg.MessageID = ptr(change.ItemID)
	}
	return msg, nil
}

func ptr[T any](v T) *T { return &v }
