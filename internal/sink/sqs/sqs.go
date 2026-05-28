package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

type Options struct {
	Name        string // required, sink name
	QueueURL    string // required
	Region      string // optional; SDK default chain when empty
	EndpointURL string // optional; LocalStack/testing
}

type Publisher struct {
	name     string
	client   *sqs.Client
	queueURL string
}

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("sqs sink: name is required")
	}
	if opts.QueueURL == "" {
		return nil, fmt.Errorf("sqs sink %q: queue_url is required", opts.Name)
	}
	if strings.HasSuffix(opts.QueueURL, ".fifo") {
		return nil, fmt.Errorf("sqs sink %q: FIFO queues are not supported in this version", opts.Name)
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if opts.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(opts.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("sqs sink %q: load aws config: %w", opts.Name, err)
	}

	clientOpts := []func(*sqs.Options){}
	if opts.EndpointURL != "" {
		endpoint := opts.EndpointURL
		clientOpts = append(clientOpts, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	client := sqs.NewFromConfig(awsCfg, clientOpts...)
	return &Publisher{name: opts.Name, client: client, queueURL: opts.QueueURL}, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error { return nil }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	body, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("sqs sink %q: marshal: %w", p.name, err)
	}

	attrs := map[string]sqstypes.MessageAttributeValue{
		"feed_url":       strAttr(change.FeedURL),
		"kind":           strAttr(string(change.Kind)),
		"schema_version": strAttr(strconv.Itoa(change.SchemaVersion)),
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		attrs["traceparent"] = strAttr(tp)
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		attrs["tracestate"] = strAttr(ts)
	}

	if change.DLQFromSink != "" {
		attrs["dlq_from_sink"] = strAttr(change.DLQFromSink)
		attrs["dlq_error"] = strAttr(change.DLQError)
		attrs["dlq_attempts"] = strAttr(strconv.Itoa(change.DLQAttempts))
	}

	_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:          aws.String(p.queueURL),
		MessageBody:       aws.String(string(body)),
		MessageAttributes: attrs,
	})
	if err != nil {
		return fmt.Errorf("sqs sink %q: SendMessage: %w", p.name, err)
	}
	return nil
}

func strAttr(v string) sqstypes.MessageAttributeValue {
	return sqstypes.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(v),
	}
}
