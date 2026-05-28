package sns

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

type Options struct {
	Name        string // required
	TopicARN    string // required
	Region      string // optional
	EndpointURL string // optional
}

type Publisher struct {
	name     string
	client   *sns.Client
	topicARN string
}

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("sns sink: name is required")
	}
	if opts.TopicARN == "" {
		return nil, fmt.Errorf("sns sink %q: topic_arn is required", opts.Name)
	}
	if strings.HasSuffix(opts.TopicARN, ".fifo") {
		return nil, fmt.Errorf("sns sink %q: FIFO topics are not supported in this version", opts.Name)
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if opts.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(opts.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("sns sink %q: load aws config: %w", opts.Name, err)
	}

	clientOpts := []func(*sns.Options){}
	if opts.EndpointURL != "" {
		endpoint := opts.EndpointURL
		clientOpts = append(clientOpts, func(o *sns.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	client := sns.NewFromConfig(awsCfg, clientOpts...)
	return &Publisher{name: opts.Name, client: client, topicARN: opts.TopicARN}, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error { return nil }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	body, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("sns sink %q: marshal: %w", p.name, err)
	}

	attrs := map[string]snstypes.MessageAttributeValue{
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

	_, err = p.client.Publish(ctx, &sns.PublishInput{
		TopicArn:          aws.String(p.topicARN),
		Message:           aws.String(string(body)),
		MessageAttributes: attrs,
	})
	if err != nil {
		return fmt.Errorf("sns sink %q: Publish: %w", p.name, err)
	}
	return nil
}

func strAttr(v string) snstypes.MessageAttributeValue {
	return snstypes.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(v),
	}
}
