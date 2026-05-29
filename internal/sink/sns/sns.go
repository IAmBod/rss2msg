package sns

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	TopicARN    string // required; ".fifo" suffix selects FIFO mode
	Region      string // optional
	EndpointURL string // optional

	// MessageGroup controls FIFO MessageGroupId derivation. Allowed values:
	//   "feed_url" (default for FIFO) — one group per feed: in-order per
	//                                    feed, parallel across feeds.
	//   "item_id"                     — one group per item: maximum
	//                                    parallelism; only useful when the
	//                                    consumer doesn't need cross-item
	//                                    ordering.
	//   "sink"                        — single group across the sink:
	//                                    strict global ordering, no
	//                                    parallelism.
	// Only valid when TopicARN is FIFO; rejected at New() otherwise.
	MessageGroup string
}

var validMessageGroups = map[string]struct{}{
	"feed_url": {},
	"item_id":  {},
	"sink":     {},
}

type Publisher struct {
	name         string
	client       *sns.Client
	topicARN     string
	fifo         bool
	messageGroup string // only meaningful when fifo
}

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("sns sink: name is required")
	}
	if opts.TopicARN == "" {
		return nil, fmt.Errorf("sns sink %q: topic_arn is required", opts.Name)
	}
	fifo := strings.HasSuffix(opts.TopicARN, ".fifo")
	messageGroup := opts.MessageGroup
	if messageGroup != "" {
		if _, ok := validMessageGroups[messageGroup]; !ok {
			return nil, fmt.Errorf("sns sink %q: unknown message_group %q (want one of feed_url, item_id, sink)", opts.Name, messageGroup)
		}
		if !fifo {
			return nil, fmt.Errorf("sns sink %q: message_group is only valid for FIFO topics (topic_arn must end with .fifo)", opts.Name)
		}
	}
	if fifo && messageGroup == "" {
		messageGroup = "feed_url"
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
	return &Publisher{
		name:         opts.Name,
		client:       client,
		topicARN:     opts.TopicARN,
		fifo:         fifo,
		messageGroup: messageGroup,
	}, nil
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

	input := &sns.PublishInput{
		TopicArn:          aws.String(p.topicARN),
		Message:           aws.String(string(body)),
		MessageAttributes: attrs,
	}
	if p.fifo {
		input.MessageGroupId = aws.String(p.fifoGroupID(change))
		input.MessageDeduplicationId = aws.String(fifoDedupID(change))
	}
	if _, err := p.client.Publish(ctx, input); err != nil {
		return fmt.Errorf("sns sink %q: Publish: %w", p.name, err)
	}
	return nil
}

func (p *Publisher) fifoGroupID(change model.Change) string {
	switch p.messageGroup {
	case "item_id":
		return change.ItemID
	case "sink":
		return p.name
	default: // "feed_url"
		return change.FeedURL
	}
}

// fifoDedupID returns a sha256-hex over (feed_url, item_id, content_hash).
// Same input → same dedup id, so re-publishes of an unchanged Change
// within SNS's 5-minute dedup window are coalesced. content_hash changes
// when the item is updated, so updates produce a fresh dedup id.
func fifoDedupID(change model.Change) string {
	h := sha256.New()
	_, _ = h.Write([]byte(change.FeedURL))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(change.ItemID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(change.ContentHash))
	return hex.EncodeToString(h.Sum(nil))
}

func strAttr(v string) snstypes.MessageAttributeValue {
	return snstypes.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(v),
	}
}
