//go:build integration

package sqs_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink/sqs"
	"github.com/iambod/rss2msg/test/awslocal"
)

const region = "us-east-1"

func setup(t *testing.T) (endpoint, queueURL string) {
	t.Helper()
	ctx := context.Background()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ls := awslocal.Run(ctx, t)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	client := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(ls.Endpoint)
	})
	q, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String("rss2msg-test")})
	if err != nil {
		t.Fatal(err)
	}
	return ls.Endpoint, aws.ToString(q.QueueUrl)
}

func receiveOne(t *testing.T, endpoint, queueURL string) map[string]string {
	t.Helper()
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	client := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		out, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:              aws.String(queueURL),
			MaxNumberOfMessages:   1,
			WaitTimeSeconds:       2,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Messages) == 0 {
			continue
		}
		msg := out.Messages[0]
		result := map[string]string{"body": aws.ToString(msg.Body)}
		for k, v := range msg.MessageAttributes {
			result[k] = aws.ToString(v.StringValue)
		}
		return result
	}
	t.Fatal("no message received within 10s")
	return nil
}

func TestSQSPublishRoundTripsEnvelopeAndAttributes(t *testing.T) {
	endpoint, queueURL := setup(t)
	pub, err := sqs.New(context.Background(), sqs.Options{
		Name: "test", QueueURL: queueURL, Region: region, EndpointURL: endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	when := time.Now().UTC().Truncate(time.Millisecond)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "f1", ItemID: "i1",
		Kind: model.ChangeNew, Title: "hi", ContentHash: "h", DetectedAt: when,
	}
	if err := pub.Publish(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	got := receiveOne(t, endpoint, queueURL)
	if got["feed_url"] != "f1" || got["kind"] != "new" || got["schema_version"] != "1" {
		t.Fatalf("missing/incorrect base attributes: %+v", got)
	}
	var round model.Change
	if err := json.Unmarshal([]byte(got["body"]), &round); err != nil {
		t.Fatal(err)
	}
	if round.Title != "hi" {
		t.Fatalf("body title=%q", round.Title)
	}
}

func TestSQSPublishIncludesTraceparentWhenSpanActive(t *testing.T) {
	endpoint, queueURL := setup(t)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	pub, err := sqs.New(context.Background(), sqs.Options{
		Name: "test", QueueURL: queueURL, Region: region, EndpointURL: endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	ctx, span := otel.Tracer("test").Start(context.Background(), "publish.test")
	defer span.End()
	c := model.Change{SchemaVersion: 1, FeedURL: "f", ItemID: "i", Kind: model.ChangeNew, ContentHash: "h", DetectedAt: time.Now().UTC()}
	if err := pub.Publish(ctx, c); err != nil {
		t.Fatal(err)
	}
	got := receiveOne(t, endpoint, queueURL)
	if got["traceparent"] == "" {
		t.Fatalf("expected traceparent in attributes, got %+v", got)
	}
}

func TestSQSPublishIncludesDLQAttributesWhenDecorated(t *testing.T) {
	endpoint, queueURL := setup(t)
	pub, err := sqs.New(context.Background(), sqs.Options{
		Name: "test", QueueURL: queueURL, Region: region, EndpointURL: endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	c := model.Change{
		SchemaVersion: 1, FeedURL: "f", ItemID: "i", Kind: model.ChangeNew,
		ContentHash: "h", DetectedAt: time.Now().UTC(),
		DLQFromSink: "primary", DLQError: "boom", DLQAttempts: 3,
	}
	if err := pub.Publish(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	got := receiveOne(t, endpoint, queueURL)
	if got["dlq_from_sink"] != "primary" || got["dlq_error"] != "boom" || got["dlq_attempts"] != "3" {
		t.Fatalf("missing DLQ attrs: %+v", got)
	}
}

func TestSQSFIFOSetsGroupAndDedup(t *testing.T) {
	endpoint, _ := setup(t)

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	admin := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	q, err := admin.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String("rss2msg-test.fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"},
	})
	if err != nil {
		t.Fatal(err)
	}
	queueURL := aws.ToString(q.QueueUrl)

	pub, err := sqs.New(ctx, sqs.Options{
		Name: "test", QueueURL: queueURL, Region: region, EndpointURL: endpoint,
		MessageGroup: "feed_url",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	when := time.Now().UTC().Truncate(time.Millisecond)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "https://e/feed", ItemID: "i-fifo",
		Kind: model.ChangeNew, ContentHash: "abc123", DetectedAt: when,
	}
	if err := pub.Publish(ctx, c); err != nil {
		t.Fatal(err)
	}

	// Receive and inspect both message attributes and system attributes
	// (MessageGroupId / MessageDeduplicationId live in the latter).
	reader := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		out, err := reader.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(queueURL),
			MaxNumberOfMessages:         1,
			WaitTimeSeconds:             2,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Messages) == 0 {
			continue
		}
		msg := out.Messages[0]
		groupID := msg.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)]
		dedupID := msg.Attributes[string(sqstypes.MessageSystemAttributeNameMessageDeduplicationId)]
		if groupID != "https://e/feed" {
			t.Errorf("MessageGroupId: want feed URL, got %q", groupID)
		}
		if dedupID == "" {
			t.Errorf("MessageDeduplicationId is empty")
		}
		if len(dedupID) != 64 {
			t.Errorf("MessageDeduplicationId: want 64-char hex sha256, got %d chars (%q)", len(dedupID), dedupID)
		}
		return
	}
	t.Fatal("no message received within 10s")
}
