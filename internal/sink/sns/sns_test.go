//go:build integration

package sns_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/iambod/rss2msg/internal/model"
	sinksns "github.com/iambod/rss2msg/internal/sink/sns"
	"github.com/iambod/rss2msg/test/awslocal"
)

const region = "us-east-1"

func setup(t *testing.T) (endpoint, topicARN, queueURL string) {
	t.Helper()
	ctx := context.Background()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ls := awslocal.Run(ctx, t)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	snsClient := sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = aws.String(ls.Endpoint) })
	sqsClient := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(ls.Endpoint) })

	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("rss2msg-test")})
	if err != nil {
		t.Fatal(err)
	}
	q, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String("rss2msg-test-q")})
	if err != nil {
		t.Fatal(err)
	}
	qAttrs, err := sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       q.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	queueARN := qAttrs.Attributes["QueueArn"]
	if _, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	}); err != nil {
		t.Fatal(err)
	}
	return ls.Endpoint, aws.ToString(topic.TopicArn), aws.ToString(q.QueueUrl)
}

func receiveOne(t *testing.T, endpoint, queueURL string) map[string]string {
	t.Helper()
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	client := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
	deadline := time.Now().Add(15 * time.Second)
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
	t.Fatal("no message received within 15s")
	return nil
}

func TestSNSPublishRoundTripsEnvelopeAndAttributes(t *testing.T) {
	endpoint, topicARN, queueURL := setup(t)
	pub, err := sinksns.New(context.Background(), sinksns.Options{
		Name: "test", TopicARN: topicARN, Region: region, EndpointURL: endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	c := model.Change{
		SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew,
		Title: "hi", ContentHash: "h", DetectedAt: time.Now().UTC(),
	}
	if err := pub.Publish(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	got := receiveOne(t, endpoint, queueURL)
	if got["feed_url"] != "f1" || got["kind"] != "new" || got["schema_version"] != "1" {
		t.Fatalf("base attrs missing: %+v", got)
	}
	var round model.Change
	if err := json.Unmarshal([]byte(got["body"]), &round); err != nil {
		t.Fatal(err)
	}
	if round.Title != "hi" {
		t.Fatalf("body title=%q", round.Title)
	}
}

func TestSNSPublishIncludesTraceparentWhenSpanActive(t *testing.T) {
	endpoint, topicARN, queueURL := setup(t)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	pub, err := sinksns.New(context.Background(), sinksns.Options{
		Name: "test", TopicARN: topicARN, Region: region, EndpointURL: endpoint,
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
		t.Fatalf("expected traceparent: %+v", got)
	}
}

func TestSNSPublishIncludesDLQAttributesWhenDecorated(t *testing.T) {
	endpoint, topicARN, queueURL := setup(t)
	pub, err := sinksns.New(context.Background(), sinksns.Options{
		Name: "test", TopicARN: topicARN, Region: region, EndpointURL: endpoint,
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
		t.Fatalf("DLQ attrs missing: %+v", got)
	}
}

func TestSNSRejectsFIFOTopicARN(t *testing.T) {
	_, err := sinksns.New(context.Background(), sinksns.Options{
		Name: "test", TopicARN: "arn:aws:sns:us-east-1:123:t.fifo",
	})
	if err == nil {
		t.Fatal("expected FIFO rejection")
	}
}
