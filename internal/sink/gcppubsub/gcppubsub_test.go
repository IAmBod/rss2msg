//go:build integration

package gcppubsub_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink/gcppubsub"
	"github.com/iambod/rss2msg/test/gcplocal"
)

const (
	topicID = "feed-changes"
	subID   = "feed-changes-sub"
)

// setup boots the emulator, creates a topic + subscription, and returns the
// endpoint and a function that pulls the next message.
func setup(t *testing.T) (endpoint string, receive func(*testing.T) (body string, attrs map[string]string)) {
	t.Helper()
	ctx := context.Background()
	ps := gcplocal.Run(ctx, t)

	client := adminClient(t, ps.Endpoint)
	topicName := fmt.Sprintf("projects/%s/topics/%s", gcplocal.ProjectID, topicID)
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", gcplocal.ProjectID, subID)

	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subName,
		Topic: topicName,
	}); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	receive = func(t *testing.T) (string, map[string]string) {
		t.Helper()
		rctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		subscriber := client.Subscriber(subID)
		var (
			once sync.Once
			body string
			at   map[string]string
		)
		err := subscriber.Receive(rctx, func(_ context.Context, m *pubsub.Message) {
			once.Do(func() {
				body = string(m.Data)
				at = m.Attributes
				cancel()
			})
			m.Ack()
		})
		if err != nil && rctx.Err() == nil {
			t.Fatalf("receive: %v", err)
		}
		if body == "" && at == nil {
			t.Fatal("no message received within 15s")
		}
		return body, at
	}
	return ps.Endpoint, receive
}

func adminClient(t *testing.T, endpoint string) *pubsub.Client {
	t.Helper()
	client, err := pubsub.NewClient(context.Background(), gcplocal.ProjectID,
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newSink(t *testing.T, endpoint, orderingKey string) *gcppubsub.Publisher {
	t.Helper()
	pub, err := gcppubsub.New(context.Background(), gcppubsub.Options{
		Name: "test", ProjectID: gcplocal.ProjectID, TopicID: topicID,
		Endpoint: endpoint, OrderingKey: orderingKey,
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

func TestPublishRoundTripsEnvelopeAndAttributes(t *testing.T) {
	endpoint, receive := setup(t)
	pub := newSink(t, endpoint, "")

	when := time.Now().UTC().Truncate(time.Millisecond)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "f1", ItemID: "i1",
		Kind: model.ChangeNew, Title: "hi", ContentHash: "h", DetectedAt: when,
	}
	if err := pub.Publish(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	body, attrs := receive(t)
	if attrs["feed_url"] != "f1" || attrs["kind"] != "new" || attrs["schema_version"] != "1" {
		t.Fatalf("missing/incorrect base attributes: %+v", attrs)
	}
	var round model.Change
	if err := json.Unmarshal([]byte(body), &round); err != nil {
		t.Fatal(err)
	}
	if round.Title != "hi" {
		t.Fatalf("body title=%q", round.Title)
	}
}

func TestPublishIncludesTraceparentWhenSpanActive(t *testing.T) {
	endpoint, receive := setup(t)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	pub := newSink(t, endpoint, "")

	ctx, span := otel.Tracer("test").Start(context.Background(), "publish.test")
	defer span.End()
	c := model.Change{SchemaVersion: 1, FeedURL: "f", ItemID: "i", Kind: model.ChangeNew, ContentHash: "h", DetectedAt: time.Now().UTC()}
	if err := pub.Publish(ctx, c); err != nil {
		t.Fatal(err)
	}
	_, attrs := receive(t)
	if attrs["traceparent"] == "" {
		t.Fatalf("expected traceparent in attributes, got %+v", attrs)
	}
}

func TestPublishIncludesDLQAttributesWhenDecorated(t *testing.T) {
	endpoint, receive := setup(t)
	pub := newSink(t, endpoint, "")

	c := model.Change{
		SchemaVersion: 1, FeedURL: "f", ItemID: "i", Kind: model.ChangeNew,
		ContentHash: "h", DetectedAt: time.Now().UTC(),
		DLQFromSink: "primary", DLQError: "boom", DLQAttempts: 3,
	}
	if err := pub.Publish(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	_, attrs := receive(t)
	if attrs["dlq_from_sink"] != "primary" || attrs["dlq_error"] != "boom" || attrs["dlq_attempts"] != "3" {
		t.Fatalf("missing DLQ attrs: %+v", attrs)
	}
}

func TestPublishWithOrderingKeyDelivers(t *testing.T) {
	endpoint, receive := setup(t)
	pub := newSink(t, endpoint, "feed_url")

	c := model.Change{SchemaVersion: 1, FeedURL: "ordered-feed", ItemID: "i", Kind: model.ChangeNew, ContentHash: "h", DetectedAt: time.Now().UTC()}
	if err := pub.Publish(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	_, attrs := receive(t)
	if attrs["feed_url"] != "ordered-feed" {
		t.Fatalf("unexpected attrs: %+v", attrs)
	}
}
