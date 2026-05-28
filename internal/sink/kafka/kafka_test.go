//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iambod/rss2msg/internal/model"
	sinkkafka "github.com/iambod/rss2msg/internal/sink/kafka"
)

func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
}

func setup(t *testing.T) ([]string, func()) {
	t.Helper()
	ctx := context.Background()
	c, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.6.0", tckafka.WithClusterID("test-cluster"))
	if err != nil {
		t.Fatal(err)
	}
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() { _ = c.Terminate(ctx) }
	return brokers, cleanup
}

func TestPublishProducesAndRoundTripsEnvelope(t *testing.T) {
	brokers, cleanup := setup(t)
	defer cleanup()

	createTopic(t, brokers, "feed.changes.test")

	pub, err := sinkkafka.New(sinkkafka.Options{
		Name: "test", Brokers: brokers, Topic: "feed.changes.test", Acks: "all",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()

	now := time.Now().UTC().Truncate(time.Millisecond)
	c := model.Change{SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew, ContentHash: "h", DetectedAt: now, Title: "hi"}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, c); err != nil {
		t.Fatal(err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("e2e-test"),
		kgo.ConsumeTopics("feed.changes.test"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer fetchCancel()
	fetches := consumer.PollFetches(fetchCtx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("poll errs: %v", errs)
	}
	var saw bool
	fetches.EachRecord(func(r *kgo.Record) {
		if string(r.Key) != "i1" {
			return
		}
		var round model.Change
		if err := json.Unmarshal(r.Value, &round); err != nil {
			t.Fatal(err)
		}
		if round.Title != "hi" {
			t.Fatalf("title mismatch %+v", round)
		}
		var feedURLHeader string
		for _, h := range r.Headers {
			if h.Key == "feed_url" {
				feedURLHeader = string(h.Value)
			}
		}
		if feedURLHeader != "f1" {
			t.Fatalf("missing feed_url header, got %q", feedURLHeader)
		}
		saw = true
	})
	if !saw {
		t.Fatal("did not observe published record")
	}
}
