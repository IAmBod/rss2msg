//go:build integration

package nats_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	"github.com/iambod/rss2msg/internal/model"
	sinknats "github.com/iambod/rss2msg/internal/sink/nats"
)

// run boots a nats container (JetStream is enabled by default by the module)
// and returns its connection string plus a cleanup func.
func run(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()
	c, err := tcnats.Run(ctx, "nats:2.10-alpine")
	if err != nil {
		t.Fatalf("run nats container: %v", err)
	}
	url, err := c.ConnectionString(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("connection string: %v", err)
	}
	return url, func() { _ = c.Terminate(ctx) }
}

func sampleChange() model.Change {
	return model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://example.com/feed.xml",
		ItemID:        "item-1",
		Kind:          model.ChangeNew,
		Title:         "Hello",
		Link:          "https://example.com/1",
		ContentHash:   "abc",
		DetectedAt:    time.Now().UTC(),
	}
}

func TestCorePublish(t *testing.T) {
	url, cleanup := run(t)
	defer cleanup()

	// Subscriber on the target subject.
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("connect subscriber: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("feed.changes")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush sub: %v", err)
	}

	p, err := sinknats.New(sinknats.Options{Name: "nats-test", URL: url, Subject: "feed.changes"})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer p.Close()

	change := sampleChange()
	if err := p.Publish(context.Background(), change); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("next msg: %v", err)
	}

	var got model.Change
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ItemID != change.ItemID || got.FeedURL != change.FeedURL {
		t.Fatalf("payload mismatch: got %+v", got)
	}
	if h := msg.Header.Get("feed_url"); h != change.FeedURL {
		t.Errorf("feed_url header = %q, want %q", h, change.FeedURL)
	}
	if h := msg.Header.Get("kind"); h != string(change.Kind) {
		t.Errorf("kind header = %q, want %q", h, change.Kind)
	}
	if h := msg.Header.Get("schema_version"); h != "1" {
		t.Errorf("schema_version header = %q, want 1", h)
	}
}

func TestJetStreamPublish(t *testing.T) {
	url, cleanup := run(t)
	defer cleanup()

	ctx := context.Background()
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "FEED",
		Subjects: []string{"feed.changes"},
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	p, err := sinknats.New(sinknats.Options{
		Name: "nats-js", URL: url, Subject: "feed.changes", JetStream: true,
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer p.Close()

	if err := p.Publish(ctx, sampleChange()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// The message must have been persisted to the stream.
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream message count = %d, want 1", info.State.Msgs)
	}
}

func TestUsernamePasswordAuth(t *testing.T) {
	ctx := context.Background()
	c, err := tcnats.Run(ctx, "nats:2.10-alpine",
		tcnats.WithUsername("rss"), tcnats.WithPassword("s3cret"))
	if err != nil {
		t.Fatalf("run nats container: %v", err)
	}
	defer func() { _ = c.Terminate(ctx) }()
	url, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	// Wrong credentials must fail to connect.
	if _, err := sinknats.New(sinknats.Options{
		Name: "nats-bad", URL: url, Subject: "feed.changes",
		Username: "rss", Password: "wrong",
	}); err == nil {
		t.Fatal("expected connect failure with wrong password")
	}

	// Correct credentials connect and publish.
	p, err := sinknats.New(sinknats.Options{
		Name: "nats-auth", URL: url, Subject: "feed.changes",
		Username: "rss", Password: "s3cret",
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer p.Close()
	if err := p.Publish(ctx, sampleChange()); err != nil {
		t.Fatalf("publish: %v", err)
	}
}
