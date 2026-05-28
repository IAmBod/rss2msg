//go:build integration

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/feed"
	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/retry"
	"github.com/iambod/rss2msg/internal/scheduler"
	"github.com/iambod/rss2msg/internal/sink"
	sinkkafka "github.com/iambod/rss2msg/internal/sink/kafka"
	sinkpg "github.com/iambod/rss2msg/internal/sink/postgres"
	statepg "github.com/iambod/rss2msg/internal/state/postgres"
)

const (
	rssV1 = `<?xml version="1.0"?>
<rss version="2.0"><channel><title>S</title>
<item><guid>a</guid><title>One</title><link>https://e/a</link><description>first</description></item>
</channel></rss>`

	rssV2 = `<?xml version="1.0"?>
<rss version="2.0"><channel><title>S</title>
<item><guid>a</guid><title>One v2</title><link>https://e/a</link><description>second</description></item>
</channel></rss>`

	kafkaTopic = "feed.changes.e2e"
)

func TestEndToEndPublishesNewAndUpdated(t *testing.T) {
	ctx := context.Background()

	// Boot Postgres.
	pgC, err := tcpg.Run(ctx, "postgres:16-alpine",
		tcpg.WithDatabase("rss2msg"),
		tcpg.WithUsername("test"),
		tcpg.WithPassword("test"),
		tcpg.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })

	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	// Boot Kafka.
	kafkaC, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.6.0", tckafka.WithClusterID("e2e-cluster"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kafkaC.Terminate(ctx) })

	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Pre-create the topic (cp-kafka image has auto-create disabled).
	createTopic(t, brokers, kafkaTopic)

	// Mutable feed body served by httptest.
	var body atomic.Value
	body.Store(rssV1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		fmt.Fprint(w, body.Load().(string))
	}))
	t.Cleanup(srv.Close)

	// Build the pieces.
	store, err := statepg.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	pgPub, err := sinkpg.New(ctx, sinkpg.Options{Name: "pg", DSN: dsn, Table: "feed_changes"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pgPub.Close() })

	kfkPub, err := sinkkafka.New(sinkkafka.Options{Name: "kafka", Brokers: brokers, Topic: kafkaTopic, Acks: "all"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kfkPub.Close() })

	reg := sink.NewRegistry()
	_ = reg.Add(pgPub)
	_ = reg.Add(kfkPub)

	wraps := []*sink.RetryingPublisher{
		sink.WithRetry(pgPub, nil, retry.Config{MaxAttempts: 2, BaseDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}),
		sink.WithRetry(kfkPub, nil, retry.Config{MaxAttempts: 2, BaseDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}),
	}

	fetcher := feed.NewFetcher(feed.Options{UserAgent: "rss2msg/e2e", Timeout: 5 * time.Second})
	det := feed.NewDetector()
	log := zerolog.Nop()
	pipe := newE2EPipeline(srv.URL, config.FeedConfig{URL: srv.URL, Interval: time.Second}, wraps, fetcher, det, store, log)

	// First run-once: rssV1 — expect a "new" change.
	if err := scheduler.RunOnce(ctx, scheduler.RunOnceConfig{Pipelines: []scheduler.FeedPipeline{pipe}, Concurrency: 1}); err != nil {
		t.Fatal(err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	var (
		kind        string
		payloadJSON []byte
	)
	if err := pool.QueryRow(ctx, `SELECT kind, payload FROM feed_changes WHERE feed_url=$1 AND item_id=$2`, srv.URL, "a").Scan(&kind, &payloadJSON); err != nil {
		t.Fatalf("expected Postgres sink row after first run: %v", err)
	}
	if kind != "new" {
		t.Fatalf("first run kind=%q, want 'new'", kind)
	}
	var c model.Change
	if err := json.Unmarshal(payloadJSON, &c); err != nil {
		t.Fatal(err)
	}
	if c.Title != "One" {
		t.Fatalf("first run title=%q, want 'One'", c.Title)
	}

	// Mutate the feed and re-run; expect an "updated" change.
	body.Store(rssV2)
	if err := scheduler.RunOnce(ctx, scheduler.RunOnceConfig{Pipelines: []scheduler.FeedPipeline{pipe}, Concurrency: 1}); err != nil {
		t.Fatal(err)
	}

	var updatedCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM feed_changes WHERE feed_url=$1 AND item_id=$2 AND kind='updated'`, srv.URL, "a").Scan(&updatedCount); err != nil {
		t.Fatal(err)
	}
	if updatedCount != 1 {
		t.Fatalf("expected 1 updated row, got %d", updatedCount)
	}

	// Verify Kafka received both records.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("e2e-consumer"),
		kgo.ConsumeTopics(kafkaTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Close)

	pollCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	saw := map[string]int{}
	deadline := time.Now().Add(15 * time.Second)
	for saw["a"] < 2 && time.Now().Before(deadline) {
		f := consumer.PollFetches(pollCtx)
		if errs := f.Errors(); len(errs) > 0 && pollCtx.Err() == nil {
			t.Fatalf("poll errs: %v", errs)
		}
		f.EachRecord(func(r *kgo.Record) {
			saw[string(r.Key)]++
		})
		if pollCtx.Err() != nil {
			break
		}
	}
	if saw["a"] < 2 {
		t.Fatalf("expected >=2 kafka records for key 'a', got %d", saw["a"])
	}
}

// createTopic pre-creates the kafka topic; the cp-kafka image disables
// auto-creation.
func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)
	if _, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}

// e2ePipe mirrors cmd/rss2msg/pipeline.go but without telemetry plumbing
// since we don't assert on spans/metrics here.
type e2ePipe struct {
	url     string
	cfg     config.FeedConfig
	sinks   []*sink.RetryingPublisher
	fetcher *feed.Fetcher
	det     *feed.Detector
	store   *statepg.Store
	log     zerolog.Logger
}

func newE2EPipeline(url string, cfg config.FeedConfig, ss []*sink.RetryingPublisher, f *feed.Fetcher, d *feed.Detector, s *statepg.Store, log zerolog.Logger) *e2ePipe {
	return &e2ePipe{url: url, cfg: cfg, sinks: ss, fetcher: f, det: d, store: s, log: log}
}

func (p *e2ePipe) FeedURL() string { return p.url }

func (p *e2ePipe) RunOnce(ctx context.Context, feedURL string, at time.Time) ([]model.Change, error) {
	res, err := p.fetcher.Fetch(ctx, feed.FetchRequest{URL: feedURL, Headers: p.cfg.HTTP.Headers, Timeout: p.cfg.HTTP.Timeout})
	if err != nil {
		return nil, err
	}
	if res.NotModified {
		return nil, nil
	}
	changes, err := p.det.Detect(ctx, feedURL, res.Feed, p.store, at)
	if err != nil {
		return nil, err
	}
	for _, c := range changes {
		allOK := true
		for _, w := range p.sinks {
			r := w.Deliver(ctx, c)
			if r.State == sink.BranchDropped {
				allOK = false
			}
		}
		if allOK {
			_ = p.store.UpsertItem(ctx, feedURL, c.ItemID, c.ContentHash, c.DetectedAt)
		}
	}
	return changes, nil
}
