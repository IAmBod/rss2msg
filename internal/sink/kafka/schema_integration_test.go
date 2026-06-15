//go:build integration

package kafka_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	avro "github.com/hamba/avro/v2"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iambod/rss2msg/internal/model"
	sinkkafka "github.com/iambod/rss2msg/internal/sink/kafka"
	"github.com/iambod/rss2msg/internal/sink/kafka/schema"
)

const kafkaAlias = "kafka"

// startSchemaRegistry starts a Confluent Schema Registry container on the
// given Docker network, connecting it to Kafka via the internal broker
// listener (kafkaAlias:9092). Returns the host-mapped HTTP base URL.
func startSchemaRegistry(t *testing.T, nwName, kafkaBootstrap string) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-schema-registry:7.6.0",
		ExposedPorts: []string{"8081/tcp"},
		Networks:     []string{nwName},
		Env: map[string]string{
			"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": kafkaBootstrap,
			"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
		},
		WaitingFor: wait.ForHTTP("/subjects").WithPort("8081/tcp").WithStartupTimeout(120 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start schema registry: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "8081")
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

func TestSchemaRegistryJSONRoundTrip(t *testing.T) {
	ctx := context.Background()

	// Create a shared Docker network so Schema Registry can reach Kafka via
	// the internal BROKER listener (kafkaAlias:9092).
	nw, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(ctx) })

	// Start Kafka on the shared network with a known alias.
	kc, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.6.0",
		tckafka.WithClusterID("test-cluster"),
		tcnetwork.WithNetwork([]string{kafkaAlias}, nw),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	createTopic(t, brokers, "feed.changes.schema")

	// Schema Registry reaches Kafka via the internal BROKER listener on port 9092.
	// The testcontainers kafka module configures: BROKER://<hostname>:9092
	// where hostname == the network alias we registered ("kafka").
	srURL := startSchemaRegistry(t, nw.Name, kafkaAlias+":9092")

	pub, err := sinkkafka.New(sinkkafka.Options{
		Name:    "schema",
		Brokers: brokers,
		Topic:   "feed.changes.schema",
		Acks:    "all",
		Schema: &schema.Options{
			URL:          srURL,
			Format:       schema.FormatJSON,
			Topic:        "feed.changes.schema",
			AutoRegister: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()

	change := model.Change{
		SchemaVersion: 1,
		FeedURL:       "f1",
		ItemID:        "i1",
		Kind:          model.ChangeNew,
		ContentHash:   "h",
		Title:         "hi",
	}
	pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
	defer pcancel()
	if err := pub.Publish(pctx, change); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics("feed.changes.schema"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	fctx, fcancel := context.WithTimeout(ctx, 15*time.Second)
	defer fcancel()
	fetches := consumer.PollFetches(fctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("poll errs: %v", errs)
	}
	var saw bool
	fetches.EachRecord(func(r *kgo.Record) {
		if string(r.Key) != "i1" {
			return
		}
		if len(r.Value) < 5 || r.Value[0] != 0 {
			t.Fatalf("value not Confluent-framed (first bytes %v)", r.Value)
		}
		if id := binary.BigEndian.Uint32(r.Value[1:5]); id == 0 {
			t.Fatal("zero schema id in framed record")
		}
		var round model.Change
		if err := json.Unmarshal(r.Value[5:], &round); err != nil {
			t.Fatalf("payload unmarshal: %v", err)
		}
		if round.Title != "hi" {
			t.Fatalf("title = %q, want %q", round.Title, "hi")
		}
		saw = true
	})
	if !saw {
		t.Fatal("did not observe framed record")
	}
}

func TestSchemaRegistryAvroRoundTrip(t *testing.T) {
	ctx := context.Background()

	// Create a shared Docker network so Schema Registry can reach Kafka via
	// the internal BROKER listener (kafkaAlias:9092).
	nw, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(ctx) })

	// Start Kafka on the shared network with a known alias.
	kc, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.6.0",
		tckafka.WithClusterID("test-cluster"),
		tcnetwork.WithNetwork([]string{kafkaAlias}, nw),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	createTopic(t, brokers, "feed.changes.avro")

	// Schema Registry reaches Kafka via the internal BROKER listener on port 9092.
	srURL := startSchemaRegistry(t, nw.Name, kafkaAlias+":9092")

	pub, err := sinkkafka.New(sinkkafka.Options{
		Name:    "schema-avro",
		Brokers: brokers,
		Topic:   "feed.changes.avro",
		Acks:    "all",
		Schema: &schema.Options{
			URL:          srURL,
			Format:       schema.FormatAvro,
			Topic:        "feed.changes.avro",
			AutoRegister: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()

	change := model.Change{
		SchemaVersion: 1,
		FeedURL:       "f1",
		ItemID:        "i1",
		Kind:          model.ChangeNew,
		ContentHash:   "h",
		Title:         "hi",
		DetectedAt:    time.Now().UTC().Truncate(time.Microsecond),
	}
	pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
	defer pcancel()
	if err := pub.Publish(pctx, change); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics("feed.changes.avro"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	fctx, fcancel := context.WithTimeout(ctx, 15*time.Second)
	defer fcancel()
	fetches := consumer.PollFetches(fctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("poll errs: %v", errs)
	}
	var saw bool
	fetches.EachRecord(func(r *kgo.Record) {
		if string(r.Key) != "i1" {
			return
		}
		// Assert Confluent framing: magic byte 0x00 followed by 4-byte schema ID.
		if len(r.Value) < 5 || r.Value[0] != 0 {
			t.Fatalf("value not Confluent-framed (first bytes %v)", r.Value)
		}
		if id := binary.BigEndian.Uint32(r.Value[1:5]); id == 0 {
			t.Fatal("zero schema id in framed record")
		}
		// Decode the Avro payload (everything after the 5-byte framing header).
		var m map[string]any
		if err := avro.Unmarshal(avro.MustParse(schema.ChangeAvroSchema()), r.Value[5:], &m); err != nil {
			t.Fatalf("avro unmarshal: %v", err)
		}
		if m["title"] != "hi" {
			t.Fatalf("title = %q, want %q", m["title"], "hi")
		}
		if m["feed_url"] != "f1" {
			t.Fatalf("feed_url = %q, want %q", m["feed_url"], "f1")
		}
		saw = true
	})
	if !saw {
		t.Fatal("did not observe framed Avro record")
	}
}
