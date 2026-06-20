//go:build integration

package rabbitmqstream

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	streamamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/iambod/rss2msg/internal/model"
)

// broker holds the connection coordinates for a started RabbitMQ Stream broker.
type broker struct {
	uri  string
	host string
	port int
}

// startBroker boots a rabbitmq:4-management container with the rabbitmq_stream
// plugin enabled and port 5552 exposed. The stream protocol makes the client
// reconnect to the hostnames the broker advertises (its container hostname,
// unreachable from the test host), so callers pin every connection to the
// returned host:port via AddressResolverHost/Port (the client's NAT/LB mode).
func startBroker(t *testing.T) broker {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:4-management",
			ExposedPorts: []string{"5552/tcp"},
			Env: map[string]string{
				"RABBITMQ_DEFAULT_USER": "guest",
				"RABBITMQ_DEFAULT_PASS": "guest",
			},
			Files: []testcontainers.ContainerFile{
				{
					Reader:            strings.NewReader("[rabbitmq_management,rabbitmq_stream].\n"),
					ContainerFilePath: "/etc/rabbitmq/enabled_plugins",
					FileMode:          0o644,
				},
			},
			WaitingFor: wait.ForListeningPort("5552/tcp").
				WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("skipping: cannot start rabbitmq stream container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	mapped, err := c.MappedPort(ctx, "5552/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}
	port, err := strconv.Atoi(mapped.Port())
	if err != nil {
		t.Fatalf("port atoi: %v", err)
	}
	return broker{
		uri:  fmt.Sprintf("rabbitmq-stream://guest:guest@%s:%d/", host, port),
		host: host,
		port: port,
	}
}

func TestRabbitMQStreamPublishConfirmsAndRoundTrips(t *testing.T) {
	b := startBroker(t)
	ctx := context.Background()

	p, err := New(ctx, Options{
		Name:                "t",
		URIs:                []string{b.uri},
		Stream:              "itest",
		Declare:             true,
		AddressResolverHost: b.host,
		AddressResolverPort: b.port,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	change := model.Change{FeedURL: "u", Kind: model.ChangeNew, ItemID: "1"}
	if err := p.Publish(ctx, change); err != nil {
		t.Fatalf("Publish: want confirmed nil, got %v", err)
	}

	// Consume the one message back and assert round-trip + app property.
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetUris([]string{b.uri}).
			SetAddressResolver(stream.AddressResolver{Host: b.host, Port: b.port}),
	)
	if err != nil {
		t.Fatalf("consumer env: %v", err)
	}
	t.Cleanup(func() { _ = env.Close() })

	got := make(chan *streamamqp.Message, 1)
	consumer, err := env.NewConsumer("itest",
		func(_ stream.ConsumerContext, msg *streamamqp.Message) {
			select {
			case got <- msg:
			default:
			}
		},
		stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()),
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	select {
	case msg := <-got:
		if len(msg.Data) == 0 {
			t.Fatalf("empty message data")
		}
		var rt model.Change
		if err := json.Unmarshal(msg.Data[0], &rt); err != nil {
			t.Fatalf("unmarshal body: %v", err)
		}
		if rt.FeedURL != "u" || rt.ItemID != "1" {
			t.Fatalf("body did not round-trip: %+v", rt)
		}
		if msg.ApplicationProperties["feed_url"] != "u" {
			t.Fatalf("feed_url app property = %v, want u", msg.ApplicationProperties["feed_url"])
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for consumed message")
	}
}
