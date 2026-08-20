package dapr

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"

	runtimev1 "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/iambod/rss2msg/internal/model"
)

// fakeDapr is a minimal Dapr runtime gRPC server that records the last
// PublishEvent request, so the sink can be exercised through the real Dapr Go
// SDK client without a sidecar or Docker.
type fakeDapr struct {
	runtimev1.UnimplementedDaprServer
	mu  sync.Mutex
	req *runtimev1.PublishEventRequest
}

func (f *fakeDapr) PublishEvent(_ context.Context, req *runtimev1.PublishEventRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.req = req
	return &emptypb.Empty{}, nil
}

func (f *fakeDapr) last() *runtimev1.PublishEventRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.req
}

// startFakeDapr serves fakeDapr on a loopback port and returns its address.
func startFakeDapr(t *testing.T) (addr string, srv *fakeDapr) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv = &fakeDapr{}
	gs := grpc.NewServer()
	runtimev1.RegisterDaprServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String(), srv
}

// TestPublishThroughRealSDK drives New (real Dapr client) → PublishEvent against
// the fake sidecar and asserts the request the broker would receive.
func TestPublishThroughRealSDK(t *testing.T) {
	addr, srv := startFakeDapr(t)

	p, err := New(context.Background(), Options{
		Name:       "out",
		Address:    addr,
		PubsubName: "rss-pubsub",
		Topic:      "rss.changes",
		Metadata:   map[string]string{"partitionKey": "feeds"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	change := sampleChange()
	if err := p.Publish(context.Background(), change); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := srv.last()
	if got == nil {
		t.Fatal("server received no PublishEvent request")
	}
	if got.GetPubsubName() != "rss-pubsub" {
		t.Errorf("pubsub_name = %q, want rss-pubsub", got.GetPubsubName())
	}
	if got.GetTopic() != "rss.changes" {
		t.Errorf("topic = %q, want rss.changes", got.GetTopic())
	}
	if got.GetDataContentType() != "application/json" {
		t.Errorf("data_content_type = %q, want application/json", got.GetDataContentType())
	}

	var decoded model.Change
	if err := json.Unmarshal(got.GetData(), &decoded); err != nil {
		t.Fatalf("unmarshal published data: %v", err)
	}
	if decoded.ItemID != change.ItemID {
		t.Errorf("published item_id = %q, want %q", decoded.ItemID, change.ItemID)
	}

	md := got.GetMetadata()
	if md["feed_url"] != change.FeedURL {
		t.Errorf("metadata feed_url = %q, want %q", md["feed_url"], change.FeedURL)
	}
	if md["partitionKey"] != "feeds" {
		t.Errorf("metadata partitionKey = %q, want feeds", md["partitionKey"])
	}
}

// TestNewHonoursContextCancellation pins that the caller's context governs the
// Dapr client's connection setup. The SDK dials with grpc.WithBlock, so an
// already-cancelled context must abort the dial instead of connecting anyway —
// which is what the deprecated, context-less constructor did.
func TestNewHonoursContextCancellation(t *testing.T) {
	addr, _ := startFakeDapr(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p, err := New(ctx, Options{
		Name:       "out",
		Address:    addr,
		PubsubName: "rss-pubsub",
		Topic:      "rss.changes",
	})
	if err == nil {
		t.Cleanup(func() { _ = p.Close() })
		t.Fatal("New with a cancelled context returned no error; the context is being ignored")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("New error = %v, want it to wrap context.Canceled", err)
	}
}
