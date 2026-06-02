package grpc_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/iambod/rss2msg/internal/model"
	sinkgrpc "github.com/iambod/rss2msg/internal/sink/grpc"
	sinkv1 "github.com/iambod/rss2msg/proto/sink/v1"
)

func TestNewRequiresName(t *testing.T) {
	t.Parallel()
	if _, err := sinkgrpc.New(sinkgrpc.Options{Target: "127.0.0.1:9"}); err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("want name error, got %v", err)
	}
}

func TestNewRequiresTarget(t *testing.T) {
	t.Parallel()
	if _, err := sinkgrpc.New(sinkgrpc.Options{Name: "x"}); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("want target error, got %v", err)
	}
}

// recordingServer captures the last Publish call so tests can assert the wire
// mapping and metadata.
type recordingServer struct {
	sinkv1.UnimplementedChangeSinkServer

	mu     sync.Mutex
	req    *sinkv1.PublishRequest
	md     metadata.MD
	accept bool
	errMsg string
}

func (s *recordingServer) Publish(ctx context.Context, req *sinkv1.PublishRequest) (*sinkv1.PublishAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.req = req
	s.md, _ = metadata.FromIncomingContext(ctx)
	return &sinkv1.PublishAck{Accepted: s.accept, Error: s.errMsg}, nil
}

// startServer spins an in-process ChangeSink on a loopback port. No Docker.
func startServer(t *testing.T, srv sinkv1.ChangeSinkServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	sinkv1.RegisterChangeSinkServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func sampleChange() model.Change {
	pub := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	upd := time.Date(2026, 6, 2, 11, 0, 0, 0, time.UTC)
	return model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://example.com/feed.xml",
		FeedTitle:     "Example",
		ItemID:        "item-1",
		Kind:          model.ChangeNew,
		Title:         "Hello",
		Link:          "https://example.com/1",
		Authors:       []string{"a", "b"},
		Summary:       "sum",
		Content:       "body",
		Categories:    []string{"news"},
		PublishedAt:   &pub,
		UpdatedAt:     &upd,
		ContentHash:   "deadbeef",
		DetectedAt:    time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC),
	}
}

func TestPublishRoundTripsChangeAndMetadata(t *testing.T) {
	t.Parallel()
	srv := &recordingServer{accept: true}
	addr := startServer(t, srv)

	pub, err := sinkgrpc.New(sinkgrpc.Options{
		Name:     "grpc-test",
		Target:   addr,
		Timeout:  5 * time.Second,
		Metadata: map[string]string{"authorization": "Bearer t"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pub.Close()

	in := sampleChange()
	if err := pub.Publish(context.Background(), in); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	got := srv.req.GetChange()
	if got.GetItemId() != in.ItemID || got.GetKind() != string(in.Kind) {
		t.Fatalf("item/kind mismatch: %+v", got)
	}
	if got.GetSchemaVersion() != int32(in.SchemaVersion) || got.GetFeedUrl() != in.FeedURL {
		t.Fatalf("schema/feed mismatch: %+v", got)
	}
	if len(got.GetAuthors()) != 2 || got.GetAuthors()[1] != "b" {
		t.Fatalf("authors mismatch: %v", got.GetAuthors())
	}
	if !got.GetPublishedAt().AsTime().Equal(*in.PublishedAt) || !got.GetDetectedAt().AsTime().Equal(in.DetectedAt) {
		t.Fatalf("timestamp mismatch: pub=%v det=%v", got.GetPublishedAt().AsTime(), got.GetDetectedAt().AsTime())
	}
	if got.GetContentHash() != in.ContentHash {
		t.Fatalf("content_hash mismatch: %q", got.GetContentHash())
	}

	// Static + canonical metadata both present.
	if v := srv.md.Get("authorization"); len(v) != 1 || v[0] != "Bearer t" {
		t.Fatalf("authorization metadata missing: %v", v)
	}
	if v := srv.md.Get("rss2msg-item-id"); len(v) != 1 || v[0] != in.ItemID {
		t.Fatalf("rss2msg-item-id metadata missing: %v", v)
	}
	if v := srv.md.Get("rss2msg-kind"); len(v) != 1 || v[0] != string(in.Kind) {
		t.Fatalf("rss2msg-kind metadata missing: %v", v)
	}
}

func TestPublishOmitsAbsentTimestamps(t *testing.T) {
	t.Parallel()
	srv := &recordingServer{accept: true}
	addr := startServer(t, srv)
	pub, err := sinkgrpc.New(sinkgrpc.Options{Name: "g", Target: addr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pub.Close()

	in := sampleChange()
	in.PublishedAt = nil
	in.UpdatedAt = nil
	if err := pub.Publish(context.Background(), in); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.req.GetChange().GetPublishedAt() != nil || srv.req.GetChange().GetUpdatedAt() != nil {
		t.Fatalf("expected absent timestamps, got pub=%v upd=%v",
			srv.req.GetChange().GetPublishedAt(), srv.req.GetChange().GetUpdatedAt())
	}
}

func TestPublishRejectedAckIsError(t *testing.T) {
	t.Parallel()
	srv := &recordingServer{accept: false, errMsg: "nope"}
	addr := startServer(t, srv)
	pub, err := sinkgrpc.New(sinkgrpc.Options{Name: "g", Target: addr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pub.Close()

	err = pub.Publish(context.Background(), sampleChange())
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("want rejection error containing reason, got %v", err)
	}
}
