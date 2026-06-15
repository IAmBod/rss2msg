package schema

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sr/srfake"
	"google.golang.org/protobuf/proto"

	"github.com/iambod/rss2msg/internal/model"
	sinkv1 "github.com/iambod/rss2msg/proto/sink/v1"
)

func TestProtobufEncoderFramesAndRoundTrips(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)
	enc, err := New(Options{URL: fake.URL(), Format: FormatProtobuf, Topic: "feed.changes", AutoRegister: true})
	if err != nil {
		t.Fatal(err)
	}
	if enc.Format() != "protobuf" {
		t.Fatalf("Format() = %q, want protobuf", enc.Format())
	}
	det := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	pub := time.Date(2026, 6, 14, 9, 30, 0, 0, time.UTC)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew,
		Title: "hi", Authors: []string{"a"}, PublishedAt: &pub, ContentHash: "h", DetectedAt: det,
	}
	framed, err := enc.Encode(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(framed) < 6 || framed[0] != 0 {
		t.Fatalf("not confluent-framed: len=%d", len(framed))
	}
	if binary.BigEndian.Uint32(framed[1:5]) == 0 {
		t.Fatal("zero schema id")
	}
	if framed[5] != 0 {
		t.Fatalf("message-index byte = %d, want 0 (single-message shortcut)", framed[5])
	}
	var pc sinkv1.Change
	if err := proto.Unmarshal(framed[6:], &pc); err != nil {
		t.Fatalf("proto payload did not decode: %v", err)
	}
	if pc.GetTitle() != "hi" || pc.GetFeedUrl() != "f1" {
		t.Fatalf("round-trip mismatch: %+v", &pc)
	}
	if !pc.GetDetectedAt().AsTime().Equal(det) {
		t.Fatalf("detected_at = %v, want %v", pc.GetDetectedAt().AsTime(), det)
	}
	if pc.GetPublishedAt() == nil || !pc.GetPublishedAt().AsTime().Equal(pub) {
		t.Fatalf("published_at mismatch")
	}
}

func TestProtobufEncoderNilOptionalTime(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)
	enc, err := New(Options{URL: fake.URL(), Format: FormatProtobuf, Topic: "t", AutoRegister: true})
	if err != nil {
		t.Fatal(err)
	}
	det := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	framed, err := enc.Encode(context.Background(), model.Change{ItemID: "i", ContentHash: "h", DetectedAt: det})
	if err != nil {
		t.Fatal(err)
	}
	var pc sinkv1.Change
	if err := proto.Unmarshal(framed[6:], &pc); err != nil {
		t.Fatal(err)
	}
	if pc.GetPublishedAt() != nil || pc.GetUpdatedAt() != nil {
		t.Fatal("nil optional times should be absent in proto")
	}
}

func TestProtobufEncoderUsesOverrideSchemaText(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)
	override := `syntax = "proto3"; package rss2msg.sink.v1; message Change { string item_id = 4; }`
	enc, err := New(Options{URL: fake.URL(), Format: FormatProtobuf, Topic: "t", AutoRegister: true, SchemaText: override})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Encode(context.Background(), model.Change{ItemID: "i"}); err != nil {
		t.Fatal(err)
	}
	got, ok := fake.GetSchema("t-value", 1)
	if !ok {
		t.Fatal("schema not registered")
	}
	if got.Schema.Schema != override {
		t.Fatalf("registered schema = %q, want override", got.Schema.Schema)
	}
}
