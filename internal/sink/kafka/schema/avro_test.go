package schema

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/iambod/rss2msg/internal/model"
)

func TestCanonicalAvroSchemaParses(t *testing.T) {
	if _, err := avro.Parse(changeAvroSchema); err != nil {
		t.Fatalf("canonical avro schema does not parse: %v", err)
	}
}

func TestAvroEncoderFramesAndRoundTrips(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)
	enc, err := New(Options{URL: fake.URL(), Format: FormatAvro, Topic: "feed.changes", AutoRegister: true})
	if err != nil {
		t.Fatal(err)
	}
	if enc.Format() != "avro" {
		t.Fatalf("Format() = %q, want avro", enc.Format())
	}
	det := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	pub := time.Date(2026, 6, 14, 9, 30, 0, 0, time.UTC)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew,
		Title: "hi", Authors: []string{"a"}, Categories: []string{"x"},
		PublishedAt: &pub, ContentHash: "h", DetectedAt: det,
	}
	framed, err := enc.Encode(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(framed) < 5 || framed[0] != 0 {
		t.Fatalf("not confluent-framed")
	}
	if binary.BigEndian.Uint32(framed[1:5]) == 0 {
		t.Fatal("zero schema id")
	}
	parsed := avro.MustParse(changeAvroSchema)
	var round avroChange
	if err := avro.Unmarshal(parsed, framed[5:], &round); err != nil {
		t.Fatalf("avro payload did not decode: %v", err)
	}
	if round.Title != "hi" || round.FeedURL != "f1" {
		t.Fatalf("round-trip mismatch: %+v", round)
	}
	if !round.DetectedAt.Equal(det) {
		t.Fatalf("detected_at = %v, want %v", round.DetectedAt, det)
	}
	if round.PublishedAt == nil || !round.PublishedAt.Equal(pub) {
		t.Fatalf("published_at = %v, want %v", round.PublishedAt, pub)
	}
	if len(round.Authors) != 1 || round.Authors[0] != "a" {
		t.Fatalf("authors = %v", round.Authors)
	}
}

func TestAvroEncoderNilOptionalTimeEncodesNull(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)
	enc, err := New(Options{URL: fake.URL(), Format: FormatAvro, Topic: "t", AutoRegister: true})
	if err != nil {
		t.Fatal(err)
	}
	det := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	framed, err := enc.Encode(context.Background(), model.Change{ItemID: "i", ContentHash: "h", DetectedAt: det})
	if err != nil {
		t.Fatal(err)
	}
	parsed := avro.MustParse(changeAvroSchema)
	var round avroChange
	if err := avro.Unmarshal(parsed, framed[5:], &round); err != nil {
		t.Fatal(err)
	}
	if round.PublishedAt != nil || round.UpdatedAt != nil {
		t.Fatalf("nil optional times should decode to nil")
	}
}
