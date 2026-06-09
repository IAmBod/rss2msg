package schema

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/iambod/rss2msg/internal/model"
)

func TestCanonicalJSONSchemaMentionsFields(t *testing.T) {
	text, err := canonicalJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"feed_url", "item_id", "content_hash"} {
		if !strings.Contains(text, field) {
			t.Fatalf("canonical schema missing %q: %s", field, text)
		}
	}
}

func TestJSONEncoderFramesWithMagicAndID(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)

	enc, err := New(Options{
		URL: fake.URL(), Format: FormatJSON, Topic: "feed.changes", AutoRegister: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if enc.Format() != "json" {
		t.Fatalf("Format() = %q, want json", enc.Format())
	}

	c := model.Change{SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew, ContentHash: "h", Title: "hi"}
	framed, err := enc.Encode(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(framed) < 5 {
		t.Fatalf("framed too short: %d bytes", len(framed))
	}
	if framed[0] != 0 {
		t.Fatalf("magic byte = %d, want 0", framed[0])
	}
	id := binary.BigEndian.Uint32(framed[1:5])
	if id == 0 {
		t.Fatal("schema id is zero")
	}
	var round model.Change
	if err := json.Unmarshal(framed[5:], &round); err != nil {
		t.Fatalf("payload not valid JSON Change: %v", err)
	}
	if round.Title != "hi" {
		t.Fatalf("payload title = %q, want hi", round.Title)
	}
}

func BenchmarkJSONEncoderEncode(b *testing.B) {
	fake := srfake.New()
	b.Cleanup(fake.Close)
	enc, err := New(Options{URL: fake.URL(), Format: FormatJSON, Topic: "feed.changes", AutoRegister: true})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	c := model.Change{SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew, ContentHash: "h", Title: "hi"}
	if _, err := enc.Encode(ctx, c); err != nil { // warm: register schema once
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(ctx, c); err != nil {
			b.Fatal(err)
		}
	}
}

func TestJSONEncoderUsesOverrideSchemaText(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)
	override := `{"type":"object","title":"custom-change"}`

	enc, err := New(Options{
		URL: fake.URL(), Format: FormatJSON, Topic: "t", AutoRegister: true, SchemaText: override,
	})
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
