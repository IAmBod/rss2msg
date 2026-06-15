package kafka

import (
	"strings"
	"testing"

	"github.com/iambod/rss2msg/internal/sink/kafka/schema"
)

func TestNewBuildsSchemaEncoderWhenConfigured(t *testing.T) {
	p, err := New(Options{
		Name: "k", Brokers: []string{"b:9092"}, Topic: "feed.changes",
		Schema: &schema.Options{URL: "http://sr:8081", Format: schema.FormatJSON, Topic: "feed.changes", AutoRegister: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if p.encoder == nil {
		t.Fatal("expected encoder to be built when Schema configured")
	}
}

func TestNewNoEncoderWhenSchemaNil(t *testing.T) {
	p, err := New(Options{Name: "k", Brokers: []string{"b:9092"}, Topic: "t"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if p.encoder != nil {
		t.Fatal("expected no encoder when Schema is nil")
	}
}

func TestNewRejectsBadSchemaFormat(t *testing.T) {
	_, err := New(Options{
		Name: "k", Brokers: []string{"b:9092"}, Topic: "t",
		Schema: &schema.Options{URL: "http://sr:8081", Format: "bogus", Topic: "t"},
	})
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("want schema error, got %v", err)
	}
}
