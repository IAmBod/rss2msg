package main

import (
	"testing"

	"github.com/iambod/rss2msg/internal/config"
	sinkschema "github.com/iambod/rss2msg/internal/sink/kafka/schema"
)

func TestSchemaOptionsFromConfigDefaultsAutoRegisterTrue(t *testing.T) {
	got, err := schemaOptionsFromConfig("feed.changes", config.SchemaRegistryConfig{URL: "http://sr:8081", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected non-nil schema options when url set")
	}
	if got.Format != sinkschema.FormatJSON || got.Topic != "feed.changes" {
		t.Fatalf("bad mapping: %+v", got)
	}
	if !got.AutoRegister {
		t.Fatal("AutoRegister should default to true when unset")
	}
}

func TestSchemaOptionsFromConfigHonorsExplicitFalse(t *testing.T) {
	no := false
	got, err := schemaOptionsFromConfig("t", config.SchemaRegistryConfig{URL: "http://sr:8081", Format: "json", AutoRegister: &no})
	if err != nil {
		t.Fatal(err)
	}
	if got.AutoRegister {
		t.Fatal("AutoRegister should be false when explicitly set false")
	}
}

func TestSchemaOptionsFromConfigNilWhenURLEmpty(t *testing.T) {
	got, err := schemaOptionsFromConfig("t", config.SchemaRegistryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil when url empty, got %+v", got)
	}
}
