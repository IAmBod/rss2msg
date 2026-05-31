package main

import (
	"context"
	"testing"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/telemetry"
)

func TestBuildPublisher_Feed(t *testing.T) {
	tel := &telemetry.Telemetry{} // zero Meter/Logger; feed New tolerates them
	sc := config.SinkConfig{Name: "out", Driver: "feed", Feed: config.FeedSinkConfig{
		Listen: "127.0.0.1:0", Link: "https://x/", MaxItems: 5,
		Store: config.FeedStoreConfig{Driver: "memory"},
	}}
	p, err := buildPublisher(context.Background(), sc, tel)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Name() != "out" {
		t.Fatalf("name: %s", p.Name())
	}
}
