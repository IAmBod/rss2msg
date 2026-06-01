package gcppubsub

import (
	"context"
	"testing"

	"github.com/iambod/rss2msg/internal/model"
)

func TestNewRejectsBadOptions(t *testing.T) {
	cases := []struct {
		name string
		opts Options
	}{
		{"missing name", Options{ProjectID: "p", TopicID: "t"}},
		{"missing project_id", Options{Name: "s", TopicID: "t"}},
		{"missing topic_id", Options{Name: "s", ProjectID: "p"}},
		{"unknown ordering_key", Options{Name: "s", ProjectID: "p", TopicID: "t", OrderingKey: "bogus"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(context.Background(), tc.opts)
			if err == nil {
				_ = p.Close()
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestNewAcceptsValidOrderingKeys(t *testing.T) {
	for _, k := range []string{"", "feed_url", "item_id", "sink"} {
		opts := Options{
			Name:      "s",
			ProjectID: "p",
			TopicID:   "t",
			Endpoint:  "localhost:8085", // emulator: WithoutAuthentication, lazy dial
		}
		if k != "" {
			opts.OrderingKey = k
		}
		p, err := New(context.Background(), opts)
		if err != nil {
			t.Fatalf("ordering_key %q: unexpected error: %v", k, err)
		}
		wantOrdering := k != ""
		if p.publisher.EnableMessageOrdering != wantOrdering {
			t.Errorf("ordering_key %q: EnableMessageOrdering = %v, want %v", k, p.publisher.EnableMessageOrdering, wantOrdering)
		}
		_ = p.Close()
	}
}

func TestOrderingKeyFor(t *testing.T) {
	change := model.Change{FeedURL: "https://feed.example/rss", ItemID: "item-123"}
	cases := []struct {
		strategy string
		want     string
	}{
		{"", ""},
		{"feed_url", "https://feed.example/rss"},
		{"item_id", "item-123"},
		{"sink", "my-sink"},
	}
	for _, tc := range cases {
		p := &Publisher{name: "my-sink", orderingKey: tc.strategy}
		if got := p.orderingKeyFor(change); got != tc.want {
			t.Errorf("orderingKeyFor(%q) = %q, want %q", tc.strategy, got, tc.want)
		}
	}
}
