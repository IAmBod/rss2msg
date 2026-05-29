package sns

import (
	"context"
	"strings"
	"testing"

	"github.com/iambod/rss2msg/internal/model"
)

func TestNewRejectsUnknownMessageGroup(t *testing.T) {
	t.Parallel()
	_, err := New(context.Background(), Options{
		Name: "x", TopicARN: "arn:aws:sns:us-east-1:123:t.fifo", MessageGroup: "broadcast",
	})
	if err == nil || !strings.Contains(err.Error(), "message_group") {
		t.Fatalf("got %v", err)
	}
}

func TestNewRejectsMessageGroupOnStandardTopic(t *testing.T) {
	t.Parallel()
	_, err := New(context.Background(), Options{
		Name: "x", TopicARN: "arn:aws:sns:us-east-1:123:t", MessageGroup: "feed_url",
	})
	if err == nil || !strings.Contains(err.Error(), "FIFO") {
		t.Fatalf("got %v", err)
	}
}

func TestFifoDedupIDStableAndContentSensitive(t *testing.T) {
	t.Parallel()
	a := fifoDedupID(model.Change{FeedURL: "f", ItemID: "i", ContentHash: "h1"})
	b := fifoDedupID(model.Change{FeedURL: "f", ItemID: "i", ContentHash: "h1"})
	c := fifoDedupID(model.Change{FeedURL: "f", ItemID: "i", ContentHash: "h2"})
	if a != b {
		t.Fatalf("expected stable id for same input, got %q vs %q", a, b)
	}
	if a == c {
		t.Fatalf("expected distinct ids for different content_hash")
	}
	if len(a) != 64 {
		t.Fatalf("expected 64-char hex sha256, got %d (%q)", len(a), a)
	}
}

func TestFifoGroupIDDispatchesOnMessageGroup(t *testing.T) {
	t.Parallel()
	change := model.Change{FeedURL: "https://e/feed", ItemID: "i1"}
	cases := []struct {
		mg   string
		want string
	}{
		{"feed_url", "https://e/feed"},
		{"item_id", "i1"},
		{"sink", "mysink"},
	}
	for _, tc := range cases {
		p := &Publisher{name: "mysink", messageGroup: tc.mg}
		got := p.fifoGroupID(change)
		if got != tc.want {
			t.Errorf("messageGroup=%q: want %q, got %q", tc.mg, tc.want, got)
		}
	}
}
