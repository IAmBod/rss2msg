package feedsource

import (
	"context"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

func TestStaticSourceReturnsFixedFeeds(t *testing.T) {
	feeds := []config.FeedConfig{{URL: "https://e/1", Interval: time.Minute}}
	s := NewStatic("static", feeds)

	got, err := s.Feeds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].URL != "https://e/1" {
		t.Fatalf("feeds = %+v", got)
	}
	if s.Name() != "static" {
		t.Fatalf("name = %q", s.Name())
	}
	// Changes never fires for a static source.
	select {
	case <-s.Changes():
		t.Fatal("static source should not signal changes")
	case <-time.After(20 * time.Millisecond):
	}
}
