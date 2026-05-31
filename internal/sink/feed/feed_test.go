package feed

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

func TestPublisher_PublishThenServe(t *testing.T) {
	ctx := context.Background()
	p, err := New(ctx, Options{
		Name: "out", Listen: "127.0.0.1:0", MaxItems: 50,
		Meta:        FeedMeta{Title: "t", Link: "https://x/"},
		StoreDriver: "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Name() != "out" {
		t.Fatalf("name: %s", p.Name())
	}
	pub := time.Unix(7000, 0).UTC()
	if err := p.Publish(ctx, model.Change{FeedURL: "f", ItemID: "1", Title: "Hi", DetectedAt: pub}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get("http://" + p.Addr() + "/rss")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(body), "Hi") {
		t.Fatalf("expected item in feed, got %d: %s", resp.StatusCode, body)
	}
}

func TestPublisher_SkipsDLQAnnotated(t *testing.T) {
	ctx := context.Background()
	p, err := New(ctx, Options{Name: "out", Listen: "127.0.0.1:0", MaxItems: 50, StoreDriver: "memory", Meta: FeedMeta{Title: "t", Link: "https://x/"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(ctx, model.Change{FeedURL: "f", ItemID: "1", DLQFromSink: "other", DetectedAt: time.Unix(1, 0)}); err != nil {
		t.Fatalf("dlq-annotated publish should be a no-op, got %v", err)
	}
	resp, _ := http.Get("http://" + p.Addr() + "/atom")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "<entry") {
		t.Fatal("dlq-annotated change must not appear in the feed")
	}
}

func TestNew_BindErrorSurfaces(t *testing.T) {
	ctx := context.Background()
	p1, err := New(ctx, Options{Name: "a", Listen: "127.0.0.1:0", MaxItems: 5, StoreDriver: "memory", Meta: FeedMeta{Title: "t", Link: "https://x/"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p1.Close()
	_, err = New(ctx, Options{Name: "b", Listen: p1.Addr(), MaxItems: 5, StoreDriver: "memory", Meta: FeedMeta{Title: "t", Link: "https://x/"}})
	if err == nil {
		t.Fatal("expected bind error on occupied address")
	}
}

func TestPublisher_SelfLinkUsesPublicURL(t *testing.T) {
	ctx := context.Background()
	p, err := New(ctx, Options{
		Name: "out", Listen: "127.0.0.1:0", MaxItems: 5, StoreDriver: "memory",
		PublicURL: "https://feeds.example", AtomPath: "/atom",
		Meta: FeedMeta{Title: "t", Link: "https://site/"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_ = p.Publish(ctx, model.Change{FeedURL: "f", ItemID: "1", Title: "Hi", DetectedAt: time.Unix(1, 0)})
	resp, _ := http.Get("http://" + p.Addr() + "/atom")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `rel="self"`) || !strings.Contains(string(body), "https://feeds.example/atom") {
		t.Fatalf("atom self link must use public_url; got:\n%s", body)
	}
}
