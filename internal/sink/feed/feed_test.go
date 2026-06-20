package feed

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestPublisher_PublishThenServe(t *testing.T) {
	ctx := context.Background()
	p, err := New(ctx, Options{
		Name: "out", Listen: "127.0.0.1:0", MaxItems: 50,
		Meta:        FeedMeta{Title: "t", Link: "https://x/"},
		StoreDriver: "memory",
		RSS:         Surface{Enabled: true}, Atom: Surface{Enabled: true},
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
	p, err := New(ctx, Options{Name: "out", Listen: "127.0.0.1:0", MaxItems: 50, StoreDriver: "memory", Meta: FeedMeta{Title: "t", Link: "https://x/"}, RSS: Surface{Enabled: true}, Atom: Surface{Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(ctx, model.Change{FeedURL: "f", ItemID: "1", DLQFromSink: "other", DetectedAt: time.Unix(1, 0)}); err != nil {
		t.Fatalf("dlq-annotated publish should be a no-op, got %v", err)
	}
	resp, err := http.Get("http://" + p.Addr() + "/atom")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "<entry") {
		t.Fatal("dlq-annotated change must not appear in the feed")
	}
}

func TestNew_DisabledSurfaceReturns404(t *testing.T) {
	ctx := context.Background()
	p, err := New(ctx, Options{
		Name: "out", Listen: "127.0.0.1:0", MaxItems: 5, StoreDriver: "memory",
		Meta: FeedMeta{Title: "t", Link: "https://x/"},
		RSS:  Surface{Enabled: false, Path: "/rss"},
		Atom: Surface{Enabled: true, Path: "/atom"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	rss, err := http.Get("http://" + p.Addr() + "/rss")
	if err != nil {
		t.Fatal(err)
	}
	rss.Body.Close()
	if rss.StatusCode != http.StatusNotFound {
		t.Fatalf("disabled rss surface: status = %d, want 404", rss.StatusCode)
	}
	atom, err := http.Get("http://" + p.Addr() + "/atom")
	if err != nil {
		t.Fatal(err)
	}
	atom.Body.Close()
	if atom.StatusCode != http.StatusOK {
		t.Fatalf("enabled atom surface: status = %d, want 200", atom.StatusCode)
	}
}

func TestMCP_RouteMountedAndAuthGated(t *testing.T) {
	ctx := context.Background()
	p, err := New(ctx, Options{
		Name: "out", Listen: "127.0.0.1:0", MaxItems: 5, StoreDriver: "memory",
		Meta:    FeedMeta{Title: "t", Link: "https://x/"},
		RSS:     Surface{Enabled: true},
		MCP:     Surface{Enabled: true, Path: "/mcp"},
		RSSAuth: &SurfaceAuth{BearerTokens: []NamedSecret{{Secret: "secret"}}},
		MCPAuth: &SurfaceAuth{BearerTokens: []NamedSecret{{Secret: "secret"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// Unauthenticated MCP request is rejected by the sink's auth (401), not 404
	// — proving the route is mounted behind the same auth as rss/atom.
	resp, err := http.Get("http://" + p.Addr() + "/mcp")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /mcp: status = %d, want 401", resp.StatusCode)
	}
	// rss is still served on the same listener (mux coexistence). Auth applies
	// to every surface, so authenticate this request.
	req, _ := http.NewRequest(http.MethodGet, "http://"+p.Addr()+"/rss", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rss, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	rss.Body.Close()
	if rss.StatusCode != http.StatusOK {
		t.Fatalf("/rss alongside mcp: status = %d, want 200", rss.StatusCode)
	}
}

func TestMCP_DisabledHasNoRoute(t *testing.T) {
	ctx := context.Background()
	p, err := New(ctx, Options{
		Name: "out", Listen: "127.0.0.1:0", MaxItems: 5, StoreDriver: "memory",
		Meta: FeedMeta{Title: "t", Link: "https://x/"},
		RSS:  Surface{Enabled: true}, // mcp left disabled
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	resp, err := http.Get("http://" + p.Addr() + "/mcp")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("disabled mcp /mcp: status = %d, want 404", resp.StatusCode)
	}
}

func TestMCP_EndToEndListRecentItems(t *testing.T) {
	ctx := context.Background()
	p, err := New(ctx, Options{
		Name: "out", Listen: "127.0.0.1:0", MaxItems: 10, StoreDriver: "memory",
		Meta: FeedMeta{Title: "t", Link: "https://x/"},
		MCP:  Surface{Enabled: true, Path: "/mcp"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(ctx, model.Change{
		FeedURL: "https://a/feed", ItemID: "1", Title: "Hello MCP",
		Content: "body", DetectedAt: time.Unix(5, 0),
	}); err != nil {
		t.Fatal(err)
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	sess, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             "http://" + p.Addr() + "/mcp",
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer sess.Close()

	res, err := sess.CallTool(ctx, &mcp.CallToolParams{
		Name:      "list_recent_items",
		Arguments: map[string]any{"limit": 5},
	})
	if err != nil {
		t.Fatalf("call list_recent_items: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res.Content)
	}
	js, _ := json.Marshal(res.StructuredContent)
	if !strings.Contains(string(js), "Hello MCP") {
		t.Fatalf("list_recent_items output missing published item: %s", js)
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
		PublicURL: "https://feeds.example", Atom: Surface{Enabled: true, Path: "/atom"},
		Meta: FeedMeta{Title: "t", Link: "https://site/"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_ = p.Publish(ctx, model.Change{FeedURL: "f", ItemID: "1", Title: "Hi", DetectedAt: time.Unix(1, 0)})
	resp, err := http.Get("http://" + p.Addr() + "/atom")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `rel="self"`) || !strings.Contains(string(body), "https://feeds.example/atom") {
		t.Fatalf("atom self link must use public_url; got:\n%s", body)
	}
}

func TestPublisher_SelfLinkFromTrustedProxy(t *testing.T) {
	p, err := New(context.Background(), Options{
		Name: "f", Listen: "127.0.0.1:0",
		Meta:           FeedMeta{Title: "t", Link: "https://site"},
		Atom:           Surface{Enabled: true, Path: "/atom"},
		TrustedProxies: []string{"private"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	r := httptest.NewRequest(http.MethodGet, "http://"+p.Addr()+"/atom", nil)
	r.RemoteAddr = "10.0.0.1:5"
	r.Header.Set("X-Forwarded-Proto", "https")
	r.Header.Set("X-Forwarded-Host", "feeds.example.com")
	w := httptest.NewRecorder()
	p.server.Handler.ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), `href="https://feeds.example.com/atom" rel="self"`) {
		t.Fatalf("self link not from proxy headers:\n%s", w.Body.String())
	}
}
