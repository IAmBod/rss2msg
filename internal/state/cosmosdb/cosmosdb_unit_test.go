package cosmosdb

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/iambod/rss2msg/internal/state"
)

// fakeContainer is an in-memory stand-in for *azcosmos.ContainerClient that
// honours the point-read / upsert semantics the state store relies on:
// ReadItem -> 404 when absent, UpsertItem -> create-or-replace by id.
type fakeContainer struct {
	mu    sync.Mutex
	items map[string][]byte // id -> body
}

func newFakeContainer() *fakeContainer {
	return &fakeContainer{items: make(map[string][]byte)}
}

func respErr(status int) error {
	return &azcore.ResponseError{StatusCode: status}
}

func idFromBody(body []byte) string {
	var d struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(body, &d)
	return d.ID
}

func (f *fakeContainer) ReadItem(_ context.Context, _ azcosmos.PartitionKey, itemID string, _ *azcosmos.ItemOptions) (azcosmos.ItemResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.items[itemID]
	if !ok {
		return azcosmos.ItemResponse{}, respErr(http.StatusNotFound)
	}
	return azcosmos.ItemResponse{Value: append([]byte(nil), body...)}, nil
}

func (f *fakeContainer) UpsertItem(_ context.Context, _ azcosmos.PartitionKey, item []byte, _ *azcosmos.ItemOptions) (azcosmos.ItemResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[idFromBody(item)] = append([]byte(nil), item...)
	return azcosmos.ItemResponse{}, nil
}

func (f *fakeContainer) Read(_ context.Context, _ *azcosmos.ReadContainerOptions) (azcosmos.ContainerResponse, error) {
	return azcosmos.ContainerResponse{}, nil
}

func TestNewValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		opts Options
	}{
		{"no auth", Options{Database: "db"}},
		{"both auth", Options{Database: "db", Endpoint: "https://x", ConnectionString: "AccountEndpoint=..."}},
		{"no database", Options{ConnectionString: "AccountEndpoint=..."}},
		{"negative ttl", Options{ConnectionString: "AccountEndpoint=...", Database: "db", ItemTTL: -time.Hour}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(ctx, tc.opts); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestDocID(t *testing.T) {
	a := docID("item-1")
	b := docID("item-2")
	if a == b {
		t.Fatal("distinct item ids produced the same doc id")
	}
	if docID("item-1") != a {
		t.Fatal("doc id is not deterministic")
	}
	if len(a) != 64 {
		t.Fatalf("doc id is not a sha256 hex string: %q", a)
	}
	// A GUID with Cosmos-illegal characters must still yield a safe id.
	id := docID("https://x/y?z#frag\\path")
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Fatalf("doc id contains non-hex char %q", c)
		}
	}
}

func TestMetaIDNeverCollidesWithItemID(t *testing.T) {
	// metaID uses underscores, which never appear in a hex item id.
	for _, raw := range []string{"meta", "__meta__", "0", "abc"} {
		if docID(raw) == metaID {
			t.Fatalf("docID(%q) collided with metaID", raw)
		}
	}
}

func TestBuildItemDocTTL(t *testing.T) {
	withTTL := newWithContainer(newFakeContainer(), time.Hour)
	doc := withTTL.buildItemDoc("feed", "item", "h", time.Unix(0, 0))
	if doc.TTL == nil || *doc.TTL != 3600 {
		t.Fatalf("ttl = %v, want 3600", doc.TTL)
	}
	if doc.ItemID != "item" || doc.FeedURL != "feed" || doc.ContentHash != "h" {
		t.Fatalf("unexpected doc fields: %+v", doc)
	}

	noTTL := newWithContainer(newFakeContainer(), 0)
	if d := noTTL.buildItemDoc("feed", "item", "h", time.Unix(0, 0)); d.TTL != nil {
		t.Fatalf("ttl should be nil when itemTTL=0, got %v", *d.TTL)
	}
}

func TestItemRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newWithContainer(newFakeContainer(), 0)

	if _, found, err := s.GetItem(ctx, "feed", "missing"); err != nil || found {
		t.Fatalf("missing item: found=%v err=%v", found, err)
	}

	seen := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	if err := s.UpsertItem(ctx, "feed", "item-1", "hash-1", seen); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, found, err := s.GetItem(ctx, "feed", "item-1")
	if err != nil || !found {
		t.Fatalf("get after upsert: found=%v err=%v", found, err)
	}
	if got.ContentHash != "hash-1" || !got.LastSeenAt.Equal(seen) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	// Overwrite is idempotent and updates the hash.
	if err := s.UpsertItem(ctx, "feed", "item-1", "hash-2", seen); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	got, _, _ = s.GetItem(ctx, "feed", "item-1")
	if got.ContentHash != "hash-2" {
		t.Fatalf("overwrite hash = %q, want hash-2", got.ContentHash)
	}
}

func TestFeedMetaRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newWithContainer(newFakeContainer(), 0)

	if _, found, err := s.GetFeedMeta(ctx, "feed"); err != nil || found {
		t.Fatalf("missing meta: found=%v err=%v", found, err)
	}

	lm := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	if err := s.UpsertFeedMeta(ctx, "feed", state.FeedMeta{ETag: `"abc"`, LastModified: lm}); err != nil {
		t.Fatalf("upsert meta: %v", err)
	}
	got, found, err := s.GetFeedMeta(ctx, "feed")
	if err != nil || !found {
		t.Fatalf("get meta: found=%v err=%v", found, err)
	}
	if got.ETag != `"abc"` || !got.LastModified.Equal(lm) {
		t.Fatalf("meta round-trip mismatch: %+v", got)
	}

	// Meta with a zero LastModified omits the field and reads back as zero.
	if err := s.UpsertFeedMeta(ctx, "feed", state.FeedMeta{ETag: `"def"`}); err != nil {
		t.Fatalf("upsert meta no-lm: %v", err)
	}
	got, _, _ = s.GetFeedMeta(ctx, "feed")
	if got.ETag != `"def"` || !got.LastModified.IsZero() {
		t.Fatalf("meta no-lm mismatch: %+v", got)
	}
}

func TestItemAndMetaShareNoID(t *testing.T) {
	ctx := context.Background()
	s := newWithContainer(newFakeContainer(), 0)

	// Upsert an item then meta under the same feed; neither should clobber the other.
	seen := time.Unix(100, 0).UTC()
	if err := s.UpsertItem(ctx, "feed", "item-1", "h", seen); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertFeedMeta(ctx, "feed", state.FeedMeta{ETag: `"e"`}); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.GetItem(ctx, "feed", "item-1"); !found {
		t.Fatal("item lost after meta upsert")
	}
	if _, found, _ := s.GetFeedMeta(ctx, "feed"); !found {
		t.Fatal("meta lost")
	}
}

func TestPruneItemsBeforeIsNoOp(t *testing.T) {
	s := &Store{} // no client needed; method must not touch it
	n, err := s.PruneItemsBefore(context.Background(), time.Now())
	if err != nil || n != 0 {
		t.Fatalf("got (%d, %v), want (0, nil)", n, err)
	}
}

func TestPruneFeedMetaBeforeIsNoOp(t *testing.T) {
	s := &Store{}
	n, err := s.PruneFeedMetaBefore(context.Background(), time.Now())
	if err != nil || n != 0 {
		t.Fatalf("got (%d, %v), want (0, nil)", n, err)
	}
}

func TestMetaDocTTLWhenConfigured(t *testing.T) {
	s := &Store{itemTTL: time.Hour, now: time.Now}
	doc := s.buildMetaDoc("https://f", state.FeedMeta{ETag: "e"})
	if doc.TTL == nil || *doc.TTL < 1 {
		t.Fatalf("expected positive ttl on meta doc, got %v", doc.TTL)
	}
}

func TestMetaDocNoTTLWhenUnset(t *testing.T) {
	s := &Store{now: time.Now} // itemTTL == 0
	doc := s.buildMetaDoc("https://f", state.FeedMeta{ETag: "e"})
	if doc.TTL != nil {
		t.Fatalf("expected no ttl on meta doc, got %v", *doc.TTL)
	}
}
