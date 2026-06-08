package cosmosdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

// fakeContainer is an in-memory stand-in for *azcosmos.ContainerClient that
// honours the Cosmos optimistic-concurrency semantics the coordinator relies
// on: CreateItem -> 409 on conflict, ReadItem -> 404 when absent, and
// ReplaceItem/DeleteItem -> 412 PreconditionFailed when the supplied
// If-Match ETag no longer matches the stored document.
type fakeContainer struct {
	mu    sync.Mutex
	items map[string]fakeDoc // id -> doc
	seq   int

	createCalls, readCalls, replaceCalls, deleteCalls int
}

type fakeDoc struct {
	body []byte
	etag azcore.ETag
}

func newFakeContainer() *fakeContainer {
	return &fakeContainer{items: make(map[string]fakeDoc)}
}

func respErr(status int) error {
	return &azcore.ResponseError{StatusCode: status}
}

func (f *fakeContainer) nextETag() azcore.ETag {
	f.seq++
	return azcore.ETag(fmt.Sprintf("etag-%d", f.seq))
}

func idFromBody(body []byte) string {
	var d leaseDoc
	_ = json.Unmarshal(body, &d)
	return d.ID
}

func (f *fakeContainer) CreateItem(_ context.Context, _ azcosmos.PartitionKey, item []byte, _ *azcosmos.ItemOptions) (azcosmos.ItemResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	id := idFromBody(item)
	if _, ok := f.items[id]; ok {
		return azcosmos.ItemResponse{}, respErr(http.StatusConflict)
	}
	etag := f.nextETag()
	f.items[id] = fakeDoc{body: append([]byte(nil), item...), etag: etag}
	return azcosmos.ItemResponse{Response: azcosmos.Response{ETag: etag}}, nil
}

func (f *fakeContainer) ReadItem(_ context.Context, _ azcosmos.PartitionKey, itemID string, _ *azcosmos.ItemOptions) (azcosmos.ItemResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCalls++
	d, ok := f.items[itemID]
	if !ok {
		return azcosmos.ItemResponse{}, respErr(http.StatusNotFound)
	}
	return azcosmos.ItemResponse{Value: append([]byte(nil), d.body...), Response: azcosmos.Response{ETag: d.etag}}, nil
}

func (f *fakeContainer) ReplaceItem(_ context.Context, _ azcosmos.PartitionKey, itemID string, item []byte, o *azcosmos.ItemOptions) (azcosmos.ItemResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replaceCalls++
	d, ok := f.items[itemID]
	if !ok {
		return azcosmos.ItemResponse{}, respErr(http.StatusNotFound)
	}
	if o == nil || o.IfMatchEtag == nil || *o.IfMatchEtag != d.etag {
		return azcosmos.ItemResponse{}, respErr(http.StatusPreconditionFailed)
	}
	etag := f.nextETag()
	f.items[itemID] = fakeDoc{body: append([]byte(nil), item...), etag: etag}
	return azcosmos.ItemResponse{Response: azcosmos.Response{ETag: etag}}, nil
}

func (f *fakeContainer) DeleteItem(_ context.Context, _ azcosmos.PartitionKey, itemID string, o *azcosmos.ItemOptions) (azcosmos.ItemResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	d, ok := f.items[itemID]
	if !ok {
		return azcosmos.ItemResponse{}, respErr(http.StatusNotFound)
	}
	if o != nil && o.IfMatchEtag != nil && *o.IfMatchEtag != d.etag {
		return azcosmos.ItemResponse{}, respErr(http.StatusPreconditionFailed)
	}
	delete(f.items, itemID)
	return azcosmos.ItemResponse{}, nil
}

func (f *fakeContainer) get(id string) (leaseDoc, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.items[id]
	if !ok {
		return leaseDoc{}, false
	}
	var ld leaseDoc
	_ = json.Unmarshal(d.body, &ld)
	return ld, true
}

func newTestCoord(c containerAPI, owner string) *Coordinator {
	return newWithContainer(c, Options{Database: "db", Owner: owner, LeaseDuration: 60 * time.Second})
}

func TestOwnerTokenIsUniquePerProcess(t *testing.T) {
	seen := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		tok := newOwnerToken()
		if tok == "" {
			t.Fatal("empty owner token")
		}
		if _, dup := seen[tok]; dup {
			t.Fatalf("duplicate owner token: %q", tok)
		}
		seen[tok] = struct{}{}
	}
	if tok := newOwnerToken(); strings.Count(tok, "-") < 2 {
		t.Fatalf("owner token %q lacks host-pid-hex shape", tok)
	}
}

func TestLockKeyDerivation(t *testing.T) {
	k1 := lockKey("https://a.example/feed")
	k2 := lockKey("https://b.example/feed")
	if k1 == k2 {
		t.Fatal("distinct feeds produced the same lock key")
	}
	if lockKey("https://a.example/feed") != k1 {
		t.Fatal("lock key is not deterministic")
	}
	if !strings.HasPrefix(k1, "rss2msg:coord:") {
		t.Fatalf("lock key missing namespace prefix: %q", k1)
	}
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(ctx, tc.opts); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestTryAcquireFirstWins(t *testing.T) {
	f := newFakeContainer()
	c := newTestCoord(f, "owner-a")

	rel, ok, err := c.TryAcquire(context.Background(), "feed1")
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	if rel == nil {
		t.Fatal("nil release func on win")
	}
	doc, ok := f.get(lockKey("feed1"))
	if !ok {
		t.Fatal("lease item not written")
	}
	if doc.Owner != "owner-a" {
		t.Fatalf("owner=%q", doc.Owner)
	}
}

func TestTryAcquireSecondInstanceBlocked(t *testing.T) {
	f := newFakeContainer()
	a := newTestCoord(f, "owner-a")
	b := newTestCoord(f, "owner-b")

	if _, ok, err := a.TryAcquire(context.Background(), "feed1"); err != nil || !ok {
		t.Fatalf("a acquire: ok=%v err=%v", ok, err)
	}
	rel, ok, err := b.TryAcquire(context.Background(), "feed1")
	if err != nil {
		t.Fatalf("b acquire err: %v", err)
	}
	if ok {
		t.Fatal("b should not have acquired a held, unexpired lease")
	}
	if rel != nil {
		t.Fatal("b release func should be nil when not acquired")
	}
}

func TestReleaseMakesReacquirable(t *testing.T) {
	f := newFakeContainer()
	a := newTestCoord(f, "owner-a")
	b := newTestCoord(f, "owner-b")

	rel, ok, _ := a.TryAcquire(context.Background(), "feed1")
	if !ok {
		t.Fatal("a should acquire")
	}
	if err := rel(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok := f.get(lockKey("feed1")); ok {
		t.Fatal("lease should be gone after release")
	}
	if _, ok, _ := b.TryAcquire(context.Background(), "feed1"); !ok {
		t.Fatal("b should acquire after a released")
	}
}

func TestExpiredLeaseCanBeStolen(t *testing.T) {
	f := newFakeContainer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a := newTestCoord(f, "owner-a")
	a.now = func() time.Time { return base }
	if _, ok, _ := a.TryAcquire(context.Background(), "feed1"); !ok {
		t.Fatal("a should acquire at t0")
	}

	b := newTestCoord(f, "owner-b")
	b.now = func() time.Time { return base.Add(2 * time.Minute) }
	if _, ok, err := b.TryAcquire(context.Background(), "feed1"); err != nil || !ok {
		t.Fatalf("b should steal expired lease: ok=%v err=%v", ok, err)
	}
	doc, _ := f.get(lockKey("feed1"))
	if doc.Owner != "owner-b" {
		t.Fatalf("owner after steal=%q, want owner-b", doc.Owner)
	}
}

func TestStaleReleaseDoesNotDeleteNewerLease(t *testing.T) {
	f := newFakeContainer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a := newTestCoord(f, "owner-a")
	a.now = func() time.Time { return base }
	relA, ok, _ := a.TryAcquire(context.Background(), "feed1")
	if !ok {
		t.Fatal("a should acquire")
	}

	b := newTestCoord(f, "owner-b")
	b.now = func() time.Time { return base.Add(2 * time.Minute) }
	if _, ok, _ := b.TryAcquire(context.Background(), "feed1"); !ok {
		t.Fatal("b should steal expired lease")
	}

	// a now belatedly releases — its ETag is stale, so it must NOT delete b's lease.
	if err := relA(context.Background()); err != nil {
		t.Fatalf("stale release should swallow precondition failure, got: %v", err)
	}
	doc, ok := f.get(lockKey("feed1"))
	if !ok {
		t.Fatal("b's lease was wrongly deleted by a's stale release")
	}
	if doc.Owner != "owner-b" {
		t.Fatalf("lease owner=%q after stale release, want owner-b", doc.Owner)
	}
}

func TestCloseReleasesHeldLeases(t *testing.T) {
	f := newFakeContainer()
	c := newTestCoord(f, "owner-a")
	if _, ok, _ := c.TryAcquire(context.Background(), "feed1"); !ok {
		t.Fatal("acquire")
	}
	if _, ok, _ := c.TryAcquire(context.Background(), "feed2"); !ok {
		t.Fatal("acquire 2")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, ok := f.get(lockKey("feed1")); ok {
		t.Fatal("Close should have released feed1")
	}
	if _, ok := f.get(lockKey("feed2")); ok {
		t.Fatal("Close should have released feed2")
	}
}

func TestDefaultLeaseDurationApplied(t *testing.T) {
	c := newWithContainer(newFakeContainer(), Options{Database: "db", Owner: "o"})
	if c.leaseDuration != DefaultLeaseDuration {
		t.Fatalf("leaseDuration=%v, want default %v", c.leaseDuration, DefaultLeaseDuration)
	}
}
