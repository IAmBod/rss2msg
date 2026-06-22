package dynamodb

import (
	"context"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/iambod/rss2msg/internal/state"
)

func TestNewRequiresTable(t *testing.T) {
	_, err := New(context.Background(), Options{})
	if err == nil {
		t.Fatal("expected error when table is empty")
	}
}

func TestNewRejectsHalfConfiguredTTL(t *testing.T) {
	cases := []Options{
		{Table: "t", TTLAttribute: "expires_at"}, // attr without duration
		{Table: "t", ItemTTL: time.Hour},         // duration without attr
	}
	for i, opts := range cases {
		if _, err := New(context.Background(), opts); err == nil {
			t.Errorf("case %d: expected error for half-configured TTL, got nil", i)
		}
	}
}

func TestNewAcceptsValidTTLAndNoTTL(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	if _, err := New(context.Background(), Options{Table: "t"}); err != nil {
		t.Fatalf("no-TTL config: %v", err)
	}
	if _, err := New(context.Background(), Options{Table: "t", TTLAttribute: "expires_at", ItemTTL: time.Hour}); err != nil {
		t.Fatalf("valid TTL config: %v", err)
	}
}

func TestItemKey(t *testing.T) {
	k := itemKey("https://example.com/feed", "guid-1")
	pk, ok := k[pkAttr].(*ddbtypes.AttributeValueMemberS)
	if !ok || pk.Value != "https://example.com/feed" {
		t.Fatalf("pk = %#v", k[pkAttr])
	}
	sk, ok := k[skAttr].(*ddbtypes.AttributeValueMemberS)
	if !ok || sk.Value != "guid-1" {
		t.Fatalf("sk = %#v", k[skAttr])
	}
}

func TestMetaSentinelSK(t *testing.T) {
	k := itemKey("f", metaSK)
	sk := k[skAttr].(*ddbtypes.AttributeValueMemberS)
	if sk.Value != "#META" {
		t.Fatalf("meta sentinel sort key = %q, want #META", sk.Value)
	}
}

func TestStringAttr(t *testing.T) {
	item := map[string]ddbtypes.AttributeValue{
		"content_hash": &ddbtypes.AttributeValueMemberS{Value: "abc"},
		"bad":          &ddbtypes.AttributeValueMemberN{Value: "1"},
	}
	got, err := stringAttr(item, "content_hash")
	if err != nil || got != "abc" {
		t.Fatalf("content_hash: got %q err %v", got, err)
	}
	// Missing attribute -> empty, no error.
	got, err = stringAttr(item, "missing")
	if err != nil || got != "" {
		t.Fatalf("missing: got %q err %v", got, err)
	}
	// Wrong type -> error.
	if _, err := stringAttr(item, "bad"); err == nil {
		t.Fatal("expected error for non-string attribute")
	}
}

func TestTimeAttr(t *testing.T) {
	want := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	item := map[string]ddbtypes.AttributeValue{
		"last_seen_at": &ddbtypes.AttributeValueMemberS{Value: want.Format(time.RFC3339Nano)},
		"bad":          &ddbtypes.AttributeValueMemberS{Value: "not-a-time"},
	}
	got, err := timeAttr(item, "last_seen_at")
	if err != nil || !got.Equal(want) {
		t.Fatalf("last_seen_at: got %v err %v", got, err)
	}
	// Missing -> zero time, no error.
	got, err = timeAttr(item, "missing")
	if err != nil || !got.IsZero() {
		t.Fatalf("missing: got %v err %v", got, err)
	}
	// Unparseable -> error.
	if _, err := timeAttr(item, "bad"); err == nil {
		t.Fatal("expected error for unparseable time")
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

func TestUpsertFeedMetaWritesTTLWhenConfigured(t *testing.T) {
	// A store configured with a TTL attribute + item_ttl must write that
	// attribute on the meta row so DynamoDB prunes stale feed_meta.
	s := &Store{ttlAttribute: "expires_at", itemTTL: time.Hour}
	item := s.buildFeedMetaItem("https://f", state.FeedMeta{ETag: "e"})
	if _, ok := item["expires_at"]; !ok {
		t.Fatal("expected ttl attribute on meta item when item_ttl set")
	}
}

func TestUpsertFeedMetaNoTTLWhenUnset(t *testing.T) {
	s := &Store{} // no ttlAttribute / itemTTL
	item := s.buildFeedMetaItem("https://f", state.FeedMeta{ETag: "e"})
	if _, ok := item["expires_at"]; ok {
		t.Fatal("did not expect a ttl attribute when item_ttl unset")
	}
}
