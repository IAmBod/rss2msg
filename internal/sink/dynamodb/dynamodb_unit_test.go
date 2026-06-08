package dynamodb

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/iambod/rss2msg/internal/model"
)

func TestNewRequiresName(t *testing.T) {
	t.Parallel()
	_, err := New(context.Background(), Options{Table: "t"})
	if err == nil || !strings.Contains(err.Error(), "name is required") {
		t.Fatalf("got %v", err)
	}
}

func TestNewRequiresTable(t *testing.T) {
	t.Parallel()
	_, err := New(context.Background(), Options{Name: "x"})
	if err == nil || !strings.Contains(err.Error(), "table is required") {
		t.Fatalf("got %v", err)
	}
}

func TestNewRejectsItemTTLWithoutAttribute(t *testing.T) {
	t.Parallel()
	_, err := New(context.Background(), Options{Name: "x", Table: "t", ItemTTL: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "ttl_attribute") {
		t.Fatalf("got %v", err)
	}
}

func TestNewDefaultsKeyNames(t *testing.T) {
	t.Parallel()
	p, err := New(context.Background(), Options{Name: "x", Table: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if p.partitionKey != DefaultPartitionKey || p.sortKey != DefaultSortKey {
		t.Fatalf("keys: pk=%q sk=%q", p.partitionKey, p.sortKey)
	}
	if p.Name() != "x" {
		t.Fatalf("name=%q", p.Name())
	}
}

func strVal(t *testing.T, item map[string]ddbtypes.AttributeValue, key string) string {
	t.Helper()
	v, ok := item[key]
	if !ok {
		t.Fatalf("missing attribute %q in %v", key, item)
	}
	s, ok := v.(*ddbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatalf("attribute %q is not a string: %T", key, v)
	}
	return s.Value
}

func TestItemUsesFeedURLAndItemIDForKeys(t *testing.T) {
	t.Parallel()
	p := &Publisher{partitionKey: DefaultPartitionKey, sortKey: DefaultSortKey}
	c := model.Change{
		SchemaVersion: 1, FeedURL: "https://e/feed", ItemID: "i1",
		Kind: model.ChangeNew, Title: "hi", ContentHash: "h",
		DetectedAt: time.Now().UTC(),
	}
	item, err := p.item(c)
	if err != nil {
		t.Fatal(err)
	}
	if got := strVal(t, item, "feed_url"); got != "https://e/feed" {
		t.Fatalf("feed_url=%q", got)
	}
	if got := strVal(t, item, "item_id"); got != "i1" {
		t.Fatalf("item_id=%q", got)
	}
	if got := strVal(t, item, "kind"); got != "new" {
		t.Fatalf("kind=%q", got)
	}
	if got := strVal(t, item, "title"); got != "hi" {
		t.Fatalf("title=%q", got)
	}
}

func TestItemHonorsCustomKeyNames(t *testing.T) {
	t.Parallel()
	p := &Publisher{partitionKey: "pk", sortKey: "sk"}
	c := model.Change{FeedURL: "f", ItemID: "i", DetectedAt: time.Now().UTC()}
	item, err := p.item(c)
	if err != nil {
		t.Fatal(err)
	}
	if got := strVal(t, item, "pk"); got != "f" {
		t.Fatalf("pk=%q", got)
	}
	if got := strVal(t, item, "sk"); got != "i" {
		t.Fatalf("sk=%q", got)
	}
}

func TestItemAddsTTLAttributeWhenConfigured(t *testing.T) {
	t.Parallel()
	when := time.Unix(1_000_000, 0).UTC()
	p := &Publisher{
		partitionKey: DefaultPartitionKey, sortKey: DefaultSortKey,
		ttlAttribute: "expires_at", itemTTL: time.Hour,
	}
	item, err := p.item(model.Change{FeedURL: "f", ItemID: "i", DetectedAt: when})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := item["expires_at"].(*ddbtypes.AttributeValueMemberN)
	if !ok {
		t.Fatalf("expires_at not a number: %T", item["expires_at"])
	}
	wantUnix := when.Add(time.Hour).Unix()
	if v.Value != strconv.FormatInt(wantUnix, 10) {
		t.Fatalf("ttl=%q want %d", v.Value, wantUnix)
	}
}

func TestItemOmitsTTLAttributeWhenUnset(t *testing.T) {
	t.Parallel()
	p := &Publisher{partitionKey: DefaultPartitionKey, sortKey: DefaultSortKey}
	item, err := p.item(model.Change{FeedURL: "f", ItemID: "i", DetectedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := item["expires_at"]; ok {
		t.Fatalf("unexpected ttl attribute present")
	}
}
