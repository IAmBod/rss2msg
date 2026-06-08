package cosmosdb

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/iambod/rss2msg/internal/model"
)

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{
			name: "missing name",
			opts: Options{ConnectionString: "x", Database: "db"},
		},
		{
			name: "no auth",
			opts: Options{Name: "s", Database: "db"},
		},
		{
			name: "both auth modes",
			opts: Options{Name: "s", Endpoint: "https://acct.documents.azure.com:443/", ConnectionString: "x", Database: "db"},
		},
		{
			name: "missing database",
			opts: Options{Name: "s", ConnectionString: "x"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(t.Context(), tt.opts); err == nil {
				t.Fatalf("New(%s): expected validation error, got nil", tt.name)
			}
		})
	}
}

func TestBuildDocument(t *testing.T) {
	change := model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://example.com/feed",
		ItemID:        "item-1",
		Kind:          model.ChangeNew,
		Title:         "Hello",
		ContentHash:   "abc",
	}

	id, pk, body, err := buildDocument(change)
	if err != nil {
		t.Fatalf("buildDocument: %v", err)
	}

	// Partition key is the feed URL (locality per feed).
	if pk != change.FeedURL {
		t.Errorf("partition key: got %q, want %q", pk, change.FeedURL)
	}

	// The document must carry a non-empty Cosmos "id" plus the full Change.
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("document is not valid JSON: %v", err)
	}
	if doc["id"] != id || id == "" {
		t.Errorf("doc id: got %v, want %q", doc["id"], id)
	}
	if doc["feed_url"] != change.FeedURL {
		t.Errorf("doc feed_url: got %v, want %q", doc["feed_url"], change.FeedURL)
	}
	if doc["item_id"] != change.ItemID {
		t.Errorf("doc item_id: got %v, want %q", doc["item_id"], change.ItemID)
	}

	// id is a deterministic function of (feed_url, item_id).
	id2, _, _, _ := buildDocument(change)
	if id2 != id {
		t.Errorf("id not deterministic: %q vs %q", id, id2)
	}
}

func TestBuildDocumentDistinctIDs(t *testing.T) {
	base := model.Change{FeedURL: "https://a/feed", ItemID: "1"}
	otherItem := model.Change{FeedURL: "https://a/feed", ItemID: "2"}
	otherFeed := model.Change{FeedURL: "https://b/feed", ItemID: "1"}

	id0, _, _, _ := buildDocument(base)
	id1, _, _, _ := buildDocument(otherItem)
	id2, _, _, _ := buildDocument(otherFeed)

	if id0 == id1 {
		t.Errorf("different item_id must yield different id, both %q", id0)
	}
	if id0 == id2 {
		t.Errorf("different feed_url must yield different id, both %q", id0)
	}
}

func TestIsConflict(t *testing.T) {
	if isConflict(nil) {
		t.Error("nil is not a conflict")
	}
	if isConflict(errString("boom")) {
		t.Error("plain error is not a conflict")
	}
	if !isConflict(&azcore.ResponseError{StatusCode: http.StatusConflict}) {
		t.Error("409 must be a conflict")
	}
	if isConflict(&azcore.ResponseError{StatusCode: http.StatusInternalServerError}) {
		t.Error("500 is not a conflict")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
