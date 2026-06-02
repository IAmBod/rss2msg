package feed

import (
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

func tPtr(sec int64) *time.Time { t := time.Unix(sec, 0).UTC(); return &t }

// newest-first, as Store.Recent returns them.
func mcpSampleChanges() []model.Change {
	return []model.Change{
		{FeedURL: "https://a/feed", FeedTitle: "Alpha", ItemID: "a2", Title: "Second Alpha",
			Link: "https://a/2", Summary: "about gophers", Content: "full gopher body",
			PublishedAt: tPtr(2000), DetectedAt: time.Unix(2001, 0)},
		{FeedURL: "https://a/feed", FeedTitle: "Alpha", ItemID: "a1", Title: "First Alpha",
			Summary: "intro", Content: "hello world", PublishedAt: tPtr(1000), DetectedAt: time.Unix(1001, 0)},
		{FeedURL: "https://b/feed", FeedTitle: "Beta", ItemID: "b1", Title: "Beta One",
			Content: "GOPHERS everywhere", PublishedAt: tPtr(1500), DetectedAt: time.Unix(1501, 0)},
	}
}

func TestFeedsFrom_GroupsAndCounts(t *testing.T) {
	feeds := feedsFrom(mcpSampleChanges())
	if len(feeds) != 2 {
		t.Fatalf("want 2 feeds, got %d (%+v)", len(feeds), feeds)
	}
	// First-seen order preserved: Alpha (2 items) then Beta (1).
	if feeds[0].FeedURL != "https://a/feed" || feeds[0].ItemCount != 2 || feeds[0].FeedTitle != "Alpha" {
		t.Fatalf("feeds[0] = %+v, want Alpha/2", feeds[0])
	}
	if feeds[1].FeedURL != "https://b/feed" || feeds[1].ItemCount != 1 {
		t.Fatalf("feeds[1] = %+v, want Beta/1", feeds[1])
	}
}

func TestRecentItems_LimitAndFeedFilterAndSummary(t *testing.T) {
	all := recentItems(mcpSampleChanges(), 2, "")
	if len(all) != 2 {
		t.Fatalf("limit=2 should yield 2 items, got %d", len(all))
	}
	if all[0].ItemID != "a2" {
		t.Fatalf("expected newest-first, got %q first", all[0].ItemID)
	}
	// list results are summaries — content stripped, but guid present.
	if all[0].Content != "" {
		t.Fatalf("list item should not carry content, got %q", all[0].Content)
	}
	if all[0].GUID == "" || all[0].GUID != syntheticID("https://a/feed", "a2") {
		t.Fatalf("list item guid = %q, want synthetic id", all[0].GUID)
	}
	only := recentItems(mcpSampleChanges(), 50, "https://b/feed")
	if len(only) != 1 || only[0].ItemID != "b1" {
		t.Fatalf("feed filter failed: %+v", only)
	}
}

func TestFindItem_ReturnsFullContent(t *testing.T) {
	it, ok := findItem(mcpSampleChanges(), "https://a/feed", "a1")
	if !ok {
		t.Fatal("expected to find a1")
	}
	if it.Content != "hello world" || it.Title != "First Alpha" {
		t.Fatalf("get_item should carry full content: %+v", it)
	}
	if _, ok := findItem(mcpSampleChanges(), "https://a/feed", "nope"); ok {
		t.Fatal("missing item should report not found")
	}
}

func TestSearchItems_QueryCaseInsensitiveAcrossFields(t *testing.T) {
	// "gopher" appears in Alpha-a2 summary/content and Beta-b1 content (uppercase).
	got, err := searchItems(mcpSampleChanges(), "gopher", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 matches for gopher, got %d (%+v)", len(got), got)
	}
}

func TestSearchItems_SinceFilter(t *testing.T) {
	since := time.Unix(1600, 0).UTC().Format(time.RFC3339)
	got, err := searchItems(mcpSampleChanges(), "", since)
	if err != nil {
		t.Fatal(err)
	}
	// Only a2 (published 2000) is at/after 1600.
	if len(got) != 1 || got[0].ItemID != "a2" {
		t.Fatalf("since filter wrong: %+v", got)
	}
	if _, err := searchItems(mcpSampleChanges(), "", "not-a-time"); err == nil {
		t.Fatal("expected error for invalid since")
	}
}
