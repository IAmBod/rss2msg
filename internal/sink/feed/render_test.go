package feed

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mmcdole/gofeed"
	"github.com/iambod/rss2msg/internal/model"
)

func TestRender_AtomSelfLinkURLIsEscaped(t *testing.T) {
	m := meta()
	m.SelfAtom = "https://feeds.example/atom?a=1&b=2" // '&' must be escaped or XML breaks
	xml, err := ToAtom(m, sampleChanges())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gofeed.NewParser().ParseString(xml); err != nil {
		t.Fatalf("atom with '&' in self url must still parse: %v", err)
	}
	if !strings.Contains(xml, "a=1&amp;b=2") {
		t.Fatalf("ampersand in self url must be escaped; got:\n%s", xml)
	}
}

func TestRender_TruncateDoesNotSplitRunes(t *testing.T) {
	// 200 euro signs (3 bytes each = 600 bytes) > summaryTruncate (512).
	// A byte-split would yield invalid UTF-8 (U+FFFD corruption in XML).
	long := strings.Repeat("€", 200)
	c := model.Change{FeedURL: "f", ItemID: "1", Title: "t", Content: long, DetectedAt: time.Unix(1, 0)}
	xmlOut, err := ToRSS(meta(), []model.Change{c})
	if err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(xmlOut) || strings.ContainsRune(xmlOut, '�') {
		t.Fatal("truncated multibyte content must remain valid UTF-8 (no U+FFFD)")
	}
	if !utf8.ValidString(truncate(long, summaryTruncate)) {
		t.Fatal("truncate produced invalid UTF-8")
	}
}

func TestRender_EmptyChangesProducesValidAtom(t *testing.T) {
	xmlOut, err := ToAtom(meta(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gofeed.NewParser().ParseString(xmlOut); err != nil {
		t.Fatalf("empty atom must parse: %v", err)
	}
	if strings.Contains(xmlOut, "<updated></updated>") {
		t.Fatal("empty feed must not emit an empty <updated> (invalid per RFC 4287)")
	}
}

func sampleChanges() []model.Change {
	pub := time.Unix(5000, 0).UTC()
	return []model.Change{
		{FeedURL: "https://a.com/feed", ItemID: "1", Title: "Hello", Link: "https://a.com/1",
			Summary: "sum", Authors: []string{"Ann"}, PublishedAt: &pub, DetectedAt: pub.Add(time.Minute)},
		{FeedURL: "https://b.com/feed", ItemID: "1", Title: "World", Link: "https://b.com/1",
			DetectedAt: pub.Add(2 * time.Minute)},
	}
}

func meta() FeedMeta {
	return FeedMeta{Title: "changes", Link: "https://site.example/", Description: "d", SelfRSS: "https://feeds.example/rss", SelfAtom: "https://feeds.example/atom"}
}

func TestRender_RSSRoundTripAndUniqueIDs(t *testing.T) {
	xml, err := ToRSS(meta(), sampleChanges())
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := gofeed.NewParser().ParseString(xml)
	if err != nil {
		t.Fatalf("rss did not parse: %v", err)
	}
	if len(parsed.Items) != 2 {
		t.Fatalf("want 2 items, got %d", len(parsed.Items))
	}
	if parsed.Items[0].GUID == parsed.Items[1].GUID {
		t.Fatal("guids must be globally unique across feeds")
	}
	if !strings.Contains(xml, `isPermaLink="false"`) {
		t.Fatal("synthetic guid must be isPermaLink=false")
	}
}

func TestRender_AtomHasSelfLink(t *testing.T) {
	xml, err := ToAtom(meta(), sampleChanges())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gofeed.NewParser().ParseString(xml); err != nil {
		t.Fatalf("atom did not parse: %v", err)
	}
	if !strings.Contains(xml, `rel="self"`) || !strings.Contains(xml, "https://feeds.example/atom") {
		t.Fatalf("atom must contain rel=self with the self url; got:\n%s", xml)
	}
}
