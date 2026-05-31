package feed

import (
	"encoding/xml"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/feeds"

	"github.com/iambod/rss2msg/internal/model"
)

const summaryTruncate = 512

// FeedMeta is the feed-level metadata sourced from config.
type FeedMeta struct {
	Title       string
	Link        string // website (rel=alternate / channel link)
	Description string
	SelfRSS     string // public URL of the rss endpoint
	SelfAtom    string // public URL of the atom endpoint (Atom rel=self)
}

func buildFeed(m FeedMeta, changes []model.Change) *feeds.Feed {
	f := &feeds.Feed{
		Title:       m.Title,
		Link:        &feeds.Link{Href: m.Link, Rel: "alternate"},
		Description: m.Description,
	}
	for _, c := range changes {
		if c.DetectedAt.After(f.Updated) {
			f.Updated = c.DetectedAt
		}
		f.Items = append(f.Items, buildItem(c))
	}
	// Atom requires atom:updated to be a valid RFC3339 date-time; an empty
	// window would otherwise render <updated></updated>. Fall back to now.
	if f.Updated.IsZero() {
		f.Updated = time.Now()
	}
	return f
}

func buildItem(c model.Change) *feeds.Item {
	title := c.Title
	if title == "" {
		title = "(untitled)"
	}
	desc := c.Summary
	if desc == "" && c.Content != "" {
		desc = truncate(c.Content, summaryTruncate)
	}
	created := c.DetectedAt
	if c.PublishedAt != nil {
		created = *c.PublishedAt
	}
	updated := created
	if c.UpdatedAt != nil {
		updated = *c.UpdatedAt
	}
	item := &feeds.Item{
		Title:       title,
		Id:          syntheticID(c.FeedURL, c.ItemID),
		IsPermaLink: "false",
		Description: desc,
		Content:     c.Content,
		Created:     created,
		Updated:     updated,
	}
	if c.Link != "" {
		item.Link = &feeds.Link{Href: c.Link, Rel: "alternate"}
	}
	if len(c.Authors) > 0 {
		item.Author = &feeds.Author{Name: c.Authors[0]}
	}
	return item
}

// truncate cuts s to at most max bytes without splitting a UTF-8 rune (a byte
// split would corrupt multibyte content to U+FFFD when XML-encoded).
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	for max > 0 && !utf8.RuneStart(s[max]) {
		max--
	}
	return s[:max]
}

// escapeXML escapes a string for safe inclusion in an XML attribute value.
func escapeXML(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// ToRSS renders RSS 2.0. gorilla sets IsPermaLink="false" on the <guid>
// element natively via feeds.Item.IsPermaLink, so no string patching is needed.
func ToRSS(m FeedMeta, changes []model.Change) (string, error) {
	return buildFeed(m, changes).ToRss()
}

// ToAtom renders Atom 1.0 and injects the rel=self link as a child of <feed>.
// gorilla's AtomFeed.Link is a single *AtomLink so it can only hold one link;
// injecting via string manipulation is the correct approach for adding rel=self.
// The raw output starts with <?xml ...?><feed ...> on one line; we locate the
// end of the <feed ...> opening tag and insert the self link immediately after.
func ToAtom(m FeedMeta, changes []model.Change) (string, error) {
	out, err := buildFeed(m, changes).ToAtom()
	if err != nil {
		return "", err
	}
	if m.SelfAtom == "" {
		return out, nil
	}
	// Find the <feed opening tag, then find its closing >.
	// We must inject after the <feed...> tag, not after <?xml...?>.
	feedTagStart := strings.Index(out, "<feed")
	if feedTagStart == -1 {
		return out, nil
	}
	feedTagEnd := strings.Index(out[feedTagStart:], ">")
	if feedTagEnd == -1 {
		return out, nil
	}
	insertAt := feedTagStart + feedTagEnd + 1
	selfLink := `<link href="` + escapeXML(m.SelfAtom) + `" rel="self"></link>`
	out = out[:insertAt] + "\n  " + selfLink + out[insertAt:]
	return out, nil
}
