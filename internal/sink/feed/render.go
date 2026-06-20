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

// ToAtom renders Atom 1.0 without a rel=self link. The handler injects the
// per-request self link via injectAtomSelf so one cached body serves every host.
func ToAtom(m FeedMeta, changes []model.Change) (string, error) {
	return buildFeed(m, changes).ToAtom()
}

// injectAtomSelf inserts <link href=selfURL rel="self"> as a child of <feed>.
// gorilla's AtomFeed.Link holds a single link, so string injection is required.
// No-op when selfURL is empty or no <feed> tag is found.
func injectAtomSelf(atomBody, selfURL string) string {
	if selfURL == "" {
		return atomBody
	}
	feedTagStart := strings.Index(atomBody, "<feed")
	if feedTagStart == -1 {
		return atomBody
	}
	feedTagEnd := strings.Index(atomBody[feedTagStart:], ">")
	if feedTagEnd == -1 {
		return atomBody
	}
	insertAt := feedTagStart + feedTagEnd + 1
	link := `<link href="` + escapeXML(selfURL) + `" rel="self"></link>`
	return atomBody[:insertAt] + "\n  " + link + atomBody[insertAt:]
}

// injectRSSSelf adds the atom namespace to <rss> and an <atom:link rel="self">
// inside <channel>. No-op when selfURL is empty or the tags are not found.
func injectRSSSelf(rssBody, selfURL string) string {
	if selfURL == "" {
		return rssBody
	}
	const ns = ` xmlns:atom="http://www.w3.org/2005/Atom"`
	rssStart := strings.Index(rssBody, "<rss")
	if rssStart == -1 {
		return rssBody
	}
	rssEnd := strings.Index(rssBody[rssStart:], ">")
	if rssEnd == -1 {
		return rssBody
	}
	if !strings.Contains(rssBody, `xmlns:atom=`) {
		nsAt := rssStart + rssEnd
		rssBody = rssBody[:nsAt] + ns + rssBody[nsAt:]
	}

	chanTag := "<channel>"
	chanAt := strings.Index(rssBody, chanTag)
	if chanAt == -1 {
		return rssBody
	}
	insertAt := chanAt + len(chanTag)
	link := `<atom:link href="` + escapeXML(selfURL) + `" rel="self" type="application/rss+xml"></atom:link>`
	return rssBody[:insertAt] + "\n    " + link + rssBody[insertAt:]
}
