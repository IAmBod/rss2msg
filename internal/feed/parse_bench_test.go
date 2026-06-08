package feed

import (
	"fmt"
	"strings"
	"testing"

	"github.com/mmcdole/gofeed"
)

// benchRSS builds a syntactically valid RSS 2.0 document with n items. Parsing
// is the CPU-bound work the fetcher performs on every successful (non-304) poll,
// so a regression here scales with feed size across every configured feed.
func benchRSS(n int) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` +
		`<rss version="2.0"><channel>` +
		`<title>Bench Feed</title>` +
		`<link>https://example.test</link>` +
		`<description>benchmark feed</description>`)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b,
			`<item><title>Item %d</title>`+
				`<link>https://example.test/item/%d</link>`+
				`<guid>guid-%d</guid>`+
				`<pubDate>Mon, 01 May 2026 0%d:00:00 GMT</pubDate>`+
				`<description>Body content for item %d with some words to parse.</description>`+
				`</item>`,
			i, i, i, i%10, i)
	}
	b.WriteString(`</channel></rss>`)
	return b.String()
}

// BenchmarkParseRSS measures gofeed parsing of a 50-item feed, the same call the
// fetcher makes on a 2xx response body.
func BenchmarkParseRSS(b *testing.B) {
	doc := benchRSS(50)
	parser := gofeed.NewParser()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := parser.Parse(strings.NewReader(doc)); err != nil {
			b.Fatal(err)
		}
	}
}
