package feed

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/metric"
)

// mcpAuthMiddleware gates the MCP route with the sink's auth (same as RSS/Atom)
// and counts requests when a meter is configured.
func mcpAuthMiddleware(a *AuthConfig, count metric.Int64Counter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(a, r) {
			writeAuthChallenge(a, w)
			return
		}
		if count != nil {
			count.Add(r.Context(), 1)
		}
		next.ServeHTTP(w, r)
	})
}

// This file exposes the feed sink's rolling window over MCP — a third rendering
// of the same changes the sink already serves as RSS/Atom. All tools are
// read-only and backed by Store.Recent(); no new storage is introduced.

// mcpFeed summarizes one feed present in the window.
type mcpFeed struct {
	FeedURL   string `json:"feed_url" jsonschema:"the feed's source URL"`
	FeedTitle string `json:"feed_title,omitempty" jsonschema:"the feed title, if known"`
	ItemCount int    `json:"item_count" jsonschema:"items from this feed currently in the window"`
}

// mcpItem is one change rendered for MCP. List/search tools return summaries
// (Content stripped); get_item returns the full item.
type mcpItem struct {
	ItemID      string   `json:"item_id" jsonschema:"the feed item's native id — the contract key for get_item"`
	GUID        string   `json:"guid" jsonschema:"synthetic rss2msg URN, matching the RSS/Atom output entry id"`
	FeedURL     string   `json:"feed_url"`
	FeedTitle   string   `json:"feed_title,omitempty"`
	Title       string   `json:"title,omitempty"`
	Link        string   `json:"link,omitempty"`
	Authors     []string `json:"authors,omitempty"`
	Categories  []string `json:"categories,omitempty"`
	Summary     string   `json:"summary,omitempty"`
	Content     string   `json:"content,omitempty"`
	PublishedAt string   `json:"published_at,omitempty" jsonschema:"RFC3339 publish time, if known"`
	UpdatedAt   string   `json:"updated_at,omitempty" jsonschema:"RFC3339 update time, if known"`
}

func toItem(c model.Change) mcpItem {
	it := mcpItem{
		ItemID: c.ItemID, GUID: syntheticID(c.FeedURL, c.ItemID),
		FeedURL: c.FeedURL, FeedTitle: c.FeedTitle,
		Title: c.Title, Link: c.Link,
		Authors: c.Authors, Categories: c.Categories,
		Summary: c.Summary, Content: c.Content,
	}
	if c.PublishedAt != nil {
		it.PublishedAt = c.PublishedAt.UTC().Format(time.RFC3339)
	}
	if c.UpdatedAt != nil {
		it.UpdatedAt = c.UpdatedAt.UTC().Format(time.RFC3339)
	}
	return it
}

func toSummary(c model.Change) mcpItem {
	it := toItem(c)
	it.Content = ""
	return it
}

// feedsFrom collapses the window into per-feed summaries, preserving first-seen
// (newest-first) order.
func feedsFrom(changes []model.Change) []mcpFeed {
	order := make([]string, 0)
	idx := map[string]*mcpFeed{}
	for _, c := range changes {
		f, ok := idx[c.FeedURL]
		if !ok {
			f = &mcpFeed{FeedURL: c.FeedURL, FeedTitle: c.FeedTitle}
			idx[c.FeedURL] = f
			order = append(order, c.FeedURL)
		}
		f.ItemCount++
		if f.FeedTitle == "" && c.FeedTitle != "" {
			f.FeedTitle = c.FeedTitle
		}
	}
	out := make([]mcpFeed, 0, len(order))
	for _, u := range order {
		out = append(out, *idx[u])
	}
	return out
}

// recentItems returns up to limit summaries (newest-first), optionally filtered
// to a single feed.
func recentItems(changes []model.Change, limit int, feedURL string) []mcpItem {
	if limit <= 0 {
		limit = 50
	}
	out := make([]mcpItem, 0, limit)
	for _, c := range changes {
		if feedURL != "" && c.FeedURL != feedURL {
			continue
		}
		out = append(out, toSummary(c))
		if len(out) >= limit {
			break
		}
	}
	return out
}

func findItem(changes []model.Change, feedURL, itemID string) (*mcpItem, bool) {
	for _, c := range changes {
		if c.FeedURL == feedURL && c.ItemID == itemID {
			it := toItem(c)
			return &it, true
		}
	}
	return nil, false
}

// searchItems returns summaries matching a case-insensitive substring over
// title/summary/content, optionally restricted to items at or after since
// (published time, falling back to detected time).
func searchItems(changes []model.Change, query, since string) ([]mcpItem, error) {
	var sinceT time.Time
	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return nil, fmt.Errorf("invalid since %q: %w", since, err)
		}
		sinceT = t
	}
	q := strings.ToLower(query)
	out := make([]mcpItem, 0)
	for _, c := range changes {
		if !sinceT.IsZero() {
			ts := c.DetectedAt
			if c.PublishedAt != nil {
				ts = *c.PublishedAt
			}
			if ts.Before(sinceT) {
				continue
			}
		}
		if q != "" {
			hay := strings.ToLower(c.Title + "\n" + c.Summary + "\n" + c.Content)
			if !strings.Contains(hay, q) {
				continue
			}
		}
		out = append(out, toSummary(c))
	}
	return out, nil
}

// --- MCP tool I/O schemas ---

type listFeedsIn struct{}
type listFeedsOut struct {
	Feeds []mcpFeed `json:"feeds"`
}

type listRecentIn struct {
	Limit   int    `json:"limit,omitempty" jsonschema:"max items to return; defaults to and is capped at the sink window size"`
	FeedURL string `json:"feed_url,omitempty" jsonschema:"restrict results to this feed URL"`
}

type itemsOut struct {
	Items []mcpItem `json:"items"`
}

type getItemIn struct {
	FeedURL string `json:"feed_url" jsonschema:"the feed's source URL"`
	ItemID  string `json:"item_id" jsonschema:"the feed item's native id"`
}
type getItemOut struct {
	Found bool     `json:"found"`
	Item  *mcpItem `json:"item,omitempty"`
}

type searchIn struct {
	Query string `json:"query" jsonschema:"case-insensitive substring matched against title, summary, and content"`
	Since string `json:"since,omitempty" jsonschema:"RFC3339; only items at or after this time"`
}

// buildMCPServer wires the four read-only content tools over the sink's store.
func buildMCPServer(store Store, maxItems int, name string) *mcp.Server {
	if maxItems <= 0 {
		maxItems = 50
	}
	s := mcp.NewServer(&mcp.Implementation{Name: "rss2msg-feed-sink:" + name, Version: "1"}, nil)

	recent := func(ctx context.Context) ([]model.Change, error) {
		return store.Recent(ctx, maxItems)
	}

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_feeds",
		Description: "List the feeds present in the rolling window, with item counts.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ listFeedsIn) (*mcp.CallToolResult, listFeedsOut, error) {
		ch, err := recent(ctx)
		if err != nil {
			return nil, listFeedsOut{}, err
		}
		return nil, listFeedsOut{Feeds: feedsFrom(ch)}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_recent_items",
		Description: "List recent items (newest first) in the rolling window, optionally filtered to one feed.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in listRecentIn) (*mcp.CallToolResult, itemsOut, error) {
		ch, err := recent(ctx)
		if err != nil {
			return nil, itemsOut{}, err
		}
		lim := in.Limit
		if lim <= 0 || lim > maxItems {
			lim = maxItems
		}
		return nil, itemsOut{Items: recentItems(ch, lim, in.FeedURL)}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_item",
		Description: "Fetch a single item by feed URL and item id, including full content.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in getItemIn) (*mcp.CallToolResult, getItemOut, error) {
		ch, err := recent(ctx)
		if err != nil {
			return nil, getItemOut{}, err
		}
		it, ok := findItem(ch, in.FeedURL, in.ItemID)
		return nil, getItemOut{Found: ok, Item: it}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "search_items",
		Description: "Search items in the rolling window by substring and/or a since time.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in searchIn) (*mcp.CallToolResult, itemsOut, error) {
		ch, err := recent(ctx)
		if err != nil {
			return nil, itemsOut{}, err
		}
		items, err := searchItems(ch, in.Query, in.Since)
		if err != nil {
			return nil, itemsOut{}, err
		}
		return nil, itemsOut{Items: items}, nil
	})

	return s
}
