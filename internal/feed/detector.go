package feed

import (
	"context"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/state"
)

type Detector struct{}

func NewDetector() *Detector { return &Detector{} }

// Detect compares each parsed item against the state store and returns the
// resulting Change list. It does NOT commit state — that happens after
// successful publication.
func (d *Detector) Detect(ctx context.Context, feedURL string, f *gofeed.Feed, st state.Store, detectedAt time.Time) ([]model.Change, error) {
	if f == nil {
		return nil, nil
	}
	out := make([]model.Change, 0, len(f.Items))
	for _, it := range f.Items {
		change, ok, err := classify(ctx, feedURL, f.Title, it, st, detectedAt)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, change)
		}
	}
	return out, nil
}

func classify(ctx context.Context, feedURL, feedTitle string, it *gofeed.Item, st state.Store, detectedAt time.Time) (model.Change, bool, error) {
	pub := time.Time{}
	if it.PublishedParsed != nil {
		pub = *it.PublishedParsed
	}
	upd := time.Time{}
	if it.UpdatedParsed != nil {
		upd = *it.UpdatedParsed
	}

	id := model.IdentityKey(it.GUID, it.Link, it.Title, pub)
	body := strings.TrimSpace(it.Content)
	if body == "" {
		body = strings.TrimSpace(it.Description)
	}
	author := ""
	if it.Author != nil {
		author = it.Author.Name
	}
	hash := model.ContentHash(it.Title, it.Link, body, author, upd)

	existing, found, err := st.GetItem(ctx, feedURL, id)
	if err != nil {
		return model.Change{}, false, err
	}
	if found && existing.ContentHash == hash {
		return model.Change{}, false, nil
	}

	kind := model.ChangeNew
	if found {
		kind = model.ChangeUpdated
	}

	change := model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       feedURL,
		FeedTitle:     feedTitle,
		ItemID:        id,
		Kind:          kind,
		Title:         it.Title,
		Link:          it.Link,
		Authors:       collectAuthors(it),
		Summary:       it.Description,
		Content:       it.Content,
		Categories:    it.Categories,
		PublishedAt:   it.PublishedParsed,
		UpdatedAt:     it.UpdatedParsed,
		ContentHash:   hash,
		DetectedAt:    detectedAt,
	}
	return change, true, nil
}

func collectAuthors(it *gofeed.Item) []string {
	if len(it.Authors) > 0 {
		out := make([]string, 0, len(it.Authors))
		for _, a := range it.Authors {
			if a != nil && a.Name != "" {
				out = append(out, a.Name)
			}
		}
		return out
	}
	if it.Author != nil && it.Author.Name != "" {
		return []string{it.Author.Name}
	}
	return nil
}
