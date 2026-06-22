package admin

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/iambod/rss2msg/internal/assign"
	"github.com/iambod/rss2msg/internal/config"
)

type feedView struct {
	URL             string  `json:"url"`
	IntervalSeconds float64 `json:"interval_seconds"`
	Owned           bool    `json:"owned"`
	ETag            *string `json:"etag"`
	LastModified    *string `json:"last_modified"`
}

func (s *Server) selfAndMembers() (self string, members []string) {
	if s.deps.Members != nil {
		return s.deps.Members.Self(), s.deps.Members.Members()
	}
	return s.deps.Self, nil
}

func (s *Server) ownedBy(self string, members []string, feedURL string) bool {
	if len(members) == 0 {
		return true // single instance owns everything
	}
	owner, ok := assign.Owner(feedURL, members)
	return ok && owner == self
}

func (s *Server) feedViews(ctx context.Context) ([]feedView, error) {
	feeds, err := s.deps.Feeds.Desired(ctx)
	if err != nil {
		return nil, err
	}
	self, members := s.selfAndMembers()
	out := make([]feedView, 0, len(feeds))
	for _, fc := range feeds {
		v := feedView{URL: fc.URL, IntervalSeconds: fc.Interval.Seconds(), Owned: s.ownedBy(self, members, fc.URL)}
		if s.deps.State != nil {
			if m, found, mErr := s.deps.State.GetFeedMeta(ctx, fc.URL); mErr == nil && found {
				if m.ETag != "" {
					etag := m.ETag
					v.ETag = &etag
				}
				if !m.LastModified.IsZero() {
					lm := m.LastModified.UTC().Format(time.RFC3339)
					v.LastModified = &lm
				}
			}
		}
		out = append(out, v)
	}
	return out, nil
}

func (s *Server) handleFeeds(w http.ResponseWriter, r *http.Request) {
	views, err := s.feedViews(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list feeds: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"feeds": views, "total": len(views)})
}

func (s *Server) handleFeedByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	feedURL, err := url.PathUnescape(id)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid feed id")
		return
	}
	views, err := s.feedViews(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list feeds: "+err.Error())
		return
	}
	for _, v := range views {
		if v.URL == feedURL {
			writeJSON(w, http.StatusOK, v)
			return
		}
	}
	writeErr(w, http.StatusNotFound, "feed not found")
}

var _ = config.FeedConfig{}
