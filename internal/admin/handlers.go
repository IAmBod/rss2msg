package admin

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/iambod/rss2msg/internal/assign"
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

func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	self, members := s.selfAndMembers()
	if len(members) == 0 {
		members = []string{self}
	}
	ownership := map[string]string{}
	if s.deps.Feeds != nil {
		if feeds, err := s.deps.Feeds.Desired(r.Context()); err == nil {
			for _, fc := range feeds {
				if owner, ok := assign.Owner(fc.URL, members); ok {
					ownership[fc.URL] = owner
				} else {
					ownership[fc.URL] = self
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"self": self, "members": members, "ownership": ownership})
}

func (s *Server) handleFeedPoll(w http.ResponseWriter, r *http.Request) {
	feedURL, err := url.PathUnescape(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid feed id")
		return
	}
	// Validate the feed is in the desired set.
	feeds, err := s.deps.Feeds.Desired(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list feeds: "+err.Error())
		return
	}
	known := false
	for _, fc := range feeds {
		if fc.URL == feedURL {
			known = true
			break
		}
	}
	if !known {
		writeErr(w, http.StatusNotFound, "feed not found")
		return
	}
	running := false
	if s.deps.PollNow != nil {
		running = s.deps.PollNow(feedURL)
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "poll triggered", "running": running})
}

func (s *Server) handleReconcile(w http.ResponseWriter, _ *http.Request) {
	if s.deps.Reconcile == nil {
		writeErr(w, http.StatusServiceUnavailable, "reconcile unavailable")
		return
	}
	s.deps.Reconcile()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "reconcile triggered"})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{}
	ok := true
	for _, c := range s.deps.Checks {
		if err := c.Fn(r.Context()); err != nil {
			checks[c.Name] = err.Error()
			ok = false
		} else {
			checks[c.Name] = "ok"
		}
	}
	code := http.StatusOK
	if !ok {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{"ok": ok, "checks": checks})
}
