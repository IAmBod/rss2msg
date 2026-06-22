// Package admin serves the opt-in admin HTTP API: JSON introspection and safe
// maintenance actions for a running serve daemon. Modeled on internal/health.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/health"
	"github.com/iambod/rss2msg/internal/httpauth"
	"github.com/iambod/rss2msg/internal/state"
	"github.com/rs/zerolog"
)

type BuildInfo struct{ Version, Commit, Date, InstanceID string }

type FeedLister interface {
	Desired(ctx context.Context) ([]config.FeedConfig, error)
}

type StateInspector interface {
	GetFeedMeta(ctx context.Context, feedURL string) (state.FeedMeta, bool, error)
	PruneItemsBefore(ctx context.Context, cutoff time.Time) (int64, error)
	PruneFeedMetaBefore(ctx context.Context, cutoff time.Time) (int64, error)
	Ping(ctx context.Context) error
}

type MembershipInspector interface {
	Self() string
	Members() []string
}

type Deps struct {
	Build     BuildInfo
	StartedAt time.Time
	Feeds     FeedLister
	State     StateInspector
	Members   MembershipInspector
	Checks    []health.Check
	Reconcile func()
	PollNow   func(feedURL string) bool
	Self      string
	ItemTTL   time.Duration
}

type Server struct {
	cfg    config.AdminConfig
	auth   *httpauth.Auth
	deps   Deps
	log    zerolog.Logger
	server *http.Server
}

func New(cfg config.AdminConfig, auth *httpauth.Auth, deps Deps, log zerolog.Logger) *Server {
	return &Server{cfg: cfg, auth: auth, deps: deps, log: log}
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("GET /v1/feeds", s.handleFeeds)
	mux.HandleFunc("GET /v1/feeds/{id}", s.handleFeedByID)
	mux.HandleFunc("POST /v1/feeds/{id}/poll", s.handleFeedPoll)
	mux.HandleFunc("POST /v1/feeds/reconcile", s.handleReconcile)
	mux.HandleFunc("GET /v1/members", s.handleMembers)
	mux.HandleFunc("POST /v1/state/prune", s.handlePrune)
	return s.withCORS(s.withAuth(mux))
}

// withAuth enforces application auth unless the configured Auth is empty.
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions { // CORS preflight handled in withCORS
			next.ServeHTTP(w, r)
			return
		}
		if s.auth.Empty() {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := s.auth.Authenticate(r); !ok {
			s.auth.WriteChallenge(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withCORS adds CORS headers for allowed origins and answers preflight. When no
// origins are configured it is a pass-through.
func (s *Server) withCORS(next http.Handler) http.Handler {
	allowed := map[string]bool{}
	wildcard := false
	for _, o := range s.cfg.CORS.AllowedOrigins {
		if o == "*" {
			wildcard = true
		}
		allowed[o] = true
	}
	if len(allowed) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (wildcard || allowed[origin]) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	self := s.deps.Self
	members := 1
	if s.deps.Members != nil {
		self = s.deps.Members.Self()
		members = len(s.deps.Members.Members())
	}
	feedCount := 0
	if s.deps.Feeds != nil {
		if feeds, err := s.deps.Feeds.Desired(r.Context()); err == nil {
			feedCount = len(feeds)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"instance_id":        self,
		"version":            s.deps.Build.Version,
		"commit":             s.deps.Build.Commit,
		"date":               s.deps.Build.Date,
		"uptime_seconds":     int(time.Since(s.deps.StartedAt).Seconds()),
		"assignment_enabled": s.deps.Members != nil,
		"feed_count":         feedCount,
		"member_count":       members,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// Start binds the listener and serves in the background. No-op when disabled.
func (s *Server) Start() error {
	if !s.cfg.Enabled {
		return nil
	}
	s.server = &http.Server{Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("admin listen on %q: %w", s.cfg.Listen, err)
	}
	go func() {
		if err := s.serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error().Err(err).Msg("admin API server stopped unexpectedly")
		}
	}()
	s.log.Info().Str("listen", s.cfg.Listen).Bool("tls", s.cfg.TLS.Enabled).Msg("admin API started")
	return nil
}

// serve runs the listener; TLS/mTLS is layered in Task 14.
func (s *Server) serve(ln net.Listener) error {
	return s.server.Serve(ln)
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}
