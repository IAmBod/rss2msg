// Package health serves Kubernetes-style HTTP health probe endpoints
// (liveness, readiness, startup) for the long-lived serve daemon.
package health

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/iambod/rss2msg/internal/config"
)

// checkTimeout bounds how long a single readiness dependency check may run.
const checkTimeout = 2 * time.Second

// Pinger is satisfied by dependencies that can report reachability. It lets the
// readiness probe verify backends (state store, coordinator) without coupling to
// their concrete types.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Check is a named readiness dependency probe.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Server hosts the probe endpoints. Liveness reports process health, startup
// flips to ready once boot completes, and readiness additionally runs the
// dependency checks and reports draining during shutdown.
type Server struct {
	cfg      config.HealthConfig
	log      zerolog.Logger
	checks   []Check
	timeout  time.Duration
	server   *http.Server
	started  atomic.Bool
	draining atomic.Bool
}

// New builds a health Server. The probe listener is not started until Start is
// called.
func New(cfg config.HealthConfig, log zerolog.Logger, checks ...Check) *Server {
	return &Server{
		cfg:     cfg,
		log:     log,
		checks:  checks,
		timeout: checkTimeout,
	}
}

// MarkStarted signals that boot has completed; startup and readiness probes
// begin returning 200.
func (s *Server) MarkStarted() { s.started.Store(true) }

// SetDraining signals that shutdown has begun; readiness returns 503 "draining".
func (s *Server) SetDraining() { s.draining.Store(true) }

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(s.cfg.LivenessPath, s.handleLive)
	mux.HandleFunc(s.cfg.ReadinessPath, s.handleReady)
	mux.HandleFunc(s.cfg.StartupPath, s.handleStartup)
	return mux
}

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeStatus(w, http.StatusOK, "ok")
}

func (s *Server) handleStartup(w http.ResponseWriter, _ *http.Request) {
	if !s.started.Load() {
		writeStatus(w, http.StatusServiceUnavailable, "starting")
		return
	}
	writeStatus(w, http.StatusOK, "ok")
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !s.started.Load() {
		writeStatus(w, http.StatusServiceUnavailable, "starting")
		return
	}
	if s.draining.Load() {
		writeStatus(w, http.StatusServiceUnavailable, "draining")
		return
	}
	for _, c := range s.checks {
		ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
		err := c.Fn(ctx)
		cancel()
		if err != nil {
			s.log.Warn().Err(err).Str("check", c.Name).Msg("readiness check failed")
			writeStatus(w, http.StatusServiceUnavailable, fmt.Sprintf("%s: %s", c.Name, err))
			return
		}
	}
	writeStatus(w, http.StatusOK, "ok")
}

func writeStatus(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(msg + "\n"))
}

// Start binds the probe listener and serves in the background. It is a no-op
// when health is disabled.
func (s *Server) Start() error {
	if !s.cfg.Enabled {
		return nil
	}
	srv := &http.Server{
		Addr:              s.cfg.Listen,
		Handler:           s.handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("health listen on %q: %w", s.cfg.Listen, err)
	}
	s.server = srv
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error().Err(err).Msg("health probe server stopped unexpectedly")
		}
	}()
	s.log.Info().Str("listen", s.cfg.Listen).Msg("health probe endpoints started")
	return nil
}

// Shutdown gracefully stops the probe listener. Safe to call when the server was
// never started.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}
