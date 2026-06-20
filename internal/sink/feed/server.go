package feed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type handlerConfig struct {
	store           Store
	meta            FeedMeta
	maxItems        int
	rssPath         string
	atomPath        string
	renderCacheTTL  time.Duration
	cacheControlTTL time.Duration
	rssAuth         *SurfaceAuth
	atomAuth        *SurfaceAuth
	proxy           proxyConfig
	logger          zerolog.Logger
	startedAt       time.Time
}

type handler struct {
	cfg   handlerConfig
	mu    sync.Mutex
	cache map[string]cachedDoc // keyed by path
	instr *instruments
}

type cachedDoc struct {
	body     []byte
	etag     string
	modified time.Time
	ct       string
	at       time.Time
}

func newHandler(cfg handlerConfig) *handler {
	if cfg.startedAt.IsZero() {
		cfg.startedAt = time.Unix(0, 0).UTC()
	}
	return &handler{cfg: cfg, cache: map[string]cachedDoc{}}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path != h.cfg.rssPath && path != h.cfg.atomPath {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	a := h.authFor(path)
	name, ok := authenticate(a, r)
	if !ok {
		reason := authFailReason(a, r)
		h.recordAuthFailure(r.Context(), h.surfaceName(path), reason)
		h.cfg.logger.Warn().
			Str("sink_surface", h.surfaceName(path)).
			Str("reason", reason).
			Str("client_ip", h.cfg.proxy.clientIP(r)).
			Msg("feed sink auth failure")
		writeAuthChallenge(a, w)
		return
	}
	if a != nil {
		h.recordAuthSuccess(r.Context(), h.surfaceName(path), name)
	}
	if h.instr != nil {
		h.instr.requests.Add(r.Context(), 1)
	}
	selfURL := h.cfg.proxy.selfURL(r, path)
	doc, err := h.document(r.Context(), path, selfURL)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h.setCacheHeaders(w, a)
	w.Header().Set("Content-Type", doc.ct)
	w.Header().Set("ETag", doc.etag)
	w.Header().Set("Last-Modified", doc.modified.UTC().Format(http.TimeFormat))
	if matchNotModified(r, doc) {
		if h.instr != nil {
			h.instr.notMod.Add(r.Context(), 1)
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(doc.body)
}

func matchNotModified(r *http.Request, doc cachedDoc) bool {
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		return inm == doc.etag
	}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !doc.modified.After(t) {
			return true
		}
	}
	return false
}

// document renders (and caches) the self-less body for path, then assembles the
// per-request response: the self link is injected and the ETag is computed over
// the final body so two hosts get distinct ETags from one cached render.
func (h *handler) document(ctx context.Context, path, selfURL string) (cachedDoc, error) {
	raw, err := h.rawBody(ctx, path)
	if err != nil {
		return cachedDoc{}, err
	}
	var body string
	if path == h.cfg.rssPath {
		body = injectRSSSelf(string(raw.body), selfURL)
	} else {
		body = injectAtomSelf(string(raw.body), selfURL)
	}
	sum := sha256.Sum256([]byte(body))
	return cachedDoc{
		body:     []byte(body),
		ct:       raw.ct,
		etag:     `"` + hex.EncodeToString(sum[:]) + `"`,
		modified: raw.modified,
		at:       raw.at,
	}, nil
}

// rawBody returns the cached, self-less rendered body for path, rendering and
// caching it on miss. The cache key is the path; the body is host-independent.
func (h *handler) rawBody(ctx context.Context, path string) (cachedDoc, error) {
	if h.cfg.renderCacheTTL > 0 {
		h.mu.Lock()
		if d, ok := h.cache[path]; ok && time.Since(d.at) < h.cfg.renderCacheTTL {
			h.mu.Unlock()
			return d, nil
		}
		h.mu.Unlock()
	}
	changes, err := h.cfg.store.Recent(ctx, h.cfg.maxItems)
	if err != nil {
		return cachedDoc{}, err
	}
	var body, ct string
	if path == h.cfg.rssPath {
		ct = "application/rss+xml"
		body, err = ToRSS(h.cfg.meta, changes)
	} else {
		ct = "application/atom+xml"
		body, err = ToAtom(h.cfg.meta, changes)
	}
	if err != nil {
		return cachedDoc{}, err
	}
	doc := cachedDoc{body: []byte(body), ct: ct, modified: h.lastModified(changes), at: time.Now()}
	if h.cfg.renderCacheTTL > 0 {
		h.mu.Lock()
		h.cache[path] = doc
		h.mu.Unlock()
	}
	return doc, nil
}

func (h *handler) lastModified(changes []model.Change) time.Time {
	mod := h.cfg.startedAt
	for _, c := range changes {
		if c.DetectedAt.After(mod) {
			mod = c.DetectedAt
		}
	}
	return mod
}

func (h *handler) setCacheHeaders(w http.ResponseWriter, a *SurfaceAuth) {
	scope := "public"
	if a != nil {
		scope = "private"
	}
	if h.cfg.cacheControlTTL > 0 {
		w.Header().Set("Cache-Control", scope+", max-age="+strconv.Itoa(int(h.cfg.cacheControlTTL.Seconds())))
	} else {
		w.Header().Set("Cache-Control", scope+", no-cache")
	}
}

func (h *handler) authFor(path string) *SurfaceAuth {
	if path == h.cfg.atomPath {
		return h.cfg.atomAuth
	}
	return h.cfg.rssAuth
}

func (h *handler) surfaceName(path string) string {
	if path == h.cfg.atomPath {
		return "atom"
	}
	return "rss"
}

func (h *handler) recordAuthSuccess(ctx context.Context, surface, cred string) {
	if h.instr == nil {
		return
	}
	h.instr.authSuccess.Add(ctx, 1, metric.WithAttributes(
		attribute.String("surface", surface),
		attribute.String("credential", cred),
	))
}

func (h *handler) recordAuthFailure(ctx context.Context, surface, reason string) {
	if h.instr == nil {
		return
	}
	h.instr.authFailure.Add(ctx, 1, metric.WithAttributes(
		attribute.String("surface", surface),
		attribute.String("reason", reason),
	))
}
