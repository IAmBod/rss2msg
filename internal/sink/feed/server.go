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
)

type handlerConfig struct {
	store           Store
	meta            FeedMeta
	maxItems        int
	rssPath         string
	atomPath        string
	renderCacheTTL  time.Duration
	cacheControlTTL time.Duration
	auth            *AuthConfig
	startedAt       time.Time
}

type handler struct {
	cfg   handlerConfig
	mu    sync.Mutex
	cache map[string]cachedDoc // keyed by path
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
	if !h.authOK(r) {
		h.writeUnauthorized(w)
		return
	}
	doc, err := h.document(r.Context(), path)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h.setCacheHeaders(w)
	w.Header().Set("Content-Type", doc.ct)
	w.Header().Set("ETag", doc.etag)
	w.Header().Set("Last-Modified", doc.modified.UTC().Format(http.TimeFormat))
	if matchNotModified(r, doc) {
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

func (h *handler) document(ctx context.Context, path string) (cachedDoc, error) {
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
	var body string
	var ct string
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
	sum := sha256.Sum256([]byte(body))
	doc := cachedDoc{
		body:     []byte(body),
		ct:       ct,
		etag:     `"` + hex.EncodeToString(sum[:]) + `"`,
		modified: h.lastModified(changes),
		at:       time.Now(),
	}
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

func (h *handler) setCacheHeaders(w http.ResponseWriter) {
	scope := "public"
	if h.cfg.auth != nil {
		scope = "private"
	}
	if h.cfg.cacheControlTTL > 0 {
		w.Header().Set("Cache-Control", scope+", max-age="+strconv.Itoa(int(h.cfg.cacheControlTTL.Seconds())))
	} else {
		w.Header().Set("Cache-Control", scope+", no-cache")
	}
}
