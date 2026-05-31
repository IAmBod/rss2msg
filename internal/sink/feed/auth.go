package feed

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

type authConfig struct {
	basicUser, basicPass string
	bearerToken          string
}

func (h *handler) authOK(r *http.Request) bool {
	a := h.cfg.auth
	if a == nil {
		return true
	}
	if a.bearerToken != "" {
		const p = "Bearer "
		got := r.Header.Get("Authorization")
		return strings.HasPrefix(got, p) && ctEqual(got[len(p):], a.bearerToken)
	}
	u, pw, ok := r.BasicAuth()
	// Evaluate both comparisons unconditionally (no && short-circuit) so the
	// response time doesn't reveal whether the username alone matched.
	userOK := ctEqual(u, a.basicUser)
	passOK := ctEqual(pw, a.basicPass)
	return ok && userOK && passOK
}

func ctEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (h *handler) writeUnauthorized(w http.ResponseWriter) {
	if h.cfg.auth != nil && h.cfg.auth.bearerToken == "" {
		w.Header().Set("WWW-Authenticate", `Basic realm="rss2msg"`)
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}
