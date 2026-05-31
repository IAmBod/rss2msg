package feed

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

type AuthConfig struct {
	BasicUser, BasicPass string
	BearerToken          string
}

func (h *handler) authOK(r *http.Request) bool {
	a := h.cfg.auth
	if a == nil {
		return true
	}
	if a.BearerToken != "" {
		const p = "Bearer "
		got := r.Header.Get("Authorization")
		return strings.HasPrefix(got, p) && ctEqual(got[len(p):], a.BearerToken)
	}
	u, pw, ok := r.BasicAuth()
	// Evaluate both comparisons unconditionally (no && short-circuit) so the
	// response time doesn't reveal whether the username alone matched.
	userOK := ctEqual(u, a.BasicUser)
	passOK := ctEqual(pw, a.BasicPass)
	return ok && userOK && passOK
}

func ctEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (h *handler) writeUnauthorized(w http.ResponseWriter) {
	if h.cfg.auth != nil && h.cfg.auth.BearerToken == "" {
		w.Header().Set("WWW-Authenticate", `Basic realm="rss2msg"`)
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}
