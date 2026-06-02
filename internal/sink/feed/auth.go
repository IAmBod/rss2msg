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

// checkAuth reports whether the request satisfies the auth config (nil => open).
// Shared by the RSS/Atom handler and the MCP route so both gate identically.
func checkAuth(a *AuthConfig, r *http.Request) bool {
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

// writeAuthChallenge writes a 401, adding a Basic challenge when basic auth is in use.
func writeAuthChallenge(a *AuthConfig, w http.ResponseWriter) {
	if a != nil && a.BearerToken == "" {
		w.Header().Set("WWW-Authenticate", `Basic realm="rss2msg"`)
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

func ctEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (h *handler) authOK(r *http.Request) bool { return checkAuth(h.cfg.auth, r) }

func (h *handler) writeUnauthorized(w http.ResponseWriter) { writeAuthChallenge(h.cfg.auth, w) }
