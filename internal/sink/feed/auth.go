package feed

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

const defaultAPIKeyHeader = "X-API-Key"

// SurfaceAuth is the resolved auth requirement for one surface. A nil
// *SurfaceAuth means the surface is public. The config layer resolves
// default+override and collapses "disabled" / "no methods" to nil before
// constructing this, so a non-nil SurfaceAuth always carries >=1 method.
type SurfaceAuth struct {
	BasicUsers   []BasicCred
	BearerTokens []NamedSecret
	APIKeys      []NamedSecret
	APIKeyHeader string // header to read API keys from; empty => X-API-Key
}

// BasicCred is one accepted HTTP Basic credential with an optional name.
type BasicCred struct {
	Name     string
	Username string
	Password string
}

// NamedSecret is one accepted opaque secret (bearer token or API key) with an
// optional name for observability.
type NamedSecret struct {
	Name   string
	Secret string
}

// authenticate reports whether r satisfies a (nil => public, always passes) and
// returns the matched credential's name (may be "" for an unnamed credential or
// the public case). Token methods are OR'd: any one valid credential passes.
func authenticate(a *SurfaceAuth, r *http.Request) (name string, ok bool) {
	if a == nil {
		return "", true
	}
	if got := r.Header.Get("Authorization"); strings.HasPrefix(got, "Bearer ") {
		tok := got[len("Bearer "):]
		for _, c := range a.BearerTokens {
			if ctEqual(tok, c.Secret) {
				return c.Name, true
			}
		}
	}
	if u, pw, has := r.BasicAuth(); has {
		for _, c := range a.BasicUsers {
			// Evaluate both comparisons unconditionally (no && short-circuit) so
			// timing doesn't reveal whether the username alone matched.
			userOK := ctEqual(u, c.Username)
			passOK := ctEqual(pw, c.Password)
			if userOK && passOK {
				return c.Name, true
			}
		}
	}
	if key := r.Header.Get(a.apiKeyHeader()); key != "" {
		for _, c := range a.APIKeys {
			if ctEqual(key, c.Secret) {
				return c.Name, true
			}
		}
	}
	return "", false
}

func (a *SurfaceAuth) apiKeyHeader() string {
	if a.APIKeyHeader == "" {
		return defaultAPIKeyHeader
	}
	return a.APIKeyHeader
}

// authFailReason classifies a failed authentication for the failure metric.
// Low-cardinality: "no_credentials" when the request presented nothing,
// "bad_token" otherwise. (PR-B adds "no_client_cert" for mTLS.)
func authFailReason(a *SurfaceAuth, r *http.Request) string {
	if a == nil {
		return ""
	}
	if r.Header.Get("Authorization") == "" && r.Header.Get(a.apiKeyHeader()) == "" {
		return "no_credentials"
	}
	return "bad_token"
}

// writeAuthChallenge writes a 401, advertising Basic when basic auth is among
// the accepted methods (otherwise Bearer).
func writeAuthChallenge(a *SurfaceAuth, w http.ResponseWriter) {
	if a != nil && len(a.BasicUsers) > 0 {
		w.Header().Set("WWW-Authenticate", `Basic realm="rss2msg"`)
	} else {
		w.Header().Set("WWW-Authenticate", `Bearer realm="rss2msg"`)
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

func ctEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
