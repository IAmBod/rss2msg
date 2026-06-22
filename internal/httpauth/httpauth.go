// Package httpauth provides shared HTTP credential checking (bearer token, HTTP
// basic, and API-key) with constant-time comparison. Used by the feed sink and
// the admin API.
package httpauth

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

const defaultAPIKeyHeader = "X-API-Key"

type BasicCred struct {
	Name     string
	Username string
	Password string
}

type NamedSecret struct {
	Name   string
	Secret string
}

// Auth is a set of accepted credentials. Token methods are OR'd: any one valid
// credential passes. The zero value (no methods) is "empty"; callers decide what
// empty means (e.g. public).
type Auth struct {
	BasicUsers   []BasicCred
	BearerTokens []NamedSecret
	APIKeys      []NamedSecret
	APIKeyHeader string // header to read API keys from; empty => X-API-Key
}

// Empty reports whether no credential method is configured.
func (a *Auth) Empty() bool {
	return a == nil || (len(a.BasicUsers) == 0 && len(a.BearerTokens) == 0 && len(a.APIKeys) == 0)
}

func (a *Auth) apiKeyHeader() string {
	if a.APIKeyHeader == "" {
		return defaultAPIKeyHeader
	}
	return a.APIKeyHeader
}

// Authenticate reports the matched credential name and whether any method passed.
func (a *Auth) Authenticate(r *http.Request) (name string, ok bool) {
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

// FailReason classifies a failed authentication for a metric. Low-cardinality:
// "no_credentials" when nothing was presented, else "bad_token".
func (a *Auth) FailReason(r *http.Request) string {
	if r.Header.Get("Authorization") == "" && r.Header.Get(a.apiKeyHeader()) == "" {
		return "no_credentials"
	}
	return "bad_token"
}

// WriteChallenge writes a 401, advertising Basic when basic auth is accepted,
// otherwise Bearer.
func (a *Auth) WriteChallenge(w http.ResponseWriter) {
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
