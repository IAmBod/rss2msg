package feed

import (
	"net/http"

	"github.com/iambod/rss2msg/internal/httpauth"
)

// SurfaceAuth is the resolved auth requirement for one surface. A nil
// *SurfaceAuth means the surface is public. It aliases httpauth.Auth so the feed
// sink and admin API share one credential-checking core.
type SurfaceAuth = httpauth.Auth

// BasicCred and NamedSecret alias the shared httpauth types.
type BasicCred = httpauth.BasicCred
type NamedSecret = httpauth.NamedSecret

// authenticate reports whether r satisfies a (nil => public, always passes) and
// returns the matched credential's name.
func authenticate(a *SurfaceAuth, r *http.Request) (name string, ok bool) {
	if a == nil {
		return "", true
	}
	return a.Authenticate(r)
}

// authFailReason classifies a failed authentication for the failure metric.
func authFailReason(a *SurfaceAuth, r *http.Request) string {
	if a == nil {
		return ""
	}
	return a.FailReason(r)
}

// writeAuthChallenge writes a 401 advertising the appropriate scheme.
func writeAuthChallenge(a *SurfaceAuth, w http.ResponseWriter) {
	a.WriteChallenge(w)
}
