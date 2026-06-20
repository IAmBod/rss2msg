package feed

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

// tlsConnState is a reusable non-nil TLS state for tests that need r.TLS set.
var tlsConnState = tls.ConnectionState{}

func reqWith(remote string, h map[string]string, useTLS bool) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://internal:8088/atom", nil)
	r.RemoteAddr = remote
	r.Host = "internal:8088"
	for k, v := range h {
		r.Header.Set(k, v)
	}
	if useTLS {
		r.TLS = &tlsConnState
	}
	return r
}

func TestProxyConfig_SelfURL(t *testing.T) {
	trusted, _ := parseTrustedProxies([]string{"private"})

	t.Run("public_url wins, ignores headers", func(t *testing.T) {
		p := proxyConfig{publicURL: "https://feeds.example.com", trusted: trusted}
		r := reqWith("10.0.0.1:5000", map[string]string{
			"X-Forwarded-Host": "evil.example", "X-Forwarded-Proto": "https",
			"X-Forwarded-Prefix": "/nope",
		}, false)
		if got := p.selfURL(r, "/atom"); got != "https://feeds.example.com/atom" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("trusted headers build base with prefix", func(t *testing.T) {
		p := proxyConfig{link: "https://site.example", trusted: trusted}
		r := reqWith("10.0.0.1:5000", map[string]string{
			"X-Forwarded-Proto": "https", "X-Forwarded-Host": "feeds.example.com",
			"X-Forwarded-Prefix": "/news/",
		}, false)
		if got := p.selfURL(r, "/atom"); got != "https://feeds.example.com/news/atom" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("untrusted peer ignores headers, uses request host", func(t *testing.T) {
		p := proxyConfig{trusted: trusted}
		r := reqWith("8.8.8.8:5000", map[string]string{
			"X-Forwarded-Proto": "https", "X-Forwarded-Host": "spoof.example",
		}, false)
		if got := p.selfURL(r, "/atom"); got != "http://internal:8088/atom" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("Forwarded header wins over X-Forwarded for proto/host", func(t *testing.T) {
		p := proxyConfig{trusted: trusted}
		r := reqWith("10.0.0.1:5000", map[string]string{
			"Forwarded":         `proto=https;host=fwd.example`,
			"X-Forwarded-Proto": "http", "X-Forwarded-Host": "xfwd.example",
		}, false)
		if got := p.selfURL(r, "/rss"); got != "https://fwd.example/rss" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("untrusted peer with TLS uses https + request host", func(t *testing.T) {
		p := proxyConfig{trusted: trusted}
		r := reqWith("8.8.8.8:5000", nil, true)
		if got := p.selfURL(r, "/atom"); got != "https://internal:8088/atom" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("trusted prefix without forwarded host is dropped", func(t *testing.T) {
		p := proxyConfig{trusted: trusted}
		r := reqWith("10.0.0.1:5000", map[string]string{"X-Forwarded-Prefix": "/news"}, false)
		if got := p.selfURL(r, "/atom"); got != "http://internal:8088/atom" {
			t.Fatalf("got %q", got)
		}
	})
}

func TestParseTrustedProxies(t *testing.T) {
	if tp, err := parseTrustedProxies(nil); err != nil || tp != nil {
		t.Fatalf("empty => (nil,nil); got (%v,%v)", tp, err)
	}
	if _, err := parseTrustedProxies([]string{"bogus"}); err == nil {
		t.Fatal("want error for bogus entry")
	}
}

func TestTrustedProxies_Trusts(t *testing.T) {
	tp, err := parseTrustedProxies([]string{"private", "203.0.113.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		addr string
		want bool
	}{
		{"10.1.2.3:5555", true},
		{"127.0.0.1:80", true},
		{"[::1]:443", true},
		{"203.0.113.9:1", true},
		{"8.8.8.8:53", false},
		{"not-an-addr", false},
	}
	for _, c := range cases {
		if got := tp.trusts(c.addr); got != c.want {
			t.Errorf("trusts(%q)=%v want %v", c.addr, got, c.want)
		}
	}
}

func TestTrustedProxies_NilSafe(t *testing.T) {
	var tp *trustedProxies
	if tp.trusts("10.0.0.1:1") {
		t.Fatal("nil trustedProxies must trust nothing")
	}
	tp2, _ := parseTrustedProxies([]string{"10.0.0.0/8"})
	if tp2.contains(nil) {
		t.Fatal("contains(nil) must be false")
	}
}

func TestTrustedProxies_All(t *testing.T) {
	tp, _ := parseTrustedProxies([]string{"all"})
	if !tp.trusts("8.8.8.8:1") || !tp.trusts("[2001:db8::1]:1") {
		t.Fatal("'all' must trust any peer")
	}
}

func TestProxyConfig_ClientIP(t *testing.T) {
	trusted, _ := parseTrustedProxies([]string{"private"})
	p := proxyConfig{trusted: trusted}

	t.Run("untrusted peer => peer IP, headers ignored", func(t *testing.T) {
		r := reqWith("8.8.8.8:1", map[string]string{"X-Forwarded-For": "1.2.3.4"}, false)
		if got := p.clientIP(r); got != "8.8.8.8" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("trusted peer => right-most untrusted in XFF chain", func(t *testing.T) {
		r := reqWith("10.0.0.1:1", map[string]string{
			"X-Forwarded-For": "203.0.113.7, 172.16.0.9, 10.0.0.2",
		}, false)
		if got := p.clientIP(r); got != "203.0.113.7" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("trusted peer, no XFF => peer IP", func(t *testing.T) {
		r := reqWith("10.0.0.1:1", nil, false)
		if got := p.clientIP(r); got != "10.0.0.1" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("all hops trusted => left-most", func(t *testing.T) {
		r := reqWith("10.0.0.1:1", map[string]string{"X-Forwarded-For": "10.0.0.9, 10.0.0.2"}, false)
		if got := p.clientIP(r); got != "10.0.0.9" {
			t.Fatalf("got %q", got)
		}
	})
	// Documents that a non-IP token in the XFF chain terminates the right-to-left
	// walk: net.ParseIP returns nil, contains(nil) is false, so the token is
	// treated as untrusted and returned verbatim as the client identifier.
	t.Run("non-IP token in XFF terminates walk, returned as client", func(t *testing.T) {
		r := reqWith("10.0.0.1:1", map[string]string{
			"X-Forwarded-For": "203.0.113.7, notanip, 10.0.0.2",
		}, false)
		if got := p.clientIP(r); got != "notanip" {
			t.Fatalf("got %q", got)
		}
	})
}

func TestPeerIP(t *testing.T) {
	cases := []struct{ remote, want string }{
		{"192.0.2.1:5000", "192.0.2.1"},
		{"[::1]:443", "::1"},
		{"192.0.2.1", "192.0.2.1"}, // no port => SplitHostPort errors => raw returned
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
		r.RemoteAddr = c.remote
		if got := peerIP(r); got != c.want {
			t.Errorf("peerIP(%q)=%q want %q", c.remote, got, c.want)
		}
	}
}
