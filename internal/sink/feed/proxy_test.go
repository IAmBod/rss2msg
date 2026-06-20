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
