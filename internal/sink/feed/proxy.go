package feed

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// trustedProxies is the set of upstream peers whose forwarding headers we honor.
// A nil *trustedProxies trusts nothing (the default), so all forwarding headers
// are ignored and the sink behaves as if directly exposed.
type trustedProxies struct {
	nets []net.IPNet
}

// presetCIDRs maps the convenience tokens to their CIDR expansions.
var presetCIDRs = map[string][]string{
	"private": {
		"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
		"127.0.0.0/8", "::1/128", "fc00::/7",
	},
	"all": {"0.0.0.0/0", "::/0"},
}

// parseTrustedProxies builds a trustedProxies from preset tokens (private, all),
// CIDRs, and bare IPs. Empty input returns (nil, nil) — trust nothing.
func parseTrustedProxies(entries []string) (*trustedProxies, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	var nets []net.IPNet
	add := func(cidr string) error {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			return err
		}
		nets = append(nets, *n)
		return nil
	}
	for _, raw := range entries {
		e := strings.TrimSpace(raw)
		if cidrs, ok := presetCIDRs[strings.ToLower(e)]; ok {
			for _, c := range cidrs {
				if err := add(c); err != nil {
					return nil, fmt.Errorf("preset %q: %w", e, err)
				}
			}
			continue
		}
		if strings.Contains(e, "/") {
			if err := add(e); err != nil {
				return nil, fmt.Errorf("cidr %q: %w", e, err)
			}
			continue
		}
		ip := net.ParseIP(e)
		if ip == nil {
			return nil, fmt.Errorf("entry %q: not a preset, CIDR, or IP", e)
		}
		bits := 32
		if ip.To4() == nil {
			bits = 128
		}
		mask := net.CIDRMask(bits, bits)
		nets = append(nets, net.IPNet{IP: ip.Mask(mask), Mask: mask})
	}
	return &trustedProxies{nets: nets}, nil
}

// contains reports whether ip falls in any trusted network. nil-safe.
func (t *trustedProxies) contains(ip net.IP) bool {
	if t == nil || ip == nil {
		return false
	}
	for i := range t.nets {
		if t.nets[i].Contains(ip) {
			return true
		}
	}
	return false
}

// trusts reports whether the peer at remoteAddr ("host:port" or "host") is
// trusted. nil-safe; unparseable addresses are untrusted.
func (t *trustedProxies) trusts(remoteAddr string) bool {
	if t == nil {
		return false
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	return t.contains(net.ParseIP(host))
}

// proxyConfig derives per-request public URLs and client IPs. publicURL and
// link are pre-trimmed of any trailing slash. trusted is nil when no proxies
// are configured (then all forwarding headers are ignored).
type proxyConfig struct {
	publicURL string
	link      string
	trusted   *trustedProxies
}

// forwarded holds the proxy-supplied scheme/host/prefix for one request, or
// zero values when the peer is untrusted or sent nothing.
type forwarded struct {
	proto, host, prefix string
}

// selfURL returns the absolute URL of a surface for this request. Precedence:
// publicURL (static, authoritative) -> trusted forwarding headers -> request
// Host -> link. A header-supplied prefix is applied only in the headers branch.
func (p proxyConfig) selfURL(r *http.Request, surfacePath string) string {
	if p.publicURL != "" {
		return p.publicURL + surfacePath
	}
	fw := p.parseForwarded(r)
	proto, host, prefix := fw.proto, fw.host, fw.prefix
	if host == "" {
		host = r.Host
		prefix = "" // a header-supplied prefix is only meaningful with a header-supplied host
	}
	if proto == "" {
		if r.TLS != nil {
			proto = "https"
		} else {
			proto = "http"
		}
	}
	if host != "" {
		return proto + "://" + host + prefix + surfacePath
	}
	if p.link != "" {
		return p.link + surfacePath
	}
	return surfacePath
}

// parseForwarded extracts scheme/host/prefix from forwarding headers, but only
// when the direct peer is trusted. RFC 7239 Forwarded wins over X-Forwarded-*
// for proto/host; X-Forwarded-Prefix is the only prefix source.
func (p proxyConfig) parseForwarded(r *http.Request) forwarded {
	if !p.trusted.trusts(r.RemoteAddr) {
		return forwarded{}
	}
	var f forwarded
	if proto := firstValue(r.Header.Get("X-Forwarded-Proto")); proto != "" {
		f.proto = proto
	}
	if host := firstValue(r.Header.Get("X-Forwarded-Host")); host != "" {
		f.host = host
	}
	// RFC 7239 Forwarded overrides the X-Forwarded-* equivalents.
	if fwd := r.Header.Get("Forwarded"); fwd != "" {
		for _, kv := range strings.Split(firstValue(fwd), ";") {
			k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
			if !ok {
				continue
			}
			v = strings.Trim(v, `"`)
			switch strings.ToLower(k) {
			case "proto":
				f.proto = v
			case "host":
				f.host = v
			}
		}
	}
	if pfx := firstValue(r.Header.Get("X-Forwarded-Prefix")); pfx != "" {
		f.prefix = normalizePrefix(pfx)
	}
	return f
}

// firstValue returns the first comma-separated token, trimmed. Proxies append
// to forwarding headers, so the left-most value is closest to the client.
func firstValue(h string) string {
	if h == "" {
		return ""
	}
	if i := strings.IndexByte(h, ','); i >= 0 {
		h = h[:i]
	}
	return strings.TrimSpace(h)
}

// normalizePrefix ensures a leading slash and no trailing slash ("" stays "").
func normalizePrefix(p string) string {
	p = strings.TrimRight(strings.TrimSpace(p), "/")
	if p == "" {
		return ""
	}
	if p[0] != '/' {
		p = "/" + p
	}
	return p
}
