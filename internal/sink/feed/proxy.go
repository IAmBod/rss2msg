package feed

import (
	"fmt"
	"net"
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
			return nil, fmt.Errorf("entry %q: not a preset, CIDR, or IP", raw)
		}
		bits := 32
		if ip.To4() == nil {
			bits = 128
		}
		nets = append(nets, net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
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
