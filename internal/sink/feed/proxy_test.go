package feed

import "testing"

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
	cases := map[string]bool{
		"10.1.2.3:5555":    true,  // RFC1918 via "private"
		"127.0.0.1:80":     true,  // loopback via "private"
		"[::1]:443":        true,  // IPv6 loopback via "private"
		"203.0.113.9:1":    true,  // explicit CIDR
		"8.8.8.8:53":       false, // public
		"not-an-addr":      false, // unparseable
	}
	for addr, want := range cases {
		if got := tp.trusts(addr); got != want {
			t.Errorf("trusts(%q)=%v want %v", addr, got, want)
		}
	}
}

func TestTrustedProxies_NilSafe(t *testing.T) {
	var tp *trustedProxies
	if tp.trusts("10.0.0.1:1") {
		t.Fatal("nil trustedProxies must trust nothing")
	}
}

func TestTrustedProxies_All(t *testing.T) {
	tp, _ := parseTrustedProxies([]string{"all"})
	if !tp.trusts("8.8.8.8:1") || !tp.trusts("[2001:db8::1]:1") {
		t.Fatal("'all' must trust any peer")
	}
}
