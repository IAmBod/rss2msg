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
