package feed

import "testing"

func TestSyntheticID_StableAndUnique(t *testing.T) {
	a := syntheticID("https://a.com/feed", "1")
	b := syntheticID("https://b.com/feed", "1") // same item id, different feed
	if a == b {
		t.Fatal("ids from different feeds must differ")
	}
	if a != syntheticID("https://a.com/feed", "1") {
		t.Fatal("id must be stable")
	}
	if len(a) < len("urn:rss2msg:") || a[:len("urn:rss2msg:")] != "urn:rss2msg:" {
		t.Fatalf("unexpected id form: %s", a)
	}
}
