package redis

import "testing"

func TestMemberKeyFormat(t *testing.T) {
	got := memberKey("host-123-abcd")
	want := "rss2msg:coord:member:host-123-abcd"
	if got != want {
		t.Fatalf("memberKey = %q, want %q", got, want)
	}
	if pfx := memberKeyPrefix(); pfx != "rss2msg:coord:member:" {
		t.Fatalf("memberKeyPrefix = %q", pfx)
	}
}
