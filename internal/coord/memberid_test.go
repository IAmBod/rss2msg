package coord

import (
	"strings"
	"testing"
)

func TestNewMemberIDUniqueAndShaped(t *testing.T) {
	a := NewMemberID()
	b := NewMemberID()
	if a == "" || b == "" {
		t.Fatal("member id must be non-empty")
	}
	if a == b {
		t.Fatal("two member ids should differ (random suffix)")
	}
	if strings.Count(a, "-") < 2 {
		t.Fatalf("member id %q should look like host-pid-rand", a)
	}
}
