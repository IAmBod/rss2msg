package assign

import (
	"fmt"
	"testing"
)

func TestOwnerEmptyMembers(t *testing.T) {
	if _, ok := Owner("https://e/feed", nil); ok {
		t.Fatal("expected ok=false for empty members")
	}
}

func TestOwnerDeterministicAndOrderIndependent(t *testing.T) {
	a := []string{"m1", "m2", "m3"}
	b := []string{"m3", "m1", "m2"}
	o1, ok1 := Owner("https://e/feed-7", a)
	o2, ok2 := Owner("https://e/feed-7", b)
	if !ok1 || !ok2 || o1 != o2 {
		t.Fatalf("owner not order-independent: %q vs %q", o1, o2)
	}
}

func TestOwnedSelfNotMember(t *testing.T) {
	got := Owned("ghost", []string{"https://e/a"}, []string{"m1", "m2"})
	if got != nil {
		t.Fatalf("expected nil when self not in members, got %v", got)
	}
}

func TestOwnedSingleMemberOwnsAll(t *testing.T) {
	feeds := []string{"https://e/a", "https://e/b", "https://e/c"}
	got := Owned("m1", feeds, []string{"m1"})
	if len(got) != len(feeds) {
		t.Fatalf("single member should own all %d feeds, got %d", len(feeds), len(got))
	}
}

func TestOwnedPartitionsCompletelyAndDisjointly(t *testing.T) {
	members := []string{"m1", "m2", "m3"}
	feeds := make([]string, 300)
	for i := range feeds {
		feeds[i] = fmt.Sprintf("https://e/feed-%d", i)
	}
	seen := map[string]int{}
	for _, m := range members {
		for _, f := range Owned(m, feeds, members) {
			seen[f]++
		}
	}
	if len(seen) != len(feeds) {
		t.Fatalf("expected every feed owned exactly once; covered %d/%d", len(seen), len(feeds))
	}
	for f, n := range seen {
		if n != 1 {
			t.Fatalf("feed %q owned by %d members, want 1", f, n)
		}
	}
}

func TestDistributionRoughlyEven(t *testing.T) {
	members := make([]string, 10)
	for i := range members {
		members[i] = fmt.Sprintf("m%d", i)
	}
	const M = 10000
	feeds := make([]string, M)
	for i := range feeds {
		feeds[i] = fmt.Sprintf("https://e/feed-%d", i)
	}
	counts := map[string]int{}
	for _, m := range members {
		counts[m] = len(Owned(m, feeds, members))
	}
	exp := M / len(members)
	for m, c := range counts {
		if c < exp*8/10 || c > exp*12/10 {
			t.Fatalf("member %s owns %d feeds, want within ±20%% of %d", m, c, exp)
		}
	}
}

func TestMinimalChurnOnRemoval(t *testing.T) {
	before := []string{"m1", "m2", "m3", "m4"}
	after := []string{"m1", "m2", "m4"} // removed m3
	const M = 5000
	feeds := make([]string, M)
	for i := range feeds {
		feeds[i] = fmt.Sprintf("https://e/feed-%d", i)
	}
	moved := 0
	for _, f := range feeds {
		o1, _ := Owner(f, before)
		o2, _ := Owner(f, after)
		if o1 != o2 {
			moved++
			if o1 != "m3" {
				t.Fatalf("feed %q moved (%s->%s) but its old owner was not the removed member", f, o1, o2)
			}
		}
	}
	// Only m3's former feeds (~M/4) should move; allow generous slack.
	if moved > M/3 {
		t.Fatalf("removal moved %d feeds, want ~%d (only the removed member's share)", moved, M/4)
	}
}
