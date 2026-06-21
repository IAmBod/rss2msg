// Package assign computes feed ownership across a fleet of instances using
// rendezvous (highest-random-weight) hashing. It is pure and does no I/O: each
// instance feeds it the same live member set and gets the same assignment, so
// owners agree without coordinating. Adding or removing one member moves only
// ~1/|members| of feeds (minimal churn); every other feed keeps its owner.
package assign

import "github.com/cespare/xxhash/v2"

// score is the rendezvous weight of (member, feedURL). The member with the
// highest score owns the feed; ties break toward the lexically larger member ID
// so the result is deterministic regardless of slice order.
func score(member, feedURL string) uint64 {
	d := xxhash.New()
	_, _ = d.WriteString(member)
	_, _ = d.Write([]byte{0})
	_, _ = d.WriteString(feedURL)
	return d.Sum64()
}

// Owner returns the member that owns feedURL under HRW hashing. ok is false when
// members is empty.
func Owner(feedURL string, members []string) (string, bool) {
	var best string
	var bestScore uint64
	found := false
	for _, m := range members {
		s := score(m, feedURL)
		if !found || s > bestScore || (s == bestScore && m > best) {
			best, bestScore, found = m, s, true
		}
	}
	return best, found
}

// Owned returns the subset of feeds owned by self given the live member set.
// Returns nil if members is empty or self is not among members.
func Owned(self string, feeds, members []string) []string {
	inSet := false
	for _, m := range members {
		if m == self {
			inSet = true
			break
		}
	}
	if !inSet {
		return nil
	}
	var owned []string
	for _, f := range feeds {
		if o, ok := Owner(f, members); ok && o == self {
			owned = append(owned, f)
		}
	}
	return owned
}
