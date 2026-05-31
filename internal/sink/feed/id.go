package feed

import (
	"crypto/sha256"
	"encoding/hex"
)

// syntheticID returns a stable, globally-unique, opaque feed entry id derived
// from (feedURL, itemID). model.ItemID is only unique within a feed, so an
// aggregated feed must namespace it to satisfy Atom's global-uniqueness rule.
func syntheticID(feedURL, itemID string) string {
	h := sha256.Sum256([]byte(feedURL + "\n" + itemID))
	return "urn:rss2msg:" + hex.EncodeToString(h[:])
}
