package cosmosdb

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/coord"
)

// memberPartitionKey is the single partition key value under which ALL member
// documents are stored. Using one constant partition key avoids cross-partition
// queries entirely — enumeration is always a single-partition query.
const memberPartitionKey = "members"

// memberDocID returns the document id for a given member self identifier.
func memberDocID(self string) string { return "member:" + self }

// memberDoc is the wire layout of a member heartbeat document.
type memberDoc struct {
	ID          string `json:"id"`
	PK          string `json:"pk"`
	LeaseExpiry int64  `json:"lease_expiry"` // epoch milliseconds
}

// Compile-time assertion: *Coordinator implements coord.MembershipProvider.
var _ coord.MembershipProvider = (*Coordinator)(nil)

// Membership returns a Cosmos-backed Membership reusing this coordinator's
// container client. Members are documents under a single "members" partition
// with id="member:<self>" and a lease_expiry field (epoch ms). The live set
// is derived via a single-partition query filtered to non-expired member docs.
func (c *Coordinator) Membership(self string) (coord.Membership, error) {
	ttl := c.memberTTL
	if ttl <= 0 {
		ttl = c.leaseDuration
	}
	return &cosmosMembership{c: c, self: self, ttl: ttl}, nil
}

type cosmosMembership struct {
	c    *Coordinator
	self string
	ttl  time.Duration
}

// Heartbeat upserts this instance's member document and returns the currently
// live member set (including self). The clock is captured once so the upsert
// and the query share the same "now".
func (m *cosmosMembership) Heartbeat(ctx context.Context) ([]string, error) {
	now := m.c.now() // capture once per call

	// Write / refresh the member document.
	if err := m.upsertMember(ctx, now); err != nil {
		return nil, err
	}

	// Query the live set in the "members" partition.
	return m.queryLiveMembers(ctx, now)
}

// upsertMember writes (or refreshes) this instance's member document under
// the "members" partition. It tries CreateItem first; on a 409 Conflict it
// falls back to ReplaceItem to update the lease_expiry.
func (m *cosmosMembership) upsertMember(ctx context.Context, now time.Time) error {
	id := memberDocID(m.self)
	pk := azcosmosMemberPK()

	doc := memberDoc{
		ID:          id,
		PK:          memberPartitionKey,
		LeaseExpiry: now.Add(m.ttl).UnixMilli(),
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	_, err = m.c.container.CreateItem(ctx, pk, body, nil)
	if err == nil {
		return nil
	}
	if !isStatus(err, http.StatusConflict) {
		return err
	}

	// Document already exists — read and replace (no ETag guard needed for
	// membership; it's idempotent and the worst case is a lost update from a
	// concurrent peer that self-writes the same doc, which can't happen since
	// each member owns a distinct id).
	read, err := m.c.container.ReadItem(ctx, pk, id, nil)
	if err != nil {
		if isStatus(err, http.StatusNotFound) {
			// Raced with deletion between CreateItem 409 and ReadItem — retry create.
			_, err = m.c.container.CreateItem(ctx, pk, body, nil)
			return err
		}
		return err
	}
	etag := read.ETag
	_, err = m.c.container.ReplaceItem(ctx, pk, id, body, &azcosmos.ItemOptions{IfMatchEtag: &etag})
	if err != nil {
		if isStatus(err, http.StatusPreconditionFailed) || isStatus(err, http.StatusNotFound) {
			// Lost a concurrent replace — not fatal; our lease_expiry may be
			// slightly stale but will be refreshed on the next heartbeat.
			return nil
		}
		return err
	}
	return nil
}

// queryLiveMembers runs a single-partition query against the "members"
// partition fetching ALL member docs (id + lease_expiry), then in Go splits
// them into live (returned) and expired (best-effort deleted).
func (m *cosmosMembership) queryLiveMembers(ctx context.Context, now time.Time) ([]string, error) {
	nowMs := now.UnixMilli()
	pk := azcosmosMemberPK()

	const q = "SELECT c.id, c.lease_expiry FROM c"
	pager := m.c.container.NewQueryItemsPager(q, pk, nil)

	var ids []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, raw := range page.Items {
			var doc struct {
				ID          string `json:"id"`
				LeaseExpiry int64  `json:"lease_expiry"`
			}
			if err := json.Unmarshal(raw, &doc); err != nil {
				log.Warn().
					Str("coord_driver", "cosmosdb").
					Str("event", "member_doc_unmarshal_error").
					Err(err).
					Msg("coord/cosmosdb: skipping malformed member document")
				continue
			}
			if doc.LeaseExpiry > nowMs {
				ids = append(ids, strings.TrimPrefix(doc.ID, "member:"))
			} else {
				// Best-effort reap of expired member entry; ignore delete errors.
				_, _ = m.c.container.DeleteItem(ctx, pk, doc.ID, nil)
			}
		}
	}
	return ids, nil
}

// Deregister removes this instance's member document from the "members"
// partition. This lets peers reassign its feeds immediately on graceful
// shutdown rather than waiting for the lease_expiry TTL.
func (m *cosmosMembership) Deregister(ctx context.Context) error {
	pk := azcosmosMemberPK()
	_, err := m.c.container.DeleteItem(ctx, pk, memberDocID(m.self), nil)
	if err != nil && isStatus(err, http.StatusNotFound) {
		// Already absent — not an error.
		return nil
	}
	return err
}

// Close is a no-op; the coordinator's shared container client manages its own
// lifecycle.
func (m *cosmosMembership) Close() error { return nil }

// azcosmosMemberPK returns the PartitionKey for the single "members" partition.
func azcosmosMemberPK() azcosmos.PartitionKey {
	return azcosmos.NewPartitionKeyString(memberPartitionKey)
}
