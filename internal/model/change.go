package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

type ChangeKind string

const (
	ChangeNew     ChangeKind = "new"
	ChangeUpdated ChangeKind = "updated"

	SchemaVersion = 1
)

type Change struct {
	SchemaVersion int        `json:"schema_version"`
	FeedURL       string     `json:"feed_url"`
	FeedTitle     string     `json:"feed_title,omitempty"`
	ItemID        string     `json:"item_id"`
	Kind          ChangeKind `json:"kind"`
	Title         string     `json:"title,omitempty"`
	Link          string     `json:"link,omitempty"`
	Authors       []string   `json:"authors,omitempty"`
	Summary       string     `json:"summary,omitempty"`
	Content       string     `json:"content,omitempty"`
	Categories    []string   `json:"categories,omitempty"`
	PublishedAt   *time.Time `json:"published_at,omitempty"`
	UpdatedAt     *time.Time `json:"updated_at,omitempty"`
	ContentHash   string     `json:"content_hash"`
	DetectedAt    time.Time  `json:"detected_at"`

	// Populated only when this Change is being delivered to a DLQ sink.
	DLQFromSink string `json:"dlq_from_sink,omitempty"`
	DLQError    string `json:"dlq_error,omitempty"`
	DLQAttempts int    `json:"dlq_attempts,omitempty"`
}

func (c Change) MarshalJSON() ([]byte, error) {
	type alias Change
	a := alias(c)
	if a.SchemaVersion == 0 {
		a.SchemaVersion = SchemaVersion
	}
	return json.Marshal(a)
}

func (c *Change) UnmarshalJSON(b []byte) error {
	type alias Change
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*c = Change(a)
	return nil
}

// ContentHash returns a stable sha256 hex over the normalised tuple.
// Normalisation: trim outer whitespace, collapse internal runs of whitespace
// to single spaces. updatedAt is rendered as RFC3339 UTC.
func ContentHash(title, link, body, author string, updatedAt time.Time) string {
	h := sha256.New()
	writeField(h, title)
	writeField(h, link)
	writeField(h, body)
	writeField(h, author)
	writeField(h, updatedAt.UTC().Format(time.RFC3339))
	return hex.EncodeToString(h.Sum(nil))
}

// IdentityKey returns the per-item stable identifier:
// GUID if non-empty, else Link, else sha256(title || publishedAt).
func IdentityKey(guid, link, title string, publishedAt time.Time) string {
	if guid != "" {
		return guid
	}
	if link != "" {
		return link
	}
	h := sha256.Sum256([]byte(strings.TrimSpace(title) + "|" + publishedAt.UTC().Format(time.RFC3339)))
	return hex.EncodeToString(h[:])
}

func writeField(h interface{ Write([]byte) (int, error) }, s string) {
	norm := strings.Join(strings.Fields(s), " ")
	_, _ = h.Write([]byte(norm))
	_, _ = h.Write([]byte{0})
}
