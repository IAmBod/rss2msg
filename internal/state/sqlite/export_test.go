package sqlite

import (
	"context"
	"time"
)

// SetFeedMetaUpdatedAtForTest overwrites a feed's updated_at. Test-only seam:
// UpsertFeedMeta always stamps time.Now(), so tests need a way to backdate.
func (s *Store) SetFeedMetaUpdatedAtForTest(ctx context.Context, feedURL string, t time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE feed_meta SET updated_at=? WHERE feed_url=?`,
		t.UTC().Format(time.RFC3339Nano), feedURL)
	return err
}
