package postgres

import (
	"context"
	"time"
)

// SetFeedMetaUpdatedAtForTest overwrites a feed's updated_at. Test-only seam:
// UpsertFeedMeta always stamps NOW(), so tests need a way to backdate.
func (s *Store) SetFeedMetaUpdatedAtForTest(ctx context.Context, feedURL string, t time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE feed_meta SET updated_at=$1 WHERE feed_url=$2`, t, feedURL)
	return err
}
