package feed

import (
	"context"
	"sort"
	"sync"

	"github.com/iambod/rss2msg/internal/model"
)

type memoryStore struct {
	mu    sync.Mutex
	max   int
	byKey map[string]model.Change
}

func newMemoryStore(max int) *memoryStore {
	return &memoryStore{max: max, byKey: make(map[string]model.Change)}
}

func key(c model.Change) string { return c.FeedURL + "\n" + c.ItemID }

func (m *memoryStore) Write(_ context.Context, c model.Change) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byKey[key(c)] = c
	if len(m.byKey) > m.max {
		m.pruneLocked()
	}
	return nil
}

func (m *memoryStore) pruneLocked() {
	all := make([]model.Change, 0, len(m.byKey))
	for _, c := range m.byKey {
		all = append(all, c)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DetectedAt.After(all[j].DetectedAt) })
	for _, c := range all[m.max:] {
		delete(m.byKey, key(c))
	}
}

func (m *memoryStore) Recent(_ context.Context, n int) ([]model.Change, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	all := make([]model.Change, 0, len(m.byKey))
	for _, c := range m.byKey {
		all = append(all, c)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DetectedAt.After(all[j].DetectedAt) })
	if n < len(all) {
		all = all[:n]
	}
	return all, nil
}

func (m *memoryStore) Ping(context.Context) error { return nil }
func (m *memoryStore) Close() error               { return nil }
