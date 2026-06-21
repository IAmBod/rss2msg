package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/coord"
	"github.com/iambod/rss2msg/internal/feed"
	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/retry"
	"github.com/iambod/rss2msg/internal/sink"
	"github.com/iambod/rss2msg/internal/state"
	"github.com/iambod/rss2msg/internal/telemetry"
	"github.com/stretchr/testify/require"
)

// --- test doubles -----------------------------------------------------------

// fakeCoord is a coord.Coordinator whose acquire outcome and error are
// scripted per test. It records whether the release func was invoked so the
// "lease is always released" contract can be asserted.
type fakeCoord struct {
	acquired   bool
	acquireErr error

	mu       sync.Mutex
	tryCalls int
	released bool
}

func (c *fakeCoord) TryAcquire(ctx context.Context, feedURL string) (coord.ReleaseFunc, bool, error) {
	c.mu.Lock()
	c.tryCalls++
	c.mu.Unlock()
	if c.acquireErr != nil {
		return nil, false, c.acquireErr
	}
	if !c.acquired {
		return nil, false, nil
	}
	return func(context.Context) error {
		c.mu.Lock()
		c.released = true
		c.mu.Unlock()
		return nil
	}, true, nil
}

func (c *fakeCoord) Close() error { return nil }

func (c *fakeCoord) wasReleased() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.released
}

// fakeStore is an in-memory state.Store. GetItem reads from items so the
// detector classifies new-vs-updated deterministically; UpsertItem appends to
// committed so tests can assert exactly which items reached durable state.
type fakeStore struct {
	mu        sync.Mutex
	items     map[string]state.ItemState // key: feedURL\x00itemID
	committed []string                   // itemIDs passed to UpsertItem, in order

	getItemErr     error // when set, GetItem fails (drives the detect-error path)
	getFeedMetaErr error // when set, GetFeedMeta fails (drives the meta-read path)
}

func newFakeStore() *fakeStore {
	return &fakeStore{items: map[string]state.ItemState{}}
}

func storeKey(feedURL, itemID string) string { return feedURL + "\x00" + itemID }

func (s *fakeStore) GetItem(ctx context.Context, feedURL, itemID string) (state.ItemState, bool, error) {
	if s.getItemErr != nil {
		return state.ItemState{}, false, s.getItemErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.items[storeKey(feedURL, itemID)]
	return st, ok, nil
}

func (s *fakeStore) UpsertItem(ctx context.Context, feedURL, itemID, hash string, seenAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[storeKey(feedURL, itemID)] = state.ItemState{ContentHash: hash, LastSeenAt: seenAt}
	s.committed = append(s.committed, itemID)
	return nil
}

func (s *fakeStore) GetFeedMeta(ctx context.Context, feedURL string) (state.FeedMeta, bool, error) {
	if s.getFeedMetaErr != nil {
		return state.FeedMeta{}, false, s.getFeedMetaErr
	}
	return state.FeedMeta{}, false, nil
}

func (s *fakeStore) UpsertFeedMeta(ctx context.Context, feedURL string, meta state.FeedMeta) error {
	return nil
}

func (s *fakeStore) Ping(ctx context.Context) error { return nil }
func (s *fakeStore) Close() error                   { return nil }

func (s *fakeStore) committedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.committed...)
}

// fakeSink is a sink.Publisher that records delivered changes and can be made
// to fail every Publish (to drive the DLQ / dropped branches).
type fakeSink struct {
	name string
	err  error

	mu        sync.Mutex
	published []model.Change
}

func (s *fakeSink) Name() string { return s.name }

func (s *fakeSink) Publish(ctx context.Context, c model.Change) error {
	if s.err != nil {
		return s.err
	}
	s.mu.Lock()
	s.published = append(s.published, c)
	s.mu.Unlock()
	return nil
}

func (s *fakeSink) Close() error { return nil }

func (s *fakeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.published)
}

// --- fixtures ----------------------------------------------------------------

const rssOneItem = `<?xml version="1.0"?>
<rss version="2.0"><channel><title>S</title>
<item><guid>a</guid><title>One</title><link>https://e/a</link><description>first</description></item>
</channel></rss>`

// fastRetry never sleeps and tries exactly once, so dropped/DLQ paths resolve
// immediately.
var fastRetry = retry.Config{MaxAttempts: 1}

func noopInstruments(t *testing.T) telemetry.Instruments {
	t.Helper()
	instr, err := telemetry.NewInstruments(metricnoop.NewMeterProvider().Meter("test"))
	require.NoError(t, err)
	return instr
}

// newTestPipeline builds a *pipeline backed by a real fetcher (pointed at an
// httptest server), a real detector, and the supplied coordinator/store/sinks.
func newTestPipeline(t *testing.T, feedURL string, cd coord.Coordinator, st state.Store, sinks ...sinkBranch) *pipeline {
	t.Helper()
	return &pipeline{
		cfg:        config.FeedConfig{URL: feedURL},
		sinks:      sinks,
		fetcher:    feed.NewFetcher(feed.Options{UserAgent: "rss2msg/test", Timeout: 5 * time.Second}),
		detect:     feed.NewDetector(),
		store:      st,
		log:        zerolog.Nop(),
		tracer:     tracenoop.NewTracerProvider().Tracer("test"),
		instr:      noopInstruments(t),
		coord:      cd,
		fetchRetry: fastRetry,
	}
}

func branch(name string, primary, dlq sink.Publisher) sinkBranch {
	return sinkBranch{name: name, wrapped: sink.WithRetry(primary, dlq, fastRetry, 0)}
}

func serveRSS(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// --- tests -------------------------------------------------------------------

func TestRunOnceHappyPathDeliversAndCommits(t *testing.T) {
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, url, cd, st, branch("s", snk, nil))

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Equal(t, model.ChangeNew, changes[0].Kind)
	require.Equal(t, 1, snk.count(), "item should be delivered to the sink")
	require.Equal(t, []string{changes[0].ItemID}, st.committedIDs(), "successful delivery commits state")
	require.True(t, cd.wasReleased(), "lease must be released after a successful poll")
}

func TestRunOnceNotAcquiredSkipsWithoutFetchingOrDelivering(t *testing.T) {
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	cd := &fakeCoord{acquired: false} // another instance owns the lease
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, url, cd, st, branch("s", snk, nil))

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.NoError(t, err)
	require.Nil(t, changes)
	require.Zero(t, snk.count(), "no delivery when the lease is not acquired")
	require.Empty(t, st.committedIDs(), "no state committed when skipped")
}

func TestRunOnceCoordErrorSkipsAndDoesNotError(t *testing.T) {
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	cd := &fakeCoord{acquireErr: errors.New("redis down")}
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, url, cd, st, branch("s", snk, nil))

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.NoError(t, err, "a coordinator failure is a skip, not a poll error")
	require.Nil(t, changes)
	require.Zero(t, snk.count())
	require.False(t, cd.wasReleased(), "nothing to release when acquire failed")
}

func TestRunOnceDroppedSinkDoesNotCommitState(t *testing.T) {
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	// No DLQ + always-failing primary => BranchDropped => allOK=false.
	failing := &fakeSink{name: "s", err: errors.New("sink boom")}
	p := newTestPipeline(t, url, cd, st, branch("s", failing, nil))

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.NoError(t, err)
	require.Len(t, changes, 1, "a dropped delivery is still a detected change")
	require.Empty(t, st.committedIDs(),
		"state must NOT be committed when a sink drops the change (so it is retried next poll)")
}

func TestRunOnceDLQCountsAsHandledAndCommitsState(t *testing.T) {
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	// Primary fails, DLQ accepts => BranchDLQ, which must NOT block the commit.
	failing := &fakeSink{name: "s", err: errors.New("primary boom")}
	dlq := &fakeSink{name: "s.dlq"}
	p := newTestPipeline(t, url, cd, st, branch("s", failing, dlq))

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Equal(t, 1, dlq.count(), "the change should land in the DLQ")
	require.Equal(t, []string{changes[0].ItemID}, st.committedIDs(),
		"a DLQ capture is treated as handled, so state IS committed")
}

func TestRunOnceNotModifiedShortCircuits(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(srv.Close)
	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, srv.URL, cd, st, branch("s", snk, nil))

	changes, err := p.RunOnce(context.Background(), srv.URL, time.Now())
	require.NoError(t, err)
	require.Nil(t, changes, "a 304 yields no changes")
	require.Zero(t, snk.count(), "nothing delivered when the feed is unchanged")
	require.Empty(t, st.committedIDs())
	require.True(t, cd.wasReleased(), "the lease is released even on a 304")
}

func TestRunOnceMetaReadErrorPropagates(t *testing.T) {
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	st.getFeedMetaErr = errors.New("meta read failed")
	cd := &fakeCoord{acquired: true}
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, url, cd, st, branch("s", snk, nil))

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.Error(t, err, "a meta-read failure must surface as a poll error")
	require.Nil(t, changes)
	require.Zero(t, snk.count())
	require.True(t, cd.wasReleased(), "the lease is released even when the poll errors")
}

func TestRunOnceFetchErrorPropagates(t *testing.T) {
	// A server that is immediately closed yields a connection error on fetch.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := srv.URL
	srv.Close()

	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, deadURL, cd, st, branch("s", snk, nil))

	changes, err := p.RunOnce(context.Background(), deadURL, time.Now())
	require.Error(t, err, "a fetch failure must surface as a poll error")
	require.Nil(t, changes)
	require.Zero(t, snk.count())
}

func TestRunOnceDetectErrorPropagates(t *testing.T) {
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	st.getItemErr = errors.New("getitem failed") // surfaces from inside Detect
	cd := &fakeCoord{acquired: true}
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, url, cd, st, branch("s", snk, nil))

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.Error(t, err, "a detect failure must surface as a poll error")
	require.Nil(t, changes)
	require.Zero(t, snk.count())
}

func TestRunOnceContextCanceledLogsAtDebugNotError(t *testing.T) {
	// A poll whose context is cancelled mid-flight is the expected result of a
	// rebalance deregistering this feed (or the daemon draining), not a failure.
	// It must surface the error to the scheduler but log at debug, not error, so
	// a routine rebalance does not spew a wall of error-level "fetch" lines.
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	p := newTestPipeline(t, url, cd, st)

	var buf bytes.Buffer
	p.log = zerolog.New(&buf).Level(zerolog.DebugLevel)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // feed deregistered by a rebalance: the poll context is already done

	_, err := p.RunOnce(ctx, url, time.Now())
	require.Error(t, err, "a cancelled poll still returns the error to the scheduler")
	require.ErrorIs(t, err, context.Canceled)

	logs := buf.String()
	require.Contains(t, logs, "context canceled")
	require.NotContains(t, logs, `"level":"error"`,
		"a context-cancelled poll is expected teardown, not an error")
}

func TestPipelineFeedURL(t *testing.T) {
	p := &pipeline{cfg: config.FeedConfig{URL: "https://e/feed.xml"}}
	require.Equal(t, "https://e/feed.xml", p.FeedURL())
}

func TestRunOnceCommitsOnlyWhenEverySinkSucceeds(t *testing.T) {
	url := serveRSS(t, rssOneItem)
	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	ok := &fakeSink{name: "ok"}
	bad := &fakeSink{name: "bad", err: errors.New("bad boom")} // dropped, no DLQ
	p := newTestPipeline(t, url, cd, st, branch("ok", ok, nil), branch("bad", bad, nil))

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Equal(t, 1, ok.count(), "the healthy sink still receives the change")
	require.Empty(t, st.committedIDs(),
		"one dropped branch keeps the change uncommitted even though another branch succeeded")
}

// serveRSSFlaky returns 503 for the first failTimes requests, then the body.
func serveRSSFlaky(t *testing.T, body string, failTimes int32) string {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= failTimes {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestRunOnceRetriesTransientThenSucceeds(t *testing.T) {
	url := serveRSSFlaky(t, rssOneItem, 2) // fail twice, succeed on 3rd
	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	snk := &fakeSink{name: "s"}
	p := newTestPipeline(t, url, cd, st, branch("s", snk, nil))
	p.fetchRetry = retry.Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		Retryable:   feed.IsRetryable,
	}

	changes, err := p.RunOnce(context.Background(), url, time.Now())
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Equal(t, 1, snk.count())
}

func TestRunOnceDoesNotRetryPermanent(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	st := newFakeStore()
	cd := &fakeCoord{acquired: true}
	p := newTestPipeline(t, srv.URL, cd, st)
	p.fetchRetry = retry.Config{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
		Retryable:   feed.IsRetryable,
	}

	_, err := p.RunOnce(context.Background(), srv.URL, time.Now())
	require.Error(t, err)
	require.Equal(t, int32(1), calls.Load(), "404 must not be retried")
}
