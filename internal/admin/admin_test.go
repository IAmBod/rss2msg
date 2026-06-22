package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/health"
	"github.com/iambod/rss2msg/internal/httpauth"
	"github.com/iambod/rss2msg/internal/state"
	"github.com/rs/zerolog"
)

func testServer(t *testing.T, auth *httpauth.Auth, cors []string, deps Deps) *Server {
	t.Helper()
	cfg := config.AdminConfig{Enabled: true, Listen: ":0", CORS: config.AdminCORSConfig{AllowedOrigins: cors}}
	return New(cfg, auth, deps, zerolog.Nop())
}

func baseDeps() Deps {
	return Deps{
		Build:     BuildInfo{Version: "v1.2.3", Commit: "abc", Date: "today", InstanceID: "inst-1"},
		StartedAt: time.Now().Add(-time.Minute),
		Self:      "inst-1",
	}
}

func TestStatusRequiresAuth(t *testing.T) {
	auth := &httpauth.Auth{BearerTokens: []httpauth.NamedSecret{{Name: "ops", Secret: "tok"}}}
	s := testServer(t, auth, nil, baseDeps())

	rec := httptest.NewRecorder()
	s.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no auth: got %d want 401", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("missing challenge header")
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	req.Header.Set("Authorization", "Bearer tok")
	s.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("authed: got %d want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("status not JSON: %v", err)
	}
	if body["version"] != "v1.2.3" || body["instance_id"] != "inst-1" {
		t.Fatalf("status body = %v", body)
	}
}

func TestAuthPassThroughWhenEmpty(t *testing.T) {
	s := testServer(t, &httpauth.Auth{}, nil, baseDeps()) // empty auth => open
	rec := httptest.NewRecorder()
	s.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("empty auth should pass through: got %d", rec.Code)
	}
}

func TestCORS(t *testing.T) {
	auth := &httpauth.Auth{}
	s := testServer(t, auth, []string{"https://ops.example.com"}, baseDeps())

	// preflight
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/v1/status", nil)
	req.Header.Set("Origin", "https://ops.example.com")
	s.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight got %d want 204", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://ops.example.com" {
		t.Fatalf("missing ACAO: %v", rec.Header())
	}
	// disallowed origin => no CORS header
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	s.handler().ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("disallowed origin should get no ACAO")
	}
}

// fakes for Task 8 tests
type fakeFeeds struct{ feeds []config.FeedConfig }

func (f fakeFeeds) Desired(context.Context) ([]config.FeedConfig, error) { return f.feeds, nil }

type fakeState struct {
	meta                       map[string]state.FeedMeta
	itemsRemoved, metaRemoved  int64
	lastItemCutoff, lastMetaCutoff time.Time
}

func (f *fakeState) GetFeedMeta(_ context.Context, feedURL string) (state.FeedMeta, bool, error) {
	m, ok := f.meta[feedURL]
	return m, ok, nil
}
func (f *fakeState) PruneItemsBefore(_ context.Context, c time.Time) (int64, error) {
	f.lastItemCutoff = c
	return f.itemsRemoved, nil
}
func (f *fakeState) PruneFeedMetaBefore(_ context.Context, c time.Time) (int64, error) {
	f.lastMetaCutoff = c
	return f.metaRemoved, nil
}
func (f *fakeState) Ping(context.Context) error { return nil }

func authedGet(s *Server, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestFeedsEnvelope(t *testing.T) {
	d := baseDeps()
	d.Feeds = fakeFeeds{feeds: []config.FeedConfig{{URL: "https://a.com/f", Interval: 5 * time.Minute}}}
	d.State = &fakeState{meta: map[string]state.FeedMeta{"https://a.com/f": {ETag: `"x"`, LastModified: time.Unix(1000, 0).UTC()}}}
	s := testServer(t, &httpauth.Auth{}, nil, d)

	rec := authedGet(s, "/v1/feeds")
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
	var env struct {
		Feeds []map[string]any `json:"feeds"`
		Total int              `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Total != 1 || len(env.Feeds) != 1 {
		t.Fatalf("envelope = %+v", env)
	}
	if env.Feeds[0]["url"] != "https://a.com/f" || env.Feeds[0]["etag"] != `"x"` || env.Feeds[0]["owned"] != true {
		t.Fatalf("feed = %+v", env.Feeds[0])
	}
}

func TestFeedByIDNotFound(t *testing.T) {
	d := baseDeps()
	d.Feeds = fakeFeeds{feeds: []config.FeedConfig{{URL: "https://a.com/f", Interval: time.Minute}}}
	d.State = &fakeState{meta: map[string]state.FeedMeta{}}
	s := testServer(t, &httpauth.Auth{}, nil, d)

	if rec := authedGet(s, "/v1/feeds/"+url.PathEscape("https://nope.com/x")); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown feed: got %d want 404", rec.Code)
	}
	if rec := authedGet(s, "/v1/feeds/"+url.PathEscape("https://a.com/f")); rec.Code != http.StatusOK {
		t.Fatalf("known feed: got %d want 200", rec.Code)
	}
}

type fakeMembers struct {
	self    string
	members []string
}

func (f fakeMembers) Self() string      { return f.self }
func (f fakeMembers) Members() []string { return f.members }

func TestMembersSingleInstance(t *testing.T) {
	d := baseDeps() // Members == nil
	s := testServer(t, &httpauth.Auth{}, nil, d)
	rec := authedGet(s, "/v1/members")
	var body struct {
		Self    string   `json:"self"`
		Members []string `json:"members"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Self != "inst-1" || len(body.Members) != 1 || body.Members[0] != "inst-1" {
		t.Fatalf("single-instance members = %+v", body)
	}
}

func TestMembersClustered(t *testing.T) {
	d := baseDeps()
	d.Members = fakeMembers{self: "inst-1", members: []string{"inst-1", "inst-2"}}
	d.Feeds = fakeFeeds{feeds: []config.FeedConfig{{URL: "https://a.com/f", Interval: time.Minute}}}
	s := testServer(t, &httpauth.Auth{}, nil, d)
	rec := authedGet(s, "/v1/members")
	var body struct {
		Self      string            `json:"self"`
		Members   []string          `json:"members"`
		Ownership map[string]string `json:"ownership"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Members) != 2 {
		t.Fatalf("members = %v", body.Members)
	}
	if owner, ok := body.Ownership["https://a.com/f"]; !ok || (owner != "inst-1" && owner != "inst-2") {
		t.Fatalf("ownership = %v", body.Ownership)
	}
}

func TestHealthEndpoint(t *testing.T) {
	d := baseDeps()
	d.Checks = []health.Check{
		{Name: "state", Fn: func(context.Context) error { return nil }},
		{Name: "coordination", Fn: func(context.Context) error { return errors.New("down") }},
	}
	s := testServer(t, &httpauth.Auth{}, nil, d)
	rec := authedGet(s, "/v1/health")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("one failing check => got %d want 503", rec.Code)
	}
	var body struct {
		OK     bool              `json:"ok"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.OK || body.Checks["state"] != "ok" || body.Checks["coordination"] != "down" {
		t.Fatalf("health body = %+v", body)
	}
}

var _ = context.Background
