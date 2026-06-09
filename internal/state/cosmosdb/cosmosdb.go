// Package cosmosdb provides a state.Store backed by Azure Cosmos DB (NoSQL /
// Core API).
//
// Schema: a single container partitioned on /feed_url. Within a feed's
// partition, per-item seen-state rows use id = sha256hex(item_id) — Cosmos
// document ids forbid the characters '/', '\', '?' and '#', so the raw RSS/Atom
// GUID is hashed and kept in an item_id field for debuggability. A feed's HTTP
// cache validators (ETag, Last-Modified) live under the same partition with a
// reserved id ("__meta__"); underscores can never collide with a 64-char hex
// item id.
//
// Cosmos is a shared, distributed-safe store: every rss2msg instance reads and
// writes the same container, so cross-instance dedup works (unlike the
// per-instance SQLite store). UpsertItem/UpsertFeedMeta are idempotent upserts.
//
// When ItemTTL is configured, item rows are written with Cosmos' reserved `ttl`
// property (integer seconds) so the service auto-prunes old seen-items; meta
// rows are never given a ttl. The container must have TTL enabled
// (DefaultTimeToLive = -1); CreateIfMissing enables it on creation.
package cosmosdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/iambod/rss2msg/internal/state"
)

const (
	// defaultContainer is the container used when Options.Container is empty.
	defaultContainer = "feed_state"
	// partitionKeyPath partitions on the feed URL so a feed's meta and items
	// share a logical partition.
	partitionKeyPath = "/feed_url"
	// metaID is the reserved document id for a feed's meta row. Underscores
	// never appear in a sha256 hex item id, so it cannot collide.
	metaID = "__meta__"
	// ttlEnabled is the DefaultTimeToLive value that turns on per-document TTL
	// without imposing a container-wide default expiry.
	ttlEnabled = int32(-1)
)

// Options configures the Cosmos DB-backed state Store. Exactly one auth field
// (Endpoint or ConnectionString) must be set.
type Options struct {
	// Endpoint is the Cosmos DB account endpoint (e.g.
	// "https://acct.documents.azure.com:443/"); when set, the store
	// authenticates with DefaultAzureCredential (env / workload identity /
	// managed identity). Mutually exclusive with ConnectionString.
	Endpoint string

	// ConnectionString authenticates with an account-key connection string.
	// Mutually exclusive with Endpoint.
	ConnectionString string

	Database  string // required
	Container string // defaults to "feed_state"

	// CreateIfMissing creates the database and container on New() if absent
	// (with TTL enabled when ItemTTL > 0). Intended for dev/test; production
	// should pre-provision.
	CreateIfMissing bool

	// Throughput, when > 0 and CreateIfMissing is set, provisions the created
	// container with manual RU/s. 0 leaves throughput unset (serverless or
	// database-shared throughput).
	Throughput int32

	// ItemTTL is how long after last_seen an item row should live before Cosmos
	// prunes it. 0 disables TTL writes. Requires the container to have TTL
	// enabled (CreateIfMissing does this automatically).
	ItemTTL time.Duration

	// ClientOptions, if non-nil, is passed through to the azcosmos client.
	// Used by integration tests to target the emulator endpoint with its
	// self-signed certificate.
	ClientOptions *azcosmos.ClientOptions
}

// containerAPI is the subset of *azcosmos.ContainerClient the store uses. It
// keeps the implementation unit-testable with an in-memory fake.
type containerAPI interface {
	ReadItem(ctx context.Context, partitionKey azcosmos.PartitionKey, itemID string, o *azcosmos.ItemOptions) (azcosmos.ItemResponse, error)
	UpsertItem(ctx context.Context, partitionKey azcosmos.PartitionKey, item []byte, o *azcosmos.ItemOptions) (azcosmos.ItemResponse, error)
	Read(ctx context.Context, o *azcosmos.ReadContainerOptions) (azcosmos.ContainerResponse, error)
}

// itemDoc is the wire layout of a per-item seen-state row.
type itemDoc struct {
	ID          string `json:"id"`
	FeedURL     string `json:"feed_url"`
	ItemID      string `json:"item_id"`
	ContentHash string `json:"content_hash"`
	LastSeenAt  string `json:"last_seen_at"`
	TTL         *int   `json:"ttl,omitempty"`
}

// metaDoc is the wire layout of a feed's meta row.
type metaDoc struct {
	ID           string `json:"id"`
	FeedURL      string `json:"feed_url"`
	ETag         string `json:"etag"`
	LastModified string `json:"last_modified,omitempty"`
	UpdatedAt    string `json:"updated_at"`
}

// Store implements state.Store on top of a single Cosmos DB container.
type Store struct {
	container containerAPI
	itemTTL   time.Duration
	// now is overridable in tests; defaults to time.Now.
	now func() time.Time
}

// New validates options, builds a Cosmos client (account key or Entra ID),
// optionally provisions the database/container, and returns a ready Store. It
// does not verify connectivity — use Ping for that.
func New(ctx context.Context, opts Options) (*Store, error) {
	hasEndpoint := opts.Endpoint != ""
	hasConn := opts.ConnectionString != ""
	switch {
	case !hasEndpoint && !hasConn:
		return nil, fmt.Errorf("state/cosmosdb: one of endpoint or connection_string is required")
	case hasEndpoint && hasConn:
		return nil, fmt.Errorf("state/cosmosdb: endpoint and connection_string are mutually exclusive")
	}
	if opts.Database == "" {
		return nil, fmt.Errorf("state/cosmosdb: database is required")
	}
	if opts.ItemTTL < 0 {
		return nil, fmt.Errorf("state/cosmosdb: item_ttl must not be negative")
	}
	container := opts.Container
	if container == "" {
		container = defaultContainer
	}

	client, err := newClient(opts)
	if err != nil {
		return nil, err
	}
	if opts.CreateIfMissing {
		if err := provision(ctx, client, opts.Database, container, opts.Throughput, opts.ItemTTL > 0); err != nil {
			return nil, fmt.Errorf("state/cosmosdb: %w", err)
		}
	}
	cc, err := client.NewContainer(opts.Database, container)
	if err != nil {
		return nil, fmt.Errorf("state/cosmosdb: container handle: %w", err)
	}
	return newWithContainer(cc, opts.ItemTTL), nil
}

// newWithContainer builds a Store around an already-constructed container
// client. Used by New and by unit tests that inject a fake.
func newWithContainer(container containerAPI, itemTTL time.Duration) *Store {
	return &Store{container: container, itemTTL: itemTTL, now: time.Now}
}

func newClient(opts Options) (*azcosmos.Client, error) {
	if opts.ConnectionString != "" {
		client, err := azcosmos.NewClientFromConnectionString(opts.ConnectionString, opts.ClientOptions)
		if err != nil {
			return nil, fmt.Errorf("state/cosmosdb: client from connection string: %w", err)
		}
		return client, nil
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("state/cosmosdb: default azure credential: %w", err)
	}
	client, err := azcosmos.NewClient(opts.Endpoint, cred, opts.ClientOptions)
	if err != nil {
		return nil, fmt.Errorf("state/cosmosdb: client for endpoint %q: %w", opts.Endpoint, err)
	}
	return client, nil
}

// provision creates the database and container if absent. When ttlEnabled is
// requested the container is created with DefaultTimeToLive = -1 so per-item
// `ttl` values are honoured. An existing resource (409 Conflict) is not an error.
func provision(ctx context.Context, client *azcosmos.Client, database, container string, throughput int32, enableTTL bool) error {
	if _, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: database}, nil); err != nil && !isStatus(err, http.StatusConflict) {
		return fmt.Errorf("create database %q: %w", database, err)
	}
	db, err := client.NewDatabase(database)
	if err != nil {
		return fmt.Errorf("database handle %q: %w", database, err)
	}
	props := azcosmos.ContainerProperties{
		ID: container,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{partitionKeyPath},
		},
	}
	if enableTTL {
		ttl := ttlEnabled
		props.DefaultTimeToLive = &ttl
	}
	var copts *azcosmos.CreateContainerOptions
	if throughput > 0 {
		tp := azcosmos.NewManualThroughputProperties(throughput)
		copts = &azcosmos.CreateContainerOptions{ThroughputProperties: &tp}
	}
	if _, err := db.CreateContainer(ctx, props, copts); err != nil && !isStatus(err, http.StatusConflict) {
		return fmt.Errorf("create container %q: %w", container, err)
	}
	return nil
}

func (s *Store) Close() error { return nil }

// Ping verifies the container is reachable by reading its properties. Used by
// validate-config.
func (s *Store) Ping(ctx context.Context) error {
	if _, err := s.container.Read(ctx, nil); err != nil {
		return fmt.Errorf("state/cosmosdb: read container: %w", err)
	}
	return nil
}

func (s *Store) GetItem(ctx context.Context, feedURL, itemID string) (state.ItemState, bool, error) {
	resp, err := s.container.ReadItem(ctx, azcosmos.NewPartitionKeyString(feedURL), docID(itemID), nil)
	if err != nil {
		if isStatus(err, http.StatusNotFound) {
			return state.ItemState{}, false, nil
		}
		return state.ItemState{}, false, fmt.Errorf("state/cosmosdb: ReadItem: %w", err)
	}
	var doc itemDoc
	if err := json.Unmarshal(resp.Value, &doc); err != nil {
		return state.ItemState{}, false, fmt.Errorf("state/cosmosdb: unmarshal item: %w", err)
	}
	seenAt, err := parseTime(doc.LastSeenAt)
	if err != nil {
		return state.ItemState{}, false, fmt.Errorf("state/cosmosdb: parse last_seen_at: %w", err)
	}
	return state.ItemState{ContentHash: doc.ContentHash, LastSeenAt: seenAt}, true, nil
}

func (s *Store) UpsertItem(ctx context.Context, feedURL, itemID, hash string, seenAt time.Time) error {
	body, err := json.Marshal(s.buildItemDoc(feedURL, itemID, hash, seenAt))
	if err != nil {
		return fmt.Errorf("state/cosmosdb: marshal item: %w", err)
	}
	if _, err := s.container.UpsertItem(ctx, azcosmos.NewPartitionKeyString(feedURL), body, nil); err != nil {
		return fmt.Errorf("state/cosmosdb: UpsertItem: %w", err)
	}
	return nil
}

// buildItemDoc renders a seen-item row. It is a pure method (apart from the TTL
// clock) so the wire layout can be unit-tested.
func (s *Store) buildItemDoc(feedURL, itemID, hash string, seenAt time.Time) itemDoc {
	doc := itemDoc{
		ID:          docID(itemID),
		FeedURL:     feedURL,
		ItemID:      itemID,
		ContentHash: hash,
		LastSeenAt:  seenAt.UTC().Format(time.RFC3339Nano),
	}
	if s.itemTTL > 0 {
		secs := int(s.itemTTL.Seconds())
		if secs < 1 {
			secs = 1
		}
		doc.TTL = &secs
	}
	return doc
}

func (s *Store) GetFeedMeta(ctx context.Context, feedURL string) (state.FeedMeta, bool, error) {
	resp, err := s.container.ReadItem(ctx, azcosmos.NewPartitionKeyString(feedURL), metaID, nil)
	if err != nil {
		if isStatus(err, http.StatusNotFound) {
			return state.FeedMeta{}, false, nil
		}
		return state.FeedMeta{}, false, fmt.Errorf("state/cosmosdb: ReadItem (meta): %w", err)
	}
	var doc metaDoc
	if err := json.Unmarshal(resp.Value, &doc); err != nil {
		return state.FeedMeta{}, false, fmt.Errorf("state/cosmosdb: unmarshal meta: %w", err)
	}
	meta := state.FeedMeta{ETag: doc.ETag}
	if doc.LastModified != "" {
		t, err := parseTime(doc.LastModified)
		if err != nil {
			return state.FeedMeta{}, false, fmt.Errorf("state/cosmosdb: parse last_modified: %w", err)
		}
		meta.LastModified = t
	}
	return meta, true, nil
}

func (s *Store) UpsertFeedMeta(ctx context.Context, feedURL string, meta state.FeedMeta) error {
	doc := metaDoc{
		ID:        metaID,
		FeedURL:   feedURL,
		ETag:      meta.ETag,
		UpdatedAt: s.now().UTC().Format(time.RFC3339Nano),
	}
	if !meta.LastModified.IsZero() {
		doc.LastModified = meta.LastModified.UTC().Format(time.RFC3339Nano)
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("state/cosmosdb: marshal meta: %w", err)
	}
	if _, err := s.container.UpsertItem(ctx, azcosmos.NewPartitionKeyString(feedURL), body, nil); err != nil {
		return fmt.Errorf("state/cosmosdb: UpsertItem (meta): %w", err)
	}
	return nil
}

// docID derives a Cosmos-safe document id from a raw item GUID. Cosmos ids
// forbid '/', '\', '?' and '#', so we hash; the partition key (feed_url)
// already scopes uniqueness, so a per-item hash is sufficient.
func docID(itemID string) string {
	h := sha256.Sum256([]byte(itemID))
	return hex.EncodeToString(h[:])
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

// isStatus reports whether err is a Cosmos DB HTTP error with the given status.
func isStatus(err error, status int) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == status
	}
	return false
}

// compile-time assurance the Store satisfies the interface.
var _ state.Store = (*Store)(nil)
