// Package cosmosdb provides a Coordinator backed by an Azure Cosmos DB (NoSQL /
// Core API) lease document with an explicit expiry timestamp.
//
// Cosmos DB has no server-side session, so crash-safe auto-release cannot come
// from a dropped connection the way the Postgres (session-scoped advisory lock)
// coordinator gets it. Instead each lease carries an explicit lease_expiry
// (epoch millis) that the acquirer checks: a competing instance may steal the
// lock once now >= lease_expiry. We deliberately do NOT rely on Cosmos native
// TTL for lock liveness — TTL deletion can lag, which is useless for a lock.
//
// Cosmos has no arbitrary conditional-write expressions, so atomicity comes
// from optimistic concurrency instead: CreateItem fails with 409 Conflict when
// the lock already exists, and a stale (expired) lock is reclaimed with
// ReplaceItem + If-Match:<etag> — a 412 PreconditionFailed means another
// instance won the race. Release is a conditional DeleteItem (If-Match:<our
// etag>) so we never delete a lease a peer re-acquired after our expiry.
//
// Each process generates one unique owner token at startup
// (hostname-pid-randomhex), mirroring the DynamoDB coordinator.
//
// IMPORTANT operational note: LeaseDuration must safely exceed the maximum
// per-feed poll time. If a poll runs longer than the lease, a peer can steal
// the lock mid-poll and both instances poll concurrently. Size it above your
// worst-case poll duration. Default is 60s.
package cosmosdb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/coord"
)

const (
	// defaultContainer is the container used when Options.Container is empty.
	defaultContainer = "coordination_locks"
	// partitionKeyPath partitions each lock into its own logical partition.
	partitionKeyPath = "/pk"
)

// DefaultLeaseDuration is used when Options.LeaseDuration is zero.
const DefaultLeaseDuration = 60 * time.Second

// Options configures the Cosmos DB-backed Coordinator. Exactly one auth field
// (Endpoint or ConnectionString) must be set.
type Options struct {
	// Endpoint is the Cosmos DB account endpoint (e.g.
	// "https://acct.documents.azure.com:443/"); when set, the coordinator
	// authenticates with DefaultAzureCredential (env / workload identity /
	// managed identity). Mutually exclusive with ConnectionString.
	Endpoint string

	// ConnectionString authenticates with an account-key connection string.
	// Mutually exclusive with Endpoint.
	ConnectionString string

	Database  string // required
	Container string // defaults to "coordination_locks"

	// CreateIfMissing creates the database and container on New() if absent.
	// Intended for dev/test; production should pre-provision.
	CreateIfMissing bool

	// Throughput, when > 0 and CreateIfMissing is set, provisions the created
	// container with manual RU/s. 0 leaves throughput unset (serverless or
	// database-shared throughput).
	Throughput int32

	// LeaseDuration is how long an acquired lease is valid before a peer may
	// steal it. 0 -> DefaultLeaseDuration.
	LeaseDuration time.Duration

	// Owner, if set, overrides the auto-generated per-process owner token.
	// Tests use this to simulate distinct instances. Leave empty in production.
	Owner string

	// ClientOptions, if non-nil, is passed through to the azcosmos client.
	// Used by integration tests to target the emulator endpoint with its
	// self-signed certificate.
	ClientOptions *azcosmos.ClientOptions
}

// containerAPI is the subset of *azcosmos.ContainerClient the coordinator uses.
// It keeps the implementation unit-testable with an in-memory fake.
type containerAPI interface {
	CreateItem(ctx context.Context, partitionKey azcosmos.PartitionKey, item []byte, o *azcosmos.ItemOptions) (azcosmos.ItemResponse, error)
	ReadItem(ctx context.Context, partitionKey azcosmos.PartitionKey, itemID string, o *azcosmos.ItemOptions) (azcosmos.ItemResponse, error)
	ReplaceItem(ctx context.Context, partitionKey azcosmos.PartitionKey, itemID string, item []byte, o *azcosmos.ItemOptions) (azcosmos.ItemResponse, error)
	DeleteItem(ctx context.Context, partitionKey azcosmos.PartitionKey, itemID string, o *azcosmos.ItemOptions) (azcosmos.ItemResponse, error)
}

// leaseDoc is the wire layout of a lock document.
type leaseDoc struct {
	ID          string `json:"id"`
	PK          string `json:"pk"`
	Owner       string `json:"owner"`
	LeaseExpiry int64  `json:"lease_expiry"` // epoch millis
}

type lease struct {
	key   string
	owner string
	etag  azcore.ETag
}

// Coordinator implements coord.Coordinator over a Cosmos DB lock container.
type Coordinator struct {
	container     containerAPI
	owner         string
	leaseDuration time.Duration

	// now is overridable in tests; defaults to time.Now.
	now func() time.Time

	mu   sync.Mutex
	held map[*lease]struct{} // nil after Close
}

// New validates options, builds a Cosmos client (account key or Entra ID),
// optionally provisions the database/container, and returns a ready
// Coordinator with a fresh owner token.
func New(ctx context.Context, opts Options) (*Coordinator, error) {
	hasEndpoint := opts.Endpoint != ""
	hasConn := opts.ConnectionString != ""
	switch {
	case !hasEndpoint && !hasConn:
		return nil, fmt.Errorf("coord/cosmosdb: one of endpoint or connection_string is required")
	case hasEndpoint && hasConn:
		return nil, fmt.Errorf("coord/cosmosdb: endpoint and connection_string are mutually exclusive")
	}
	if opts.Database == "" {
		return nil, fmt.Errorf("coord/cosmosdb: database is required")
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
		if err := provision(ctx, client, opts.Database, container, opts.Throughput); err != nil {
			return nil, fmt.Errorf("coord/cosmosdb: %w", err)
		}
	}
	cc, err := client.NewContainer(opts.Database, container)
	if err != nil {
		return nil, fmt.Errorf("coord/cosmosdb: container handle: %w", err)
	}
	return newWithContainer(cc, opts), nil
}

// newWithContainer builds a Coordinator around an already-constructed container
// client. Used by New and by unit tests that inject a fake.
func newWithContainer(container containerAPI, opts Options) *Coordinator {
	ld := opts.LeaseDuration
	if ld <= 0 {
		ld = DefaultLeaseDuration
	}
	owner := opts.Owner
	if owner == "" {
		owner = newOwnerToken()
	}
	return &Coordinator{
		container:     container,
		owner:         owner,
		leaseDuration: ld,
		now:           time.Now,
		held:          make(map[*lease]struct{}),
	}
}

func newClient(opts Options) (*azcosmos.Client, error) {
	if opts.ConnectionString != "" {
		client, err := azcosmos.NewClientFromConnectionString(opts.ConnectionString, opts.ClientOptions)
		if err != nil {
			return nil, fmt.Errorf("coord/cosmosdb: client from connection string: %w", err)
		}
		return client, nil
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("coord/cosmosdb: default azure credential: %w", err)
	}
	client, err := azcosmos.NewClient(opts.Endpoint, cred, opts.ClientOptions)
	if err != nil {
		return nil, fmt.Errorf("coord/cosmosdb: client for endpoint %q: %w", opts.Endpoint, err)
	}
	return client, nil
}

// provision creates the database and container if they do not already exist.
// An existing resource (409 Conflict) is not an error.
func provision(ctx context.Context, client *azcosmos.Client, database, container string, throughput int32) error {
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

// Owner returns this coordinator's owner token (exported for tests/debugging).
func (c *Coordinator) Owner() string { return c.owner }

// TryAcquire writes the lease document. It first tries CreateItem (wins when no
// lock exists); on a 409 Conflict it reads the current lease and, only if it
// has expired, reclaims it with a ReplaceItem guarded by the document's ETag.
func (c *Coordinator) TryAcquire(ctx context.Context, feedURL string) (coord.ReleaseFunc, bool, error) {
	key := lockKey(feedURL)
	pk := azcosmos.NewPartitionKeyString(key)
	now := c.now()

	body, err := marshalLease(key, c.owner, now.Add(c.leaseDuration))
	if err != nil {
		return nil, false, fmt.Errorf("coord/cosmosdb: marshal lease: %w", err)
	}

	resp, err := c.container.CreateItem(ctx, pk, body, nil)
	if err != nil {
		if !isStatus(err, http.StatusConflict) {
			return nil, false, fmt.Errorf("coord/cosmosdb: CreateItem: %w", err)
		}
		// A lock document exists; reclaim it iff its lease has expired.
		return c.tryReclaim(ctx, key, pk, now)
	}
	return c.recordHeld(&lease{key: key, owner: c.owner, etag: resp.ETag})
}

// tryReclaim reads the current lease and, if expired, steals it via an
// ETag-guarded ReplaceItem. Any lost race (412/404) or a live lease yields
// (nil, false, nil): the caller simply skips this poll cycle.
func (c *Coordinator) tryReclaim(ctx context.Context, key string, pk azcosmos.PartitionKey, now time.Time) (coord.ReleaseFunc, bool, error) {
	read, err := c.container.ReadItem(ctx, pk, key, nil)
	if err != nil {
		if isStatus(err, http.StatusNotFound) {
			// Raced with a delete between our CreateItem conflict and this read.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("coord/cosmosdb: ReadItem: %w", err)
	}
	var cur leaseDoc
	if err := json.Unmarshal(read.Value, &cur); err != nil {
		return nil, false, fmt.Errorf("coord/cosmosdb: unmarshal lease: %w", err)
	}
	if cur.LeaseExpiry > now.UnixMilli() {
		// A live owner holds the lease.
		return nil, false, nil
	}

	body, err := marshalLease(key, c.owner, now.Add(c.leaseDuration))
	if err != nil {
		return nil, false, fmt.Errorf("coord/cosmosdb: marshal lease: %w", err)
	}
	etag := read.ETag
	resp, err := c.container.ReplaceItem(ctx, pk, key, body, &azcosmos.ItemOptions{IfMatchEtag: &etag})
	if err != nil {
		if isStatus(err, http.StatusPreconditionFailed) || isStatus(err, http.StatusNotFound) {
			// Another instance reclaimed or deleted the lease first.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("coord/cosmosdb: ReplaceItem: %w", err)
	}
	return c.recordHeld(&lease{key: key, owner: c.owner, etag: resp.ETag})
}

// recordHeld registers a freshly-won lease and returns its release func. If the
// coordinator is already closing it self-releases the lease immediately.
func (c *Coordinator) recordHeld(l *lease) (coord.ReleaseFunc, bool, error) {
	c.mu.Lock()
	if c.held == nil {
		c.mu.Unlock()
		_ = c.conditionalDelete(l)
		return nil, false, nil
	}
	c.held[l] = struct{}{}
	c.mu.Unlock()

	release := func(_ context.Context) error {
		// Ignore the caller's ctx on purpose: the pipeline defers release on the
		// poll ctx, which is canceled on SIGTERM or per-feed timeout. Releasing
		// on a canceled ctx would error and leak the lease until it expires. Use
		// a fresh bounded ctx instead (mirrors the postgres/dynamodb coordinators).
		c.mu.Lock()
		if c.held == nil {
			c.mu.Unlock()
			return nil
		}
		if _, ok := c.held[l]; !ok {
			c.mu.Unlock()
			return nil
		}
		delete(c.held, l)
		c.mu.Unlock()
		return c.conditionalDelete(l)
	}
	return release, true, nil
}

// conditionalDelete deletes the lease only if we still own the exact version we
// wrote (If-Match on our ETag), on a fresh 5s background ctx. A 412
// PreconditionFailed or 404 means the lease already expired and a different
// owner reclaimed it (or it was reaped) — not an error; we must NOT delete
// their lease.
func (c *Coordinator) conditionalDelete(l *lease) error {
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	etag := l.etag
	_, err := c.container.DeleteItem(delCtx, azcosmos.NewPartitionKeyString(l.key), l.key, &azcosmos.ItemOptions{IfMatchEtag: &etag})
	if err != nil {
		if isStatus(err, http.StatusPreconditionFailed) || isStatus(err, http.StatusNotFound) {
			return nil
		}
		log.Warn().
			Str("coord_driver", "cosmosdb").
			Str("event", "release_error").
			Err(err).
			Msg("coord/cosmosdb: conditional delete failed")
		return fmt.Errorf("coord/cosmosdb: DeleteItem: %w", err)
	}
	return nil
}

// Close best-effort releases every still-held lease, then marks the coordinator
// closed so in-flight TryAcquire winners self-release.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	if c.held == nil {
		c.mu.Unlock()
		return nil
	}
	leases := make([]*lease, 0, len(c.held))
	for l := range c.held {
		leases = append(leases, l)
	}
	c.held = nil
	c.mu.Unlock()

	for _, l := range leases {
		_ = c.conditionalDelete(l)
	}
	return nil
}

// marshalLease renders a lock document for key/owner with the given expiry.
func marshalLease(key, owner string, expiry time.Time) ([]byte, error) {
	return json.Marshal(leaseDoc{
		ID:          key,
		PK:          key,
		Owner:       owner,
		LeaseExpiry: expiry.UnixMilli(),
	})
}

// lockKey derives the partition-key/id value for feedURL. We use the SHA-256
// hex of the URL (the same scheme as the Redis and Postgres coordinators) so
// every backend keys locks identically. Hashing is also required here because
// the value is used as the Cosmos document id and partition key, which forbid
// '/', '\', '?' and '#' — characters that feed URLs routinely contain.
func lockKey(feedURL string) string {
	sum := sha256.Sum256([]byte(feedURL))
	return "rss2msg:coord:" + hex.EncodeToString(sum[:])
}

// newOwnerToken returns a unique per-process owner token:
// hostname-pid-randomhex. crypto/rand makes two processes on the same host with
// recycled PIDs still distinct.
func newOwnerToken() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is catastrophic; fall back to time so we still get
		// a value rather than panicking the whole service at startup.
		return fmt.Sprintf("%s-%d-%x", host, os.Getpid(), time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

// isStatus reports whether err is a Cosmos DB HTTP error with the given status.
func isStatus(err error, status int) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == status
	}
	return false
}

// compile-time assurance the Coordinator satisfies the interface.
var _ coord.Coordinator = (*Coordinator)(nil)
