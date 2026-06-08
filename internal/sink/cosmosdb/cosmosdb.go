// Package cosmosdb implements the sink.Publisher interface against an Azure
// Cosmos DB (NoSQL / Core API) container via the official azcosmos SDK. Each
// Change is written as a JSON document keyed by a stable id derived from
// (feed_url, item_id); the container is partitioned on /feed_url. Delivery is
// idempotent: a 409 Conflict (document already present) is treated as a no-op,
// mirroring the postgres sink's ON CONFLICT DO NOTHING.
package cosmosdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/iambod/rss2msg/internal/model"
)

const (
	defaultContainer = "feed_changes"
	partitionKeyPath = "/feed_url"
)

// Options configures a Cosmos DB Publisher. Exactly one auth field (Endpoint or
// ConnectionString) must be set.
type Options struct {
	Name string // sink name (required)

	// Endpoint is the Cosmos DB account endpoint (e.g.
	// "https://acct.documents.azure.com:443/"); when set, the sink
	// authenticates with DefaultAzureCredential (env / workload identity /
	// managed identity). Mutually exclusive with ConnectionString.
	Endpoint string

	// ConnectionString authenticates with an account-key connection string.
	// Mutually exclusive with Endpoint.
	ConnectionString string

	Database  string // required
	Container string // defaults to "feed_changes"

	// CreateIfMissing creates the database and container on New() if absent.
	// Intended for dev/test; production should pre-provision.
	CreateIfMissing bool

	// Throughput, when > 0 and CreateIfMissing is set, provisions the created
	// container with manual RU/s. 0 leaves throughput unset (serverless or
	// database-shared throughput).
	Throughput int32

	// ClientOptions, if non-nil, is passed through to the azcosmos client.
	// Used by integration tests to target the emulator endpoint with its
	// self-signed certificate.
	ClientOptions *azcosmos.ClientOptions
}

// Publisher writes Change documents to a Cosmos DB container.
type Publisher struct {
	name      string
	container *azcosmos.ContainerClient
}

// New validates options, builds a Cosmos client (account key or Entra ID),
// optionally provisions the database/container, and returns a ready Publisher.
func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("cosmosdb sink: name is required")
	}
	hasEndpoint := opts.Endpoint != ""
	hasConn := opts.ConnectionString != ""
	switch {
	case !hasEndpoint && !hasConn:
		return nil, fmt.Errorf("cosmosdb sink %q: one of endpoint or connection_string is required", opts.Name)
	case hasEndpoint && hasConn:
		return nil, fmt.Errorf("cosmosdb sink %q: endpoint and connection_string are mutually exclusive", opts.Name)
	}
	if opts.Database == "" {
		return nil, fmt.Errorf("cosmosdb sink %q: database is required", opts.Name)
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
			return nil, fmt.Errorf("cosmosdb sink %q: %w", opts.Name, err)
		}
	}

	cc, err := client.NewContainer(opts.Database, container)
	if err != nil {
		return nil, fmt.Errorf("cosmosdb sink %q: container handle: %w", opts.Name, err)
	}
	return &Publisher{name: opts.Name, container: cc}, nil
}

func newClient(opts Options) (*azcosmos.Client, error) {
	if opts.ConnectionString != "" {
		client, err := azcosmos.NewClientFromConnectionString(opts.ConnectionString, opts.ClientOptions)
		if err != nil {
			return nil, fmt.Errorf("cosmosdb sink %q: client from connection string: %w", opts.Name, err)
		}
		return client, nil
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("cosmosdb sink %q: default azure credential: %w", opts.Name, err)
	}
	client, err := azcosmos.NewClient(opts.Endpoint, cred, opts.ClientOptions)
	if err != nil {
		return nil, fmt.Errorf("cosmosdb sink %q: client for endpoint %q: %w", opts.Name, opts.Endpoint, err)
	}
	return client, nil
}

// provision creates the database and container if they do not already exist.
// An existing resource (409 Conflict) is not an error.
func provision(ctx context.Context, client *azcosmos.Client, database, container string, throughput int32) error {
	if _, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: database}, nil); err != nil && !isConflict(err) {
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
	if _, err := db.CreateContainer(ctx, props, copts); err != nil && !isConflict(err) {
		return fmt.Errorf("create container %q: %w", container, err)
	}
	return nil
}

func (p *Publisher) Name() string { return p.name }

// Close releases the Publisher. The azcosmos client holds no long-lived
// connections (it is HTTP-based), so there is nothing to close.
func (p *Publisher) Close() error { return nil }

// Publish upserts the change as a document. Delivery is idempotent on the
// derived id: a 409 Conflict means the document already exists, which is
// treated as a successful (already-delivered) no-op.
func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	_, pk, body, err := buildDocument(change)
	if err != nil {
		return fmt.Errorf("cosmosdb sink %q: %w", p.name, err)
	}
	_, err = p.container.CreateItem(ctx, azcosmos.NewPartitionKeyString(pk), body, nil)
	if err != nil {
		if isConflict(err) {
			return nil
		}
		return fmt.Errorf("cosmosdb sink %q: create item: %w", p.name, err)
	}
	return nil
}

// buildDocument renders a Change into a Cosmos document. It is a pure function
// (no network) so the wire layout can be unit-tested. It returns the document
// id, the partition-key value (feed_url), and the marshalled body.
//
// The Cosmos-required "id" is spliced into the marshalled Change rather than
// added via struct embedding: model.Change has a custom MarshalJSON, which
// embedding would promote and thereby drop an outer id field.
func buildDocument(change model.Change) (id, partitionKey string, body []byte, err error) {
	id = docID(change.FeedURL, change.ItemID)
	raw, err := json.Marshal(change)
	if err != nil {
		return "", "", nil, fmt.Errorf("marshal: %w", err)
	}
	idField := fmt.Sprintf(`{"id":%q`, id)
	if len(raw) > 2 { // non-empty object: "{...}" -> {"id":"..",...}
		body = append([]byte(idField+","), raw[1:]...)
	} else { // defensive: empty "{}" -> {"id":".."}
		body = []byte(idField + "}")
	}
	return id, change.FeedURL, body, nil
}

// docID is a stable, collision-resistant id derived from (feedURL, itemID).
// The NUL separator prevents ambiguity between feed/item boundaries.
func docID(feedURL, itemID string) string {
	h := sha256.Sum256([]byte(feedURL + "\x00" + itemID))
	return hex.EncodeToString(h[:])
}

// isConflict reports whether err is an HTTP 409 from Cosmos DB (document or
// resource already exists).
func isConflict(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == http.StatusConflict
	}
	return false
}
