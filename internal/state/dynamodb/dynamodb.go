// Package dynamodb provides a state.Store backed by Amazon DynamoDB.
//
// Schema: a single table with a composite primary key — partition key
// "feed_url" (string) and sort key "item_id" (string). Per-item seen-state
// rows use the item's ID as the sort key; a feed's HTTP cache validators
// (ETag, Last-Modified) live under the same partition with a reserved
// sentinel sort key (metaSK) so a feed's meta and its items share a
// partition and read/write together cheaply.
//
// DynamoDB is a shared, distributed-safe store: every rss2msg instance reads
// and writes the same table, so cross-instance dedup works (unlike the
// per-instance SQLite store). UpsertItem/UpsertFeedMeta are idempotent
// PutItems.
//
// When ItemTTL is configured, item rows are written with a TTL attribute
// (epoch seconds) so DynamoDB auto-prunes old seen-items; meta rows are never
// given a TTL.
package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/iambod/rss2msg/internal/state"
)

const (
	// pkAttr / skAttr are the composite primary key attribute names.
	pkAttr = "feed_url"
	skAttr = "item_id"

	// metaSK is the reserved sort key under which a feed's meta row lives.
	// "#" is not a valid leading character for an RSS/Atom item GUID in
	// practice, so it never collides with a real item ID.
	metaSK = "#META"
)

// Options configures the DynamoDB-backed state Store.
type Options struct {
	// Table is the DynamoDB table name. Required.
	Table string

	// Region selects the AWS region. Empty falls back to the SDK default
	// chain (env, shared config, instance metadata).
	Region string

	// EndpointURL overrides the service endpoint, e.g. for LocalStack or
	// DynamoDB Local. Empty uses the real AWS endpoint.
	EndpointURL string

	// TTLAttribute, when set, names the DynamoDB TTL attribute written on
	// item rows (epoch seconds). It must match the attribute the table's
	// TimeToLiveSpecification points at for auto-pruning to take effect.
	// Empty disables TTL writes.
	TTLAttribute string

	// ItemTTL is how long after last_seen an item row should live before
	// DynamoDB prunes it. Only meaningful when TTLAttribute is set; zero
	// disables TTL writes.
	ItemTTL time.Duration
}

// Store implements state.Store on top of a single DynamoDB table.
type Store struct {
	client       *dynamodb.Client
	table        string
	ttlAttribute string
	itemTTL      time.Duration
}

// New constructs a Store. It loads AWS config (region/credentials) via the
// default chain and applies the optional endpoint override. It does not
// create the table — operators provision the table (and its TTL spec) out of
// band; use Ping to verify reachability.
func New(ctx context.Context, opts Options) (*Store, error) {
	if opts.Table == "" {
		return nil, fmt.Errorf("state/dynamodb: table is required")
	}
	if (opts.TTLAttribute == "") != (opts.ItemTTL <= 0) {
		return nil, fmt.Errorf("state/dynamodb: ttl_attribute and item_ttl must both be set or both empty")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if opts.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(opts.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("state/dynamodb: load aws config: %w", err)
	}

	clientOpts := []func(*dynamodb.Options){}
	if opts.EndpointURL != "" {
		endpoint := opts.EndpointURL
		clientOpts = append(clientOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	client := dynamodb.NewFromConfig(awsCfg, clientOpts...)

	return &Store{
		client:       client,
		table:        opts.Table,
		ttlAttribute: opts.TTLAttribute,
		itemTTL:      opts.ItemTTL,
	}, nil
}

func (s *Store) Close() error { return nil }

// Ping verifies the table is reachable and ACTIVE via DescribeTable. Used by
// validate-config.
func (s *Store) Ping(ctx context.Context) error {
	out, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(s.table),
	})
	if err != nil {
		return fmt.Errorf("state/dynamodb: describe table %q: %w", s.table, err)
	}
	if out.Table != nil && out.Table.TableStatus != ddbtypes.TableStatusActive {
		return fmt.Errorf("state/dynamodb: table %q is %s, not ACTIVE", s.table, out.Table.TableStatus)
	}
	return nil
}

func (s *Store) GetItem(ctx context.Context, feedURL, itemID string) (state.ItemState, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key:       itemKey(feedURL, itemID),
		// Strongly consistent: cross-instance dedup must observe the most
		// recent write, not a possibly-stale eventual replica.
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return state.ItemState{}, false, fmt.Errorf("state/dynamodb: GetItem: %w", err)
	}
	if len(out.Item) == 0 {
		return state.ItemState{}, false, nil
	}
	hash, err := stringAttr(out.Item, "content_hash")
	if err != nil {
		return state.ItemState{}, false, err
	}
	seenAt, err := timeAttr(out.Item, "last_seen_at")
	if err != nil {
		return state.ItemState{}, false, err
	}
	return state.ItemState{ContentHash: hash, LastSeenAt: seenAt}, true, nil
}

func (s *Store) UpsertItem(ctx context.Context, feedURL, itemID, hash string, seenAt time.Time) error {
	seenUTC := seenAt.UTC()
	item := map[string]ddbtypes.AttributeValue{
		pkAttr:         &ddbtypes.AttributeValueMemberS{Value: feedURL},
		skAttr:         &ddbtypes.AttributeValueMemberS{Value: itemID},
		"content_hash": &ddbtypes.AttributeValueMemberS{Value: hash},
		"last_seen_at": &ddbtypes.AttributeValueMemberS{Value: seenUTC.Format(time.RFC3339Nano)},
	}
	if s.ttlAttribute != "" && s.itemTTL > 0 {
		expiry := seenUTC.Add(s.itemTTL).Unix()
		item[s.ttlAttribute] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expiry)}
	}
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("state/dynamodb: PutItem (item): %w", err)
	}
	return nil
}

// PruneItemsBefore is a no-op for DynamoDB: old item rows are pruned by the
// service from the write-time TTL attribute (see ItemTTL), so the application
// never scans or deletes. It always returns (0, nil) to satisfy state.Store.
func (s *Store) PruneItemsBefore(_ context.Context, _ time.Time) (int64, error) {
	return 0, nil
}

func (s *Store) GetFeedMeta(ctx context.Context, feedURL string) (state.FeedMeta, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            itemKey(feedURL, metaSK),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return state.FeedMeta{}, false, fmt.Errorf("state/dynamodb: GetItem (meta): %w", err)
	}
	if len(out.Item) == 0 {
		return state.FeedMeta{}, false, nil
	}
	etag, err := stringAttr(out.Item, "etag")
	if err != nil {
		return state.FeedMeta{}, false, err
	}
	meta := state.FeedMeta{ETag: etag}
	// last_modified is optional (absent => zero time).
	if av, ok := out.Item["last_modified"]; ok {
		if sv, ok := av.(*ddbtypes.AttributeValueMemberS); ok && sv.Value != "" {
			t, err := time.Parse(time.RFC3339Nano, sv.Value)
			if err != nil {
				return state.FeedMeta{}, false, fmt.Errorf("state/dynamodb: parse last_modified: %w", err)
			}
			meta.LastModified = t
		}
	}
	return meta, true, nil
}

func (s *Store) UpsertFeedMeta(ctx context.Context, feedURL string, meta state.FeedMeta) error {
	item := map[string]ddbtypes.AttributeValue{
		pkAttr:       &ddbtypes.AttributeValueMemberS{Value: feedURL},
		skAttr:       &ddbtypes.AttributeValueMemberS{Value: metaSK},
		"etag":       &ddbtypes.AttributeValueMemberS{Value: meta.ETag},
		"updated_at": &ddbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
	}
	if !meta.LastModified.IsZero() {
		item["last_modified"] = &ddbtypes.AttributeValueMemberS{Value: meta.LastModified.UTC().Format(time.RFC3339Nano)}
	}
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("state/dynamodb: PutItem (meta): %w", err)
	}
	return nil
}

// itemKey builds the composite primary key for a given partition (feedURL)
// and sort key (itemID or metaSK).
func itemKey(feedURL, sk string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		pkAttr: &ddbtypes.AttributeValueMemberS{Value: feedURL},
		skAttr: &ddbtypes.AttributeValueMemberS{Value: sk},
	}
}

func stringAttr(item map[string]ddbtypes.AttributeValue, name string) (string, error) {
	av, ok := item[name]
	if !ok {
		return "", nil
	}
	sv, ok := av.(*ddbtypes.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("state/dynamodb: attribute %q is not a string", name)
	}
	return sv.Value, nil
}

func timeAttr(item map[string]ddbtypes.AttributeValue, name string) (time.Time, error) {
	s, err := stringAttr(item, name)
	if err != nil {
		return time.Time{}, err
	}
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("state/dynamodb: parse %s: %w", name, err)
	}
	return t, nil
}

// compile-time assurance the Store satisfies the interface.
var _ state.Store = (*Store)(nil)
