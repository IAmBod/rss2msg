// Package dynamodb implements a sink.Publisher that writes each change as a
// single item into an Amazon DynamoDB table.
//
// DynamoDB is a datastore target, not a pub/sub broker: Publish performs one
// idempotent PutItem keyed by (partition=feed_url, sort=item_id), so
// re-publishing the same item overwrites the existing row (dedup-friendly).
// Downstream consumers observe new/updated rows via DynamoDB Streams or by
// polling the table; the sink itself does not deliver notifications.
package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/iambod/rss2msg/internal/model"
)

const (
	// DefaultPartitionKey / DefaultSortKey are the attribute names used for the
	// table's primary key when not overridden in config.
	DefaultPartitionKey = "feed_url"
	DefaultSortKey      = "item_id"
)

type Options struct {
	Name        string // required, sink name
	Table       string // required, target table name
	Region      string // optional; SDK default chain when empty
	EndpointURL string // optional; LocalStack/testing

	// PartitionKey / SortKey are the attribute names for the table's primary
	// key. Empty falls back to DefaultPartitionKey / DefaultSortKey. The
	// partition attribute is filled with the change's feed_url and the sort
	// attribute with its item_id.
	PartitionKey string
	SortKey      string

	// TTLAttribute, when set, adds a Number attribute holding a Unix-epoch
	// (seconds) expiry computed as detected_at + ItemTTL. It must match the
	// table's configured TTL attribute for DynamoDB to expire the row.
	TTLAttribute string
	ItemTTL      time.Duration
}

type Publisher struct {
	name         string
	client       *dynamodb.Client
	table        string
	partitionKey string
	sortKey      string
	ttlAttribute string
	itemTTL      time.Duration
}

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("dynamodb sink: name is required")
	}
	if opts.Table == "" {
		return nil, fmt.Errorf("dynamodb sink %q: table is required", opts.Name)
	}
	if opts.TTLAttribute == "" && opts.ItemTTL != 0 {
		return nil, fmt.Errorf("dynamodb sink %q: item_ttl set but ttl_attribute is empty", opts.Name)
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if opts.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(opts.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("dynamodb sink %q: load aws config: %w", opts.Name, err)
	}

	clientOpts := []func(*dynamodb.Options){}
	if opts.EndpointURL != "" {
		endpoint := opts.EndpointURL
		clientOpts = append(clientOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	client := dynamodb.NewFromConfig(awsCfg, clientOpts...)

	partitionKey := opts.PartitionKey
	if partitionKey == "" {
		partitionKey = DefaultPartitionKey
	}
	sortKey := opts.SortKey
	if sortKey == "" {
		sortKey = DefaultSortKey
	}

	return &Publisher{
		name:         opts.Name,
		client:       client,
		table:        opts.Table,
		partitionKey: partitionKey,
		sortKey:      sortKey,
		ttlAttribute: opts.TTLAttribute,
		itemTTL:      opts.ItemTTL,
	}, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error { return nil }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	item, err := p.item(change)
	if err != nil {
		return fmt.Errorf("dynamodb sink %q: marshal: %w", p.name, err)
	}
	if _, err := p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(p.table),
		Item:      item,
	}); err != nil {
		return fmt.Errorf("dynamodb sink %q: PutItem: %w", p.name, err)
	}
	return nil
}

// item builds the attribute map persisted for a change. The change is rendered
// through its json marshaller (so attribute names match the snake_case json
// schema and the model's normalisation applies), decoded into a generic map,
// then marshalled to DynamoDB attribute values. The primary-key attributes are
// overlaid afterwards so they always carry feed_url/item_id regardless of the
// configured attribute names. An optional TTL attribute is added.
func (p *Publisher) item(change model.Change) (map[string]ddbtypes.AttributeValue, error) {
	raw, err := json.Marshal(change)
	if err != nil {
		return nil, err
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, err
	}
	item, err := attributevalue.MarshalMap(generic)
	if err != nil {
		return nil, err
	}
	item[p.partitionKey] = &ddbtypes.AttributeValueMemberS{Value: change.FeedURL}
	item[p.sortKey] = &ddbtypes.AttributeValueMemberS{Value: change.ItemID}

	if p.ttlAttribute != "" {
		expiry := change.DetectedAt.Add(p.itemTTL).Unix()
		item[p.ttlAttribute] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(expiry, 10)}
	}
	return item, nil
}
