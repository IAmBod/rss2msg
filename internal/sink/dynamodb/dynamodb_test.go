//go:build integration

package dynamodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink/dynamodb"
	"github.com/iambod/rss2msg/test/awslocal"
)

const (
	region    = "us-east-1"
	tableName = "rss2msg-changes"
)

func setup(t *testing.T) (endpoint string, client *awsddb.Client) {
	t.Helper()
	ctx := context.Background()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ls := awslocal.RunWithServices(ctx, t, "dynamodb")

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	client = awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(ls.Endpoint)
	})

	_, err = client.CreateTable(ctx, &awsddb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("feed_url"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("item_id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("feed_url"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("item_id"), KeyType: ddbtypes.KeyTypeRange},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter := awsddb.NewTableExistsWaiter(client)
	if err := waiter.Wait(ctx, &awsddb.DescribeTableInput{TableName: aws.String(tableName)}, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	return ls.Endpoint, client
}

func getItem(t *testing.T, client *awsddb.Client, feedURL, itemID string) map[string]ddbtypes.AttributeValue {
	t.Helper()
	out, err := client.GetItem(context.Background(), &awsddb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]ddbtypes.AttributeValue{
			"feed_url": &ddbtypes.AttributeValueMemberS{Value: feedURL},
			"item_id":  &ddbtypes.AttributeValueMemberS{Value: itemID},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	return out.Item
}

func strAttr(t *testing.T, item map[string]ddbtypes.AttributeValue, key string) string {
	t.Helper()
	v, ok := item[key].(*ddbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatalf("attribute %q missing or not a string: %v", key, item[key])
	}
	return v.Value
}

func TestPublishWritesItem(t *testing.T) {
	endpoint, client := setup(t)
	pub, err := dynamodb.New(context.Background(), dynamodb.Options{
		Name: "test", Table: tableName, Region: region, EndpointURL: endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	when := time.Now().UTC().Truncate(time.Second)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "https://e/feed", ItemID: "i1",
		Kind: model.ChangeNew, Title: "hello", ContentHash: "h1", DetectedAt: when,
	}
	if err := pub.Publish(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	item := getItem(t, client, "https://e/feed", "i1")
	if len(item) == 0 {
		t.Fatal("item not found")
	}
	if got := strAttr(t, item, "feed_url"); got != "https://e/feed" {
		t.Fatalf("feed_url=%q", got)
	}
	if got := strAttr(t, item, "item_id"); got != "i1" {
		t.Fatalf("item_id=%q", got)
	}
	if got := strAttr(t, item, "kind"); got != "new" {
		t.Fatalf("kind=%q", got)
	}
	if got := strAttr(t, item, "title"); got != "hello" {
		t.Fatalf("title=%q", got)
	}
	if got := strAttr(t, item, "content_hash"); got != "h1" {
		t.Fatalf("content_hash=%q", got)
	}
}

func TestPublishIsIdempotent(t *testing.T) {
	endpoint, client := setup(t)
	pub, err := dynamodb.New(context.Background(), dynamodb.Options{
		Name: "test", Table: tableName, Region: region, EndpointURL: endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	ctx := context.Background()
	base := model.Change{
		SchemaVersion: 1, FeedURL: "https://e/feed", ItemID: "dup",
		Kind: model.ChangeNew, Title: "v1", ContentHash: "h1", DetectedAt: time.Now().UTC(),
	}
	if err := pub.Publish(ctx, base); err != nil {
		t.Fatal(err)
	}
	// Re-publish the same item with updated fields: same key overwrites in place.
	updated := base
	updated.Kind = model.ChangeUpdated
	updated.Title = "v2"
	if err := pub.Publish(ctx, updated); err != nil {
		t.Fatal(err)
	}

	// Scan the table: exactly one row for the (feed_url,item_id) key.
	out, err := client.Scan(ctx, &awsddb.ScanInput{TableName: aws.String(tableName), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 1 {
		t.Fatalf("expected exactly 1 item after idempotent re-publish, got %d", out.Count)
	}
	item := getItem(t, client, "https://e/feed", "dup")
	if got := strAttr(t, item, "title"); got != "v2" {
		t.Fatalf("title=%q want overwrite to v2", got)
	}
	if got := strAttr(t, item, "kind"); got != "updated" {
		t.Fatalf("kind=%q want updated", got)
	}
}
