//go:build integration

package dynamodb_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	coordddb "github.com/iambod/rss2msg/internal/coord/dynamodb"
	"github.com/iambod/rss2msg/test/awslocal"
)

// lockKeyFor mirrors production lockKey for test use: sha256 hex of the feed URL.
func lockKeyFor(feedURL string) string {
	sum := sha256.Sum256([]byte(feedURL))
	return "rss2msg:coord:" + hex.EncodeToString(sum[:])
}

const (
	region = "us-east-1"
	table  = "rss2msg-coord-locks"
	pkAttr = "pk"
)

// setup boots LocalStack with DynamoDB and creates the lock table (HASH key "pk").
func setup(t *testing.T) (endpoint string, admin *awsddb.Client) {
	t.Helper()
	ctx := context.Background()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ls := awslocal.RunWithServices(ctx, t, "dynamodb")

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	admin = awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(ls.Endpoint)
	})

	_, err = admin.CreateTable(ctx, &awsddb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String(pkAttr), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String(pkAttr), KeyType: ddbtypes.KeyTypeHash},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	waiter := awsddb.NewTableExistsWaiter(admin)
	if err := waiter.Wait(ctx, &awsddb.DescribeTableInput{TableName: aws.String(table)}, 60*time.Second); err != nil {
		t.Fatalf("wait table: %v", err)
	}
	return ls.Endpoint, admin
}

func newCoord(t *testing.T, endpoint, owner string, lease time.Duration) *coordddb.Coordinator {
	t.Helper()
	c, err := coordddb.New(context.Background(), coordddb.Options{
		Table:         table,
		Region:        region,
		EndpointURL:   endpoint,
		LeaseDuration: lease,
		Owner:         owner,
	})
	if err != nil {
		t.Fatalf("new coordinator (%s): %v", owner, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDynamoDBFirstAcquireWinsSecondBlocked(t *testing.T) {
	endpoint, _ := setup(t)
	a := newCoord(t, endpoint, "owner-a", time.Minute)
	b := newCoord(t, endpoint, "owner-b", time.Minute)

	rel, ok, err := a.TryAcquire(context.Background(), "feedX")
	if err != nil || !ok {
		t.Fatalf("a acquire: ok=%v err=%v", ok, err)
	}
	if _, ok, err := b.TryAcquire(context.Background(), "feedX"); err != nil || ok {
		t.Fatalf("b should be blocked: ok=%v err=%v", ok, err)
	}

	// After a releases, b can acquire.
	if err := rel(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok, err := b.TryAcquire(context.Background(), "feedX"); err != nil || !ok {
		t.Fatalf("b should acquire after release: ok=%v err=%v", ok, err)
	}
}

func TestDynamoDBExpiredLeaseStolenByDifferentOwner(t *testing.T) {
	endpoint, admin := setup(t)
	// Short lease so we can let it lapse; then steal with a different owner.
	a := newCoord(t, endpoint, "owner-a", time.Hour) // long, then we force-expire it
	if _, ok, err := a.TryAcquire(context.Background(), "feedE"); err != nil || !ok {
		t.Fatalf("a acquire: ok=%v err=%v", ok, err)
	}

	// Manually expire a's lease by rewriting lease_expiry into the past.
	key := lockKeyFor("feedE")
	past := strconv.FormatInt(time.Now().Add(-time.Minute).UnixMilli(), 10)
	_, err := admin.UpdateItem(context.Background(), &awsddb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbtypes.AttributeValue{
			pkAttr: &ddbtypes.AttributeValueMemberS{Value: key},
		},
		UpdateExpression: aws.String("SET lease_expiry = :p"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":p": &ddbtypes.AttributeValueMemberN{Value: past},
		},
	})
	if err != nil {
		t.Fatalf("force-expire: %v", err)
	}

	b := newCoord(t, endpoint, "owner-b", time.Minute)
	if _, ok, err := b.TryAcquire(context.Background(), "feedE"); err != nil || !ok {
		t.Fatalf("b should steal expired lease: ok=%v err=%v", ok, err)
	}

	// b now owns the lease.
	got, err := admin.GetItem(context.Background(), &awsddb.GetItemInput{
		TableName: aws.String(table),
		Key:       map[string]ddbtypes.AttributeValue{pkAttr: &ddbtypes.AttributeValueMemberS{Value: key}},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if owner := got.Item["owner"].(*ddbtypes.AttributeValueMemberS).Value; owner != "owner-b" {
		t.Fatalf("lease owner=%q after steal, want owner-b", owner)
	}
}

func TestDynamoDBStaleReleaseDoesNotDeleteNewerLease(t *testing.T) {
	endpoint, admin := setup(t)
	a := newCoord(t, endpoint, "owner-a", time.Hour)
	relA, ok, err := a.TryAcquire(context.Background(), "feedS")
	if err != nil || !ok {
		t.Fatalf("a acquire: ok=%v err=%v", ok, err)
	}

	// Force-expire a's lease and let b steal it.
	key := lockKeyFor("feedS")
	past := strconv.FormatInt(time.Now().Add(-time.Minute).UnixMilli(), 10)
	if _, err := admin.UpdateItem(context.Background(), &awsddb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       map[string]ddbtypes.AttributeValue{pkAttr: &ddbtypes.AttributeValueMemberS{Value: key}},
		UpdateExpression:          aws.String("SET lease_expiry = :p"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":p": &ddbtypes.AttributeValueMemberN{Value: past}},
	}); err != nil {
		t.Fatalf("force-expire: %v", err)
	}

	b := newCoord(t, endpoint, "owner-b", time.Minute)
	if _, ok, err := b.TryAcquire(context.Background(), "feedS"); err != nil || !ok {
		t.Fatalf("b should steal: ok=%v err=%v", ok, err)
	}

	// a belatedly releases — its conditional delete (owner = owner-a) must fail
	// and leave b's lease intact.
	if err := relA(context.Background()); err != nil {
		t.Fatalf("stale release should swallow conditional failure: %v", err)
	}

	got, err := admin.GetItem(context.Background(), &awsddb.GetItemInput{
		TableName: aws.String(table),
		Key:       map[string]ddbtypes.AttributeValue{pkAttr: &ddbtypes.AttributeValueMemberS{Value: key}},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Item == nil {
		t.Fatal("b's lease was wrongly deleted by a's stale release")
	}
	owner := got.Item["owner"].(*ddbtypes.AttributeValueMemberS).Value
	if owner != "owner-b" {
		t.Fatalf("lease owner=%q after stale release, want owner-b", owner)
	}
}
