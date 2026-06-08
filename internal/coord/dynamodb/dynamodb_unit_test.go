package dynamodb

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeDDB is an in-memory stand-in for the DynamoDB client that honours the
// exact condition expressions the coordinator issues.
type fakeDDB struct {
	mu    sync.Mutex
	items map[string]map[string]ddbtypes.AttributeValue // pk -> item

	putCalls int
	delCalls int
}

func newFakeDDB() *fakeDDB {
	return &fakeDDB{items: make(map[string]map[string]ddbtypes.AttributeValue)}
}

func (f *fakeDDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls++

	pk := in.Item[pkAttr].(*ddbtypes.AttributeValueMemberS).Value
	existing := f.items[pk]

	// Evaluate: attribute_not_exists(#pk) OR #exp < :now
	if existing != nil {
		nowStr := in.ExpressionAttributeValues[":now"].(*ddbtypes.AttributeValueMemberN).Value
		now, _ := strconv.ParseInt(nowStr, 10, 64)
		expStr := existing[expiryAttr].(*ddbtypes.AttributeValueMemberN).Value
		exp, _ := strconv.ParseInt(expStr, 10, 64)
		if exp >= now {
			return nil, &ddbtypes.ConditionalCheckFailedException{}
		}
	}
	f.items[pk] = in.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeDDB) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delCalls++

	pk := in.Key[pkAttr].(*ddbtypes.AttributeValueMemberS).Value
	existing := f.items[pk]
	// Condition: #owner = :me
	me := in.ExpressionAttributeValues[":me"].(*ddbtypes.AttributeValueMemberS).Value
	if existing == nil {
		return nil, &ddbtypes.ConditionalCheckFailedException{}
	}
	owner := existing[ownerAttr].(*ddbtypes.AttributeValueMemberS).Value
	if owner != me {
		return nil, &ddbtypes.ConditionalCheckFailedException{}
	}
	delete(f.items, pk)
	return &dynamodb.DeleteItemOutput{}, nil
}

func (f *fakeDDB) get(pk string) map[string]ddbtypes.AttributeValue {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.items[pk]
}

func TestOwnerTokenIsUniquePerProcess(t *testing.T) {
	seen := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		tok := newOwnerToken()
		if tok == "" {
			t.Fatal("empty owner token")
		}
		if _, dup := seen[tok]; dup {
			t.Fatalf("duplicate owner token: %q", tok)
		}
		seen[tok] = struct{}{}
	}
	// Token shape: host-pid-hex.
	tok := newOwnerToken()
	if strings.Count(tok, "-") < 2 {
		t.Fatalf("owner token %q lacks host-pid-hex shape", tok)
	}
}

func TestLockKeyDerivation(t *testing.T) {
	k1 := lockKey("https://a.example/feed")
	k2 := lockKey("https://b.example/feed")
	if k1 == k2 {
		t.Fatal("distinct feeds produced the same lock key")
	}
	if lockKey("https://a.example/feed") != k1 {
		t.Fatal("lock key is not deterministic")
	}
	if !strings.HasPrefix(k1, "rss2msg:coord:") {
		t.Fatalf("lock key missing namespace prefix: %q", k1)
	}
}

func newTestCoord(client ddbAPI, owner string) *Coordinator {
	return newWithClient(client, Options{Table: "locks", Owner: owner, LeaseDuration: 60 * time.Second})
}

func TestTryAcquireFirstWins(t *testing.T) {
	f := newFakeDDB()
	c := newTestCoord(f, "owner-a")

	rel, ok, err := c.TryAcquire(context.Background(), "feed1")
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	if rel == nil {
		t.Fatal("nil release func on win")
	}
	item := f.get(lockKey("feed1"))
	if item == nil {
		t.Fatal("lease item not written")
	}
	if got := item[ownerAttr].(*ddbtypes.AttributeValueMemberS).Value; got != "owner-a" {
		t.Fatalf("owner=%q", got)
	}
}

func TestTryAcquireSecondInstanceBlocked(t *testing.T) {
	f := newFakeDDB()
	a := newTestCoord(f, "owner-a")
	b := newTestCoord(f, "owner-b")

	if _, ok, err := a.TryAcquire(context.Background(), "feed1"); err != nil || !ok {
		t.Fatalf("a acquire: ok=%v err=%v", ok, err)
	}
	rel, ok, err := b.TryAcquire(context.Background(), "feed1")
	if err != nil {
		t.Fatalf("b acquire err: %v", err)
	}
	if ok {
		t.Fatal("b should not have acquired a held, unexpired lease")
	}
	if rel != nil {
		t.Fatal("b release func should be nil when not acquired")
	}
}

func TestReleaseMakesReacquirable(t *testing.T) {
	f := newFakeDDB()
	a := newTestCoord(f, "owner-a")
	b := newTestCoord(f, "owner-b")

	rel, ok, _ := a.TryAcquire(context.Background(), "feed1")
	if !ok {
		t.Fatal("a should acquire")
	}
	if err := rel(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if f.get(lockKey("feed1")) != nil {
		t.Fatal("lease should be gone after release")
	}
	if _, ok, _ := b.TryAcquire(context.Background(), "feed1"); !ok {
		t.Fatal("b should acquire after a released")
	}
}

func TestExpiredLeaseCanBeStolen(t *testing.T) {
	f := newFakeDDB()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a := newTestCoord(f, "owner-a")
	a.now = func() time.Time { return base }
	if _, ok, _ := a.TryAcquire(context.Background(), "feed1"); !ok {
		t.Fatal("a should acquire at t0")
	}

	// b runs well after a's 60s lease has expired.
	b := newTestCoord(f, "owner-b")
	b.now = func() time.Time { return base.Add(2 * time.Minute) }
	if _, ok, err := b.TryAcquire(context.Background(), "feed1"); err != nil || !ok {
		t.Fatalf("b should steal expired lease: ok=%v err=%v", ok, err)
	}
	if got := f.get(lockKey("feed1"))[ownerAttr].(*ddbtypes.AttributeValueMemberS).Value; got != "owner-b" {
		t.Fatalf("owner after steal=%q, want owner-b", got)
	}
}

func TestStaleReleaseDoesNotDeleteNewerLease(t *testing.T) {
	f := newFakeDDB()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a := newTestCoord(f, "owner-a")
	a.now = func() time.Time { return base }
	relA, ok, _ := a.TryAcquire(context.Background(), "feed1")
	if !ok {
		t.Fatal("a should acquire")
	}

	// b steals after a's lease expires.
	b := newTestCoord(f, "owner-b")
	b.now = func() time.Time { return base.Add(2 * time.Minute) }
	if _, ok, _ := b.TryAcquire(context.Background(), "feed1"); !ok {
		t.Fatal("b should steal expired lease")
	}

	// a now belatedly releases — it must NOT delete b's lease.
	if err := relA(context.Background()); err != nil {
		t.Fatalf("stale release should swallow conditional failure, got: %v", err)
	}
	item := f.get(lockKey("feed1"))
	if item == nil {
		t.Fatal("b's lease was wrongly deleted by a's stale release")
	}
	if got := item[ownerAttr].(*ddbtypes.AttributeValueMemberS).Value; got != "owner-b" {
		t.Fatalf("lease owner=%q after stale release, want owner-b", got)
	}
}

func TestCloseReleasesHeldLeases(t *testing.T) {
	f := newFakeDDB()
	c := newTestCoord(f, "owner-a")
	if _, ok, _ := c.TryAcquire(context.Background(), "feed1"); !ok {
		t.Fatal("acquire")
	}
	if _, ok, _ := c.TryAcquire(context.Background(), "feed2"); !ok {
		t.Fatal("acquire 2")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if f.get(lockKey("feed1")) != nil || f.get(lockKey("feed2")) != nil {
		t.Fatal("Close should have released all held leases")
	}
}

func TestDefaultLeaseDurationApplied(t *testing.T) {
	c := newWithClient(newFakeDDB(), Options{Table: "locks", Owner: "o"})
	if c.leaseDuration != DefaultLeaseDuration {
		t.Fatalf("leaseDuration=%v, want default %v", c.leaseDuration, DefaultLeaseDuration)
	}
}

func TestConditionExpressionShape(t *testing.T) {
	// Guard the exact attribute names / expression so a refactor can't silently
	// break the conditional-write contract.
	var gotPut *dynamodb.PutItemInput
	var gotDel *dynamodb.DeleteItemInput
	capt := &captureDDB{
		onPut: func(in *dynamodb.PutItemInput) { gotPut = in },
		onDel: func(in *dynamodb.DeleteItemInput) { gotDel = in },
	}
	c := newTestCoord(capt, "owner-a")
	rel, ok, err := c.TryAcquire(context.Background(), "feed1")
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if got := aws.ToString(gotPut.ConditionExpression); got != "attribute_not_exists(#pk) OR #exp < :now" {
		t.Fatalf("put condition=%q", got)
	}
	if _, ok := gotPut.Item[expiryAttr]; !ok {
		t.Fatal("put item missing lease_expiry")
	}
	if err := rel(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := aws.ToString(gotDel.ConditionExpression); got != "#owner = :me" {
		t.Fatalf("delete condition=%q", got)
	}
}

type captureDDB struct {
	onPut func(*dynamodb.PutItemInput)
	onDel func(*dynamodb.DeleteItemInput)
}

func (c *captureDDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	c.onPut(in)
	return &dynamodb.PutItemOutput{}, nil
}

func (c *captureDDB) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	c.onDel(in)
	return &dynamodb.DeleteItemOutput{}, nil
}
