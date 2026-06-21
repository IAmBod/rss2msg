// Package dynamodb provides a Coordinator backed by a DynamoDB lease item with
// an explicit expiry timestamp.
//
// DynamoDB has no server-side session, so crash-safe auto-release cannot come
// from a dropped connection the way the Postgres (session-scoped advisory lock)
// coordinator gets it. Instead each lease carries an explicit lease_expiry
// (epoch millis) that is checked *inside the conditional write*: a competing
// instance may steal the lock once now >= lease_expiry. We deliberately do NOT
// rely on DynamoDB's native TTL for lock liveness — native TTL deletion can lag
// by up to 48h, which is useless for a lock. (A TTL attribute may still be set
// purely to reap abandoned rows as housekeeping; it is never trusted for
// correctness.)
//
// Each process generates one unique owner token at startup
// (hostname-pid-randomhex). TryAcquire conditionally PutItem's the lease;
// release conditionally DeleteItem's it only when we still own it, so we never
// delete a lease another instance re-acquired after our expiry.
//
// IMPORTANT operational note: LeaseDuration must safely exceed the maximum
// per-feed poll time. If a poll runs longer than the lease, a peer can steal
// the lock mid-poll and both instances poll concurrently. Size it above your
// worst-case poll duration. Default is 60s.
package dynamodb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/coord"
)

// pkAttr is the partition-key attribute name for the lock table. The table must
// be created with this attribute as its HASH key.
const pkAttr = "pk"

const (
	ownerAttr  = "owner"
	expiryAttr = "lease_expiry"
)

// DefaultLeaseDuration is used when Options.LeaseDuration is zero.
const DefaultLeaseDuration = 60 * time.Second

// Options configures the DynamoDB-backed Coordinator.
type Options struct {
	Table         string        // required; the lock table (HASH key attribute "pk")
	Region        string        // optional; SDK default chain when empty
	EndpointURL   string        // optional; LocalStack/testing BaseEndpoint override
	LeaseDuration time.Duration // 0 -> DefaultLeaseDuration

	// MemberTTL is how long a member heartbeat item stays valid before it is
	// considered expired and removed from the live set. 0 falls back to
	// LeaseDuration so membership has a sane TTL even if unset.
	MemberTTL time.Duration

	// Owner, if set, overrides the auto-generated per-process owner token. Tests
	// use this to simulate distinct instances. Leave empty in production.
	Owner string
}

// ddbAPI is the subset of the DynamoDB client the coordinator uses. It keeps
// the implementation unit-testable with a fake.
type ddbAPI interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Scan(ctx context.Context, in *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

type lease struct {
	key   string
	owner string
}

// Coordinator implements coord.Coordinator over a DynamoDB lock table.
type Coordinator struct {
	client        ddbAPI
	table         string
	owner         string
	leaseDuration time.Duration
	memberTTL     time.Duration // 0 means fall back to leaseDuration in Membership

	// now is overridable in tests; defaults to time.Now.
	now func() time.Time

	mu   sync.Mutex
	held map[*lease]struct{} // nil after Close
}

// New loads AWS config (region/endpoint overrides like the SQS sink), builds a
// DynamoDB client, and returns a ready Coordinator with a fresh owner token.
func New(ctx context.Context, opts Options) (*Coordinator, error) {
	if opts.Table == "" {
		return nil, fmt.Errorf("coord/dynamodb: table is required")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if opts.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(opts.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("coord/dynamodb: load aws config: %w", err)
	}

	clientOpts := []func(*dynamodb.Options){}
	if opts.EndpointURL != "" {
		endpoint := opts.EndpointURL
		clientOpts = append(clientOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	client := dynamodb.NewFromConfig(awsCfg, clientOpts...)

	return newWithClient(client, opts), nil
}

// newWithClient builds a Coordinator around an already-constructed client. Used
// by New and by unit tests that inject a fake.
func newWithClient(client ddbAPI, opts Options) *Coordinator {
	ld := opts.LeaseDuration
	if ld <= 0 {
		ld = DefaultLeaseDuration
	}
	owner := opts.Owner
	if owner == "" {
		owner = newOwnerToken()
	}
	return &Coordinator{
		client:        client,
		table:         opts.Table,
		owner:         owner,
		leaseDuration: ld,
		memberTTL:     opts.MemberTTL,
		now:           time.Now,
		held:          make(map[*lease]struct{}),
	}
}

// Owner returns this coordinator's owner token (exported for tests/debugging).
func (c *Coordinator) Owner() string { return c.owner }

// TryAcquire conditionally writes the lease item. The condition allows the
// write when the row does not exist OR its lease has expired, so a crashed
// instance's stale lock is reclaimable after LeaseDuration.
func (c *Coordinator) TryAcquire(ctx context.Context, feedURL string) (coord.ReleaseFunc, bool, error) {
	key := lockKey(feedURL)
	now := c.now()
	expiry := now.Add(c.leaseDuration).UnixMilli()

	_, err := c.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.table),
		Item: map[string]ddbtypes.AttributeValue{
			pkAttr:     &ddbtypes.AttributeValueMemberS{Value: key},
			ownerAttr:  &ddbtypes.AttributeValueMemberS{Value: c.owner},
			expiryAttr: &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(expiry, 10)},
		},
		// Acquire if the row is absent, or its lease has expired (now > stored
		// lease_expiry). attribute_not_exists keys off pk so a brand-new row wins.
		ConditionExpression: aws.String("attribute_not_exists(#pk) OR #exp < :now"),
		ExpressionAttributeNames: map[string]string{
			"#pk":  pkAttr,
			"#exp": expiryAttr,
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":now": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(now.UnixMilli(), 10)},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			// Another instance holds an unexpired lease.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("coord/dynamodb: PutItem: %w", err)
	}

	l := &lease{key: key, owner: c.owner}

	c.mu.Lock()
	if c.held == nil {
		// Coordinator is closing; release the lease we just took.
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
		// a fresh bounded ctx instead (mirrors the postgres coordinator).
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

// conditionalDelete deletes the lease only if we still own it, on a fresh 5s
// background ctx. A ConditionalCheckFailedException means the lease already
// expired and a different owner re-acquired it (or it was reaped) — not an
// error; we must NOT delete their lease.
func (c *Coordinator) conditionalDelete(l *lease) error {
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.client.DeleteItem(delCtx, &dynamodb.DeleteItemInput{
		TableName: aws.String(c.table),
		Key: map[string]ddbtypes.AttributeValue{
			pkAttr: &ddbtypes.AttributeValueMemberS{Value: l.key},
		},
		ConditionExpression: aws.String("#owner = :me"),
		ExpressionAttributeNames: map[string]string{
			"#owner": ownerAttr,
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":me": &ddbtypes.AttributeValueMemberS{Value: l.owner},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			// Lease already lost/expired and re-acquired by another owner, or
			// reaped. Nothing to do.
			return nil
		}
		log.Warn().
			Str("coord_driver", "dynamodb").
			Str("event", "release_error").
			Err(err).
			Msg("coord/dynamodb: conditional delete failed")
		return fmt.Errorf("coord/dynamodb: DeleteItem: %w", err)
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

// lockKey derives the partition-key value for feedURL. We use the SHA-256 hex
// of the URL (the same scheme as the Redis and Postgres coordinators) so every
// backend keys locks identically and the key is a fixed-length, safe string
// regardless of the URL's contents.
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

// isConditionalCheckFailed reports whether err is a DynamoDB
// ConditionalCheckFailedException.
func isConditionalCheckFailed(err error) bool {
	var ccf *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}
