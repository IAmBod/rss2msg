package dynamodb

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/iambod/rss2msg/internal/coord"
)

// Compile-time assertion: *Coordinator implements coord.MembershipProvider.
var _ coord.MembershipProvider = (*Coordinator)(nil)

func memberPKPrefix() string      { return "member:" }
func memberPK(self string) string { return memberPKPrefix() + self }

// Membership returns a DynamoDB-backed Membership reusing this coordinator's
// client and table. Members are items pk="member:<id>" with a lease_expiry
// (epoch ms). The live set is derived via a Scan filtered to non-expired member
// items; Scan paginates using LastEvaluatedKey so no members are dropped past
// the first page. Cost scales with member count, not feed count.
func (c *Coordinator) Membership(self string) (coord.Membership, error) {
	ttl := c.memberTTL
	if ttl <= 0 {
		ttl = c.leaseDuration
	}
	return &dynamoMembership{c: c, self: self, ttl: ttl}, nil
}

type dynamoMembership struct {
	c    *Coordinator
	self string
	ttl  time.Duration
}

// Heartbeat upserts this instance's member item and returns the set of
// currently live member IDs (including self).
func (m *dynamoMembership) Heartbeat(ctx context.Context) ([]string, error) {
	now := m.c.now()
	expiry := now.Add(m.ttl).UnixMilli()
	_, err := m.c.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(m.c.table),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":           &ddbtypes.AttributeValueMemberS{Value: memberPK(m.self)},
			"lease_expiry": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(expiry, 10)},
		},
	})
	if err != nil {
		return nil, err
	}

	nowMs := now.UnixMilli()
	var ids []string
	var startKey map[string]ddbtypes.AttributeValue
	for {
		out, err := m.c.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:        aws.String(m.c.table),
			FilterExpression: aws.String("begins_with(pk, :p)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":p": &ddbtypes.AttributeValueMemberS{Value: memberPKPrefix()},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, it := range out.Items {
			pkAV, ok := it["pk"].(*ddbtypes.AttributeValueMemberS)
			if !ok {
				continue
			}
			expAV, ok := it["lease_expiry"].(*ddbtypes.AttributeValueMemberN)
			if !ok {
				continue
			}
			expMs, _ := strconv.ParseInt(expAV.Value, 10, 64)
			if expMs > nowMs {
				ids = append(ids, strings.TrimPrefix(pkAV.Value, memberPKPrefix()))
			} else {
				// Best-effort reap of expired member entry; ignore delete errors.
				_, _ = m.c.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String(m.c.table),
					Key: map[string]ddbtypes.AttributeValue{
						"pk": &ddbtypes.AttributeValueMemberS{Value: pkAV.Value},
					},
				})
			}
		}
		if out.LastEvaluatedKey == nil {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return ids, nil
}

// Deregister removes this instance's member item from the table immediately.
func (m *dynamoMembership) Deregister(ctx context.Context) error {
	_, err := m.c.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(m.c.table),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: memberPK(m.self)},
		},
	})
	return err
}

// Close is a no-op; the coordinator's shared DynamoDB client manages its own
// lifecycle.
func (m *dynamoMembership) Close() error { return nil }
