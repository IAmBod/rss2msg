---
title: SNS sink
type: how-to
tags: [rss2msg/docs, sinks, sns, aws]
summary: Publish Changes to an SNS topic; FIFO topics, message groups, and RawMessageDelivery.
updated: 2026-05-30
---

# SNS sink

```yaml
- name: sns-main
  driver: sns
  sns:
    topic_arn: arn:aws:sns:us-east-1:123456789012:feed-changes
    region: us-east-1
    # message_group: feed_url               # FIFO only — see below
```

| field           | required | notes |
| --------------- | -------- | ----- |
| `topic_arn`     | yes      | Full SNS topic ARN. A `.fifo` suffix selects FIFO mode (see below). |
| `region`        | no       | AWS SDK fallback chain. |
| `endpoint_url`  | no       | LocalStack override. |
| `message_group` | no       | FIFO only: `feed_url` (default) \| `item_id` \| `sink`. Rejected when set on a standard (non-FIFO) topic. |

Message attributes mirror the SQS sink. Credentials follow the same chain.

## FIFO topics

When `topic_arn` ends with `.fifo`, the sink sets the two FIFO-required
fields on every `Publish` call. The semantics mirror the SQS FIFO support:

- **`MessageGroupId`** — derived from `message_group` (`feed_url` default, `item_id`, or `sink`).
- **`MessageDeduplicationId`** — `sha256(feed_url || item_id || content_hash)` hex-encoded. Re-publishes of an unchanged Change within SNS's 5-minute dedup window are coalesced; updates produce a fresh id and are delivered.

If you fan a FIFO topic out to a FIFO SQS queue subscription, set
`RawMessageDelivery=true` on the subscription so the `MessageGroupId` /
`MessageDeduplicationId` SNS produced are propagated through to the queue
(otherwise the SQS consumer sees the SNS-envelope JSON, not the raw
message).

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
