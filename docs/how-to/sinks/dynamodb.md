---
title: DynamoDB sink
type: how-to
tags: [rss2msg/docs, sinks, dynamodb, aws]
summary: Write each Change as an idempotent PutItem into a DynamoDB table; key model, custom key names, and optional TTL.
updated: 2026-06-08
---

# DynamoDB sink

```yaml
- name: dynamodb-main
  driver: dynamodb
  dynamodb:
    table: feed-changes
    region: us-east-1
    # endpoint_url: http://localhost:4566   # DynamoDB Local / LocalStack
    # partition_key: feed_url               # default feed_url
    # sort_key: item_id                     # default item_id
    # ttl_attribute: expires_at             # must match the table's TTL attribute
    # item_ttl: 720h                        # detected_at + item_ttl, written as Unix epoch seconds
```

| field           | required | notes |
| --------------- | -------- | ----- |
| `table`         | yes      | Target table name. The table must already exist; the sink does not create it. |
| `region`        | no       | AWS SDK falls back to env/profile. |
| `endpoint_url`  | no       | Override for DynamoDB Local / LocalStack-style endpoints. |
| `partition_key` | no       | Partition-key attribute name. Default `feed_url`; filled with the change's feed URL. |
| `sort_key`      | no       | Sort-key attribute name. Default `item_id`; filled with the change's item id. |
| `ttl_attribute` | no       | Attribute name for a Number TTL value. Must match the table's configured TTL attribute for DynamoDB to expire rows. |
| `item_ttl`      | no       | Duration added to `detected_at` to compute the TTL value (Unix epoch seconds). Requires `ttl_attribute`. |

Credentials come from the standard AWS SDK credential chain
(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, shared `~/.aws/credentials`,
instance metadata, etc.).

## Item model

Each `Publish` is one **idempotent `PutItem`** keyed by
(partition = `feed_url`, sort = `item_id`). Re-publishing the same item
overwrites the existing row in place, so the table holds at most one row per
item — convenient for deduplication and for keeping a current-state change-log.

The remaining `Change` fields are stored as item attributes using the
snake_case names from the change envelope (`kind`, `title`, `link`,
`summary`, `content`, `content_hash`, `detected_at`, `schema_version`,
optional DLQ annotations, etc.). The partition/sort attributes always carry
`feed_url` / `item_id` even if you rename them via `partition_key` /
`sort_key`.

## Not pub/sub

DynamoDB is a **datastore target**, not a message broker. Publishing only
upserts a row; it does not notify consumers. Downstream systems observe new
and updated rows via **DynamoDB Streams** (change-data-capture) or by polling
the table.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
