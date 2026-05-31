---
title: Change Envelope
type: reference
tags: [rss2msg/docs, schema, output]
summary: The canonical JSON Change object published to every sink, and its field semantics.
updated: 2026-05-30
---

# Change Envelope

Every published message is a JSON `Change` (see
[`internal/model/change.go`](../../internal/model/change.go)):

```json
{
  "schema_version": 1,
  "feed_url": "https://example.com/blog/rss.xml",
  "feed_title": "Example Blog",
  "item_id": "https://example.com/blog/post-1",
  "kind": "new",
  "title": "Hello world",
  "link": "https://example.com/blog/post-1",
  "authors": ["alice"],
  "summary": "...",
  "content": "...",
  "categories": ["go"],
  "published_at": "2026-05-29T12:00:00Z",
  "updated_at":   "2026-05-29T12:00:00Z",
  "content_hash": "5e88489..." ,
  "detected_at":  "2026-05-29T12:00:05Z"
}
```

- `item_id` is the stable identity: GUID if the feed provides one, else
  `link`, else `sha256(title || publishedAt)`.
- `content_hash` is the sha256 over the normalised tuple
  `(title, link, body, author, updated_at)` — whitespace runs are collapsed
  to single spaces before hashing.
- `kind` is `new` on first sighting, `updated` when the content hash
  changes for a known `item_id`. Unchanged items are not published.
- DLQ deliveries add `dlq_from_sink`, `dlq_error`, `dlq_attempts` to the
  envelope (and as headers/attributes on Kafka/SQS/SNS); see [Choose a Sink](../how-to/choose-a-sink.md).

## Related

- [Sink Wire Formats](wire-formats.md) — how the envelope maps onto each sink's key/body/metadata.
- [Choose a Sink](../how-to/choose-a-sink.md) — DLQ annotations and sink selection.
- [How It Works](../explanation/how-it-works.md) — where `kind` and `content_hash` are computed in the pipeline.
