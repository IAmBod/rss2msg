---
title: Integrate with External Systems
type: how-to
tags: [rss2msg/docs, sinks, http, webhook, automation, integrations]
summary: Drive external automation and workflow platforms from feed changes by pointing the HTTP sink at a webhook URL.
updated: 2026-06-09
---

# Integrate with External Systems

Many automation and workflow platforms start a job when they receive an HTTP
request. rss2msg drives them with the [HTTP sink](sinks/http.md): every detected
change is POSTed as a JSON [Change envelope](../reference/change-envelope.md) to a
webhook URL the platform gives you. No platform-specific integration is needed — to
rss2msg it is just another webhook receiver.

The flow is the same for every platform:

1. Create a webhook trigger in the platform and copy the URL it generates.
2. Add an `http` sink pointing at that URL (see below).
3. Reference the sink from the feeds whose changes should trigger the workflow.

## Per-platform guides

| Platform | Guide |
| --- | --- |
| Zapier | [Connect Zapier](integrations/zapier.md) |
| n8n | [Connect n8n](integrations/n8n.md) |

More integrations will be added here. Any platform that accepts an inbound webhook
works with the HTTP-sink setup on this page — the per-platform guides only cover
where to get the URL and any platform-specific quirks.

## Configure the HTTP sink

Add a sink with `driver: http` and the URL from your platform (see the per-platform
guide for where to get it). The full field surface is on the
[HTTP sink](sinks/http.md) page; the essentials:

```yaml
sinks:
  - name: automation
    driver: http
    http:
      url: ${AUTOMATION_WEBHOOK_URL}   # the catch-hook / webhook URL
      method: POST                     # POST (default) | PUT
      headers:                         # optional static headers
        X-Webhook-Secret: ${AUTOMATION_SECRET}
      timeout: 10s                     # default 30s
      success_codes: [200, 201, 202, 204]

feeds:
  - url: https://example.com/blog/rss.xml
    interval: 5m
    sinks: [automation]
```

Keep the URL and any shared secret out of the file with `${VAR}` substitution.
The sink sends **one request per change**, and delivery is **at-least-once** — the
same change can arrive more than once after a retry, so make the downstream
workflow idempotent (de-duplicate on `item_id` + `content_hash`). See
[Operational Notes](../explanation/operations.md) for the delivery guarantees.

### What each request looks like

- **Body** — the JSON `Change` envelope (`Content-Type: application/json`). The
  fields you will reference downstream — `title`, `link`, `summary`, `content`,
  `authors`, `categories`, `published_at`, `feed_title`, and `kind` (`new` vs
  `updated`) — are documented in the [Change Envelope](../reference/change-envelope.md).
- **Headers** — your static `headers` plus the canonical per-record headers
  `X-Feed-Url`, `X-Item-Id`, `X-Kind`, and `X-Schema-Version`. On dead-letter
  deliveries rss2msg also adds `X-Dlq-From-Sink`, `X-Dlq-Error`, and
  `X-Dlq-Attempts`.

## Reliability

Wrap the sink with a [dead-letter sink](choose-a-sink.md) so changes that exhaust
retries are not lost while the platform is unreachable or rate-limiting:

```yaml
sinks:
  - name: automation
    driver: http
    dead_letter: automation-dlq
    http:
      url: ${AUTOMATION_WEBHOOK_URL}
  - name: automation-dlq
    driver: stdout
```

The dead-letter delivery carries the `X-Dlq-From-Sink`, `X-Dlq-Error`, and
`X-Dlq-Attempts` annotations so you can see why it failed.

## Related

- [Connect Zapier](integrations/zapier.md) — Catch Hook trigger and field mapping.
- [Connect n8n](integrations/n8n.md) — Webhook node trigger and authentication.
- [HTTP sink](sinks/http.md) — every field, header, and success-code detail.
- [Change Envelope](../reference/change-envelope.md) — the JSON payload fields to map.
- [Choose a Sink](choose-a-sink.md) — dead-letter routing and the driver table.
- [Configure Feeds](configure-feeds.md) — attaching sinks to feeds.
- [Operational Notes](../explanation/operations.md) — at-least-once delivery and DLQs.
