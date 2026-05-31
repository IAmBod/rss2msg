---
title: Connect Zapier and n8n
type: how-to
tags: [rss2msg/docs, sinks, http, webhook, zapier, n8n, automation]
summary: Drive Zapier Zaps and n8n workflows from feed changes by pointing the HTTP sink at a catch-hook URL.
updated: 2026-06-01
---

# Connect Zapier and n8n

[Zapier](https://zapier.com) and [n8n](https://n8n.io) are automation platforms
that start a workflow when they receive an HTTP request. rss2msg drives them with
the [HTTP sink](sinks/http.md): every detected change is POSTed as a JSON
[Change envelope](../reference/change-envelope.md) to a webhook URL the platform
gives you. No platform-specific integration is needed — to rss2msg it is just
another webhook receiver.

The flow is the same for both:

1. Create a webhook trigger in the platform and copy the URL it generates.
2. Add an `http` sink pointing at that URL.
3. Reference the sink from the feeds whose changes should trigger the workflow.

## 1. Configure the HTTP sink

Add a sink with `driver: http` and the URL from your platform (see the
per-platform sections below for where to get it). The full field surface is on the
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

## 2. Zapier

1. Create a Zap and choose **Webhooks by Zapier** as the trigger app, with the
   **Catch Hook** event. (Webhooks by Zapier is a premium feature on Zapier's
   plans.)
2. Zapier generates a **Custom Webhook URL** — copy it into `http.url` (e.g. via
   `AUTOMATION_WEBHOOK_URL`).
3. Start rss2msg (or use `validate-config` plus a manual `run-once`) so a real
   change is delivered, then click **Test trigger** in Zapier to pull in a sample
   and map the envelope fields into later actions.

Notes:

- Zapier flattens the JSON body, so the envelope's arrays appear as indexed fields
  (`authors.0`, `categories.0`, …).
- A Catch Hook returns `200`, which is already in the default `success_codes`.
- Catch Hook URLs are unguessable but are **not** authenticated — Zapier does not
  check your `headers`. Treat the URL itself as the secret. To filter unwanted
  requests, add a **Filter** step in the Zap.
- Each delivery consumes a Zapier task; a busy feed can run through a task quota
  quickly. Tighten the feed `interval` or the feeds attached to the sink if needed.

## 3. n8n

1. Add a **Webhook** node as the workflow's trigger and set its HTTP method to
   match the sink (`POST` by default).
2. Copy the node's **Production URL** into `http.url`. Use the **Test URL** with
   *Listen for test event* while building the workflow.
3. The node exposes the JSON body and headers to downstream nodes; reference the
   envelope fields from there.
4. **Activate** the workflow so the Production URL accepts live deliveries.

Notes:

- n8n keeps nested JSON intact, so `authors`, `categories`, and the timestamps are
  available as-is.
- The Webhook node responds `200` by default. If you set a custom response code,
  add it to `success_codes`. If the node is configured to respond only after the
  workflow finishes, raise `timeout` so rss2msg waits for slow downstream steps.
- To authenticate, set the Webhook node's authentication to **Header Auth** and
  send the matching key/value from the sink's `headers` block (as in the example
  above).

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

- [HTTP sink](sinks/http.md) — every field, header, and success-code detail.
- [Change Envelope](../reference/change-envelope.md) — the JSON payload fields to map.
- [Choose a Sink](choose-a-sink.md) — dead-letter routing and the driver table.
- [Configure Feeds](configure-feeds.md) — attaching sinks to feeds.
- [Operational Notes](../explanation/operations.md) — at-least-once delivery and DLQs.
