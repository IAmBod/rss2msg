---
title: Connect Zapier
type: how-to
tags: [rss2msg/docs, sinks, http, webhook, zapier, automation, integrations]
summary: Drive Zapier Zaps from feed changes by pointing the HTTP sink at a Catch Hook URL.
updated: 2026-06-09
---

# Connect Zapier

[Zapier](https://zapier.com) starts a Zap when it receives an HTTP request. rss2msg
drives it with the [HTTP sink](../sinks/http.md): every detected change is POSTed as
a JSON [Change envelope](../../reference/change-envelope.md) to a Catch Hook URL.

First set up the sink as shown in
[Integrate with External Systems](../integrations.md#configure-the-http-sink), then
point its `http.url` at the Catch Hook URL below.

## Steps

1. Create a Zap and choose **Webhooks by Zapier** as the trigger app, with the
   **Catch Hook** event. (Webhooks by Zapier is a premium feature on Zapier's
   plans.)
2. Zapier generates a **Custom Webhook URL** — copy it into `http.url` (e.g. via
   `AUTOMATION_WEBHOOK_URL`).
3. Start rss2msg (or use `validate-config` plus a manual `run-once`) so a real
   change is delivered, then click **Test trigger** in Zapier to pull in a sample
   and map the envelope fields into later actions.

## Notes

- Zapier flattens the JSON body, so the envelope's arrays appear as indexed fields
  (`authors.0`, `categories.0`, …).
- A Catch Hook returns `200`, which is already in the default `success_codes`.
- Catch Hook URLs are unguessable but are **not** authenticated — Zapier does not
  check your `headers`. Treat the URL itself as the secret. To filter unwanted
  requests, add a **Filter** step in the Zap.
- Each delivery consumes a Zapier task; a busy feed can run through a task quota
  quickly. Tighten the feed `interval` or the feeds attached to the sink if needed.

## Related

- [Integrate with External Systems](../integrations.md) — the shared HTTP-sink setup and reliability options.
- [Connect n8n](n8n.md) — the same flow on n8n.
- [HTTP sink](../sinks/http.md) — every field, header, and success-code detail.
- [Change Envelope](../../reference/change-envelope.md) — the JSON payload fields to map.
