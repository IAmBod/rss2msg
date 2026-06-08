---
title: Connect n8n
type: how-to
tags: [rss2msg/docs, sinks, http, webhook, n8n, automation, integrations]
summary: Drive n8n workflows from feed changes by pointing the HTTP sink at a Webhook node URL.
updated: 2026-06-09
---

# Connect n8n

[n8n](https://n8n.io) starts a workflow when its Webhook node receives an HTTP
request. rss2msg drives it with the [HTTP sink](../sinks/http.md): every detected
change is POSTed as a JSON [Change envelope](../../reference/change-envelope.md) to
the node's URL.

First set up the sink as shown in
[Integrate with External Systems](../integrations.md#configure-the-http-sink), then
point its `http.url` at the Webhook node URL below.

## Steps

1. Add a **Webhook** node as the workflow's trigger and set its HTTP method to
   match the sink (`POST` by default).
2. Copy the node's **Production URL** into `http.url`. Use the **Test URL** with
   *Listen for test event* while building the workflow.
3. The node exposes the JSON body and headers to downstream nodes; reference the
   envelope fields from there.
4. **Activate** the workflow so the Production URL accepts live deliveries.

## Notes

- n8n keeps nested JSON intact, so `authors`, `categories`, and the timestamps are
  available as-is.
- The Webhook node responds `200` by default. If you set a custom response code,
  add it to `success_codes`. If the node is configured to respond only after the
  workflow finishes, raise `timeout` so rss2msg waits for slow downstream steps.
- To authenticate, set the Webhook node's authentication to **Header Auth** and
  send the matching key/value from the sink's `headers` block.

## Related

- [Integrate with External Systems](../integrations.md) — the shared HTTP-sink setup and reliability options.
- [Connect Zapier](zapier.md) — the same flow on Zapier.
- [HTTP sink](../sinks/http.md) — every field, header, and success-code detail.
- [Change Envelope](../../reference/change-envelope.md) — the JSON payload fields to map.
