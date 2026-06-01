---
title: HTTP sink
type: how-to
tags: [rss2msg/docs, sinks, http, webhook]
summary: POST/PUT each Change as JSON to a webhook URL; headers, success codes, HTTP/3, and canonical metadata.
updated: 2026-06-02
---

# HTTP sink

POSTs (or PUTs) each Change as a JSON request body to a configured URL.
Suitable for webhook integrations — Slack incoming-webhook, Discord,
custom HTTP receivers, etc.

```yaml
- name: hook
  driver: http
  http:
    url: https://example.com/hook                # required; http:// or https://
    method: POST                                  # POST (default) | PUT
    headers:                                      # optional static headers
      Authorization: "Bearer ${HOOK_TOKEN}"
      X-Source: rss2msg
    timeout: 10s                                  # default 30s
    success_codes: [200, 201, 202, 204]           # default; status codes treated as success
    http3: false                                  # dial over HTTP/3 (QUIC); requires an https:// url
```

| field           | required | default                | notes |
| --------------- | -------- | ---------------------- | ----- |
| `url`           | yes      | —                      | Must be `http://` or `https://`. `${ENV}` substitution works. |
| `method`        | no       | `POST`                 | `POST` \| `PUT`. |
| `headers`       | no       | (none)                 | Static request headers. Useful for auth tokens, custom routing keys, etc. Per-record canonical headers (see below) cannot be overridden. |
| `timeout`       | no       | `30s`                  | Per-request timeout (Go `time.Duration`). |
| `success_codes` | no       | `[200, 201, 202, 204]` | HTTP status codes treated as success; everything else surfaces as a publish error. |
| `http3`         | no       | `false`                | Dial the upstream over HTTP/3 (QUIC) instead of HTTP/1.1+H2. HTTP/3 is TLS-only, so the `url` must be `https://`. The `tls` block (custom CA / mTLS) still applies. |
| `tls`           | no       | (off)                  | Structured client TLS (custom CA / mTLS / verification) for `https://` targets. See [Secure Connections (TLS)](../secure-connections-tls.md#sinks). |

Request layout:
- Body: JSON `Change` envelope.
- `Content-Type: application/json`.
- Canonical per-record headers: `X-Feed-Url`, `X-Item-Id`, `X-Kind`, `X-Schema-Version`, optional `X-Dlq-From-Sink` / `X-Dlq-Error` / `X-Dlq-Attempts`. Set after the static `headers` block so operator typos can't clobber per-record metadata.
- W3C trace context (`traceparent`, `tracestate`) injected automatically when a span is active.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Secure Connections (TLS)](../secure-connections-tls.md#sinks) — custom CA / mTLS for this sink.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
- [Connect Zapier and n8n](../connect-zapier-and-n8n.md) — drive automation platforms with this sink.
