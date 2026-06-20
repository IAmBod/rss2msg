---
title: Send to Discord and Slack with Redpanda Connect
type: how-to
tags: [rss2msg/docs, sinks, http, kafka, redpanda-connect, discord, slack, integrations]
summary: Bridge feed changes into Discord and Slack by POSTing the Change envelope to Redpanda Connect and shaping it with Bloblang before its discord / slack_post outputs.
updated: 2026-06-20
---

# Send to Discord and Slack with Redpanda Connect

rss2msg has no Discord or Slack driver of its own — it speaks generic transports.
[Redpanda Connect](https://docs.redpanda.com/redpanda-connect/about/) (the stream
processor formerly known as Benthos) fills the gap: it receives the
[Change envelope](../../reference/change-envelope.md) over HTTP, reshapes it with a
[Bloblang](https://docs.redpanda.com/redpanda-connect/guides/bloblang/about/)
mapping, and writes the result to Discord and Slack with its native
[`discord`](https://docs.redpanda.com/redpanda-connect/components/outputs/discord/)
and [`slack_post`](https://docs.redpanda.com/redpanda-connect/components/outputs/slack_post/)
outputs.

The pipeline has three stages:

```
rss2msg  ──(HTTP POST: Change JSON)──▶  Redpanda Connect  ──▶  Discord channel
 http sink         http_server input         map + output    └─▶  Slack channel
```

1. **rss2msg** POSTs each change to Redpanda Connect's `http_server` input.
2. **Redpanda Connect** maps the envelope into a chat message.
3. **Discord / Slack** outputs deliver it.

No broker is needed — to rss2msg, Redpanda Connect is just another webhook receiver.

## Stage 1 — point the HTTP sink at Redpanda Connect

Add an [HTTP sink](../sinks/http.md) — same setup as
[Integrate with External Systems](../integrate-with-external-systems.md#configure-the-http-sink) —
pointing at the address Redpanda Connect's `http_server` input listens on
(default path `/post`):

```yaml
sinks:
  - name: chat
    driver: http
    http:
      url: http://redpanda-connect:4195/post   # http_server input address + path

feeds:
  - url: https://example.com/blog/rss.xml
    interval: 5m
    sinks: [chat]
```

The `http_server` input consumes the entire request body as one message, so each
Change arrives as a single JSON document.

## Stage 2 — the Redpanda Connect config

The `http_server` input receives the POSTs; the `pipeline:` mapping shapes each
envelope, and the `output:` broker fans it out to both chat platforms.

```yaml
# config.yaml for Redpanda Connect

input:
  http_server:
    path: /post
    allowed_verbs: [POST]

pipeline:
  processors:
    # Shape the Change envelope into a single human-readable line, reused by both
    # outputs. Adjust to taste — every envelope field is available on `this`.
    - mapping: |
        root.text = "**%s**\n%s".format(this.title, this.link)

output:
  broker:
    pattern: fan_out          # deliver every message to BOTH outputs
    outputs:
      - discord:
          channel_id: "${DISCORD_CHANNEL_ID}"
          bot_token: "${DISCORD_BOT_TOKEN}"
      - slack_post:
          channel_id: "${SLACK_CHANNEL_ID}"
          bot_token: "${SLACK_BOT_TOKEN}"
          text: "${! this.text }"
```

`fan_out` sends each change to both chat platforms; drop one entry from `outputs`
to target just Discord or just Slack. Redpanda Connect resolves `${VAR}` from the
environment, so keep tokens out of the file.

### Discord output

The [`discord`](https://docs.redpanda.com/redpanda-connect/components/outputs/discord/)
output POSTs to the Discord API as a bot. It needs:

- `channel_id` — the target channel's ID.
- `bot_token` — a bot token with permission to post in that channel.

If the message is already a JSON object matching Discord's message schema it is sent
as-is; otherwise Redpanda Connect wraps the string as the message `content`. The
`root.text` mapping above produces a plain string, so it lands as the message body.
To send a Discord [embed](https://discord.com/developers/docs/resources/message#embed-object)
instead, map a full message object:

```yaml
- mapping: |
    root.embeds = [{
      "title": this.title,
      "url": this.link,
      "description": this.summary,
    }]
```

Prefer an **incoming webhook** over a bot? Swap the `discord` output for an
[`http_client`](https://docs.redpanda.com/redpanda-connect/components/outputs/http_client/)
output posting `{ "content": "…" }` to the channel's webhook URL.

### Slack output

The [`slack_post`](https://docs.redpanda.com/redpanda-connect/components/outputs/slack_post/)
output calls Slack's `chat.postMessage`. It needs:

- `bot_token` — a Slack bot user OAuth token with `chat:write`.
- `channel_id` — the encoded channel ID (not the `#name`).
- `text` — the message body; **interpolation functions are supported**, so
  `${! this.text }` injects the mapped field. (Set `text` *or* `blocks`, not both.)

`markdown` defaults to `true`, so Slack `mrkdwn` (`*bold*`, `<url|label>`) renders.
For an incoming-webhook setup instead of a bot, use an `http_client` output posting
`{ "text": "…" }` to the Slack webhook URL.

## Alternative: consume from a Redpanda / Kafka broker

If you **already run Redpanda or Kafka**, you can put a topic between rss2msg and
Redpanda Connect instead of POSTing directly. This decouples the two services and
buffers changes on the broker while a chat API is rate-limiting or down, at the cost
of operating a broker. The mapping and outputs from Stage 2 are unchanged — only the
sink and the Redpanda Connect `input:` block differ.

Use the [Kafka sink](../sinks/kafka.md) instead of the HTTP sink:

```yaml
sinks:
  - name: chat
    driver: kafka
    kafka:
      brokers: ["redpanda:9092"]
      topic: feed.changes
      acks: all
```

Then swap the `http_server` input for a `redpanda` input that consumes the topic:

```yaml
input:
  redpanda:
    seed_brokers: ["redpanda:9092"]
    topics: ["feed.changes"]
    consumer_group: rss2msg-chat
```

With the broker buffering changes, the `dedupe` processor below also survives a
Redpanda Connect restart, since it re-reads from the consumer group's last commit.

## Reliability

Delivery from rss2msg is **at-least-once**, so a change can arrive at Redpanda
Connect more than once after a retry. Discord and Slack have no idempotency key, so
a duplicate becomes a duplicate message. Two mitigations:

- **De-duplicate in the pipeline:** add a [`dedupe`](https://docs.redpanda.com/redpanda-connect/components/processors/dedupe/)
  processor keyed on `this.item_id + this.content_hash` (backed by a `cache`
  resource) so a re-POSTed change is dropped before it reaches the outputs.
- **Don't lose changes while chat is down:** wrap the rss2msg sink with a
  [dead-letter sink](../choose-a-sink.md) so changes survive while Redpanda Connect —
  or the chat API behind it — is unreachable:

  ```yaml
  sinks:
    - name: chat
      driver: http
      dead_letter: chat-dlq
      http:
        url: http://redpanda-connect:4195/post
    - name: chat-dlq
      driver: stdout
  ```

See [Operational Notes](../../explanation/operations.md) for the delivery
guarantees on the rss2msg side.

## Related

- [Integrate with External Systems](../integrate-with-external-systems.md) — the shared HTTP-sink setup and reliability options.
- [HTTP sink](../sinks/http.md) — every field, header, and success-code detail for the sink driving Redpanda Connect.
- [Kafka sink](../sinks/kafka.md) — the broker alternative for feeding Redpanda Connect.
- [Change Envelope](../../reference/change-envelope.md) — the JSON fields available to the Bloblang mapping.
- [Choose a Sink](../choose-a-sink.md) — dead-letter routing and the driver table.
