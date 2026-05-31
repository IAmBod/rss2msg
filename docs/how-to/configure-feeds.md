---
title: Configure Feeds
type: how-to
tags: [rss2msg/docs, feeds]
summary: Declare feeds to poll — url, interval, per-feed sinks, HTTP overrides, and conditional GET.
updated: 2026-05-30
---

# Configure Feeds

A non-empty list of feeds to poll.

```yaml
feeds:
  - url: https://example.com/blog/rss.xml
    interval: 5m
    sinks: [pg-main, kafka-main]
  - url: https://other.example/atom.xml
    interval: 15m
    http:
      timeout: 10s
      headers:
        Authorization: "Bearer ${OTHER_FEED_TOKEN}"
```

| field            | required | notes |
| ---------------- | -------- | ----- |
| `url`            | yes      | RSS / Atom / JSON Feed URL (parsed by `gofeed`). |
| `interval`       | yes      | `time.Duration`. Minimum `1s`. Used by `serve`; `run-once` ignores it. |
| `sinks`          | no       | Names of declared sinks. Empty falls back to a sink named `default` (validation requires `default` to exist if any feed omits the list). See [Choose a Sink](choose-a-sink.md). |
| `http.timeout`   | no       | Per-feed override of `http.timeout`. |
| `http.headers`   | no       | Extra request headers. `If-Modified-Since` and `If-None-Match` are reserved (the fetcher manages them); validation rejects overrides of either. |

The fetcher sends `If-None-Match` / `If-Modified-Since` from `feed_meta`
when present, and updates the row from the response's `ETag` /
`Last-Modified`. A 304 short-circuits parsing.

## Related

- [Dynamic Feed Sources](dynamic-feed-sources.md) — reconcile the feed list at runtime from `feed_sources:`.
- [Choose a Sink](choose-a-sink.md) — the sink names referenced by `feeds[].sinks`.
- [Configuration Reference](../reference/configuration.md#http) — global `http` defaults feeds can override.
