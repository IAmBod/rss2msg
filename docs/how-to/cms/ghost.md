---
title: Ghost feeds
type: how-to
tags: [rss2msg/docs, feeds, cms, ghost]
summary: Find the RSS feed URL Ghost publishes and point rss2msg at it.
updated: 2026-06-09
---

# Ghost feeds

Ghost publishes a feed at a predictable URL. rss2msg polls it like any other feed —
there is nothing platform-specific to configure once you have the URL.

## Feed URL

| Feed | URL |
| --- | --- |
| Main | `https://site/rss/` |
| Tag archive | `https://site/tag/<slug>/rss/` |
| Author archive | `https://site/author/<slug>/rss/` |

## Add it to rss2msg

```yaml
feeds:
  - url: https://blog.example.com/rss/
    interval: 15m
    sinks: [default]
```

If a documented path 404s, fall back to the page's autodiscovery `<link>` — see
[Finding any feed URL](../get-feeds-from-cms-platforms.md#finding-any-feed-url).

## Related

- [Get Feeds from CMS Platforms](../get-feeds-from-cms-platforms.md) — the platform index and feed-URL shortcuts.
- [Configure Feeds](../configure-feeds.md) — the `feeds[]` fields these URLs go into.
