---
title: Jekyll feeds
type: how-to
tags: [rss2msg/docs, feeds, cms, jekyll]
summary: Find the Atom feed URL Jekyll publishes and point rss2msg at it.
updated: 2026-06-09
---

# Jekyll feeds

Jekyll publishes a feed at a predictable URL. rss2msg polls it like any other feed —
there is nothing platform-specific to configure once you have the URL.

## Feed URL

| Feed | URL |
| --- | --- |
| Atom | `https://site/feed.xml` |

Produced by the `jekyll-feed` plugin (Atom).

## Add it to rss2msg

```yaml
feeds:
  - url: https://site.example.com/feed.xml
    interval: 15m
    sinks: [default]
```

If a documented path 404s, fall back to the page's autodiscovery `<link>` — see
[Finding any feed URL](../feeds-from-cms-platforms.md#finding-any-feed-url).

## Related

- [Feeds from CMS Platforms](../feeds-from-cms-platforms.md) — the platform index and feed-URL shortcuts.
- [Configure Feeds](../configure-feeds.md) — the `feeds[]` fields these URLs go into.
