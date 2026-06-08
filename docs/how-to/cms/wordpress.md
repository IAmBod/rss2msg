---
title: WordPress feeds
type: how-to
tags: [rss2msg/docs, feeds, cms, wordpress]
summary: Find the RSS/Atom feed URL WordPress publishes and point rss2msg at it.
updated: 2026-06-09
---

# WordPress feeds

WordPress publishes a feed at a predictable URL. rss2msg polls it like any other
feed — there is nothing platform-specific to configure once you have the URL.

## Feed URL

| Feed | URL |
| --- | --- |
| Main (RSS) | `https://site/feed/` |
| Main (Atom) | `https://site/feed/atom/` |
| Category | `https://site/category/<slug>/feed/` |
| Tag | `https://site/tag/<slug>/feed/` |
| Author | `https://site/author/<name>/feed/` |
| Comments | `https://site/comments/feed/` |

## Add it to rss2msg

```yaml
feeds:
  - url: https://blog.example.com/feed/
    interval: 15m
    sinks: [default]
```

If a documented path 404s, fall back to the page's autodiscovery `<link>` — see
[Finding any feed URL](../feeds-from-cms-platforms.md#finding-any-feed-url).

## Related

- [Feeds from CMS Platforms](../feeds-from-cms-platforms.md) — the platform index and feed-URL shortcuts.
- [Configure Feeds](../configure-feeds.md) — the `feeds[]` fields these URLs go into.
