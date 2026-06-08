---
title: Discourse feeds
type: how-to
tags: [rss2msg/docs, feeds, cms, discourse]
summary: Find the RSS feed URL Discourse publishes and point rss2msg at it.
updated: 2026-06-09
---

# Discourse feeds

Discourse publishes a feed at a predictable URL. rss2msg polls it like any other
feed — there is nothing platform-specific to configure once you have the URL.

## Feed URL

| Feed | URL |
| --- | --- |
| Latest | `https://forum/latest.rss` |
| Category | `https://forum/c/<slug>.rss` |
| Tag | `https://forum/tag/<slug>.rss` |
| Topic | `https://forum/t/<slug>/<id>.rss` |

Most Discourse listings support an `.rss` suffix.

## Add it to rss2msg

```yaml
feeds:
  - url: https://forum.example.net/c/announcements.rss
    interval: 15m
    sinks: [default]
```

If a documented path 404s, fall back to the page's autodiscovery `<link>` — see
[Finding any feed URL](../get-feeds-from-cms-platforms.md#finding-any-feed-url).

## Related

- [Get Feeds from CMS Platforms](../get-feeds-from-cms-platforms.md) — the platform index and feed-URL shortcuts.
- [Configure Feeds](../configure-feeds.md) — the `feeds[]` fields these URLs go into.
