---
title: Hugo feeds
type: how-to
tags: [rss2msg/docs, feeds, cms, hugo]
summary: Find the RSS feed URL Hugo publishes and point rss2msg at it.
updated: 2026-06-09
---

# Hugo feeds

Hugo publishes a feed at a predictable URL. rss2msg polls it like any other feed —
there is nothing platform-specific to configure once you have the URL.

## Feed URL

| Feed | URL |
| --- | --- |
| Default (RSS) | `https://site/index.xml` |
| Section | `https://site/<section>/index.xml` |

## Add it to rss2msg

```yaml
feeds:
  - url: https://site.example.com/index.xml
    interval: 15m
    sinks: [default]
```

If a documented path 404s, fall back to the page's autodiscovery `<link>` — see
[Finding any feed URL](../get-feeds-from-cms-platforms.md#finding-any-feed-url).

## Related

- [Get Feeds from CMS Platforms](../get-feeds-from-cms-platforms.md) — the platform index and feed-URL shortcuts.
- [Configure Feeds](../configure-feeds.md) — the `feeds[]` fields these URLs go into.
