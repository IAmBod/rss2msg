---
title: YouTube feeds
type: how-to
tags: [rss2msg/docs, feeds, cms, youtube]
summary: Find the Atom feed URL YouTube publishes and point rss2msg at it.
updated: 2026-06-09
---

# YouTube feeds

YouTube publishes a feed at a predictable URL. rss2msg polls it like any other feed —
there is nothing platform-specific to configure once you have the URL.

## Feed URL

| Feed | URL |
| --- | --- |
| Channel uploads | `https://www.youtube.com/feeds/videos.xml?channel_id=<ID>` |
| Playlist | `https://www.youtube.com/feeds/videos.xml?playlist_id=<ID>` |

## Add it to rss2msg

```yaml
feeds:
  - url: https://www.youtube.com/feeds/videos.xml?channel_id=UC_x5XG1OV2P6uZZ5FSM9Ttw
    interval: 30m
    sinks: [default]
```

If a documented path 404s, fall back to the page's autodiscovery `<link>` — see
[Finding any feed URL](../get-feeds-from-cms-platforms.md#finding-any-feed-url).

## Related

- [Get Feeds from CMS Platforms](../get-feeds-from-cms-platforms.md) — the platform index and feed-URL shortcuts.
- [Configure Feeds](../configure-feeds.md) — the `feeds[]` fields these URLs go into.
