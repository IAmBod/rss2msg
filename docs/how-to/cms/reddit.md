---
title: Reddit feeds
type: how-to
tags: [rss2msg/docs, feeds, cms, reddit]
summary: Find the RSS feed URL Reddit publishes and point rss2msg at it.
updated: 2026-06-09
---

# Reddit feeds

Reddit publishes a feed at a predictable URL. rss2msg polls it like any other feed —
there is nothing platform-specific to configure once you have the URL.

## Feed URL

| Feed | URL |
| --- | --- |
| Subreddit | `https://www.reddit.com/r/<sub>/.rss` |
| User | `https://www.reddit.com/user/<name>/.rss` |

Search results also expose an `.rss` feed. Reddit rate-limits aggressive polling —
keep `interval` generous.

## Add it to rss2msg

```yaml
feeds:
  - url: https://www.reddit.com/r/golang/.rss
    interval: 30m
    sinks: [default]
```

If a documented path 404s, fall back to the page's autodiscovery `<link>` — see
[Finding any feed URL](../feeds-from-cms-platforms.md#finding-any-feed-url).

## Related

- [Feeds from CMS Platforms](../feeds-from-cms-platforms.md) — the platform index and feed-URL shortcuts.
- [Configure Feeds](../configure-feeds.md) — the `feeds[]` fields these URLs go into.
