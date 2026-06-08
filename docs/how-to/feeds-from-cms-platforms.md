---
title: Feeds from CMS Platforms
type: how-to
tags: [rss2msg/docs, feeds, cms]
summary: Find the RSS/Atom feed URL published by common CMS and publishing platforms, and point rss2msg at it.
updated: 2026-06-09
---

# Feeds from CMS Platforms

Most content management and publishing platforms expose an RSS or Atom feed at a
predictable, platform-specific URL. rss2msg polls any such URL — to the service it
is just another feed. There is **nothing CMS-specific to configure**: once you know
the feed URL, you add it like any other feed (see [Configure Feeds](configure-feeds.md)):

```yaml
feeds:
  - url: https://example.com/feed/   # the platform's feed URL (see per-platform pages)
    interval: 15m
    sinks: [default]
```

The per-platform pages below list where each platform publishes its feed so you
don't have to reverse-engineer it. Replace the example hosts with your own.

## Finding any feed URL

Before reaching for a platform page, two shortcuts work on most sites:

- **Autodiscovery link.** Standards-compliant pages advertise their feed in the
  HTML `<head>`. View source and look for:
  `<link rel="alternate" type="application/rss+xml" href="…">` (or
  `type="application/atom+xml"`). The `href` is the feed URL.
- **Try the common path.** Many engines respond at `/feed/`, `/feed`, `/rss`,
  `/rss.xml`, `/atom.xml`, or `/index.xml`. Open the candidate in a browser — a
  feed renders as XML (or as the browser's feed view) rather than a normal page.

Once you have a working URL, paste it into a `feeds[].url` entry.

## Platforms

Each page gives the feed URL pattern(s) and a ready-to-paste config snippet.

| Platform | Feed URL | Page |
| --- | --- | --- |
| WordPress | `https://site/feed/` | [WordPress](cms/wordpress.md) |
| Ghost | `https://site/rss/` | [Ghost](cms/ghost.md) |
| Drupal | `https://site/rss.xml` | [Drupal](cms/drupal.md) |
| Joomla | `https://site/index.php?format=feed&type=rss` | [Joomla](cms/joomla.md) |
| Squarespace | `https://site/<collection>?format=rss` | [Squarespace](cms/squarespace.md) |
| Wix | `https://site/blog-feed.xml` | [Wix](cms/wix.md) |
| Blogger / Blogspot | `https://site/feeds/posts/default` | [Blogger](cms/blogger.md) |
| Medium | `https://medium.com/feed/@<user>` | [Medium](cms/medium.md) |
| Tumblr | `https://<blog>.tumblr.com/rss` | [Tumblr](cms/tumblr.md) |
| Substack | `https://<name>.substack.com/feed` | [Substack](cms/substack.md) |
| Hugo | `https://site/index.xml` | [Hugo](cms/hugo.md) |
| Jekyll | `https://site/feed.xml` | [Jekyll](cms/jekyll.md) |
| Discourse | `https://forum/latest.rss` | [Discourse](cms/discourse.md) |
| Reddit | `https://www.reddit.com/r/<sub>/.rss` | [Reddit](cms/reddit.md) |
| MediaWiki | `https://wiki/w/index.php?action=feed&feed=rss` | [MediaWiki](cms/mediawiki.md) |
| YouTube | `https://www.youtube.com/feeds/videos.xml?channel_id=<ID>` | [YouTube](cms/youtube.md) |

Feed paths are set by each platform and can change between major versions or be
disabled by site owners. If a documented URL 404s, fall back to the autodiscovery
`<link>` in the page source.

## Example: watch a blog and a forum

```yaml
feeds:
  - url: https://blog.example.com/feed/            # WordPress
    interval: 10m
    sinks: [default]
  - url: https://news.example.org/category/releases/feed/   # WordPress category
    interval: 5m
    sinks: [default]
  - url: https://forum.example.net/c/announcements.rss      # Discourse category
    interval: 15m
    sinks: [default]
```

rss2msg parses RSS, Atom, and JSON Feed identically (via `gofeed`), so feeds from
different platforms can be mixed freely in one config. Per-feed `http.headers` are
available if a platform's feed needs authentication — see
[Configure Feeds](configure-feeds.md).

## Related

- [Configure Feeds](configure-feeds.md) — the `feeds[]` fields these URLs go into.
- [Dynamic Feed Sources](dynamic-feed-sources.md) — load the feed list at runtime instead of hard-coding it.
- [Getting Started](../getting-started.md) — your first feed end to end.
- [`config.example.full.yaml`](../../examples/config.example.full.yaml) — a runnable example with a large real-world feed list.
