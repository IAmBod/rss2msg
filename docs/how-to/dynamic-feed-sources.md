---
title: Dynamic Feed Sources
type: how-to
tags: [rss2msg/docs, feeds, dynamic]
summary: Reconcile the serve daemon's feed list at runtime from ordered sources (file, static), with SIGHUP and file-watch reload.
updated: 2026-05-31
---

# Dynamic Feed Sources

By default, the `feeds:` block is the complete feed list. `feed_sources:`
lets the `serve` daemon reconcile its feed list at runtime from one or more
ordered sources — no restart required.

```yaml
feed_sources:
  - type: file          # JSON array of feed specs; watched, reloads on change
    name: control-plane
    path: /etc/rss2msg/feeds.json
  - type: static        # injects the feeds: block at this precedence position
```

`feed_sources:` is an **ordered** list. Each entry has a `type` plus its own
fields. **Order is precedence**: when the same feed `url` appears in multiple
sources, the earlier entry wins. Feeds are deduplicated by `url`.

If `feed_sources:` is omitted entirely, the `feeds:` block is the sole source
(unchanged behavior).

## Source types

| type     | status      | description |
| -------- | ----------- | ----------- |
| `static` | implemented | Injects the top-level `feeds:` block at this position in the precedence order. |
| `file`   | implemented | Reads a JSON file of feed specs; watches the file for changes and reloads automatically. |
| `http`, `postgres`, `sqlite`, `redis`, `s3`, `env` | planned | Not yet implemented. |

### `type: file`

```yaml
- type: file
  name: control-plane       # optional label for logs
  path: /etc/rss2msg/feeds.json
```

The file must contain a JSON array of feed spec objects. Both `url` and
`interval` are required for `serve` to schedule the feed — a feed without
a valid interval causes that entire reload to be rejected atomically, leaving
the previously-running feed set unchanged. `sinks` and `http` are optional:

```json
[
  {
    "url": "https://example.com/feed.xml",
    "interval": "5m",
    "sinks": ["out"],
    "http": {
      "timeout": "10s",
      "headers": { "X-Token": "abc" }
    }
  }
]
```

The daemon watches the file for changes and reloads the feed list
automatically when it is modified.

### `type: static`

```yaml
- type: static
```

Splices the top-level `feeds:` block into the source list at this position.
Useful when you want the static feeds to have lower precedence than a
`file` source but higher precedence than another.

## Runtime reload

The `serve` daemon reloads the merged feed list whenever any `file` source
changes. Sending **SIGHUP** to the process forces an immediate reload of all
sources.

Reload scope is **feeds only**. The following require a full restart:

- Adding, removing, or reconfiguring a `feed_sources` entry itself.
- Changes to sinks, coordination, state, or any other top-level config.

## Reload semantics

- **Sink resolution.** A feed with no `sinks` list falls back to a sink named
  `default`. If a feed references a sink name that is not declared, the
  **entire reload fails atomically** — the previously-running feed set keeps
  running unchanged.
- **Removed feeds keep their state.** Re-adding the same `url` later resumes
  without re-emitting already-seen items.
- **Interval changes reset the ticker.** If a feed's `interval` changes across
  a reload, its poll ticker is restarted with the new value.

## Related

- [Configure Feeds](configure-feeds.md) — the static `feeds:` block these sources merge.
- [Configuration Reference](../reference/configuration.md) — top-level config structure.
- [Deploy in Production](deploy.md) — `serve` and signal handling (SIGHUP).
