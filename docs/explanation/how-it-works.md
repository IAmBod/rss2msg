---
title: How It Works
type: explanation
tags: [rss2msg/docs, architecture]
summary: The poll → detect → store → publish pipeline and where coordination fits.
updated: 2026-05-30
---

# How It Works

> **Architecture:** [pipeline canvas](architecture.canvas) — `feeds → fetcher/detector → state store → sinks`, with a coordinator gating poll cycles. (Opens in Obsidian.)

Each poll cycle begins with the fetcher sending a conditional HTTP GET using `If-None-Match` and `If-Modified-Since` headers sourced from the stored `feed_meta` row; a 304 response short-circuits parsing for that feed. When new content arrives, the detector compares a content hash for each item against the `seen_items` store, classifying items as new, updated, or unchanged — unchanged items are not published. Each resulting [Change Envelope](../reference/change-envelope.md) is then fanned out to every configured sink, with per-sink retry and optional dead-letter on failure. Before any of this occurs, a coordinator (memory, postgres, or redis) gates whether this instance may poll a given feed in this cycle — see [Run Multiple Instances](../how-to/run-multiple-instances.md); losing instances skip the cycle silently without leader election. The state store used by both the detector and the fetcher cache layer is configured under [`state`](../how-to/state-stores.md).

## Related

- [Change Envelope](../reference/change-envelope.md) — what "publish" emits.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — the coordinator gate.
- [Operational Notes](operations.md) — delivery guarantees and failure handling.
