# rss2msg User Documentation (Obsidian) — Design

- **Date:** 2026-05-30
- **Status:** Approved (design); implementation plan pending
- **Audience of the docs being built:** End users / integrators — people pointing rss2msg at feeds and consuming the change envelope from a sink.
- **Audience of this spec:** Whoever implements the docs restructure.

## Problem

All end-user content currently lives in a single 791-line `README.md`. It is
well-written but flat: one long scroll with an anchor-only table of contents and
no navigation *between* concepts. The repository is also an Obsidian vault
(vault name `rss2msg`, `.obsidian/` at the repo root), so the same files are
already openable in Obsidian — but nothing exploits Obsidian's structure
(backlinks, graph, Maps of Content, properties).

We want navigable, task-oriented documentation that reads well in **both**
Obsidian and GitHub, with a single source of truth.

## Decisions (locked during brainstorming)

1. **Audience:** end users / integrators (setup + output contract), not
   contributors or operators-only.
2. **README relationship:** decompose the README into focused linked pages;
   single source of truth lives in the vault. README shrinks to a landing page.
   Content **moves**, it is not duplicated.
3. **Link style:** standard markdown links with relative paths
   (`[text](../dir/page.md)`). Renders in both Obsidian and GitHub; Obsidian
   still derives backlinks and the graph from them. No `[[wikilinks]]`.
4. **Structure:** Diátaxis-lite (Tutorial / How-to / Reference / Explanation),
   borrowing one-page-per-sink-driver for the dense sink content.
5. **No Compound-Vault machinery** (hot cache / log / folds) and **no heavy
   plugins** (Dataview). Those serve a second-brain, not polished user docs.

## Non-goals

- Rewriting or re-deriving any documented behavior. This is restructure + relink
  + light prose tightening. No invented behavior.
- Contributor docs (architecture-for-hacking, testing workflow). `Testing` is
  intentionally dropped from the user docs; build-from-source basics fold into
  *Getting Started*.
- Publishing to an external docs site. Out of scope for now; the portable-link
  choice keeps that door open later.

## Page map

New content lives under a top-level `docs/` tree. Internal specs/plans stay
under `docs/superpowers/`. The arrow (`←`) shows the README section each page is
sourced from.

```
README.md                          → trimmed: intro + architecture diagram + "what it does" + linked TOC into docs/
docs/
  index.md                         → Docs home / Map of Content (in-vault entry point)

  getting-started.md               ← Quickstart + Build and run + requirements        [Tutorial]

  how-to/
    configure-feeds.md             ← feeds (url, interval, sinks, http overrides, conditional GET)
    choose-a-sink.md               ← sinks intro + common fields + DLQ + decision table linking ↓
    sinks/
      postgres.md                  ← driver: postgres   (config table, YAML, schema, wire layout)
      kafka.md                     ← driver: kafka      (record layout, acks, compression)
      sqs.md                       ← driver: sqs        (+ FIFO queues)
      sns.md                       ← driver: sns        (+ FIFO topics, RawMessageDelivery note)
      rabbitmq.md                  ← driver: rabbitmq   (exchange/routing, impl notes)
      stdout.md                    ← driver: stdout
      http.md                      ← driver: http       (webhook integration)
    run-multiple-instances.md      ← coordination (memory/postgres/redis) + acquire/release flow
    secure-connections-tls.md      ← Postgres TLS + Redis TLS (covers state & coordination)

  reference/
    configuration.md               ← loading order + env vars + top-level structure + log/telemetry/http/retry/runtime/state
    change-envelope.md             ← Change JSON + field semantics (item_id, content_hash, kind, DLQ annotations)
    wire-formats.md                ← per-sink key/body/metadata table
    telemetry.md                   ← OTEL instrument table + trace/log correlation
    cli.md                         ← serve / run-once / validate-config + flags + signal handling

  explanation/
    how-it-works.md                ← pipeline diagram + data-flow narrative
    operations.md                  ← operational notes (at-least-once, acks, DLQ, multi-instance, shutdown, LocalStack)
```

~20 pages. The 7 per-sink pages are deliberate: sinks are the densest,
most integrator-relevant content, and "wire up sink X" is a natural per-page
task.

## Conventions

### Frontmatter (every page)

```yaml
---
title: Configure Feeds
type: how-to          # tutorial | how-to | reference | explanation
tags: [rss2msg/docs, feeds]
summary: One-line description shown in hover previews and search.
updated: 2026-05-30
---
```

`type` mirrors the Diátaxis quadrant. `tags` are namespaced under `rss2msg/docs`
so the doc set is filterable as a unit. `summary` powers hover previews/search.

### Links

- Standard markdown, relative paths only: `[Choose a Sink](../how-to/choose-a-sink.md)`.
- Every page ends with a short **Related** section of cross-links, so the graph
  is densely connected rather than a star around `index.md`.
- Before renaming/moving any page, check `obsidian backlinks path=<page>` to
  avoid link rot (the main risk of decomposition).

### Obsidian-native touches (sparingly, all GitHub-safe)

- `docs/index.md` is a hand-curated **Map of Content**: links grouped under the
  four Diátaxis headings. The single "start here" note.
- **Callouts** for inline warnings the README already flags — e.g.
  *Kafka `acks: none` is unsafe*, *`insecure_skip_verify` is test-only*. Render
  as plain blockquotes on GitHub.
- No Dataview, no `.base` files (can be added later if desired), no
  hot-cache/log/folds.

### README transformation

Shrinks from 791 lines to a landing page: intro paragraph, the ASCII
architecture diagram, a one-paragraph "what it does," then a linked table of
contents pointing into `docs/`. Deep reference content **moves** into the vault
pages (single source of truth). The README keeps the "Design docs" pointer to
`docs/superpowers/`.

## Authoring workflow (how Obsidian fits)

Because links are portable markdown and the vault *is* the repo, routine
authoring is plain `.md` editing — no special tooling required. The `obsidian`
CLI (PATH shim at `~/.local/bin/obsidian`, transport recorded in
`.vault-meta/transport.json`) is used for what plain editing does poorly:

- `obsidian search query="..."` — locate existing coverage before duplicating.
- `obsidian backlinks path=...` — audit inbound links before a rename/move.
- `obsidian create path=... content=...` / `obsidian open path=...` — scaffold
  and navigate while the app stays in sync.
- `obsidian properties` / `property:set` — inspect/patch frontmatter in bulk.

The live Obsidian app must be running for the CLI to respond; if it is closed,
direct filesystem editing is the documented fallback (`preferred: cli`,
`fallback_chain: [cli, filesystem]`).

## Git handling

These are tracked repo files; work lands as normal commits. Recommendation
(final call at implementation time):

- **Commit** `.vault-meta/transport.json` (small, stable, useful to collaborators).
- **`.gitignore`** the churny `.obsidian/workspace.json` (and similar
  per-user state) while keeping shareable `.obsidian/*.json` if desired.

## Build sequence (for the implementation plan)

1. `docs/index.md` + **one exemplar page** (e.g. `reference/change-envelope.md`)
   — lock the frontmatter + link + Related conventions on a real page first.
2. **Reference** pages (densest, most cross-linked).
3. **How-to** pages (incl. the 7 sink pages).
4. **Tutorial** (`getting-started.md`) + **Explanation** pages.
5. **README transform last** — so its links point at pages that already exist.
6. Link-rot pass: `obsidian backlinks` / a relative-link checker over `docs/`.

## Success criteria

- Every former README section is reachable from `docs/index.md` within two
  clicks, organized by task.
- All internal links resolve in both Obsidian (backlinks/graph populated) and
  GitHub (no broken-link rendering).
- README is a concise landing page; no end-user reference content is duplicated
  between README and `docs/`.
- No documented behavior changed or invented relative to the current README.
```
