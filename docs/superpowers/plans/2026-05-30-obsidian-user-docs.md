# Obsidian End-User Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompose the 791-line `README.md` into ~20 task-oriented, cross-linked Obsidian pages under `docs/`, leaving the README a short landing page.

**Architecture:** Diátaxis-lite (Tutorial / How-to / Reference / Explanation). Content **moves** verbatim from named README line ranges with a fixed set of transforms (add frontmatter, rewrite intra-doc anchor links as relative `.md` links, wrap inline warnings as callouts, add a "Related" footer). Links are portable markdown so they render in both GitHub and Obsidian; a `scripts/check-doc-links.sh` checker enforces no link rot and is run green after every task. `docs/index.md` (a Map of Content) grows one entry per page as pages are built, so the checker stays green at every commit.

**Tech Stack:** Markdown, Obsidian (vault = repo root, CLI shim at `~/.local/bin/obsidian`), bash + grep for the link checker. No new app code, no plugins.

**Spec:** [`docs/superpowers/specs/2026-05-30-obsidian-user-docs-design.md`](../specs/2026-05-30-obsidian-user-docs-design.md)

---

## Source-of-truth: README line ranges

The current `README.md` (791 lines) is the content source. Each page moves one range:

| README section | lines | destination page |
| --- | --- | --- |
| Title + intro + ASCII diagram | 1–24 | stays in README (trimmed); diagram also copied to `explanation/how-it-works.md` |
| Quickstart | 54–85 | `docs/getting-started.md` |
| Build and run | 89–100 | `docs/getting-started.md` |
| CLI | 104–120 | `docs/reference/cli.md` |
| Configuration intro + Loading order | 124–151 | `docs/reference/configuration.md` |
| Top-level structure | 153–165 | `docs/reference/configuration.md` |
| `log` | 167–172 | `docs/reference/configuration.md` |
| `telemetry` (config) | 174–203 | `docs/reference/configuration.md` |
| `http` | 205–213 | `docs/reference/configuration.md` |
| `retry` | 215–228 | `docs/reference/configuration.md` |
| `runtime` | 230–235 | `docs/reference/configuration.md` |
| `state` | 237–291 | `docs/reference/configuration.md` |
| `coordination` (driver table + acquire/release) | 293–321, 359–373 | `docs/how-to/run-multiple-instances.md` |
| Postgres TLS + Redis TLS | 322–357 | `docs/how-to/secure-connections-tls.md` |
| `sinks` intro + common fields | 375–387 | `docs/how-to/choose-a-sink.md` |
| `driver: postgres` | 389–420 | `docs/how-to/sinks/postgres.md` |
| `driver: kafka` | 422–445 | `docs/how-to/sinks/kafka.md` |
| `driver: sqs` (+FIFO) | 447–487 | `docs/how-to/sinks/sqs.md` |
| `driver: sns` (+FIFO) | 488–520 | `docs/how-to/sinks/sns.md` |
| `driver: rabbitmq` | 522–555 | `docs/how-to/sinks/rabbitmq.md` |
| `driver: stdout` | 557–583 | `docs/how-to/sinks/stdout.md` |
| `driver: http` | 585–616 | `docs/how-to/sinks/http.md` |
| `feeds` | 618–645 | `docs/how-to/configure-feeds.md` |
| The change envelope | 649–682 | `docs/reference/change-envelope.md` |
| Sink wire formats | 686–696 | `docs/reference/wire-formats.md` |
| Telemetry (instruments) | 700–718 | `docs/reference/telemetry.md` |
| Operational notes | 722–758 | `docs/explanation/operations.md` |
| Testing | 761–778 | **dropped** (contributor-facing) |
| Design docs | 782–791 | stays in README |

> Line numbers are from the README at plan-authoring time (commit `6761adb`). If the README was edited since, re-locate by heading text, not by number.

## Page authoring recipe (applies to every page task below)

For each page, in order:

1. **Create the file** with the exact frontmatter block given in its task.
2. **Move** the README lines for that page *verbatim* below the frontmatter — do not reword behavior. Demote the README `###`/`####` heading to a single `#` H1 matching the frontmatter `title` (Obsidian uses the H1, not the filename).
3. **Rewrite cross-references** per the task's rewrite list: any `[label](#anchor)` that pointed at another README section becomes a relative link to that section's new page, e.g. `[coordination](#coordination)` → `[Run Multiple Instances](../how-to/run-multiple-instances.md)`. Links to source files become repo-relative, e.g. `[change.go](./internal/model/change.go)` → `[change.go](../../internal/model/change.go)`.
4. **Wrap inline warnings as callouts** where the task lists them (text unchanged).
5. **Append a `## Related` section** with the relative links the task specifies.
6. **Add the page's link to `docs/index.md`** under the correct heading (link text = page title).
7. **Run the link checker** (`bash scripts/check-doc-links.sh`) — must print `OK`.
8. **Commit.**

Relative-path reference (depth from each dir to repo root): `docs/*` → `..`; `docs/reference/*`, `docs/how-to/*`, `docs/explanation/*` → `../..`; `docs/how-to/sinks/*` → `../../..`.

---

### Task 1: Foundation — link checker, tree, MOC, exemplar page

**Files:**
- Create: `scripts/check-doc-links.sh`
- Create: `docs/index.md`
- Create: `docs/reference/change-envelope.md`

- [ ] **Step 1: Write the link checker**

Create `scripts/check-doc-links.sh`:

```bash
#!/usr/bin/env bash
# Verify every relative markdown link in README.md and docs/ resolves to a file.
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
fail=0
files=$(find "$root/docs" -name '*.md' 2>/dev/null; echo "$root/README.md")
for f in $files; do
  [ -f "$f" ] || continue
  dir="$(dirname "$f")"
  while IFS= read -r target; do
    path="${target%%#*}"                       # strip #anchor
    [ -z "$path" ] && continue                 # pure in-page anchor
    case "$path" in http://*|https://*|mailto:*) continue ;; esac
    if [ ! -e "$dir/$path" ]; then
      echo "BROKEN: ${f#"$root"/} -> $target"
      fail=1
    fi
  done < <(grep -oE '\]\([^)]+\)' "$f" | sed -E 's/^\]\(//; s/\)$//')
done
[ "$fail" -eq 0 ] && echo "OK: all relative doc links resolve"
exit "$fail"
```

- [ ] **Step 2: Make it executable and verify it runs (no docs yet → OK)**

Run:
```bash
chmod +x scripts/check-doc-links.sh && bash scripts/check-doc-links.sh
```
Expected: `OK: all relative doc links resolve` (only README.md exists; its existing links are repo-relative and resolve).

> If README currently has anchor-only links like `(#quickstart)`, the checker skips them (empty path after stripping `#`). Confirmed safe.

- [ ] **Step 3: Create the exemplar page `docs/reference/change-envelope.md`**

This page establishes every convention. Frontmatter:

```yaml
---
title: Change Envelope
type: reference
tags: [rss2msg/docs, schema, output]
summary: The canonical JSON Change object published to every sink, and its field semantics.
updated: 2026-05-30
---
```

Then `# Change Envelope`, then move README lines 649–682 verbatim. Rewrites:
- `[`internal/model/change.go`](./internal/model/change.go)` → `[`internal/model/change.go`](../../internal/model/change.go)`
- the DLQ bullet's implicit reference to sinks: add a relative link "see [Choose a Sink](../how-to/choose-a-sink.md)" inline where DLQ annotations are mentioned.

Append:
```markdown
## Related

- [Sink Wire Formats](wire-formats.md) — how the envelope maps onto each sink's key/body/metadata.
- [Choose a Sink](../how-to/choose-a-sink.md) — DLQ annotations and sink selection.
- [How It Works](../explanation/how-it-works.md) — where `kind` and `content_hash` are computed in the pipeline.
```

- [ ] **Step 4: Create `docs/index.md` (Map of Content)**

```markdown
---
title: rss2msg Documentation
type: index
tags: [rss2msg/docs, moc]
summary: Map of Content for rss2msg end-user and integrator documentation.
updated: 2026-05-30
---

# rss2msg Documentation

Start here. Pages are grouped the way you use them.

## Tutorial

_(added in Task 5)_

## How-to guides

_(added in Tasks 3–4)_

## Reference

- [Change Envelope](reference/change-envelope.md)

## Explanation

_(added in Task 5)_
```

- [ ] **Step 5: Run the checker**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`

> The `_(added in Task N)_` placeholders contain no links, so the checker passes. They are replaced with real links as those tasks land.

- [ ] **Step 6: Commit**

```bash
git add scripts/check-doc-links.sh docs/index.md docs/reference/change-envelope.md
git commit -m "docs: scaffold docs tree, link checker, and change-envelope reference"
```

---

### Task 2: Reference pages

**Files:**
- Create: `docs/reference/configuration.md`
- Create: `docs/reference/wire-formats.md`
- Create: `docs/reference/telemetry.md`
- Create: `docs/reference/cli.md`
- Modify: `docs/index.md` (Reference section)

- [ ] **Step 1: `docs/reference/configuration.md`**

Frontmatter:
```yaml
---
title: Configuration Reference
type: reference
tags: [rss2msg/docs, configuration]
summary: Loading order, environment variables, and every config field except sinks, coordination, and feeds.
updated: 2026-05-30
---
```
Move README lines 124–291 (intro, loading order, top-level structure, `log`, `telemetry`, `http`, `retry`, `runtime`, `state`) verbatim under `# Configuration Reference`. Rewrites:
- `[`config.example.yaml`](./config.example.yaml)` → `(../../config.example.yaml)`
- the `state.postgres.tls` note "see the Postgres TLS subsection under coordination (#postgres-tls)" → `[Secure Connections (TLS)](../how-to/secure-connections-tls.md)`
- the dead-letter mention in `retry` ("[dead-letter sink](#sinks)") → `[Choose a Sink](../how-to/choose-a-sink.md)`
- the top-level structure list items for `coordination`, `sinks`, `feeds` get inline links to `../how-to/run-multiple-instances.md`, `../how-to/choose-a-sink.md`, `../how-to/configure-feeds.md` respectively.

Callout: none required.

Append:
```markdown
## Related

- [CLI](cli.md) — flags that point at this config file.
- [Configure Feeds](../how-to/configure-feeds.md) — the `feeds` list.
- [Choose a Sink](../how-to/choose-a-sink.md) — the `sinks` list.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — the `coordination` block.
- [Secure Connections (TLS)](../how-to/secure-connections-tls.md) — TLS for Postgres state and coordination.
```

- [ ] **Step 2: `docs/reference/wire-formats.md`**

Frontmatter:
```yaml
---
title: Sink Wire Formats
type: reference
tags: [rss2msg/docs, sinks, output]
summary: Per-sink key, body, and metadata layout for the published Change envelope.
updated: 2026-05-30
---
```
Move README lines 686–696 under `# Sink Wire Formats`. Append:
```markdown
## Related

- [Change Envelope](change-envelope.md) — the payload these formats carry.
- [Choose a Sink](../how-to/choose-a-sink.md) — picking and configuring a sink.
```

- [ ] **Step 3: `docs/reference/telemetry.md`**

Frontmatter:
```yaml
---
title: Telemetry
type: reference
tags: [rss2msg/docs, observability]
summary: OTEL instruments, their attributes, and trace/log correlation.
updated: 2026-05-30
---
```
Move README lines 700–718 under `# Telemetry`. Append:
```markdown
## Related

- [Configuration Reference](configuration.md) — the `telemetry` config block and OTLP env vars.
- [Operational Notes](../explanation/operations.md) — enabling exporters in production.
```

- [ ] **Step 4: `docs/reference/cli.md`**

Frontmatter:
```yaml
---
title: CLI
type: reference
tags: [rss2msg/docs, cli]
summary: The serve, run-once, and validate-config commands, their flags, and signal handling.
updated: 2026-05-30
---
```
Move README lines 104–120 under `# CLI`. Rewrite the `runtime.shutdown_drain_timeout` mention to link `[Configuration Reference](configuration.md)`. Append:
```markdown
## Related

- [Getting Started](../getting-started.md) — first run of each command.
- [Configuration Reference](configuration.md) — the config file the `--config` flag loads.
```

- [ ] **Step 5: Update `docs/index.md` Reference section**

Replace the Reference list with:
```markdown
## Reference

- [Configuration Reference](reference/configuration.md)
- [Change Envelope](reference/change-envelope.md)
- [Sink Wire Formats](reference/wire-formats.md)
- [Telemetry](reference/telemetry.md)
- [CLI](reference/cli.md)
```

- [ ] **Step 6: Run the checker**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`

> Forward links to not-yet-created pages (`configure-feeds.md`, `choose-a-sink.md`, etc.) will make the checker FAIL here. To keep the checker green per-task, this task's forward links are allowed only if their targets exist. **Resolution:** the cross-links in Steps 1–4 that point at how-to/explanation pages are added in this task but those targets are created in Tasks 3–5. Therefore: in Step 6, run `bash scripts/check-doc-links.sh` and expect it to report BROKEN links to the how-to/explanation pages. Defer the commit gate: re-run after Task 5. **Simpler rule adopted:** create empty-but-valid stub targets now — see Step 7.

- [ ] **Step 7: Create stubs for forward-link targets**

To keep the checker green at every commit, create minimal valid stubs for pages referenced but authored later. Each stub is a real page with frontmatter + H1 + a one-line note; later tasks overwrite the body. Create with these exact paths and a uniform stub body:

Paths: `docs/how-to/configure-feeds.md`, `docs/how-to/choose-a-sink.md`, `docs/how-to/run-multiple-instances.md`, `docs/how-to/secure-connections-tls.md`, `docs/getting-started.md`, `docs/explanation/operations.md`, `docs/explanation/how-it-works.md`.

Stub body (substitute `TITLE`):
```markdown
---
title: TITLE
type: stub
tags: [rss2msg/docs]
summary: Stub — authored in a later task.
updated: 2026-05-30
---

# TITLE

> Authored in a later task of the docs restructure.
```

- [ ] **Step 8: Re-run checker and commit**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`
```bash
git add docs/reference docs/how-to docs/getting-started.md docs/explanation docs/index.md
git commit -m "docs: reference pages (configuration, wire-formats, telemetry, cli) + forward-link stubs"
```

---

### Task 3: Core how-to pages

**Files (overwrite stubs):**
- `docs/how-to/configure-feeds.md`
- `docs/how-to/choose-a-sink.md`
- `docs/how-to/run-multiple-instances.md`
- `docs/how-to/secure-connections-tls.md`
- Modify: `docs/index.md` (How-to section)

- [ ] **Step 1: `configure-feeds.md`**

Frontmatter:
```yaml
---
title: Configure Feeds
type: how-to
tags: [rss2msg/docs, feeds]
summary: Declare feeds to poll — url, interval, per-feed sinks, HTTP overrides, and conditional GET.
updated: 2026-05-30
---
```
Move README lines 618–645 under `# Configure Feeds`. Rewrite the `default` sink mention to link `[Choose a Sink](choose-a-sink.md)`. Append:
```markdown
## Related

- [Choose a Sink](choose-a-sink.md) — the sink names referenced by `feeds[].sinks`.
- [Configuration Reference](../reference/configuration.md#http) — global `http` defaults feeds can override.
```

- [ ] **Step 2: `choose-a-sink.md`**

Frontmatter:
```yaml
---
title: Choose a Sink
type: how-to
tags: [rss2msg/docs, sinks]
summary: Common sink fields, dead-letter routing, and a decision table linking to each driver.
updated: 2026-05-30
---
```
Move README lines 375–387 (sinks intro + common-fields table) under `# Choose a Sink`. Then add a driver decision table linking to each sink page:
```markdown
## Drivers

| driver | use it for | page |
| --- | --- | --- |
| postgres | durable SQL store, queryable history | [postgres](sinks/postgres.md) |
| kafka | high-throughput streaming, co-partition by item | [kafka](sinks/kafka.md) |
| sqs | AWS queue, optional FIFO ordering | [sqs](sinks/sqs.md) |
| sns | AWS pub/sub fan-out, optional FIFO | [sns](sinks/sns.md) |
| rabbitmq | AMQP routing (topic/direct/fanout) | [rabbitmq](sinks/rabbitmq.md) |
| stdout | local dev, debugging, ad-hoc pipelines | [stdout](sinks/stdout.md) |
| http | webhooks (Slack, Discord, custom receivers) | [http](sinks/http.md) |
```
Append:
```markdown
## Related

- [Sink Wire Formats](../reference/wire-formats.md) — the on-the-wire layout per driver.
- [Operational Notes](../explanation/operations.md) — at-least-once delivery and DLQ behavior.
```

- [ ] **Step 3: `run-multiple-instances.md`**

Frontmatter:
```yaml
---
title: Run Multiple Instances
type: how-to
tags: [rss2msg/docs, coordination, scaling]
summary: Gate poll cycles across horizontally-scaled instances with the memory, postgres, or redis coordinator.
updated: 2026-05-30
---
```
Move README lines 293–321 (coordination intro + YAML, **excluding** the `#### Postgres TLS` and `#### Redis TLS` subsections at 322–357) and 359–373 (driver table + acquire/release narrative) under `# Run Multiple Instances`. Rewrite the two TLS subsection references to `[Secure Connections (TLS)](secure-connections-tls.md)`. Append:
```markdown
## Related

- [Secure Connections (TLS)](secure-connections-tls.md) — TLS for the postgres/redis coordinators.
- [Configuration Reference](../reference/configuration.md#state) — state store, which the postgres coordinator's DSN falls back to.
- [Operational Notes](../explanation/operations.md) — no-leader-election semantics and crash recovery.
```

- [ ] **Step 4: `secure-connections-tls.md`**

Frontmatter:
```yaml
---
title: Secure Connections (TLS)
type: how-to
tags: [rss2msg/docs, tls, security]
summary: Structured TLS config for Postgres (state + coordination) and Redis (coordination).
updated: 2026-05-30
---
```
Move README lines 322–357 (Postgres TLS + Redis TLS subsections) under `# Secure Connections (TLS)`. Add a short lead paragraph (one sentence, no new behavior): "TLS applies to the Postgres state store, the Postgres coordinator, and the Redis coordinator; the field surface is identical across them." Wrap the two `insecure_skip_verify` rows' "Test only" note as a callout:
```markdown
> [!warning]
> `insecure_skip_verify: true` disables server certificate verification. Test only — it is logged at `warn` on startup.
```
Append:
```markdown
## Related

- [Run Multiple Instances](run-multiple-instances.md) — the coordinator backends these TLS blocks secure.
- [Configuration Reference](../reference/configuration.md#state) — `state.postgres.tls` shares this field surface.
```

- [ ] **Step 5: Update `docs/index.md` How-to section**

```markdown
## How-to guides

- [Configure Feeds](how-to/configure-feeds.md)
- [Choose a Sink](how-to/choose-a-sink.md)
- [Run Multiple Instances](how-to/run-multiple-instances.md)
- [Secure Connections (TLS)](how-to/secure-connections-tls.md)
```

- [ ] **Step 6: Run checker and commit**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK` — note `choose-a-sink.md` now links to `sinks/*.md` which do **not** exist yet. Create sink stubs first:

```bash
for s in postgres kafka sqs sns rabbitmq stdout http; do
  mkdir -p docs/how-to/sinks
  printf -- '---\ntitle: %s sink\ntype: stub\ntags: [rss2msg/docs, sinks]\nsummary: Stub — authored in Task 4.\nupdated: 2026-05-30\n---\n\n# %s sink\n\n> Authored in Task 4.\n' "$s" "$s" > "docs/how-to/sinks/$s.md"
done
bash scripts/check-doc-links.sh
```
Expected: `OK: all relative doc links resolve`
```bash
git add docs/how-to docs/index.md
git commit -m "docs: core how-to pages (feeds, choose-a-sink, multi-instance, tls) + sink stubs"
```

---

### Task 4: Sink driver pages

**Files (overwrite stubs):** `docs/how-to/sinks/{postgres,kafka,sqs,sns,rabbitmq,stdout,http}.md`

Each uses this frontmatter shape (substitute per driver) and moves its README range under an H1 `# <Driver> sink`. Every sink page ends with the same Related block:
```markdown
## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
```

- [ ] **Step 1: `postgres.md`** — move README 389–420.
```yaml
---
title: Postgres sink
type: how-to
tags: [rss2msg/docs, sinks, postgres]
summary: Write each Change as a JSONB row; schema, table validation, and history semantics.
updated: 2026-05-30
---
```

- [ ] **Step 2: `kafka.md`** — move README 422–445.
```yaml
---
title: Kafka sink
type: how-to
tags: [rss2msg/docs, sinks, kafka]
summary: Publish Changes to a topic with configurable acks and compression; record/header layout.
updated: 2026-05-30
---
```
Wrap the `acks: none` warning as a callout (rewrite the "(see Operational notes)" anchor to a relative link):
```markdown
> [!warning]
> `acks: none` is unsafe. Combined with the commit-on-success model it can drop messages without the state store knowing. See [Operational Notes](../../explanation/operations.md). Use the default `all` unless you accept the trade-off.
```

- [ ] **Step 3: `sqs.md`** — move README 447–487 (incl. `##### FIFO queues`).
```yaml
---
title: SQS sink
type: how-to
tags: [rss2msg/docs, sinks, sqs, aws]
summary: Send Changes to an SQS queue; standard vs FIFO, message groups, and dedup ids.
updated: 2026-05-30
---
```

- [ ] **Step 4: `sns.md`** — move README 488–520 (incl. `##### FIFO topics`).
```yaml
---
title: SNS sink
type: how-to
tags: [rss2msg/docs, sinks, sns, aws]
summary: Publish Changes to an SNS topic; FIFO topics, message groups, and RawMessageDelivery.
updated: 2026-05-30
---
```

- [ ] **Step 5: `rabbitmq.md`** — move README 522–555.
```yaml
---
title: RabbitMQ sink
type: how-to
tags: [rss2msg/docs, sinks, rabbitmq, amqp]
summary: Publish Changes to an AMQP exchange; routing, declaration, and connection caveats.
updated: 2026-05-30
---
```

- [ ] **Step 6: `stdout.md`** — move README 557–583.
```yaml
---
title: Stdout sink
type: how-to
tags: [rss2msg/docs, sinks, stdout]
summary: Write one Change per line to stdout/stderr for local dev and ad-hoc pipelines.
updated: 2026-05-30
---
```

- [ ] **Step 7: `http.md`** — move README 585–616.
```yaml
---
title: HTTP sink
type: how-to
tags: [rss2msg/docs, sinks, http, webhook]
summary: POST/PUT each Change as JSON to a webhook URL; headers, success codes, and canonical metadata.
updated: 2026-05-30
---
```

- [ ] **Step 8: Run checker and commit**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`
```bash
git add docs/how-to/sinks
git commit -m "docs: per-driver sink pages (postgres, kafka, sqs, sns, rabbitmq, stdout, http)"
```

---

### Task 5: Tutorial + Explanation pages

**Files (overwrite stubs):**
- `docs/getting-started.md`
- `docs/explanation/how-it-works.md`
- `docs/explanation/operations.md`
- Modify: `docs/index.md` (Tutorial + Explanation sections)

- [ ] **Step 1: `getting-started.md`**

Frontmatter:
```yaml
---
title: Getting Started
type: tutorial
tags: [rss2msg/docs, quickstart]
summary: Build rss2msg and run your first one-shot and daemon polls against a feed.
updated: 2026-05-30
---
```
Move README lines 54–85 (Quickstart) then 89–100 (Build and run) under `# Getting Started`, with an H2 `## Build` before the build block and `## First run` before the quickstart steps (reorder so build precedes run — build is a prerequisite). Rewrite "see coordination below" → `[Run Multiple Instances](how-to/run-multiple-instances.md)`. Append:
```markdown
## Related

- [CLI](reference/cli.md) — every command and flag.
- [Configure Feeds](how-to/configure-feeds.md) — replace the example feeds with your own.
- [Choose a Sink](how-to/choose-a-sink.md) — send changes somewhere other than the example.
```

- [ ] **Step 2: `how-it-works.md`**

Frontmatter:
```yaml
---
title: How It Works
type: explanation
tags: [rss2msg/docs, architecture]
summary: The poll → detect → store → publish pipeline and where coordination fits.
updated: 2026-05-30
---
```
Under `# How It Works`, copy the ASCII architecture diagram from README lines 10–24, then write a short narrative (3–5 sentences, no new behavior) tracing a poll cycle: fetch (conditional GET via `feed_meta`) → classify new/updated against `seen_items` by content hash → publish the Change to each configured sink with retry/DLQ → coordinator gates which instance polls. Reference the relevant pages inline. Append:
```markdown
## Related

- [Change Envelope](../reference/change-envelope.md) — what "publish" emits.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — the coordinator gate.
- [Operational Notes](operations.md) — delivery guarantees and failure handling.
```

- [ ] **Step 3: `operations.md`**

Frontmatter:
```yaml
---
title: Operational Notes
type: explanation
tags: [rss2msg/docs, operations]
summary: Delivery semantics, DLQs, multi-instance behavior, AWS creds, LocalStack, and shutdown.
updated: 2026-05-30
---
```
Move README lines 722–758 under `# Operational Notes`. Rewrite the `coordination.driver` mention → `[Run Multiple Instances](../how-to/run-multiple-instances.md)`. Wrap the at-least-once bullet's consumer guidance and the `acks: none` bullet as `> [!note]` / `> [!warning]` callouts (text unchanged). Append:
```markdown
## Related

- [Choose a Sink](../how-to/choose-a-sink.md) — declaring a `dead_letter` sink.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — coordinator crash recovery.
- [Telemetry](../reference/telemetry.md) — what to monitor.
```

- [ ] **Step 4: Update `docs/index.md` Tutorial + Explanation sections**

```markdown
## Tutorial

- [Getting Started](getting-started.md)
```
```markdown
## Explanation

- [How It Works](explanation/how-it-works.md)
- [Operational Notes](explanation/operations.md)
```

- [ ] **Step 5: Run checker and commit**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`
```bash
git add docs/getting-started.md docs/explanation docs/index.md
git commit -m "docs: getting-started tutorial + how-it-works/operations explanation pages"
```

---

### Task 6: Transform the README into a landing page

**Files:** Modify `README.md`

- [ ] **Step 1: Rewrite README**

Keep lines 1–24 (title, intro paragraph, ASCII diagram) and the `## Design docs` section (782–791, links already repo-relative to `docs/superpowers/`). Replace everything in between (the anchor TOC and all moved sections, lines 28–780) with a "Documentation" section that links into the vault:

```markdown
## Documentation

Full documentation lives in [`docs/`](./docs/index.md). Start there, or jump in:

**Get started**
- [Getting Started](./docs/getting-started.md) — build and run your first poll.

**How-to**
- [Configure Feeds](./docs/how-to/configure-feeds.md)
- [Choose a Sink](./docs/how-to/choose-a-sink.md) · drivers: [postgres](./docs/how-to/sinks/postgres.md) · [kafka](./docs/how-to/sinks/kafka.md) · [sqs](./docs/how-to/sinks/sqs.md) · [sns](./docs/how-to/sinks/sns.md) · [rabbitmq](./docs/how-to/sinks/rabbitmq.md) · [stdout](./docs/how-to/sinks/stdout.md) · [http](./docs/how-to/sinks/http.md)
- [Run Multiple Instances](./docs/how-to/run-multiple-instances.md)
- [Secure Connections (TLS)](./docs/how-to/secure-connections-tls.md)

**Reference**
- [Configuration](./docs/reference/configuration.md) · [Change Envelope](./docs/reference/change-envelope.md) · [Wire Formats](./docs/reference/wire-formats.md) · [Telemetry](./docs/reference/telemetry.md) · [CLI](./docs/reference/cli.md)

**Understand it**
- [How It Works](./docs/explanation/how-it-works.md) · [Operational Notes](./docs/explanation/operations.md)
```

> Do not keep the deep reference content in README — it has moved (single source of truth). The intro paragraph and diagram are the only retained explanatory content.

- [ ] **Step 2: Verify README links and that no moved content remains**

Run:
```bash
bash scripts/check-doc-links.sh
grep -nE '^### `(log|telemetry|http|retry|runtime|state|coordination|sinks|feeds)`' README.md && echo "LEFTOVER CONFIG SECTIONS — remove" || echo "clean"
```
Expected: `OK: all relative doc links resolve` and `clean`.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: trim README to a landing page linking into docs/"
```

---

### Task 7: Stub sweep, link-rot audit, git policy

**Files:** `.gitignore`, possibly any remaining `type: stub` pages

- [ ] **Step 1: Confirm no stubs remain**

Run:
```bash
grep -rl 'type: stub' docs/ && echo "STUBS REMAIN — author or delete" || echo "no stubs"
```
Expected: `no stubs`. (All stubs from Tasks 2–3 were overwritten in Tasks 3–5.)

- [ ] **Step 2: Full link audit (checker + Obsidian backlinks spot check)**

Run:
```bash
bash scripts/check-doc-links.sh
```
Expected: `OK: all relative doc links resolve`.

If the Obsidian app is running, spot-check inbound links on the most-referenced page:
```bash
obsidian backlinks path=docs/reference/change-envelope.md
```
Expected: lists `wire-formats.md`, `choose-a-sink.md`, `how-it-works.md`, and the sink pages. (Skip if the app is closed — the filesystem checker is the gate.)

- [ ] **Step 3: Git policy — ignore churny Obsidian state, keep transport snapshot**

Append to `.gitignore` (create if absent):
```gitignore
# Obsidian per-user UI state (churny, machine-specific)
.obsidian/workspace.json
.obsidian/workspace-mobile.json
```
Then stage the transport snapshot so collaborators inherit the CLI transport decision:
```bash
git add .vault-meta/transport.json .gitignore
git rm --cached .obsidian/workspace.json 2>/dev/null || true
```

- [ ] **Step 4: Commit**

```bash
git commit -m "docs: gitignore churny Obsidian state, track vault transport snapshot"
```

- [ ] **Step 5: Final verification**

Run:
```bash
bash scripts/check-doc-links.sh && echo "--- page count ---" && find docs -name '*.md' -not -path 'docs/superpowers/*' | wc -l
```
Expected: `OK: all relative doc links resolve` and a count of `20` (index + getting-started + 4 reference + 11 how-to + 2 explanation… = index(1)+getting-started(1)+reference(5: configuration, change-envelope, wire-formats, telemetry, cli)+how-to(11: configure-feeds, choose-a-sink, run-multiple-instances, secure-connections-tls + 7 sinks)+explanation(2) = 20).

---

## Self-Review

**Spec coverage:**
- Page map (20 pages) → Tasks 1–5 create every page; counts reconciled in Task 7 Step 5. ✓
- README decomposition / single source of truth → Task 6 moves content and removes it from README; Task 6 Step 2 greps for leftover config sections. ✓
- Portable markdown links + no link rot → `scripts/check-doc-links.sh` (Task 1) run green after every task. ✓
- Frontmatter convention → exact blocks in every page step. ✓
- Callouts for inline warnings (acks:none, insecure_skip_verify) → Tasks 3, 4, 5 specify exact callout text. ✓
- Related footer for graph density → every page step appends one. ✓
- index.md as MOC, grown incrementally → Tasks 1–5 update it; checker stays green. ✓
- No heavy plugins / no Compound-Vault machinery → none introduced. ✓
- Git policy (ignore workspace.json, track transport.json) → Task 7. ✓
- Build sequence (exemplar → reference → how-to → tutorial/explanation → README last → link-rot pass) → Tasks 1→2→3→4→5→6→7. ✓
- Testing section dropped → not assigned to any page; noted in source table. ✓

**Placeholder scan:** The `type: stub` pages are intentional, temporary, and explicitly overwritten in later tasks and swept in Task 7 Step 1 — not plan placeholders. The `_(added in Task N)_` notes in index.md are likewise transient and replaced. No "TBD"/"add appropriate X" instructions remain. ✓

**Type/name consistency:** Page titles, filenames, and the relative links pointing at them match across tasks (e.g. `secure-connections-tls.md` titled "Secure Connections (TLS)" everywhere; `run-multiple-instances.md` titled "Run Multiple Instances" everywhere; sink Related block uses `../../reference/` depth correct for `docs/how-to/sinks/`). ✓

**One correction applied during review:** Task 2's original per-task green-checker gate conflicted with forward links to unbuilt how-to/explanation pages. Resolved by adding Step 7 (forward-link stubs) so the checker is green at every commit, and by creating sink stubs at the end of Task 3 before `choose-a-sink.md`'s driver table is committed.
