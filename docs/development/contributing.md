---
title: Contributing
type: how-to
tags: [rss2msg/docs, development]
summary: Branch, commit, test, and pull-request conventions used in this repository, plus the license terms and the Contributor License Agreement.
updated: 2026-07-21
---

# Contributing

These are the conventions this repository already follows (visible in its git
history and tooling). Nothing here changes the code's behavior — it describes how
changes get made.

## Workflow

1. Branch off `main`. Branch names use a type prefix: `feat/…`, `docs/…`, `chore/…`.
2. Make focused commits (see message style below).
3. Before opening a PR, make the checks pass:
   ```bash
   task test                # unit tests with -race
   task vet                 # go vet
   task lint                # golangci-lint (CI runs this; needs golangci-lint v2)
   bash scripts/check-doc-links.sh   # only needed when you touched docs/ or README
   ```
   Run `task test-integration` too if your change touches a sink, the state store,
   or a coordinator backend (requires Docker).
4. Open a pull request against `main`.
5. Sign the [Contributor License Agreement](#licensing-and-the-cla) — a bot asks on
   your first pull request.

## Commit messages

Commits use a conventional-style type prefix matching the branch type — e.g.
`feat(sink/http): …`, `docs: …`, `chore: …`. Keep the subject in the imperative
mood and scoped to one change. The release pipeline parses these prefixes to build
the changelog and derive version bumps — see [Releasing](releasing.md).

## Tests

Follow the existing test layout: colocated `*_test.go` unit tests in each package,
integration tests behind the `integration` build tag, and the full-pipeline test
in [`test/e2e`](../../test/e2e). See [Building and Testing](building-and-testing.md)
for how to run each suite.

## Documentation

Docs live under [`docs/`](../index.md) and follow a Diátaxis structure
(tutorial / how-to / reference / explanation). When adding or moving a page:

- Use relative markdown links (not `[[wikilinks]]`) so pages render in both GitHub
  and Obsidian.
- Give every page the standard frontmatter (`title`, `type`, `tags`, `summary`,
  `updated`) and a `## Related` footer.
- Run `bash scripts/check-doc-links.sh` and confirm it prints `OK` before committing.

## Licensing and the CLA

rss2msg is **source-available, not open source**. It ships under the
[Business Source License 1.1](../../LICENSE): use, modification, and redistribution are
permitted, including commercially, with one carve-out — offering rss2msg to third
parties as a hosted or managed "Feed-to-Message Service" requires a separate commercial
license from the maintainer. Each version converts to the Apache License 2.0 on its
Change Date, four years after that version is released (see
[Releasing](releasing.md#change-date)).

Because of that model, contributions are covered by a
[Contributor License Agreement](../../CLA.md), adapted from the Apache Software
Foundation's Individual CLA. You keep your copyright; the CLA grants the maintainer the
right to offer your contribution under the licenses above, including the separate
commercial licenses and the eventual Apache-2.0 conversion.

Signing is automated by [`.github/workflows/cla.yml`](../../.github/workflows/cla.yml):
open a pull request, and a bot comments with a link to the document and the exact reply
that records your signature. You are only asked once — signatures live in the
`cla-signatures` branch. Contributing on behalf of a company? Email
<info@randombullsh.it> for a Corporate CLA before submitting.

## Related

- [Building and Testing](building-and-testing.md) — the commands the checks run.
- [Project Layout](project-layout.md) — where to make a given change.
- [Releasing](releasing.md) — how conventional commits become tagged releases and a changelog.
- [CLA.md](../../CLA.md) — the Contributor License Agreement itself.
