---
title: Contributing
type: how-to
tags: [rss2msg/docs, development]
summary: Branch, commit, test, and pull-request conventions used in this repository.
updated: 2026-05-31
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
   bash scripts/check-doc-links.sh   # only needed when you touched docs/ or README
   ```
   Run `task test-integration` too if your change touches a sink, the state store,
   or a coordinator backend (requires Docker).
4. Open a pull request against `main`.

## Commit messages

Commits use a conventional-style type prefix matching the branch type — e.g.
`feat(sink/http): …`, `docs: …`, `chore: …`. Keep the subject in the imperative
mood and scoped to one change.

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

## Related

- [Building and Testing](building-and-testing.md) — the commands the checks run.
- [Project Layout](project-layout.md) — where to make a given change.
