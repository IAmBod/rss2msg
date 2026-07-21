# Contributing to rss2msg

Thanks for your interest in improving rss2msg! Contributions of all kinds are
welcome — bug reports, fixes, new sinks or feed sources, and documentation.

This file is a short entry point. The full, authoritative guidance lives in two
places:

- **[AGENTS.md](AGENTS.md)** — the single source of truth for repository
  conventions: the worktree/branch workflow, commit style, the `task` commands,
  the staging hazard, and documentation rules. Read this first.
- **[docs/development/contributing.md](docs/development/contributing.md)** — the
  same conventions written as user-facing documentation, with links into the rest
  of the developer docs (building & testing, project layout, releasing).

## TL;DR

1. **Open an issue first** for anything non-trivial so the design can be agreed on
   before you write code.
2. **Branch off `main`** with a type prefix — `feat/…`, `fix/…`, `docs/…`,
   `chore/…`.
3. **Use [Conventional Commits](https://www.conventionalcommits.org)**
   (`feat:`, `fix:`, `docs:`, `chore:`, `test:`). The release pipeline parses these
   to build the changelog and derive version bumps.
4. **Make the checks pass** before opening a PR:
   ```bash
   task test    # unit tests with -race
   task vet     # go vet
   task lint    # golangci-lint (CI runs this; needs golangci-lint v2)
   ```
   Run `task test-integration` too if your change touches a sink, the state store,
   or a coordinator backend (requires Docker), and
   `bash scripts/check-doc-links.sh` if you touched `docs/` or the `README`.
5. **Open a pull request** against `main` and fill in the template.
6. **Sign the CLA** — a bot asks on your first PR; see [below](#contributor-license-agreement).

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating, you agree to uphold it.

## Security

Please do **not** open public issues for security vulnerabilities — see
[SECURITY.md](SECURITY.md) for how to report them privately.

## License

rss2msg is **source-available**, not open source: it is distributed under the
[Business Source License 1.1](LICENSE). You may use, modify, and redistribute it,
including commercially — the one carve-out is offering rss2msg to third parties as a
hosted or managed "Feed-to-Message Service", which needs a separate commercial license.
Each version converts to the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
on its Change Date. See [`LICENSE`](LICENSE) for the exact terms.

## Contributor License Agreement

Before your first pull request can be merged, you need to sign the
[Contributor License Agreement](CLA.md). It is adapted from the Apache Software
Foundation's Individual CLA.

**You keep your copyright** — the CLA is a license grant, not a transfer. What it adds
is permission for the project to offer your contribution under the terms above,
including the separate commercial licenses and the eventual Apache-2.0 conversion.
Without it, contributed code could only be offered under the exact terms it arrived
under, which would fracture the licensing of the codebase.

Signing is automated: open a PR, and a bot comments with a link to the document and the
one-line comment to reply with. You are only asked once. Contributing on behalf of a
company? Email <info@randombullsh.it> for a Corporate CLA first.
