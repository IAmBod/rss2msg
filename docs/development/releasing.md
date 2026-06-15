---
title: Releasing
type: how-to
tags: [rss2msg/docs, development, release]
summary: The release pipeline — golangci-lint in CI, git-cliff for the changelog and version bumps, and GoReleaser for multi-platform binaries, Linux packages (.deb/.rpm/.apk), a multi-arch Docker image, and a Homebrew formula, all driven by a semver tag.
updated: 2026-06-16
---

# Releasing

rss2msg ships through a tag-driven pipeline built from three tools:

| Tool | Config | Role |
| --- | --- | --- |
| [golangci-lint](https://golangci-lint.run) | [`.golangci.yml`](../../.golangci.yml) | Code-quality gate on every PR and push to `main`. |
| [git-cliff](https://git-cliff.org) | [`cliff.toml`](../../cliff.toml) | Builds `CHANGELOG.md` and per-release notes from [Conventional Commits](https://www.conventionalcommits.org); derives the next semver. |
| [GoReleaser](https://goreleaser.com) | [`.goreleaser.yaml`](../../.goreleaser.yaml) | Builds multi-platform binaries, Linux packages (`.deb`/`.rpm`/`.apk`), a multi-arch Docker image, and a Homebrew formula, and publishes the GitHub Release. |

Two GitHub Actions workflows wire them together:

- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) — runs on PRs and pushes
  to `main`: golangci-lint, `go vet`, unit tests (`-race`), `go build`, and `goreleaser check`.
- [`.github/workflows/release.yml`](../../.github/workflows/release.yml) — runs on a
  pushed `v*.*.*` tag: generates release notes with git-cliff and runs GoReleaser to
  build and publish artifacts and the Docker image. The changelog is **not** touched by
  this workflow — `CHANGELOG.md` is regenerated and committed to `main` *before* tagging
  (see [Cut a release](#cut-a-release)), so the tagged commit already carries the
  populated file that GoReleaser bundles into the archives and packages.

## Cut a release

In Claude Code, run **`/release [version]`** — it drives the steps below via the
`cut-release` skill (preflight checks, version derivation, the changelog commit, and the
confirmed tag push). To do it by hand:

1. Make sure `main` is green (CI passing) and the commits since the last tag follow
   [Conventional Commits](https://www.conventionalcommits.org) — git-cliff groups them
   by `feat:` / `fix:` / `docs:` / etc., and unconventional commits (including merge
   commits) are skipped.
2. Pick the version. git-cliff can derive it from the commit history:
   ```bash
   git cliff --bumped-version      # e.g. prints v0.3.0
   ```
3. Regenerate `CHANGELOG.md` for the new version and commit it to `main`. This commit
   carries the populated changelog that GoReleaser bundles into the release artifacts:
   ```bash
   git cliff --config cliff.toml --tag v0.3.0 --output CHANGELOG.md
   git add CHANGELOG.md            # explicit pathspec only
   git commit -m "chore(release): update CHANGELOG.md for v0.3.0 [skip ci]"
   git push origin main
   ```
   `cliff.toml` skips `chore(release):` commits, so this never pollutes a future
   changelog, and `[skip ci]` avoids a redundant CI run.
4. Tag that commit and push. The tag is what triggers the release workflow:
   ```bash
   git tag v0.3.0
   git push origin v0.3.0
   ```

The release workflow then:

- builds binaries for linux/macOS/Windows on amd64/arm64 (Windows arm64 excluded),
  with `version`, `commit`, and `date` stamped into the binary via `-ldflags`
  (see [`builds:` in `.goreleaser.yaml`](../../.goreleaser.yaml));
- writes `checksums.txt` and `.tar.gz` / `.zip` archives — each bundles the binary,
  `README.md`, `LICENSE`, `CHANGELOG.md`, the example config, and the user-facing
  `docs/` tree (the internal `docs/superpowers/` specs and plans are excluded);
- builds Linux packages — `.deb`, `.rpm`, and `.apk` for amd64 and arm64 — from the
  same binaries via GoReleaser's [nFPM](https://nfpm.goreleaser.com) integration
  (see [`nfpms:` in `.goreleaser.yaml`](../../.goreleaser.yaml)); each installs the
  binary to `/usr/bin/rss2msg`, a sample config to `/etc/rss2msg/config.example.yaml`,
  and docs (`README.md`, `CHANGELOG.md`, and the `docs/` tree minus `superpowers/`) to
  `/usr/share/doc/rss2msg/`;
- builds and pushes a multi-arch image to `ghcr.io/iambod/rss2msg:<version>` and
  `:latest` — the `production` (final) stage of the single
  [`Dockerfile`](../../Dockerfile), which packages the cross-compiled binary GoReleaser
  stages per platform (`COPY $TARGETPLATFORM/rss2msg`);
- updates the [Homebrew tap](#homebrew-tap) — generates the `rss2msg` cask from the
  macOS archives and commits it to `IAmBod/homebrew-tap` (skipped for prerelease tags,
  see [`homebrew_casks:` in `.goreleaser.yaml`](../../.goreleaser.yaml));
- publishes a GitHub Release whose notes are the git-cliff section for that tag.

## Version metadata

The binary's build metadata comes from `-ldflags -X main.version=… -X main.commit=…
-X main.date=…`. Check it at runtime:

```bash
rss2msg version
```

A plain `go build` (no ldflags) reports `dev` / `none` / `unknown`; release builds
carry the real tag, commit, and timestamp. `goreleaser build --snapshot` stamps a
`-next` pseudo-version so you can verify injection locally.

## Dry-run locally

You need `golangci-lint` (v2), `git-cliff`, and `goreleaser` installed (plus Docker for
the image build). All are wrapped as `task` targets:

```bash
task lint               # golangci-lint run ./...
task changelog          # regenerate CHANGELOG.md
task release-check      # goreleaser check — validate .goreleaser.yaml
task release-snapshot   # full dry-run into ./dist, nothing published
```

`./dist/` is git-ignored.

## Prerequisites for publishing

- The release job uses the built-in `GITHUB_TOKEN`; the workflow requests
  `contents: write` (create the Release) and `packages: write` (push to GHCR). No extra
  secrets are required.
- `CHANGELOG.md` is committed to `main` before tagging (see [Cut a release](#cut-a-release)),
  so if `main` is branch-protected against direct pushes, do that changelog commit through
  whatever path your protection allows before pushing the tag — the workflow itself no
  longer pushes to `main`.

## Homebrew tap

The release publishes a Homebrew cask so macOS users can `brew install IAmBod/tap/rss2msg`
(see [Install](../getting-started.md#install)). GoReleaser builds the cask from the macOS
archives and commits it to a **separate tap repository**. It uses `homebrew_casks` rather
than the deprecated `brews` formula section; the cask ships the prebuilt binary and a
postflight that clears the quarantine attribute (the binary isn't notarized). Homebrew
Cask is macOS-only — Linux users install via the `.deb`/`.rpm`/`.apk` packages or the
container image. Setup is one-time:

1. Create a public repo **`IAmBod/homebrew-tap`**. Homebrew maps the tap name
   `IAmBod/tap` to a repo named `homebrew-tap`, so the cask lands at
   `Casks/rss2msg.rb` there and `brew install IAmBod/tap/rss2msg` resolves.
2. Add a repository (or organization) secret **`TAP_GITHUB_TOKEN`** — a fine-grained
   personal access token with `contents: write` on the tap repo. The built-in
   `GITHUB_TOKEN` only has access to *this* repo, so a cross-repo push needs its own
   token; the release workflow passes it to GoReleaser as `$TAP_GITHUB_TOKEN`.

Until both exist, the rest of the release (binaries, packages, image, GitHub Release)
still publishes — only the `brews` step fails. Prerelease tags (e.g. `v1.0.0-rc.1`)
never touch the tap, because the formula uses `skip_upload: auto`.

## Related

- [Contributing](contributing.md) — branch, commit, and PR conventions (Conventional Commits).
- [Building and Testing](building-and-testing.md) — the local build and test commands CI mirrors.
- [Run with Docker](../how-to/run-with-docker.md) — the development image and running the published production image.
- [CLI](../reference/cli.md) — the `version` command this pipeline feeds.
