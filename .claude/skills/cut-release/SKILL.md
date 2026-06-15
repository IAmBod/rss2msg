---
name: cut-release
description: Use when cutting a new rss2msg release / version — derives the next semver from Conventional Commits with git-cliff, commits the regenerated CHANGELOG.md to main, then tags and pushes to trigger the GoReleaser pipeline. Triggers on "cut a release", "new version", "release vX.Y.Z", "/release".
---

# Cutting a new rss2msg release

rss2msg ships through a **tag-driven** pipeline: pushing a `vX.Y.Z` tag triggers
[`.github/workflows/release.yml`](../../../.github/workflows/release.yml), which builds
multi-platform binaries, Linux packages, a multi-arch Docker image, the Homebrew cask,
and the GitHub Release. The version comes entirely from the pushed tag.

This skill owns the changelog locally: it regenerates and **commits `CHANGELOG.md` to
`main` before tagging**, so the tagged commit already carries the populated changelog
that GoReleaser bundles into the artifacts. The workflow does **not** regenerate or sync
the changelog — that is this skill's job.

See [docs/development/releasing.md](../../../docs/development/releasing.md) for the full
pipeline reference.

## When to use

Cutting a new version of rss2msg. Invoked by the `/release [version]` command, or
directly when the user asks to "cut a release" / "ship a new version".

## This runs on `main`, not a worktree

Releasing is the documented exception to the worktree convention: the tag must point at
`main`'s HEAD. Run these steps in the primary checkout on `main`, never in a `.worktrees/`
copy.

## Hard rules

- **Never `git add -A` / `git add .`** — this repo is an Obsidian vault with
  auto-staging. Stage **only** `CHANGELOG.md` with an explicit pathspec.
- **Never** `--no-verify` or bypass hooks.
- **Confirm with the user before pushing the tag** — that push triggers the public
  release. Pushing the changelog commit to `main` happens first and is safe (`[skip ci]`).
- **Abort** on any failed preflight check and report exactly what failed; do not paper over it.

## Procedure

Track these as TodoWrite items and do them in order.

### 1. Preflight — abort on any failure

```bash
git rev-parse --abbrev-ref HEAD          # must be: main
git status --porcelain                   # must be empty (clean tree)
git fetch origin main --tags
git rev-list --count main..origin/main   # must be 0 (not behind origin/main)
```

- If not on `main`, the tree is dirty, or `main` is behind `origin/main`: stop and tell
  the user how to resolve it.
- **CI must be green on `main`.** Check the latest CI run:
  ```bash
  gh run list --branch main --workflow CI --limit 1 \
    --json status,conclusion,headSha,url
  ```
  If `conclusion` is not `success` (or status is still `in_progress`), warn the user with
  the run URL and require an explicit confirmation before continuing.

### 2. Determine the version

- **If the user passed a version** (e.g. `/release v1.0.0` or `v1.0.0-rc.1`): validate it
  is semver with a leading `v` — `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`. Reject
  anything else.
- **Otherwise** derive it from the commit history:
  ```bash
  git cliff --bumped-version    # e.g. prints v0.3.0
  ```
- **Ensure the tag does not already exist** (abort if either prints anything):
  ```bash
  git tag --list "<version>"
  git ls-remote --tags origin "refs/tags/<version>"
  ```

### 3. Preview the release notes and confirm

Show the user the resolved version and the notes that will be published:

```bash
git cliff --config cliff.toml --tag "<version>" --unreleased --strip header
```

Get explicit go-ahead on the version before changing any files. If the commits since the
last tag are thin or unconventional (git-cliff skips non-Conventional and merge commits),
point that out.

### 4. Regenerate and commit CHANGELOG.md to main

Regenerate the full changelog with the new tag label, then stage **only** that file:

```bash
git cliff --config cliff.toml --tag "<version>" --output CHANGELOG.md
git add CHANGELOG.md                       # explicit pathspec — nothing else
git status                                 # verify ONLY CHANGELOG.md is staged
git commit -m "chore(release): update CHANGELOG.md for <version> [skip ci]"
git push origin main
```

Notes:
- The exact `chore(release): … [skip ci]` message matters: `cliff.toml` skips
  `^chore\(release\):` commits, so this commit never pollutes a future changelog, and
  `[skip ci]` avoids a redundant CI run on `main`.
- Verify the staged set is **only** `CHANGELOG.md` before committing (auto-staging hazard).

### 5. Tag, confirm, and push

```bash
git tag "<version>"        # tags the changelog commit you just pushed
```

**Now confirm with the user one more time** — the next command kicks off the public
release. On confirmation:

```bash
git push origin "<version>"
```

### 6. Report

Print the link to the running release workflow so the user can watch it:

```bash
gh run list --workflow Release --limit 1 --json url,status --jq '.[0].url'
```

Tell the user the release is building and what the pipeline will produce (binaries,
`.deb`/`.rpm`/`.apk`, GHCR image `ghcr.io/iambod/rss2msg:<version>` and `:latest`,
Homebrew cask for non-prerelease tags, and the GitHub Release).

## Notes & edge cases

- **Prerelease tags** (`-rc.1`, etc.): valid and supported. GoReleaser skips the Homebrew
  cask for prereleases automatically (`skip_upload: auto`).
- **`v0.0.*` tags** are ignored by git-cliff (`skip_tags`), so don't use them.
- If the `git push origin main` in step 4 is rejected because `main` is branch-protected
  against direct pushes, stop — the changelog must reach `main` before the tag. Surface
  this to the maintainer rather than forcing it.
