---
description: Cut a new rss2msg release — derive the version, roll the BUSL Change Date, commit the changelog to main, then tag and push to trigger the release pipeline.
argument-hint: "[version]   e.g. v1.0.0 (optional; derived from commits if omitted)"
---

Cut a new rss2msg release using the **`cut-release`** skill.

Version argument (optional, validated semver like `v1.2.3` or `v1.2.3-rc.1`; if omitted,
derive it from the commit history with `git cliff --bumped-version`):

$ARGUMENTS

Invoke the `cut-release` skill and follow it exactly. Run on `main` (not a worktree),
stage only `CHANGELOG.md` and `LICENSE`, and **confirm with me before pushing the tag** — that push
triggers the public release pipeline.
