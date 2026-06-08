<!--
Thanks for contributing to rss2msg! Please read CONTRIBUTING.md first.
Keep the PR focused on one change. Use a Conventional Commit title
(e.g. "feat(sink/http): ...", "fix: ...", "docs: ...").
-->

## Summary

<!-- What does this change do, and why? -->

## Related issue

<!-- e.g. "Closes #123". Open an issue first for non-trivial changes. -->

## Type of change

- [ ] Bug fix (`fix:`)
- [ ] New feature (`feat:`)
- [ ] Documentation (`docs:`)
- [ ] Refactor / chore / tests (`refactor:` / `chore:` / `test:`)
- [ ] Breaking change

## Checklist

- [ ] Branch is off `main` with a type prefix (`feat/…`, `fix/…`, `docs/…`, `chore/…`).
- [ ] Commits follow [Conventional Commits](https://www.conventionalcommits.org).
- [ ] `task test` and `task vet` pass.
- [ ] `task lint` passes (CI gates on golangci-lint).
- [ ] `task test-integration` run if this touches a sink, the state store, or a
      coordinator backend — or I have noted below that I skipped it (needs Docker).
- [ ] Docs / config examples updated for any user-visible change; if I touched
      `docs/` or the `README`, `bash scripts/check-doc-links.sh` prints `OK`.
- [ ] Only intended files are staged (no auto-staged vault/editor noise).

## Notes for reviewers

<!-- Anything reviewers should focus on, trade-offs, or follow-ups. -->
