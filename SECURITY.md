# Security Policy

## Supported versions

rss2msg is pre-1.0 software under active development. Security fixes are applied to
the latest released version and to `main`. Older tagged releases are not patched —
please upgrade to the latest release before reporting an issue.

| Version | Supported          |
| ------- | ------------------ |
| latest release / `main` | :white_check_mark: |
| older releases | :x: |

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

Instead, use one of these private channels:

1. **GitHub private vulnerability reporting** (preferred) — open the repository's
   **Security** tab and click **Report a vulnerability**. This opens a private
   advisory visible only to the maintainers.
2. **Email** — send the details to **info@iambod.dev** with a subject line
   starting `SECURITY:`.

Please include as much of the following as you can:

- The type of issue and the component affected (e.g. a specific sink, the
  coordinator, config parsing).
- Steps to reproduce, or a proof-of-concept.
- The version / commit you tested against and your configuration (with secrets
  redacted).
- The potential impact as you see it.

## What to expect

- **Acknowledgement** of your report within **5 business days**.
- An assessment and, if confirmed, a plan and rough timeline for a fix.
- Coordinated disclosure: we will agree on a disclosure date with you and credit
  you in the advisory unless you prefer to remain anonymous.

Thank you for helping keep rss2msg and its users safe.
