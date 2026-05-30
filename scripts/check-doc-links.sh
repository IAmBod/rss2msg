#!/usr/bin/env bash
# Verify every relative markdown link in README.md and docs/ resolves to a file.
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
fail=0
files=$(find "$root/docs" -name '*.md' -not -path '*/superpowers/*' 2>/dev/null; echo "$root/README.md")
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
