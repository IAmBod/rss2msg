#!/usr/bin/env bash
# Verify every relative markdown link in README.md and docs/ resolves to a file.
# Assumes link targets are not inside fenced code blocks and use no "title" attribute
# (the rss2msg doc set follows both conventions).
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
fail=0

check_file() {
  local f="$1" dir target path
  [ -f "$f" ] || return 0
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
}

while IFS= read -r f; do
  check_file "$f"
done < <(find "$root/docs" -name '*.md' -not -path '*/superpowers/*' 2>/dev/null)
check_file "$root/README.md"

[ "$fail" -eq 0 ] && echo "OK: all relative doc links resolve"
exit "$fail"
