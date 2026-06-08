#!/usr/bin/env bash
#
# Compare Go benchmarks on the current working tree against a base git ref and
# fail when any hot-path benchmark has regressed beyond a threshold.
#
# Statistical significance is delegated to benchstat: it prints "~" in the
# "vs base" column when a change is within run-to-run noise and only prints a
# signed percentage when the difference is significant at its alpha (0.05 by
# default). This gate therefore reacts only to *significant* slowdowns, which
# keeps it from flapping on the noisy shared CI runners.
#
# Higher is worse for every metric we emit (sec/op, B/op, allocs/op), so a
# positive "vs base" percentage is a regression and a negative one is a win.
#
# Usage:
#   scripts/bench-compare.sh <base-ref>
#
# Environment knobs:
#   BENCH_COUNT      repetitions per side passed to `go test -count` (default 6)
#   BENCH_THRESHOLD  regression threshold, in percent (default 10)
#   BENCH_PKGS       package pattern to benchmark (default ./...)
#   BENCHSTAT        benchstat binary to use (default: `benchstat` on PATH)
#
# Progress is written to stderr; the benchstat table and the gate verdict are
# written to stdout so a caller can capture them (e.g. into a CI step summary).
set -euo pipefail

base_ref="${1:?usage: bench-compare.sh <base-ref>}"
count="${BENCH_COUNT:-6}"
threshold="${BENCH_THRESHOLD:-10}"
pkgs="${BENCH_PKGS:-./...}"
benchstat="${BENCHSTAT:-benchstat}"

if ! command -v "$benchstat" >/dev/null 2>&1; then
	echo "bench-compare: benchstat not found (set BENCHSTAT or 'go install golang.org/x/perf/cmd/benchstat@latest')" >&2
	exit 2
fi

bench_flags=(-run='^$' -bench=. -benchmem -count="$count" "$pkgs")

workdir="$(mktemp -d)"
base_tree="$workdir/base"
cleanup() {
	git worktree remove --force "$base_tree" >/dev/null 2>&1 || true
	rm -rf "$workdir"
}
trap cleanup EXIT

new="$workdir/new.txt"
old="$workdir/old.txt"

echo "==> Benchmarking working tree (HEAD), count=$count ..." >&2
go test "${bench_flags[@]}" >"$new"

echo "==> Benchmarking base ($base_ref) in a detached worktree ..." >&2
git worktree add --quiet --detach "$base_tree" "$base_ref"
( cd "$base_tree" && go test "${bench_flags[@]}" ) >"$old"

# Human-readable comparison table -> stdout (captured into the CI summary).
"$benchstat" "$old" "$new"
echo

# Gate: scan the CSV form and fail on any significant regression over threshold.
# benchstat emits one table per metric; the metric label is the second field of
# each "vs base" header row, and each data row carries the signed delta in the
# "vs base" column ($6), which is "~" when the change is not significant.
regressions="$(
	"$benchstat" -format=csv "$old" "$new" 2>/dev/null | awk -F, -v threshold="$threshold" '
		$1 == "" && ($2 == "sec/op" || $2 == "B/op" || $2 == "allocs/op") { metric = $2; next }
		metric != "" && $1 != "" && $1 != "geomean" {
			vs = $6
			if (vs ~ /^\+[0-9.]+%$/) {
				pct = vs; sub(/%$/, "", pct); sub(/^\+/, "", pct)
				if (pct + 0 > threshold + 0)
					printf "  %-28s %-10s %s (> %s%%)\n", $1, metric, vs, threshold
			}
		}
	'
)"

if [[ -n "$regressions" ]]; then
	echo "FAIL: benchmark regressions beyond ${threshold}% threshold:"
	echo "$regressions"
	exit 1
fi

echo "OK: no significant benchmark regressions beyond ${threshold}% threshold."
