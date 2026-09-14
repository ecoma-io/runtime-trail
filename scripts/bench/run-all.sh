#!/usr/bin/env bash
# The benchmark harness (the contract is docs/benchmarks/README.md; that page
# owns the why, this script owns the mechanics).
#
#   - Every executable file under cases/ is a registered case and is run.
#   - With ZERO registered cases this exits non-zero, loudly: an empty
#     harness must not read as a passing one. (Phase 1 registered the
#     memory-path cases — idle-rss, ingest-overload, retention-bound — per
#     docs/benchmarks/README.md; a case is still only ever added by the
#     phase that ships the capability it measures.)
#   - Each case's output is captured verbatim under results/raw/<epoch>/
#     beside a record naming the commit it measured and the machine that
#     measured it. Raw runs are local artefacts (git-ignored); nothing here
#     judges a number against a target — a case that wants to assert a
#     budget asserts it by exiting non-zero itself.
set -euo pipefail
cd "$(dirname "$0")/../.."

cases_dir="scripts/bench/cases"
results_dir="scripts/bench/results"

# Registered = present AND executable. A non-executable file under cases/ is
# a case whose author forgot `chmod +x` — report it, don't silently skip it.
cases=()
unregistered=()
for path in "$cases_dir"/*; do
  [ -e "$path" ] || continue
  if [ -x "$path" ]; then
    cases+=("$path")
  else
    unregistered+=("$path")
  fi
done

for path in ${unregistered[@]+"${unregistered[@]}"}; do
  echo "✗ $path is not executable — a case that cannot run cannot report." >&2
  exit 1
done

if [ "${#cases[@]}" -eq 0 ]; then
  echo "✗ no benchmark cases are registered under $cases_dir/." >&2
  echo "  This harness must not pass vacuously (docs/benchmarks/README.md:" >&2
  echo "  a case is added by the phase that ships the capability it" >&2
  echo "  measures) — an empty harness must not read as a passing one," >&2
  echo "  so this run FAILS rather than pretending to have measured nothing." >&2
  echo "  Register a case, or accept that there is nothing to measure yet." >&2
  exit 1
fi

# The record every result carries, per the contract: which commit, which
# machine, when. A dirty tree is recorded as dirty — results from a tree that
# does not match its own commit are exactly the ones worth doubting later.
commit="$(git rev-parse HEAD)"
[ -z "$(git status --porcelain)" ] || commit="$commit (dirty tree)"
machine="$(uname -s) $(uname -m)"
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  machine="$machine, $(. /etc/os-release && echo "${PRETTY_NAME:-}")"
fi

run_dir="$results_dir/raw/$(date +%s)"
mkdir -p "$run_dir"

failed=0
for case_path in "${cases[@]}"; do
  name="$(basename "$case_path")"
  echo "==> $name"
  record="$run_dir/$name.out"
  {
    echo "case: $name"
    echo "commit: $commit"
    echo "machine: $machine"
    echo "measured_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "---"
  } >"$record"
  if "$case_path" | tee -a "$record"; then
    echo "    ✓ PASS (record: $record)"
  else
    echo "    ✗ FAIL (record: $record)" >&2
    failed=$((failed + 1))
  fi
done

if [ "$failed" -gt 0 ]; then
  echo "✗ $failed of ${#cases[@]} case(s) failed." >&2
  exit 1
fi
echo "✓ ${#cases[@]} case(s) passed; records under $run_dir/"
