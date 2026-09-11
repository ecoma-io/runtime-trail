#!/usr/bin/env bash
# The test entry every Rust moon project calls (see each `moon.yml` `test`
# task). Scoped to the calling crate's package, so a per-crate test task
# tests only that crate.
#
# Loud about a vacuous run on purpose: `cargo test` exits 0 for a crate with
# zero tests, and a test gate that passes without running a test is a
# placeholder, not a gate — every crate here carries at least one real test
# (AGENTS.md, "Tests are not optional at any layer").
set -euo pipefail

project_root="${MOON_PROJECT_ROOT:-$PWD}"
manifest="$project_root/Cargo.toml"
if [ ! -f "$manifest" ]; then
  echo "✗ cargo-test.sh: no Cargo.toml at $project_root (set MOON_PROJECT_ROOT or run from the crate)" >&2
  exit 2
fi
package="$(sed -nE 's/^name[[:space:]]*=[[:space:]]*"(.*)"/\1/p' "$manifest" | head -n 1)"
if [ -z "$package" ]; then
  echo "✗ cargo-test.sh: could not read the package name from $manifest" >&2
  exit 2
fi

output="$(mktemp)"
trap 'rm -f "$output"' EXIT

if ! cargo test --locked -p "$package" | tee "$output"; then
  echo "✗ cargo test $package failed" >&2
  exit 1
fi
if ! grep -Eq '^running [1-9][0-9]* tests?' "$output"; then
  echo "✗ vacuous test run: cargo ran zero tests for $package. A crate with no real test is a red gate here, not a pass (see AGENTS.md)." >&2
  exit 1
fi
