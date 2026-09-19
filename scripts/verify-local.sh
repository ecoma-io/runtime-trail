#!/usr/bin/env bash
# One command, whole gate. `pnpm verify` is what "done" means on this machine
# (AGENTS.md, "Definition of done"); CI runs the same checks with the desktop
# system libraries installed.
#
# The desktop checks are the one honest exception: where the platform webview
# libraries are missing they are SKIPPED with this line saying so — never
# silently, never faked. Everything else must be green for the script to exit
# zero.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() {
  echo "✗ verify-local: $1" >&2
  exit 1
}

echo "==> format (prettier --check)"
pnpm exec prettier --check --ignore-unknown . || fail "formatting drift — run: pnpm format"

echo "==> lint (all non-desktop projects)"
pnpm exec moon run :lint --query 'project!=desktop' || fail "lint"

echo "==> typecheck (rust + web)"
pnpm exec moon run :typecheck --query 'project!=desktop' || fail "typecheck"

echo "==> tests (rust + web)"
pnpm exec moon run :test --query 'project!=desktop' || fail "tests"

echo "==> architecture (archkeep on the real tree)"
pnpm arch || fail "architecture check"

echo "==> architecture canaries (the law must be able to bite)"
pnpm arch:canary || fail "arch canary"

echo "==> docs references"
pnpm check-docs-links || fail "docs references"

echo "==> vendored skills"
pnpm check-skills || fail "vendored skills"

echo "==> build (web + server)"
pnpm build || fail "build"

echo "==> e2e (playwright, chromium)"
pnpm exec moon run web:e2e || fail "e2e"
if pkg-config --exists webkit2gtk-4.1 && pkg-config --exists glib-2.0; then
  echo "==> desktop checks (webview libraries present)"
  pnpm exec moon run desktop:lint desktop:typecheck desktop:test || fail "desktop checks"
else
  echo "==> SKIP desktop checks: webview libraries missing (pkg-config webkit2gtk-4.1 not satisfied)." >&2
  echo "   CI runs them on every PR. To run locally:" >&2
  echo "   sudo apt-get install -y libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libayatana-appindicator3-dev pkg-config" >&2
fi

echo "✓ verify-local: every gate that could run, ran green"
