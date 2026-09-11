#!/usr/bin/env bash
# The pre-push desktop gate, conditional on the machine being able to run it.
# CI always runs the desktop checks (`.github/workflows/ci.yml`); a developer
# machine without the webview libraries gets this loud skip instead of a red
# pre-push it cannot fix — and never a silent pass.
set -euo pipefail
cd "$(dirname "$0")/.."

if pkg-config --exists webkit2gtk-4.1 && pkg-config --exists glib-2.0; then
  exec pnpm exec moon run desktop:lint desktop:typecheck desktop:test
fi

echo "SKIP desktop checks: webview libraries not found (pkg-config webkit2gtk-4.1)." >&2
echo "  CI runs them. To run locally:" >&2
echo "  sudo apt-get install -y libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libayatana-appindicator3-dev pkg-config" >&2
