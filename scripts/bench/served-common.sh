#!/usr/bin/env bash
# served-common.sh — the served-runtime cases' shared machinery (sourced,
# not executed). They measure the real server binary (runtime-trail-server)
# from outside the process (docs/architecture/runtime-constraints.md,
# "Enforcement trajectory"), so the common pieces are: build it, start it
# on the cases' loopback port, poll /healthz until it serves, and drain it
# with SIGTERM per the server's shutdown contract (≤ 5 s, exit 0).
set -euo pipefail

# The loopback port the served-runtime cases run their server on. Fixed
# and verified free up front: a port collision is a loud failure, never a
# silent measure of the wrong process. The served cases never overlap —
# each case starts and drains its own server.
served_port=18666

# The server binary the cases build and launch.
server_bin="target/release/runtime-trail-server"

# Verifies the cases' port is free; refuses to measure on a busy one.
served_check_port() {
  if (exec 3<>"/dev/tcp/127.0.0.1/$served_port") 2>/dev/null; then
    exec 3>&- 2>/dev/null || true
    echo "✗ served port $served_port is already taken — refusing to measure the wrong process." >&2
    return 1
  fi
}

# Builds the real server binary the cases measure. Build chatter goes to
# stderr (the harness captures stdout as the record).
served_build() {
  cargo build --release --locked -p runtime-trail-server 1>&2
}

# Starts the server on the cases' port (sets the global served_pid) and
# waits until /healthz answers 200, bounded: a server that never serves is
# a loud failure, not a measurement of nothing.
served_start() {
  "$server_bin" --bind "127.0.0.1:$served_port" >/dev/null 2>&1 &
  served_pid=$!
  local code=000
  for _ in $(seq 1 200); do
    code="$(curl -s -o /dev/null -m 1 -w '%{http_code}' "http://127.0.0.1:$served_port/healthz" 2>/dev/null || true)"
    [ "$code" = "200" ] && return 0
    if ! kill -0 "$served_pid" 2>/dev/null; then
      echo "✗ served runtime exited before /healthz answered (last code $code)." >&2
      return 1
    fi
    sleep 0.05
  done
  echo "✗ served runtime never answered /healthz 200 within 10 s." >&2
  return 1
}

# Drains the server: SIGTERM, then wait. The shutdown contract is exit 0;
# the caller decides what a non-zero drain means.
served_drain() {
  kill -TERM "$served_pid"
  wait "$served_pid"
}