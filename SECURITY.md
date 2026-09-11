# Security Policy

## Scope

Runtime Trail is a **local** developer tool: it binds a loopback HTTP server,
reads telemetry you feed it, and never phones home. Vulnerabilities of
interest include anything that lets telemetry or investigation data escape
the machine, anything that lets a remote party reach the core (the server
must never bind anything but loopback by default), prompt-injection or
tool-abuse surfaces in the MCP surface once it lands, and supply-chain
problems in the dependency set.

## Reporting a vulnerability

**Do not open a public issue.** Use GitHub's
[private vulnerability reporting](https://github.com/ecoma-io/runtime-trail/security/advisories/new)
for this repository, or email **john.itvn@gmail.com** if you prefer.

Please include what you observed, how to reproduce it, and — if you have
one — your assessment of impact. You will get an acknowledgement, then
findings and a fix timeline. Credit is yours unless you prefer otherwise.

## Supported versions

The foundation has no releases yet; only `main` is supported. Fix releases
will be cut from `main` until there is a maintenance policy worth documenting.

## Design commitments this policy leans on

- The core binds `127.0.0.1` by default and never requires a database or
  external service (`docs/decisions/0003-storage-strategy.md`).
- The MCP surface is a peer of the UI with no agent-only paths beneath the
  Investigation API (`docs/architecture/mcp-model.md`).
- CI runs CodeQL, Semgrep and secret scanning on every PR
  (`.github/workflows/analysis.yml`).
