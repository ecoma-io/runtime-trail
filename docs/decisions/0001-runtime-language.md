# 0001: The native core is Rust, with tokio + axum confined to the server crate

## Status

Accepted (2026-09-11, foundation commit)

## Context

`runtime-trail` needs a native core that ingests OTLP, keeps correlated
telemetry in bounded memory, and runs as one process on a laptop, in Docker
and inside a desktop shell ([system.md](../architecture/system.md)). The hard
constraints from [runtime-constraints.md](../architecture/runtime-constraints.md):
idle RSS under 50 MB, startup under a second, no external database, single
small binary.

Repository and organisation evidence considered:

- The org already dogfoods Rust where native performance matters (`ecoma`'s
  desktop crate, `archkeep`'s rule SDK), with a settled house style: edition
  2024, MSRV carried per-crate via `rust-version` (floor justified by edition
  2024 needing 1.85), `[workspace.lints]` with `unsafe_code = "forbid"` and
  clippy pedantic warnings, `--locked` on every cargo invocation, `Cargo.lock`
  committed, no `rust-toolchain.toml` (CI pins the `stable` channel
  explicitly; the MSRV floor is the compatibility contract).
- Go is also in the org (`ecoma`), but a second language ecosystem here would
  buy nothing the product needs that Rust does not, and would split the org's
  native-core conventions.
- A garbage-collected runtime (Node/TS for the core) works against the RSS
  targets and the in-process desktop story; TypeScript stays where it belongs,
  in the UI.

The org had no precedent HTTP _server_ framework (its only Rust service crate
is a Tauri shell), so the async stack was chosen on merits: **tokio** as the
async runtime and **axum** as the HTTP layer — the smallest mainstream stack
that gives OTLP/gRPC + OTLP/HTTP ingestion a credible future without a custom
protocol layer.

## Decision

1. The native core is **Rust**, organised as the `crates/*` Cargo workspace.
2. Edition **2024**; MSRV floor `rust-version = "1.85"` on every crate
   (edition 2024 stabilised in Rust 1.85 — that fact is what makes the floor,
   not a remembered version). Contributors and CI use the `stable` toolchain;
   the MSRV is the compatibility promise. There is deliberately **no**
   `rust-toolchain.toml`, matching org convention.
3. House lints via `[workspace.lints]`: `unsafe_code = "forbid"`, clippy
   `all = deny`, `pedantic = warn`, inherited with `[lints] workspace = true`.
4. Every cargo invocation that matters uses `--locked`; `Cargo.lock` is
   committed.
5. **tokio + axum live only in the `server` crate.** HTTP is a
   distribution/transport concern ([boundaries.md](../architecture/boundaries.md):
   the server and desktop are `layer-app`, the only projects allowed to know
   how the core reaches a network). No other crate may take an HTTP, gRPC or
   Tauri dependency; that is what keeps the core embeddable in the desktop
   shell and testable without sockets.

## Consequences

- The RSS and startup budgets are plausible targets rather than aspirations:
  no GC, no VM, one binary per platform.
- The desktop shell (Tauri 2, per `ecoma` precedent) can embed this core
  in-process — the same-core invariant in
  [system.md](../architecture/system.md) is mechanically cheap.
- Rust compile times are accepted as a cost; moon tasks and CI caching exist
  to keep them off the edit loop.
- MSRV bumps are deliberate changes to this record's floor, not incidental
  lockfile drift.
