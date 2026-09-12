# Agent guidance

For working **on** this repository. Read it before the first edit — the rules
below are the ones a diff gets rejected for violating, and most of them are not
inferable from the code. This file is the single authority for agent behavior
in this repository; host instruction files import it and add nothing.

## What this repository is

Runtime Trail is a **local developer observability and investigation runtime**:
one native core that ingests OpenTelemetry signals, keeps them under bounded
resources, and answers investigation questions through a single Investigation
API consumed equally by a developer UI and an MCP surface. It is not a Grafana
clone, not a collector, not a SaaS — `docs/product/vision.md` owns the product
sentence, `docs/product/scope.md` the commitments, `docs/product/non-goals.md`
the refusals. If a change makes the product closer to "hosted metrics
platform", it is a change to those documents first and code second.

**The repository has landed Phase 1's sanctioned slice: telemetry ingestion.**
The foundation commit holds the toolchain, the architecture law, the gates
and the smoke core (a health surface, a version surface, and static UI
serving when supplied); OTLP/HTTP and OTLP/gRPC receivers now decode into
the telemetry model and a bounded in-memory store under backpressure. No
telemetry is queried, correlated, or rendered yet: the Investigation API
and every read surface are future phases.
`docs/roadmap/phases.md` owns what lands when; do not pull a future phase's
work into the present one, and do not ship a capability whose phase has not
started.

## Where the authority lives

One fact, one owner. When documents disagree, the owner wins; fix the other
document in the same commit.

| Fact                                                                                              | Owner                                         |
| ------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| What the product is / is not                                                                      | `docs/product/`                               |
| What lands in which phase                                                                         | `docs/roadmap/phases.md`                      |
| Component map and runtime modes                                                                   | `docs/architecture/system.md`                 |
| The dependency law (allowed and forbidden edges)                                                  | `docs/architecture/boundaries.md`             |
| Telemetry data shapes                                                                             | `docs/architecture/telemetry-model.md`        |
| The Investigation API's contract                                                                  | `docs/architecture/investigation-model.md`    |
| The query budget and search contract                                                              | `docs/architecture/query-model.md`            |
| Relation types, evidence and provenance                                                           | `docs/architecture/correlation-model.md`      |
| Storage modes and retention                                                                       | `docs/architecture/storage-model.md`          |
| MCP surface rules                                                                                 | `docs/architecture/mcp-model.md`              |
| Resource targets and backpressure                                                                 | `docs/architecture/runtime-constraints.md`    |
| Recorded decisions (the "why")                                                                    | `docs/decisions/`                             |
| Which performance measurements exist (targets live in `docs/architecture/runtime-constraints.md`) | `docs/benchmarks/README.md`                   |
| Executable form of the boundary law                                                               | `module-boundaries.config.mjs`                |
| Project map, tags, tasks                                                                          | `.moon/workspace.yml`, per-project `moon.yml` |
| The scopes a commit may carry                                                                     | `commitlint.config.mjs`                       |

Architecture facts are verified from the repository, not from memory: re-read
the owner document before relying on one, because the file you remember may
have moved in someone else's commit.

## Before you change anything

1. **Read the owner document** for the area you are touching (table above).
2. **Inspect the mechanical constraints**: `module-boundaries.config.mjs` for
   the tag rows your crate or app must satisfy, the project's `moon.yml` for
   its tags and tasks.
3. **Run the architecture skills where your host has them** — `arch-context`
   before the edit, `arch-check` after it, `arch-review` on the finished
   change. The commands behind them are read-only (`archkeep context`,
   `archkeep check`); the constraint table is a file you cannot edit as part
   of a product change.
4. **New module = one commit, three files**: its directory with `moon.yml`
   (tags included), a row in `module-boundaries.config.mjs` that judges its
   tag, and its scope in `commitlint.config.mjs`. A module the boundary table
   does not judge is a module with no law; archkeep refuses an unconstrained
   project tag, so this is enforced, not advisory.

## The boundary law is mechanical

`docs/architecture/boundaries.md` states the law in prose; `pnpm arch`
(`archkeep check`) is what actually judges it, from the project graph moon
builds. Two invariants are worth naming because they are easy to violate by
accident:

- **The UI and the MCP surface never import workspace code.** `layer-view`
  may import Loom and itself only; `layer-agent` may import the Investigation
  API only. A compile-time import from either into query, correlation,
  storage or ingestion is a second door around the API and fails the gate.
- **Only `layer-app` names concrete drivers.** A storage driver chosen
  anywhere else inverts ADR 0003 and fails the gate.

`pnpm arch:canary` proves both directions on every run: two fixture
workspaces that violate the law in the exact ways the boundary document
forbids must fail with the exact violations, and the real tree must stay
clean. Never weaken a constraint row, widen a fixture's tolerance, or add a
suppression to make a gate green — if the law is wrong, change
`docs/architecture/boundaries.md` and the table together, with the reasoning,
in the open. Fixing the product to satisfy the gate is the correct direction;
the reverse is sabotage.

## Conventions

**Rust** (workspace `crates/`, plus `apps/desktop/src-tauri`): edition 2024,
MSRV 1.85 via `rust-version`, no `rust-toolchain.toml` — the ambient stable
toolchain is the toolchain (ADR 0001). Every command that can take it gets
`--locked`; `Cargo.lock` is committed. Lints are workspace-inherited
(`unsafe_code` forbidden, clippy all denied, pedantic warned) — do not add
per-crate lint escapes. tokio and axum live in `crates/server` and nowhere
else; a new dependency needs an architectural justification in the PR, not
just a need.

**TypeScript / Vue** (`apps/web`): strict via `tsconfig.base.json`, no path
aliases (ADR 0004), Vue script-setup only, Loom components from the npm
package — never vendored. Formatting is prettier's job alone;
`eslint-config-prettier` is last in `eslint.config.mjs` and stays last.

**Tests are not optional at any layer.** Every crate carries at least one
real test (the test task fails a suite that runs zero tests); every moon
project has `lint`, `typecheck`, `test` and — where it produces an artifact
(desktop `build` lands with Phase 5) — `build`. `scripts/cargo-test.sh` is the test entry for Rust crates and fails
loudly on a vacuous run. Do not write a test that asserts what the compiler
already enforces unless the assertion is the point (see the desktop shell's
`shells_the_same_core`).

**Resource budgets are architecture, not tuning.** The targets in
`docs/architecture/runtime-constraints.md` (idle RSS, startup, bounded
retention and query memory, zero external services) constrain design
decisions now, and `scripts/bench/` will hold the measurements later. A
dependency or design that makes a target unreachable needs an ADR before the
code lands. Never claim a target is met: targets and measurements live in
separate places on purpose, and `docs/benchmarks/README.md` is the only file
allowed to carry numbers with a machine attached to them.

## Definition of done

A change is done when, on a clean checkout of the branch: `pnpm verify`
passes (format, lint, typecheck, tests, architecture check, docs link check);
`pnpm arch:canary` passes; new user-facing behavior is named in the docs that
own it; new dependencies are justified in the PR description; and nothing in
the diff claims a capability `docs/roadmap/phases.md` has not started.
`scripts/verify-local.sh` runs the whole local gate — use it instead of
hand-picking checks, because hand-picking is how a red gate gets shipped.

## Prohibited shortcuts

- No `--no-verify`, no skipped hooks, no disabled gate to land a diff.
- No placeholder commands that report success without performing the
  advertised check. A check that cannot run on this machine says so loudly
  and exits non-zero.
- No product dependency "temporarily" added without its architectural
  justification; no database required for startup, ever (ADR 0003).
- No claim of an unimplemented feature in any user-facing surface. The UI at
  bootstrap says no telemetry exists — that honesty is a feature; keep it.
- No editing generated or lock files by hand (`pnpm-lock.yaml`,
  `Cargo.lock`); regenerate them with the toolchain.

## Commits, PRs, and security

Conventional Commits with the scopes in `commitlint.config.mjs`; a new module
brings its scope in the same commit. Hooks run the fast gates per commit and
the full suite on push; the merge queue requires `ci-gate` and
`analysis-gate` (see `.github/workflows/`; the required-checks ruleset is
recorded in `.github/repository-settings.json`) — never bypass them, never push to
`main` directly. Keep PRs small enough to review in one sitting; a PR that
changes the architecture law links the architecture document diff beside the
code diff.

Security issues never travel through issues or PRs — follow `SECURITY.md`
(private advisory) and stop.

## Working with subagents

When a task decomposes into independent units, dispatch concurrent subagents
rather than serial work, each with the issue number, branch and draft PR in
its prompt, and its own worktree if it edits the same repository as another.
Agents that change the same moon project conflict by construction; agents in
different projects do not. The coordinating session synthesizes and routes
follow-ups; the architecture gate (`pnpm arch`, `pnpm arch:canary`) is the
arbiter when parallel edits drift into each other — run it after integrating,
not before dispatching.
