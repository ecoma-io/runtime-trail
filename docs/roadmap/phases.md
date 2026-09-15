# Delivery phases

> Phase detail for [README.md](README.md). Status values: `done` ·
> `in progress` · `next` · `planned`. This file tracks delivery order, not
> design — design authority stays in [../architecture/](../architecture/).

## Phase 0 — Engineering foundation — `done`

The repository's first commit: toolchain (Rust workspace + Vue/Loom frontend),
architecture contracts, agent workflow, CI/CD gates, documentation, governance,
and a minimal executable skeleton (native server with health/version surfaces,
Loom web app, desktop shell, Docker image — all smoke-level only).

**Acceptance:** the foundation commit builds and tests green locally and in CI;
Archkeep enforces the documented boundaries; the phase-1 routing document.
Phase 0 shipped that; the OTLP ingestion capability it defined as _not yet
existing_ has since landed with Phase 1.

## Phase 1 — Telemetry ingestion — `in progress`

OTLP ingestion of traces, logs and metrics into the in-memory runtime:
OTLP/gRPC and OTLP/HTTP receivers, bounded ingestion queues, backpressure
signals, and the first real population of the telemetry model.

**Landed so far** (2026-09-12): the sanctioned Phase-1 slice is landed,
adversarially reviewed and benchmarked — telemetry model with the
admission ledger (ADR 0008); OTLP/HTTP and OTLP/gRPC receivers with the
bounded queue/backpressure wire gates; bounded memory driver, composition
root, and the memory-path measurements under
[`docs/benchmarks/README.md`](../benchmarks/README.md).

**Acceptance (open):** an OTLP SDK configured against a locally started
`runtime-trail` has its traces/logs/metrics **queryable through the internal
API**; overload produces backpressure instead of unbounded memory growth. The
queryable-through-internal-API half is Phase 2's Investigation-runtime work;
this phase closes when the full acceptance sketch is met. The
[telemetry model](../architecture/telemetry-model.md) and
[numeric-limit](../architecture/runtime-constraints.md) contracts are
binding for this phase.

## Phase 2 — Investigation runtime — `in progress`

The product's centre of gravity: query engine over the telemetry model,
correlation engine (trace↔logs, log→trace, metric-window↔traces), one
Investigation API, and the first Loom UI investigation surfaces (trace
waterfall/span tree, log list, logs↔trace navigation).

**Landed so far** (2026-09-13): the Query Engine's budgeted records flow —
ordered, snapshot-bounded, cursor-chained pages over the ingested signals
under the five-dimension budget (issue #6). The Investigation API and the
correlation engine's committed strategies (span identity, trace identity,
temporal co-activity — the last only under an explicit caller window, the
runtime picks no window by default) are wired end to end (issues #12,
#28): one `POST /v1/investigations/traces` flow composes waterfall spans,
related logs, surrounding metrics and the correlated relations under one
caller budget. The investigation surfaces (Loom UI) and MCP are later
milestones of the phase.

**Acceptance sketch:** from one ingested trace, a developer reaches its logs
and surrounding metrics in a handful of interactions; the same answers are
available through the Investigation API without the UI.

## Phase 3 — Persistence — `planned`

Embedded file-backed storage mode alongside the (already first-class)
in-memory mode: session save/load, bounded retention on disk, automatic
recovery. In-memory remains the default and the mandatory critical path is
never blocked by persistence.

## Phase 4 — MCP interface — `planned`

MCP server exposing investigation capabilities to AI agents through the same
Investigation API the UI uses. No agent-only data paths, no storage access.

## Phase 5 — Distribution hardening — `planned`

Docker image and desktop app move from smoke-level to releasable: image
size and startup budgets, desktop packaging for the three OSes, update story.
Release automation stays on its own track (see [README.md](README.md)) and
never rides on ordinary CI.

## Phase 6 — Budget enforcement — `planned` (harness structure began in Phase 0 by design; measurement and CI gating are this phase's work)

Resource budgets from
[../architecture/runtime-constraints.md](../architecture/runtime-constraints.md)
move from targets to measured, CI-enforced facts: the benchmark harness runs
the published scenarios and regression gates the numbers. This work starts in
Phase 0 (harness and structure) and finishes when the budgets are machine-
checked on every change that can move them.

## Sequencing rules

- A phase's capabilities may start early only when their architecture
  (boundaries, models) is already documented here.
- Bugs and blocking defects in shipped phases outrank new phase work.
- Anything in [../product/non-goals.md](../product/non-goals.md) is never a
  phase, whatever the temptation.
