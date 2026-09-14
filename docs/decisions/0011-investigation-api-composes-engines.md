# 0011 — The Investigation API composes the engines into named flows

- **Status:** Proposed
- **Status note:** promoted to Accepted by the M3 implementation PR after
  `pnpm arch` + `pnpm arch:canary` pass with no storage edge added to
  `crates/investigation` (verification gate in decision 4).
- **Date:** 2026-09-15
- **Part of:** Phase 2 — Investigation runtime (M3, issue #12)

## Context

[investigation-model.md](../architecture/investigation-model.md) fixes the
contract: one envelope named `Investigation`, five parts
(subject / execution / correlated / evidence / limits), seven invariants,
transport-independent — and one door. Both surfaces (the developer UI over
HTTP, the MCP server in-process) consume `crates/investigation` (`layer-api`)
and nothing else in the core; it composes the query and correlation engines.

The boundary law
([boundaries.md](../architecture/boundaries.md)) lets `layer-api` depend on
query, correlation and the model — never on storage, drivers or ingestion.
But the query engine's records entry takes the store as
`&dyn TelemetryStore`, a trait owned by the storage crate
(`crates/query/src/engine.rs:671-674`, `pub fn records(store: &dyn
TelemetryStore, …)`). The API therefore cannot hand the engine a store
without either naming a storage type or hiding it behind an engine-owned
face.

Two facts about the query engine shape this decision:

- The engine is the sole budget enforcer (query-model.md: "the engine alone
  spends"); the API composes and reports, it never re-enforces or re-expires
  a dimension. A continuation page is a new execution admitted with the
  caller-set budget (the M2 per-page decision — query-model.md now owns that
  paragraph).
- The engine is already the crate that reads through the storage contract
  (ADR 0003: "the engine sees the contract, never a concrete driver").

`crates/investigation` already declares query, correlation and model as
dependencies (no manifest churn needed).

## Decision

1. **The envelope and the flows live in `crates/investigation`.** Requests
   are named flows; responses are `Investigation` envelopes. Public request
   types carry investigation vocabulary only: dimension budgets (deadline,
   `max_scan`, `max_results`, `max_bytes`, `max_aggregation_memory`), opaque
   cursors, service and time-window filters. No engine or transport types
   leak into request shapes.

2. **The first slice ships the owner-documented committed capability: one
   trace investigation flow whose envelope composes the trace waterfall
   (recent spans), the related logs, and the surrounding metrics as a single
   response** (the committed-capabilities paragraph of investigation-model.md).
   The flow maps to the records executions it needs, all over the
   five-dimension budget. Additional flows arrive by phase, when the
   contract's phase text reserves them; none are promised here.

3. **Budget decomposition: caller → flow → engine, enforcement stays in the
   engine.** The caller sets the flow budget; the flow derives the per-execution
   `QueryBudget` (flow fixed costs are charged against the caller's
   dimensions), and each engine run enforces it. The envelope's execution and
   limits parts report per-execution spend vs budget, truncations, stuck
   cursors and coverage — machine-checkable, run-facts, never recomputed.

4. **The `layer-api` storage edge resolves as a query-owned facade.** The
   query crate re-exports the contract interface it already reads
   (`pub use runtime_trail_storage::TelemetryStore;`), and the investigation
   crate names it as `runtime_trail_query::TelemetryStore`. This is an
   interface passthrough, not a driver naming: the concrete store still
   enters only at `layer-app`'s composition root, and no storage manifest
   edge is added to `crates/investigation`. It is the same
   engine-owns-the-contract reading of ADR 0003. **Verification gate for
   this slice:** `pnpm arch` + `pnpm arch:canary` green with no
   `runtime-trail-storage` entry in `crates/investigation/Cargo.toml`.

5. **Composition root stays in `layer-app` (the server).** `crates/server`
   builds the memory store + ledger hook exactly as today, passes
   `&dyn TelemetryStore` through the query facade into the flow
   constructors, and exposes the flows over HTTP as JSON-rendered envelopes
   in a thin adapter — transport-independent contract (investigation-model.md
   invariant 1), HTTP being the first transport. The MCP surface reuses the
   same crate in-process in Phase 4; it does not open a second door.

6. **Correlation rides in the envelope's `correlated` part.** Where a flow
   composes correlation, typed, evidenced, ordered relations per
   correlation-model.md land in the envelope; layer-api composes correlation
   exactly as it composes query.

7. **Docs and ADR move together, never silently.** Shape changes forced by
   implementation amend this ADR or add a successor, alongside the
   investigation-model.md status update (that status section is the doc's
   to own).

## Consequences

- **One door, law-abiding:** both surfaces depend on `layer-api` only;
  `crates/investigation/Cargo.toml` gains no storage edge; the arch gate is
  re-run in the M3 slice and must stay green (verification gate in decision 4) — asserted, not assumed.
- **Sole enforcer preserved:** the engine spends, the API reports; no second
  budget book in the API layer.
- **No new dependencies:** query, correlation, model are already declared in
  `crates/investigation`; the re-export adds no edge.
- **Acceptance criteria carried by this ADR (demonstrated by the M3 slice
  before ready, not asserted here):** behavioral tests through the real API
  (empty runtime, three signal kinds, filters, pagination and cursor
  chaining, eviction, deadline/byte/scan/result limits, refusal, degradation
  and coverage honesty), `pnpm arch` + `pnpm arch:canary` green with no
  storage edge added, and then the Phase-1 acceptance (issue #4) closes on
  top of the API (OTLP SDK → runtime → storage-memory → Investigation API →
  queryable).
