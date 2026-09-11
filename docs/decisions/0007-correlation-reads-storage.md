# 0007: Correlation reads storage, like query

## Status

Accepted (2026-09-12, M0 architecture contract hardening)

## Context

The foundation's boundary table gave the Correlation Engine
`layer-correlation → model` — model types, nothing below it. Meanwhile M0's
hardening pinned the correlation contract
([correlation-model.md](../architecture/correlation-model.md)) as "a derived
read over the resident telemetry: a pure function of a fixed snapshot of
stored signals". The two contradict: with `model` alone, correlation's input
is unobtainable by any legal path.

The legal paths were examined before the law was changed, not after:

- **layer-api shuttles snapshots to correlation?** No — the API cannot touch
  storage itself (`api → storage` is forbidden), so it has no resident set to
  shuttle; and the envelope it composes is view material, not an engine's
  working input.
- **layer-query feeds correlation?** No — `query → correlation` is forbidden
  (query reads facts; correlation builds them), and only layer-api composes
  the two engines.
- **correlation derives relations from query results only?** No — relation
  strategies must range over the whole resident set (all spans in a trace,
  every log record's trace context, shared resources), not over one flow's
  page of results.

No path exists, so the row was wrong, not the contract.

## Decision

`layer-correlation → model, storage` — the same read access the Query
Engine has. Correlation reads the resident set through the storage
abstraction and never any other way.

- The engine stays **read-only over storage**: it never writes back, never
  spills, never names a driver.
- The engine stays **model-shaped in its output**: relations cite
  model-vocabulary fields; no storage type crosses the relation boundary.
- The engine stays **below the composition line**: only layer-api composes
  correlation with query; `query → correlation` remains forbidden.

## Consequences

- `docs/architecture/boundaries.md` and `module-boundaries.config.mjs`
  change together; the prose row and the executable row are one law.
- No canary changes: the existing fixtures prove `view → storage` (TS) and
  `driver → correlation` (Rust) fail — neither direction is affected by
  widening a downward read edge. `query → correlation` stays forbidden and
  keeps its prose statement; a canary for it remains optional
  ([boundaries.md](../architecture/boundaries.md): negative fixtures are
  worth naming for the invariants the architecture exists to keep).
- Phase 2 implementation note: the Correlation Engine declares a dependency
  on the storage abstraction exactly as `crates/query` does, and its
  strategies receive model-typed records from it — never storage-internal
  shapes.
