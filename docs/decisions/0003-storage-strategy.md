# 0003: In-memory first-class; SQLite as the embedded file-backed engine; no external database ever

## Status

Accepted (2026-09-11, foundation commit)

## Context

[Scope](../product/scope.md) commits `runtime-trail` to two storage modes —
in-memory and embedded file-backed — with **zero external database
requirement** ([non-goals](../product/non-goals.md)), bounded retention in
both, and a hard rule that persistence never blocks ingestion
([storage model](../architecture/storage-model.md)).

Candidates for the embedded engine:

- **SQLite** — ubiquitous, single-file, synchronous and embeddable, decades of
  operational maturity; "copy one file, reopen the session" is native
  behaviour. Not columnar: aggregation-heavy analytics are not its strength —
  but bounded local investigation windows are exactly the workload it is
  strong at.
- **DuckDB** — stronger OLAP aggregation, but heavier, analytics-shaped, and
  it would pull an analytical-engine runtime into a 50 MB-RSS budget for a
  workload (bounded windows, correlation lookups) that is not analytics at
  grid scale.
- **A hand-rolled log-structured store** — maximal control, maximal cost; not
  before the real access patterns exist to justify it.
- **RocksDB/Sled-class KV** — great raw storage, but every query shape would
  be hand-built; the investigation model needs relations, not just keys.

## Decision

1. **Memory mode is first-class** and is the default runtime mode: no
   filesystem writes, instant start, bounded retention enforced in-process.
2. **SQLite is the embedded file-backed engine**, behind the storage
   abstraction (`crates/storage-sqlite`), selected only when the user asks
   for file-backed mode. Startup never creates or migrates a database unless
   that mode was chosen.
3. **No external database, ever, in any mode.** A dependency that requires a
   running database server violates [non-goals](../product/non-goals.md).
4. **Persistence is off the ingestion critical path**: admission never waits
   on durable I/O; overload resolves as backpressure toward emitters.
5. The storage abstraction (`crates/storage`) is the only thing above the
   drivers that knows storage exists at all; only the composition root names
   a driver ([boundaries](../architecture/boundaries.md), `layer-app`).

## Consequences

- The DuckDB/KV door stays open behind the abstraction if real workloads
  prove the analytical need; nothing above `crates/storage` would change.
- SQLite's concurrent-writer limits are irrelevant at local scale and are
  additional cover for the "single writer, read-mostly" shape the runtime has.
- The retention/eviction contract must be implemented identically in both
  drivers, or the memory↔file switch would silently change what an
  investigation can see — that symmetry is part of this decision.
- The `storage-sqlite` crate ships as a declared boundary with scaffolding
  only in the foundation commit; its implementation lands with Phase 3
  ([roadmap](../roadmap/phases.md)).
