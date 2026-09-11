# Storage model

> Authoritative home for the storage abstraction, the runtime storage modes,
> retention and the ingestion/persistence relationship.

## The abstraction

`crates/storage` (tag `layer-storage`) owns the storage _contract_: what it
means to keep telemetry and answer the [Query Engine](investigation-model.md)
against it. The contract knows the [telemetry model](telemetry-model.md) and
nothing else in this repository — in particular it knows no concrete backend,
no UI type, no wire format.

Concrete modes live behind it, one crate each:

- `crates/storage-memory` — **in-memory mode**. Ephemeral, zero-setup, the
  default for a quick session.
- `crates/storage-sqlite` — **embedded file-backed mode**. Durable-enough
  local persistence for sessions worth keeping.

Both are `layer-storage-driver`: they implement the abstraction and depend on
nothing above it ([boundaries.md](boundaries.md)). Only the composition root
(`layer-app`) may name a driver; everyone else speaks the abstraction.

## Memory mode is first-class

Memory mode is not a degraded fallback or a test double — it is the mode a
developer gets by default, and it must be excellent: instant start, no
filesystem writes, bounded retention enforced in-process. File-backed mode is
a _variant of the runtime_, never a requirement: no code path may assume a
database exists, and no startup may create or migrate one unless the user
chose file-backed mode.

## Persistence is never on the ingestion critical path

Ingestion admits telemetry and hands it to the active store; it must never
block on a durable write. The contract between
[ingestion](system.md) and storage is therefore an admitted-then-kept
pipeline with explicit backpressure: when a store cannot keep up, the
runtime signals backpressure to emitters ([runtime-constraints.md](runtime-constraints.md))
rather than buffering without bound or stalling the hot path on I/O.
A slow disk degrades durability guarantees — it never stops the runtime from
running, and it never turns memory mode into file-backed mode.

## Bounded retention in every mode

Both modes are bounded by explicit budgets — record counts, byte ceilings,
time windows — configured at startup, enforced by the store. There is no
unbounded mode and no "grow until OOM then evict" behaviour; eviction order
and its observability are part of the contract, because an investigator must
be able to see what has been evicted from the window they are looking at.
The numbers themselves live in [runtime-constraints.md](runtime-constraints.md).

## Why an embedded store, and which one

SQLite is the committed file-backed engine — a decision with its own record:
[../decisions/0003-storage-strategy.md](../decisions/0003-storage-strategy.md).
The short version: zero external database is a product commitment
([non-goals](../product/non-goals.md)); an embedded, single-file, ubiquitous
engine is what makes "copy one file, reopen the session" possible.

## Status

**Bootstrap.** All three crates exist as declared boundaries with scaffolding
only. The abstraction's real trait surface lands with Phase 1 (memory mode)
and Phase 3 (file-backed mode) per [../roadmap/phases.md](../roadmap/phases.md).
