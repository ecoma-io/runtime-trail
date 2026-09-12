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
time windows — configured at startup, enforced by the store. The **record**
that retention counts and evicts is the model's record:
[telemetry-model.md](telemetry-model.md) owns record identity and the
[accounted size](telemetry-model.md) that byte ceilings count; this document
owns the eviction law, and the numbers live in
[runtime-constraints.md](runtime-constraints.md).

There is no unbounded mode and no "grow until OOM then evict" behaviour.
Eviction order and its observability are part of the contract: eviction is
observable where it happens (dropped-record counters) **and** downstream,
because [query responses report gaps](query-model.md) and
[the Investigation envelope](investigation-model.md) carries them in its
coverage — an investigator must be able to see what has been evicted from
the window they are looking at. "Oldest" is by **admission time** — the
model's added metadata ([telemetry-model.md](telemetry-model.md)) — never
by emitter event time, which out-of-order emitters would make
unpredictable.

**The byte ceiling counts what residency pins — including stream
identities.** A metric point references a stream identity (resource,
scope, name, kind, temporality) whose content the record's own accounted
size does not carry. The store therefore charges each distinct resident
stream's identity accounted size **exactly once** — added when the
stream's first point enters residency, released when its last point
leaves — so a session of single-point streams cannot park unbounded
identity content under a byte ceiling that only saw the points
([telemetry-model.md](telemetry-model.md) owns the definition;
[ADR 0008](../decisions/0008-admission-ledger-design.md) owns the
lifecycle). An identity the ceiling cannot hold on its own is refused,
not evicted into: a keep whose stream identity's accounted size alone
exceeds the byte ceiling is refused before anything is inserted —
non-retryable, naming the ceiling and the identity's size, with an
observable counter — because no amount of eviction could make room for a
charge that is over the cap by itself. The **series cap**
([runtime-constraints.md](runtime-constraints.md))
is the second half of the same law: the store refuses a keep that would
establish a new distinct stream beyond the cap, with an observable
counter, and a slot frees when the stream's last point is evicted. Both
refusals end the refused record's ledger identity through the hook
(below), so a re-delivery after a refusal re-admits as a fresh admission —
a re-attempt is measured against the ceilings as they stand then, never
shadowed by an identity whose record never entered residency.

Descriptor content — a stream's `description`, `unit` and `metadata` — is
bounded by this ceiling and by nothing smaller: admission gates the
descriptor's attribute budgets (metadata count and per-value size) but sets
no aggregate size gate on the descriptor, and `description` and `unit` carry
no size gate at all. The identity is therefore charged to the byte ceiling
byte-exactly once resident, and refused whole at keep when it cannot fit —
never truncated and never silently clipped.

**The store owns no clock.** Window expiry is evaluated against the
composition root's reading of time (`enforce_retention(now)`); the
composition root therefore owns the retention timer and must call it
periodically — at a granularity well inside the shortest configured
window — so an idle session still expires records. Ceilings on records
and bytes bound memory while idle regardless; the timer exists so the
_window_ stays truthful, not to prevent unbounded growth.

**The hook runs after the store's own state settles, and it must not
panic.** The hook carries three delivery kinds, and every one fires only
after the store's own bookkeeping is complete — for an eviction, counters
and indexes updated; for a keep refusal, the refusal counted with
residency exactly as it was, nothing having been inserted. The kinds: an
**evicted record** (`evicted`), a **stream whose last resident point
left** (`stream_released`), and a **keep refusal that inserted nothing**
(`keep_refused` — the oversized, series-cap and identity-over-ceiling
refusals, with the refused record's interned stream when it carries one; a
duplicate keep reports nothing, the record it names being resident). A
misbehaving hook can never corrupt the store — but the hook is where the
composition root releases the record's ledger identity
([ADR 0008](../decisions/0008-admission-ledger-design.md)), so a panicking
hook would leave identity behind after its record's story ended. A
fallible hook is the composition root's to wrap; the store reports
evictions, hook deliveries, keep refusals and refusal deliveries as
separate counters, so a delivery that does not complete — evicted but not
released, refused but not released — is observable, not silent.

When the file-backed store's disk is full, the runtime degrades durability
and observability — it never blocks admission and never blocks the hot path
([persistence rule](#persistence-is-never-on-the-ingestion-critical-path),
[runtime-constraints.md](runtime-constraints.md)). A full disk is a logged,
surfaced condition, never a stall.

## Why an embedded store, and which one

SQLite is the committed file-backed engine — a decision with its own record:
[../decisions/0003-storage-strategy.md](../decisions/0003-storage-strategy.md).
The short version: zero external database is a product commitment
([non-goals](../product/non-goals.md)); an embedded, single-file, ubiquitous
engine is what makes "copy one file, reopen the session" possible.

## Status

**Phase 1 implements the abstraction and memory mode** — the retention
contract above has a real trait surface and an in-memory driver enforced
against it. File-backed mode lands with Phase 3 per
[../roadmap/phases.md](../roadmap/phases.md).
