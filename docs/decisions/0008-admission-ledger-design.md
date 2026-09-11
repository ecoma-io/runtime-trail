# 0008 — The admission ledger shares payloads, never clones them

- **Status:** Accepted
- **Date:** 2026-09-12
- **Part of:** Phase 1 — OTLP ingestion

## Context

Delivery is at-least-once, so admission must be idempotent: a re-delivered
span or metric point must collapse onto the record already admitted, and a
conflicting re-delivery must leave the first record standing with the conflict
observable. That needs a ledger: for every record, a memory of its natural
identity, so "have I seen this identity?" is answerable for as long as the
record is resident.

The first implementation of this ledger cloned comparison payloads into the
ledger keys. Measured on the reviewer's fixture set, that multiplied real
memory by 3.6×–78× over the record itself — a per-record overhead the
retention ceilings never see, because the ceilings bound accounted content
bytes, and the ledger's cloned keys are not content.

## Decision

The ledger shares, never clones. For every retained record, the ledger entry
holds an `Arc` to the same payload the store holds — one allocation, two
owners, zero duplication:

- **Span identities** are keyed by `(TraceId, SpanId)` — a 24-byte key — to
  the `(EntityId, Arc<Span>)` the store holds.
- **Metric point identities** are keyed by the tuple
  `(Arc<StreamIdentity>, Attributes, Option<u64> start_time, u64 time, u32 flags)`.
  The stream identity (resource, scope, name, kind, temporality) is
  **interned**: one `Arc<StreamIdentity>` per distinct stream, shared between
  the ledger, the store and every point of the stream.
- **Log records carry no ledger entry.** They have no natural identity to
  remember; their ledger cost is zero by design, and no log-specific memory
  is held to deduplicate what must never be deduplicated.

Equality in the ledger is byte-exact by construction: it compares through the
`Arc`s by dereference — the same comparison law as the model's own `PartialEq`
— with no hashing anywhere in the ledger. There is no collision case to
rule out, because there is no hash.

**Eviction and identity are one lifecycle.** Evicting a record calls
`forget(identity)`: the identity entry lives exactly as long as the record's
residency. A re-delivery after eviction is therefore admitted as a fresh
record — first-stands applies within residency, not across it. The ledger's
capacity tracks the store's record count; it cannot outgrow what the store
retains.

## Consequences

- Idempotent admission costs ~100–200 bytes per retained record — the ledger
  entry plus interning — a term in the runtime's fixed overhead outside the
  accounted ceilings ([runtime-constraints.md](../architecture/runtime-constraints.md)),
  and a measured one once [../benchmarks/README.md](../benchmarks/README.md)
  carries numbers.
- Eviction visibly ends identity: re-delivery after eviction re-admits, which
  is the documented semantics ([telemetry-model.md](../architecture/telemetry-model.md)),
  not an edge case.
- Ledger equality is O(payload size) per candidate comparison and collision-
  free by construction; the ledger never approximates what the model defines
  as exact.
- Alternatives rejected: **cloning payloads into ledger keys** — the measured
  3.6×–78× multiplication above; **hashing payloads into keys** — canonical
  float forms break bit-exact equality, and the hash-plus-reverify shape still
  stores the payload twice, reproducing the multiplication it was meant to
  avoid.
