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
- **Metric point identities** are keyed by
  `Arc<PointIdentity>` — the key _is_ the sharing: it holds the interned
  `Arc<StreamIdentity>` and the admitted `Arc<MetricPoint>` itself, and
  projects (resource, scope, name, kind, temporality; the point's
  attribute set; gauge-normalised `start_time`; `time`; `flags`) into its
  equality and total order. No attribute set is ever copied into a key —
  the attribute set lives once, inside the shared record. The stream
  identity (resource, scope, name, descriptor — description, unit,
  metadata — kind, temporality) is **interned**: one `Arc<StreamIdentity>`
  per distinct stream, owned by a content-keyed ordered set, shared
  between the ledger, the store and every point of the stream.
- **The point key's projection is descriptor-blind.** The descriptor
  (description, unit, metadata) is full stream identity, but it is
  projected _out_ of the collapse key on purpose: a re-delivery of a
  standing point under a changed descriptor must land on the standing key
  so the ledger can record a stream conflict — first descriptor standing —
  instead of silently admitting a parallel stream. A different point under
  the changed descriptor projects to a different key and admits as the
  second stream it is. The full identities, descriptor included, are what
  the ledger compares once the key lands.
- **Log records carry no ledger entry.** They have no natural identity to
  remember; their ledger cost is zero by design, and no log-specific memory
  is held to deduplicate what must never be deduplicated.

The ledger's maps are **ordered** (`BTreeMap`/`BTreeSet`) keyed by total
orders over identity content. Equality in the ledger is byte-exact by
construction: it compares through the `Arc`s by dereference — the same
comparison law as the model's own `PartialEq` — with no hashing anywhere in
the ledger. There is no collision case to rule out, because there is no
hash. The orders themselves are comparison orders, not canonical encodings:
floats order by IEEE-754 bit pattern (equal bits `Equal`, different bits
never equal — `-0.0` and `0.0` stay distinct, a NaN equals only its own bit
pattern), exactly the equality the model defines.

**Eviction and identity are one lifecycle.** Evicting a record calls
`forget(identity)`: the identity entry lives exactly as long as the record's
residency. A re-delivery after eviction is therefore admitted as a fresh
record — first-stands applies within residency, not across it. **Stream
identities obey the same law.** The ledger interns a stream identity on the
stream's first admitted point and drops it when the stream's last resident
point leaves: the store's eviction hook reports the stream reference
alongside the entity id, and the composition root releases the stream's
interning in the ledger at refcount zero. The ledger's capacity therefore
tracks what the store retains — records _and_ resident streams — and can
outgrow neither. **The hook must not panic**: it runs after the store's own
removal completes, and a fallible hook is the composition root's to wrap;
the store reports evictions and hook deliveries as separate counters, so an
evicted-but-not-released divergence is observable, never silent.

## Consequences

- Idempotent admission costs ~100–200 bytes per retained record — the ledger
  entry plus interning — a term in the runtime's fixed overhead outside the
  accounted ceilings ([runtime-constraints.md](../architecture/runtime-constraints.md)),
  and a measured one once [../benchmarks/README.md](../benchmarks/README.md)
  carries numbers. For a **single-point stream** the interned identity's own
  content is a per-record cost — which is exactly why the byte ceiling
  charges each resident stream's identity content once
  ([telemetry-model.md](../architecture/telemetry-model.md)); the interning
  pointers themselves stay fixed overhead.
- Interned stream identities are released at refcount zero. The review of
  the first ledger build proved the un-released variant pins every distinct
  stream ever seen for the whole session (822 KB for zero resident records
  at 1,000 streams), so release is load-bearing, not an optimisation.
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
