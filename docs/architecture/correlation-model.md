# Correlation model

> Authoritative home for what it means for two signals to be related: the
> relation types, the evidence each carries, and the rules that keep
> correlation deterministic and honest. The envelope a consumer receives
> relations in is part of the [Investigation shape](investigation-model.md);
> the model fields evidence may cite live in
> [telemetry-model.md](telemetry-model.md).

## What correlation is

Correlation is a **derived read** over the resident telemetry: a pure function
of a fixed snapshot of [stored signals](storage-model.md) plus a versioned set
of strategies. It discovers typed relations between records and returns them
with their evidence. It never mutates the model, never writes back into
[storage](storage-model.md), and never invents signal data. Two investigations
run with the same resident set and the same strategy versions produce the
same relations.

## A relation is a typed fact with evidence

A relation is not a boolean `related = true`. Every relation carries:

- **`type`** — one of the taxonomy below. Never a free-form string.
- **`from` / `to`** — [entity ids](telemetry-model.md) of resident records.
  A relation to a non-resident record is not produced (see
  [eviction](#relations-and-eviction)).
- **`tier`** — the strength tier of its type (below). Tiers are a property of
  the type, never a numeric score attached to an instance.
- **`evidence`** — the model fields and values that justify it, as
  (field, value) pairs drawn from the model's own vocabulary — for example
  `trace_id = 4bf9…` for a SpanIdentity relation. Evidence is inspectable;
  a consumer can always ask "why are these related?" and get an answer made
  of stored data, not of an opaque score.
- **`strategy`** — the name and version of the strategy that produced it.
- **`window`** — where a type is time-bounded, the window that was applied.

## The taxonomy

| Type                 | Connects                         | Evidence                                             | Tier       |
| -------------------- | -------------------------------- | ---------------------------------------------------- | ---------- |
| `SpanIdentity`       | log record → span                | `trace_id` + `span_id` both match                    | identity   |
| `TraceIdentity`      | log record → any span in a trace | `trace_id` matches                                   | identity   |
| `ParentChild`        | span → span                      | `parent_span_id` of the child                        | structural |
| `ResourceContext`    | any → any                        | equal resource identity                              | context    |
| `TemporalCoActivity` | any → any                        | overlap or proximity within a caller-supplied window | temporal   |
| `ExemplarAttachment` | metric data point → span         | exemplar trace context                               | attachment |
| `Inferred`           | —                                | —                                                    | —          |

- `SpanIdentity` is the strongest log↔trace fact: both halves of the trace
  context match. When it holds, `TraceIdentity` relations from the same log
  record are **suppressed** — the stronger evidence wins, and a log attached
  to its exact span is not also listed against every sibling span. Evidence
  for suppression is retained in coverage, not lost.
- `ParentChild` is derived from the span's own parent field; it adds no fact
  the model did not already carry, which is exactly why it is structural.
- `ResourceContext` is the weakest relation worth carrying: shared resource
  identity is context, not causation. It exists so that "what else happened
  in this service" is answerable without a time window.
- `TemporalCoActivity` requires an explicit window supplied by the caller
  through the [Investigation](investigation-model.md); the runtime picks no
  window by default. Its evidence includes the window.
- `ExemplarAttachment` is the model's metrics↔trace hook
  ([telemetry-model.md](telemetry-model.md)) made navigable.
- **`Inferred` is a reserved name with zero instances.** Machine-learned or
  heuristic relation inference is not committed for Phase 2; if it ever
  lands it needs its own decision record and its own evidence rules. No
  shipped strategy may emit it.

## Strength tiers

Identity > structural > attachment > context > temporal. The tier is an
ordering over types, used for presentation priority and for the
strongest-evidence-wins rule above. Tiers are never numerically scored,
weighted, or averaged — a relation's strength is its type, full stop.

## Direction and navigation

Each type has a canonical direction (the taxonomy's "connects" column);
navigation in the inverse direction is **the same relation seen backwards**
and adds no facts. Traversing `A → B → C` is a query-engine navigation over
two relations, never a synthesised `A → C` relation: **correlation produces
no second-order relations.**

## Determinism and strategies

- A strategy is (name, version). Relation output is deterministic given the
  resident set and strategy versions: same inputs, same relations, same
  order.
- Relations are ordered deterministically — by type, then by endpoints — so
  that pagination and repeated queries are stable
  ([query-model.md](query-model.md) owns ordering and cursors).
- Strategy versions are reported in
  [execution](investigation-model.md) so an investigation can be reproduced
  later even across runtime upgrades.

## Guards

- **No self-loops.** `from` ≠ `to`, always.
- **No second-order relations** (above).
- **No relations to absent records.** Endpoints must be resident.
- **Bounded expansion.** Multi-hop navigation is bounded by the
  investigation's budget — `max_hops` (default ≤ 2) and `max_relations` are
  layer-api flow parameters per [query-model.md](query-model.md); the
  Correlation Engine refuses beyond them rather than growing unbounded.

## Budgeting correlation

Correlation spends from the caller's investigation budget, decomposed by
layer-api into a share the Correlation Engine may spend on scan work —
accounted with the same [scan unit](query-model.md) the Query Engine uses.
Correlation never widens a budget, never spills to disk, and on exhaustion
reports the truncated traversal truthfully through the
[envelope](investigation-model.md).

## Relations and eviction

- Relations are **derived on read**, never stored as first-class data. The
  stored facts are the signals themselves; relations exist only inside an
  investigation response.
- A relation is returned only when **all** of its endpoints are resident.
  When [eviction](storage-model.md) removes a span, relations that touched
  it stop existing — and the response's coverage reports the shrinkage,
  so a shrinking evidence trail is visible, never silent.
- A log whose `trace_id` names a trace with no resident span yields **no
  relation** — the absence is a completeness fact surfaced in coverage, not
  a relation to an absent entity.

## Invariants

1. Every relation's evidence cites only model-vocabulary fields of records
   that are resident.
2. Same resident set + same strategy versions ⇒ identical relations, in
   identical order.
3. `TraceIdentity` never coexists with a `SpanIdentity` from the same log
   record.
4. No relation has `from == to`, a non-resident endpoint, or a second-order
   shape.
5. `Inferred` has zero instances; no strategy emits it.
6. Relations appear in a response only under its `max_relations`; traversal
   depth never exceeds `max_hops`.
7. Eviction of any endpoint removes every relation that touched it, and the
   removal is visible in the response's coverage.
8. Correlation is read-only: no relation is persisted, and no relation
   changes what storage keeps.

## Status

**Contracts pinned (M0).** The taxonomy, evidence rules and invariants above
are the contract the Phase 2 Correlation Engine implements. The engine crate
remains scaffolding until then per [the roadmap](../roadmap/phases.md).
