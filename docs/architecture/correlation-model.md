# Correlation model

> Authoritative home for what it means for two signals to be related: the
> relation types, the evidence each carries, and the rules that keep
> correlation deterministic and honest. The envelope a consumer receives
> relations in is part of the [Investigation shape](investigation-model.md);
> the model fields evidence may cite live in
> [telemetry-model.md](telemetry-model.md).

## What correlation is

Correlation is a **derived read** over the resident telemetry: a pure function
of a fixed snapshot of [stored signals](storage-model.md), the request's
parameters (windows, relation bounds) and a versioned set of strategies. It
discovers typed relations between records and returns them with their
evidence. It never mutates the model, never writes back into
[storage](storage-model.md), and never invents signal data. Two investigations
run with the same resident set, the same request parameters and the same
strategy versions produce the same relations.

Its input path is legal by construction: the Correlation Engine reads the
resident set through the [storage abstraction](storage-model.md) — the same
access the Query Engine uses, declared as `layer-correlation → model,
storage` in [boundaries.md](boundaries.md) — and receives model-typed
records no other way. It is never handed raw storage shapes, and nothing
shuttles data to it around that edge.

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
  to its exact span is not also listed against every sibling span. The
  suppression is recorded as
  [coverage](investigation-model.md), scoped to the log record it concerns,
  and emitted in every response where a `TraceIdentity` would otherwise have
  appeared — including degraded ones; suppression accounting never waits on
  the budget.
- `ParentChild` is derived from the span's own parent field; it adds no fact
  the model did not already carry, which is exactly why it is structural.
- `ResourceContext` is the weakest relation worth carrying: shared resource
  identity is context, not causation. It exists so that "what else happened
  in this service" is answerable without a time window.
- `TemporalCoActivity` requires an explicit window supplied by the caller
  through the [Investigation](investigation-model.md); the runtime picks no
  window by default. The relation's `window` **field** records the parameter
  that was applied — evidence cites only model fields, and the window is a
  request parameter, not a model field. **Time semantics:** span time is the
  interval `[start, end)`; log-record time is its event time; data-point
  time is its `time`. Two records are **overlapping** when their intervals
  intersect, and **proximate** when the distance between their time values
  (interval edges for spans) is ≤ the window. Both count as co-activity
  within the window; which one held is part of the relation's evidence
  (the cited times make it derivable).
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
and adds no facts. Traversing `A → B → C` is the
[Investigation API](investigation-model.md) composing two single-hop reads —
two relations, each produced and evidenced on its own — never a synthesised
`A → C` relation: **correlation produces no second-order relations.**

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
  layer-api flow parameters per [query-model.md](query-model.md); when the
  bound is reached, the Correlation Engine **degrades truthfully** — it
  returns the relations found, names the depth or count at which it stopped,
  and reports it through coverage — rather than refusing the whole read or
  growing unbounded. (`max_relations` is a traversal bound, so the outcome
  is degradation; refusing is for aggregation-shaped work —
  [query-model.md](query-model.md).)

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
2. Same resident set + same request parameters + same strategy versions ⇒
   identical relations, in identical order.
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

**Contracts pinned (M0), engine committed (issue #28).** The taxonomy,
evidence rules and invariants above are the contract the Phase 2
Correlation Engine implements. The engine crate now ships the committed
strategies — span identity, trace identity, temporal co-activity — wired
into the Investigation API's trace flow under the caller's budget. The
temporal strategy runs only under an explicit window the caller supplies
through the Investigation; without one the runtime picks no window and
the identity strategies run alone, the skip named in the coverage. The
remaining taxonomy (parent/child, resource context, exemplar attachment,
inferred) stays unstarted per [the roadmap](../roadmap/phases.md).
