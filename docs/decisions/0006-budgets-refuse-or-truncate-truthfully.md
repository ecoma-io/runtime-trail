# 0006: Budgets refuse or truncate truthfully; they never rewrite emitter data

## Status

Accepted (2026-09-12, M0 architecture contract hardening)

## Context

`runtime-trail` makes two promises that collide under load. The first is
faithfulness: the [telemetry model](../architecture/telemetry-model.md)
preserves what emitters sent, verbatim — rule 1. The second is boundedness:
[runtime-constraints.md](../architecture/runtime-constraints.md) bounds
retention, queues and query memory with hard ceilings. The collision: a span
with ten thousand attributes cannot be both faithfully kept and bounded.

The runtime answers questions with numbers — an aggregation (a percentile, a
count) is an answer an investigator acts on. A partial aggregate returned as
if complete is a false number, and "the answer is bounded" is not honesty if
the honesty is not inspectable.

## Decision

1. **Model-level budgets are admission gates, not mutation triggers.** A
   record over its caps is refused at admission — retryable backpressure or
   an OTLP `partial_success` naming the budget
   (wire signals in
   [runtime-constraints.md](../architecture/runtime-constraints.md)) — never
   truncated into something the emitter did not send.
   Everything admitted is complete and immutable.
2. **Query budgets refuse or degrade, per dimension** — traversal-shaped
   work truncates truthfully (subset of the true set answer, truncation
   point named, cursor or coverage); aggregation-shaped work refuses
   (a partial aggregate is a false number; a false number is worse than no
   number). The per-dimension policy is
   [query-model.md](../architecture/query-model.md)'s to own.
3. **Every refusal and every truncation is observable.** A refusal names the
   dimension, the limit and the observed spend. A truncation names its
   truncation point and carries a cursor/coverage entry. The
   [Investigation envelope](../architecture/investigation-model.md) makes
   coverage machine-checkable in its `limits` part.
4. **Post-admission shrinking is eviction only, and eviction is observable
   twice** — at the store (dropped-record counters) and in every response
   whose result set it touched (coverage). Silent shrinkage does not exist.
5. A driver or layer that cannot honour a budget honestly says so through
   coverage ("cannot produce a faithful count") rather than redefining the
   [scan unit](../architecture/query-model.md).

## Consequences

- The investigator can always tell whose loss it was: emitter-side loss
  (preserved dropped-counts), admission refusal (budget error), or eviction
  (coverage + counters). Nothing shrinks without a name on it.
- Storage eviction is the only force that shrinks data after admission; no
  query or surface may shrink what storage keeps.
- Both storage drivers implement the refuse-or-degrade law identically —
  mode symmetry includes budget law, per
  [ADR 0003](0003-storage-strategy.md).
- Phase 2 engines must implement per-dimension policy exactly; "we truncate
  everything a bit" is not an implementation of this decision.
