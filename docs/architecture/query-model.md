# Query model

> Authoritative home for the Query Engine's contract: what a query is, the
> budget every query carries, ordering, cursors and pagination, and the
> refuse-or-degrade policy. Owned here; the numeric values of the resource
> targets these budgets honour live in
> [runtime-constraints.md](runtime-constraints.md); the investigation flows
> that compose queries and decompose budgets into query and correlation
> shares live in [investigation-model.md](investigation-model.md).

## What a query is

A query is a named [Investigation flow](investigation-model.md) executed by
the Query Engine over the [resident telemetry](storage-model.md). Its
vocabulary is the [model's](telemetry-model.md) vocabulary — entity ids,
signal kinds, resource and scope identity, severity, metric streams — never
storage concepts: no table, no index, no driver type appears in a query or
its result. That is what makes one contract serve both
[storage modes](storage-model.md).

## Every query carries a budget

Every query admits with a budget. A query without a budget is invalid — not
"unbudgeted", invalid. The budget has five dimensions:

| Dimension                | Meaning                                                                                   | On expiry (by the shape of the work in flight)                                                    |
| ------------------------ | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `deadline`               | Monotonic duration captured **at admission**; the engine works against the remaining time | traversal degrades truthfully; aggregation refuses                                                |
| `max_results`            | Cap on entities returned                                                                  | degrade with a cursor for the rest                                                                |
| `max_bytes`              | Cap on the canonical encoding of the answer's evidence (below)                            | degrade with a cursor for the rest; the omission is named by count and position, never enumerated |
| `max_scan`               | Cap on scan work (below)                                                                  | traversal degrades with coverage (cursor where a total order exists); aggregation refuses         |
| `max_aggregation_memory` | Cap on memory an aggregation may hold                                                     | refuse                                                                                            |

**Every expiry follows the shape of the work in flight when the dimension
expired** — traversal-shaped work degrades truthfully, aggregation-shaped
work refuses ([Refuse or degrade](#refuse-or-degrade)); the table names each
dimension's default shape. A query that mixes shapes expires per part: each
part of the answer follows its own shape's rule, and the
[envelope](investigation-model.md) reports each part's outcome (complete /
degraded / refused) in its execution block. A flow fails outright only when
its subject itself cannot be resolved within the budget — never merely
because one aggregating part expired.

`max_bytes` bounds the **evidence** part of the answer — the record views'
canonical encoding, not any transport's encoding (the API names its own
canonical byte accounting so neither the UI nor MCP can move the goalposts
by choosing an encoding). The envelope's execution, coverage and limits
parts are the answer's truth-telling and sit **outside** `max_bytes`; they
are themselves bounded by a small fixed allowance, so honesty never crowds
out data and data never crowds out honesty.

## Scan work is driver-symmetric

The scan unit is **one entity examined** — one span, log record, metric
point, relation candidate, index probe counted as one entity. One unit has
the same meaning whether the resident set lives in memory or in SQLite:
driver symmetry per [ADR 0003](../decisions/0003-storage-strategy.md). A
driver may use an index and report fewer units examined; it may never widen
the meaning of the unit itself.

A driver may never inflate accounting either: the engine cross-checks the
reported count against the budget, and a driver that cannot produce a
faithful count reports its in-exactness through coverage. A driver that
cannot bound its own work within the ceiling produces a budget error — the
error remains a budget error; the engine never exceeds the caller's ceiling
by accepting made-up units.

## Deadlines

The `deadline` dimension is a monotonic duration captured **at admission**
— not a wall-clock expiry. The engine works against the remaining time of a
monotonic reading taken when the query admitted; a wall-clock deadline
would let a paused VM or a long page-in eat the budget invisibly.

## Ordering, cursors and pagination

Every result is totally ordered. The order is deterministic across
identical queries — same resident set, same query, same order — and every
tie in the order keys is broken by the record's
[entity id](telemetry-model.md). Without a total order there is no stable
pagination, and an investigator paging through results would see records
appear twice or vanish between pages.

A **cursor** is opaque to callers. It encodes (position in the total order,
last entity id, query fingerprint). The engine **rejects a cursor whose
embedded fingerprint differs from the query it is presented to** — a cursor
belongs to one query's result set (same shape and parameters), and
presenting it anywhere else is an error. The fingerprint tracks the query,
not residency; gaps from later eviction are coverage's job.

When [eviction](storage-model.md) has removed records the cursor points
into, the response still returns what remains resident, ordered as before,
and names the gap in coverage: an evicted record is a hole in the result
with a name, never a silent skip.

A cursor continues **within the snapshot its first page evaluated**: the
result set is fixed at that page's admission, and records admitted after it
are outside every later page of the same continuation — a declared boundary
(the snapshot point is part of coverage), not a silent skip. A caller
wanting newer data issues a new query.

Pagination never discards the order for speed; there is no
"fast approximate page N".

## Refuse or degrade

Every budget expiry is one of exactly two honest outcomes:

- **Degrade** — return a truncated answer that is true: the records
  returned are a subset of the true answer set, the truncation point is
  named, and a cursor or coverage entry says what was not visited. Allowed
  for traversal-shaped work (search, scan, relation traversal) because a
  subset of a set answer is still a true set answer.
- **Refuse** — fail with a budget error naming the dimension, the limit and
  the observed spend. Required for aggregation-shaped work (percentiles,
  averages, counts over more data than was actually aggregated): a partial
  aggregate would be a false number, and a false number is worse than no
  number.

The choice is per dimension and pinned in the table above. The engine never
returns a partial aggregate as if it were complete, and never refuses a
traversal that could have degraded truthfully — refusals are for wrongness,
not for slowness.

## Who enforces what

- The **Query Engine enforces** every dimension; storage drivers report
  their work in the scan unit and may be cross-checked, but the ceiling is
  the engine's, not the driver's.
- The **caller sets** the budget; flow-scoped defaults for the committed
  investigation flows are owned by layer-api, and the same defaults serve
  both [surfaces](mcp-model.md) — the UI and MCP ask for the same flows,
  so neither surface gets a fatter default. Surface-specific overrides are
  flow parameters, not new defaults.
- **Correlation** spends a share of the flow budget, decomposed by
  layer-api into a scan-work share and the correlation flow parameters
  (`max_hops`, `max_relations` — caps of the
  [correlation contract](correlation-model.md), not budget dimensions).
- **No disk spill, ever.** A query that exceeds `max_aggregation_memory` is
  refused; it never spills to disk to keep going. (Consistent with the
  runtime's zero-external-services posture and
  [ADR 0003](../decisions/0003-storage-strategy.md) — spill would
  make the query engine a batch system.)

## Invariants

1. Every query admits with a budget; a budgetless query is invalid, and the
   engine is the budget's only enforcer.
2. Ordering is total and deterministic; ties break by entity id.
3. A cursor is valid only for its query fingerprint, and gaps under
   eviction are named in coverage.
4. Traversal truncation always names its truncation point (cursor or
   coverage); aggregation never returns a partial aggregate as complete.
5. Scan accounting means the same thing in both storage modes.
6. A refused query names the dimension, the limit and the observed spend.
7. No query path ever writes to disk.
8. Identical query + identical resident set + identical budget, evaluated
   by the same driver ⇒ identical first page. Cross-driver page content is
   not promised (a driver may use an index — see scan accounting);
   cross-mode equality is the [envelope's shape](investigation-model.md),
   not page bytes.

## Status

**Contracts pinned (M0).** The five-dimension budget, scan accounting,
deadline semantics, ordering/cursor rules and refuse-or-degrade policy are
the contract the Phase 2 Query Engine implements. The Query Engine crate
remains scaffolding until then per [the roadmap](../roadmap/phases.md);
[the benchmarks README](../benchmarks/README.md) will hold the measurements
that later prove the ceilings honoured in practice.
