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

The records flow admits **content filters** drawn from that same vocabulary:
service (resource identity), a half-open time range over the record's
kind-specific model timestamps, a minimum severity (log records — the filter
is unexpressible for spans and metric points), and scope identity (name and
version, exact). Filters are predicates over the record view: they never
reorder the total order and never enter a storage driver. A filtered-out
record was still examined — the scan charge stands and the deadline ran on
it — but it is never returned, never byte-charged, and never counted into an
omission. The cursor's query fingerprint binds the filter set, so a cursor
continues only under the filters that minted it.

## Every query carries a budget

Every query admits with a budget. A query without a budget is invalid — not
"unbudgeted", invalid. The budget has five dimensions:

| Dimension                | Meaning                                                                                   | On expiry (by the shape of the work in flight)                                                                                                                                                                           |
| ------------------------ | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `deadline`               | Monotonic duration captured **at admission**; the engine works against the remaining time | traversal degrades truthfully; aggregation refuses                                                                                                                                                                       |
| `max_results`            | Cap on entities returned                                                                  | degrade with a cursor for the rest                                                                                                                                                                                       |
| `max_bytes`              | Cap on the canonical encoding of the answer's evidence (below)                            | degrade with a cursor where the page has a continuation to offer; a first page that included nothing names the last-examined entity instead (invariant 4); the omission is named by count and position, never enumerated |
| `max_scan`               | Cap on scan work (below)                                                                  | traversal degrades with coverage (cursor where a total order exists); aggregation refuses                                                                                                                                |
| `max_aggregation_memory` | Cap on memory an aggregation may hold                                                     | refuse                                                                                                                                                                                                                   |

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

A budget belongs to **one query execution**, and a continuation page is a
new execution: the caller presents a fresh budget for the page, the engine
binds no budget to a chain, and a page never inherits an earlier page's
ceilings. Paging flows therefore pay per page and own their chain-level
limits themselves — summing a paging flow's per-page budgets is the
caller's arithmetic, never the engine's.
**Admission consumes the budget**:
the budget is spent exactly once, by value, into the session that enforces
it — it cannot be duplicated, re-admitted, or defaulted, so a second
execution is a second budget, explicitly declared.

## Scan work is driver-symmetric

The scan unit is **one entity examined** — one span, log record, metric
point, relation candidate, index probe counted as one entity. One unit has
the same meaning whether the resident set lives in memory or in SQLite:
driver symmetry per [ADR 0003](../decisions/0003-storage-strategy.md). A
driver may use an index and report fewer units examined; it may never widen
the meaning of the unit itself.

Scan accounting is the engine's, charged as it walks. A driver reports its
work only by yielding records through the ordered scan surface — it reports
no separate count for the engine to cross-check. The engine accounts each
examination against the budget itself (the scan charge stands whether or
not the record is returned, and a filtered-out record still consumed its
scan allowance), and a driver that cannot bound its own work within the
ceiling produces a budget error — the error remains a budget error; the
engine never exceeds the caller's ceiling by accepting made-up units. A
batched walk may pull up to a bounded batch ahead of examination; records
pulled but not examined at a stop consume scan allowance up to the
remaining ceiling at the moment of the stop; the engine never charges
fabricated units and never exceeds the ceiling. There is no driver-reported
count — the engine charges what it walks, and coverage names the answer's
own gaps and boundaries, not a driver's internal accounting. The one
driver-exactness fact coverage carries is the stall guard: a driver that
yields an empty page while claiming a successor cursor stops the walk, and
the stop is named, never spun on (below).

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

A **cursor** is opaque to users and callers. It encodes (position in the
total order, last entity id, query fingerprint, and the snapshot boundary
the continuation stays within). The position is the order key value — for
the records flow, the anchor record's admission-time nanoseconds — so
position and last entity id together reconstruct the anchor's admission
key, and a resume is exact under eviction, needing neither a re-walk nor
the anchor record's residency. The engine **rejects a cursor whose
embedded fingerprint differs from the query it is presented to** — a cursor
belongs to one query's result set (same shape and parameters), and
presenting it anywhere else is an error. The fingerprint tracks the query,
not residency; the snapshot boundary travels in the cursor, so a stateless
surface can hand back the continuation and the engine can bound it at the
first page's residency frontier — an admission key in the storage
contract's residency order ([storage-model.md](storage-model.md)) — and
gaps from later eviction are coverage's job.

Cursor bytes are not authenticated: beyond canonical decoding, the
fingerprint is the only validity test, so a hand-altered cursor that still
decodes continues a view its page never minted. The runtime's local-trust
posture covers this — the caller is the operator's own process on this
machine; a multi-tenant surface would need an authenticator.

When [eviction](storage-model.md) has removed records the cursor points
into, the response still returns what remains resident, ordered as before,
and names the gap in coverage: an evicted record is a hole in the result
with a name, never a silent skip.

A cursor continues **within the snapshot its first page evaluated**: the
result set is fixed at that page's admission, and records admitted after it
are outside every later page of the same continuation — a declared boundary
(the snapshot point is part of coverage), not a silent skip. A caller
wanting newer data issues a new query.

A driver that yields an empty page while claiming a successor cursor is a
stall, not progress: after three consecutive empty continuations the engine
stops the walk, keeps the coverage it already presented, mints no further
cursor, and names where it stopped — `PartOutcome::Stalled` as the part
outcome and `CoverageEntry::DriverStall { after }` in coverage, naming the
last examined record, or the driver's own successor-cursor anchor when
nothing was examined. A conforming driver never produces a stall; the entry
exists so a violated storage contract is named, never silently spun on.

Pagination never discards the order for speed; there is no
"fast approximate page N".

## Snapshot leases

An investigation pins its pagination to **one snapshot** with a
**snapshot lease**: a token naming the investigation and the fingerprint
of the snapshot boundary its first page minted. The fingerprint is
computed over the **canonical boundary** — the same bytes the cursor
encodes as its snapshot half — never over the opaque cursor bytes, so the
lease and the cursor speak identical truth. The compose flow presents the
lease with every continuation page, and the engine rejects any page whose
continuation's snapshot fingerprint differs from the lease's, before any
budget work.

A lease belongs to one investigation and one snapshot. It cannot be
cloned or re-targeted: presentation is on the original token, so a lease
from one investigation can never page another investigation's snapshot,
and a cursor cannot be replayed under a foreign lease. Drift after
issuance keeps its force: records admitted past the leased boundary stay
outside the leased view — the continuation answers exactly the snapshot it
was leased over, and the boundary is named in coverage.

Like cursor bytes, a lease is not authenticated: the fingerprint is its
only validity test, under the runtime's local-trust posture. What the
lease adds is **separation**: two investigations paging the same store
cannot trip over each other's continuations, because each page carries the
lease of the investigation that minted it.

## Refuse or degrade

Every budget expiry is one of exactly two honest outcomes:

- **Degrade** — return a truncated answer that is true: the records
  returned are a subset of the true answer set, the truncation point is
  named, and a cursor, a coverage entry, or the last-examined entity says
  what was not visited. Allowed
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
not for slowness. "Could have degraded truthfully" is the operative qualifier:
a traversal that **cannot** name a truncation point because it examined
**nothing** — a continuation handed an already-spent deadline before any new
examination — has no truth to degrade to. Echoing the presented cursor would
name no truncation point (invariant 4) and would be indistinguishable from
progress. Such a request refuses with a budget error naming the deadline,
its limit and the observed spend, exactly as a first page dead before any
examination does. A page that examined anything and then expired still
degrades with a cursor, per the table.

## Who enforces what

- The **Query Engine enforces** every dimension; storage drivers report
  their work in the scan unit by yielding records, and the ceiling is the
  engine's, not the driver's.
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
4. Traversal truncation always names its truncation point (a cursor, a
   coverage entry, or the last-examined entity); aggregation never returns
   a partial aggregate as complete.
5. Scan accounting means the same thing in both storage modes.
6. A refused query names the dimension, the limit and the observed spend.
7. No query path ever writes to disk.
8. Identical query + identical resident set + identical budget, evaluated
   by the same driver ⇒ identical first page. Cross-driver page content is
   not promised (a driver may use an index — see scan accounting);
   cross-mode equality is the [envelope's shape](investigation-model.md),
   not page bytes.
9. A cursor page presented under a snapshot lease continues only within
   the leased snapshot; a lease from one investigation cannot page
   another's.

## Status

**Contracts pinned (M0); the records flow implemented (Phase 2, in
progress).** The five-dimension budget, scan accounting, deadline
semantics, ordering/cursor rules and refuse-or-degrade policy are the
contract the Phase 2 Query Engine implements; the budgeted records flow is
implemented in the Query Engine crate and adversarially reviewed. The
remaining flows compose at the Investigation API in a later milestone of
the phase per [the roadmap](../roadmap/phases.md); [the benchmarks
README](../benchmarks/README.md) will hold the measurements that prove the
ceilings honoured in practice.
Continuations can be pinned per investigation with **snapshot leases**;
the records flow implements the lease-checked entry.
