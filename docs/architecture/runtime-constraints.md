# Runtime constraints

> Authoritative home for resource budgets and the backpressure architecture.
> These are **engineering targets**: commitments the architecture is built to
> honour, not measured results. What has actually been measured lives in
> [../benchmarks/README.md](../benchmarks/README.md) — the two are kept apart
> on purpose. All memory quantities in this document are MiB (2^20 bytes).

## Why budgets are architecture here

Low resource use is a product requirement ([scope](../product/scope.md)), not
a tuning pass. A local investigation tool that silently eats a laptop's RAM is
failing at its one job, no matter how good the UI is. So the budgets below are
named in the architecture, the storage and query designs must honour them by
construction ([storage-model.md](storage-model.md),
[query-model.md](query-model.md),
[investigation-model.md](investigation-model.md)), and the benchmark harness
exists from the foundation commit onward so they become measured, gated facts
rather than vibes.

## Engineering targets

| Budget                     | Target                               | Notes                                                                                                                                                         |
| -------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Idle RSS                   | **< 50 MiB**                         | runtime started, nothing ingested, UI closed                                                                                                                  |
| Typical local workload RSS | **< 100 MiB**                        | a typical session below the retention ceilings — the ceilings are the bound, not the typical state                                                            |
| Startup to serving         | **< 1 s**                            | cold start to health endpoint answering                                                                                                                       |
| External database          | **none**                             | zero install, zero migration step, every mode                                                                                                                 |
| Deployment floor           | **single core / small VPS / laptop** | no multi-node anything                                                                                                                                        |
| Telemetry retention        | **bounded**                          | explicit ceilings (below) in every mode                                                                                                                       |
| Query memory               | **bounded**                          | every query carries a [budget](query-model.md); refuse-don't-grow                                                                                             |
| Ingestion under overload   | **backpressure**                     | signal, never buffer-without-bound, never block on persistence                                                                                                |
| Overload steady-state RSS  | **bounded, not fixed**               | sustained saturation holds within retention ceilings + fixed overhead; never unbounded. The [retention-bound case](../benchmarks/README.md) asserts the bound |

The RSS targets describe typical operation; the retention ceilings are the
hard bound a long or overloaded session grows toward. A session that reaches
its ceilings is legal and signalled — its RSS is bounded by the ceilings, not
by the typical-workload number.

### Numeric limits (targets)

These are the concrete numbers the targets table names. They are
**startup-configurable targets**: the architecture owns these defaults, an
operator may tune them at startup only — never mid-session — and changing a
default is an architecture change (the PR states its effect on the table
above).

| Limit                                       | Default                                             | Applies to                                                                                                                                                                                                                                                          |
| ------------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| OTLP payload ceiling                        | 4 MiB                                               | one OTLP export request; rejected non-retryably at the transport edge                                                                                                                                                                                               |
| Attributes per signal                       | ≤ 256                                               | per span, log record, data point, resource, scope                                                                                                                                                                                                                   |
| Attributes per span event, link or exemplar | ≤ 64                                                | each carries its own attribute set                                                                                                                                                                                                                                  |
| Attribute value size                        | ≤ 4 KiB                                             | one value's accounted size ([model](telemetry-model.md) accounting — key, entry overhead and payload all count)                                                                                                                                                     |
| Events / links per span                     | ≤ 128 / ≤ 32                                        | span events; links per span                                                                                                                                                                                                                                         |
| Exemplars per data point                    | ≤ 4                                                 | metric exemplars                                                                                                                                                                                                                                                    |
| Key-value list depth                        | ≤ 8                                                 | every container nesting a record carries — key-value lists and arrays alike, in attribute values, log bodies, exemplar filtered attributes, metric metadata and event/link attributes                                                                               |
| Data points per export                      | ≤ 10,000                                            | one export; overflow rejects the whole export — **non-retryable** (a payload property; retrying cannot shrink it)                                                                                                                                                   |
| Series cap (active)                         | ≤ 100,000                                           | distinct resident streams; enforced by the store at keep time ([mechanism](storage-model.md)); refusal names the cap and is non-retryable, with an observable counter; a slot frees when the stream's last point is [evicted](storage-model.md)                     |
| In-flight per queue                         | ≤ 64 MiB accounted                                  | each bounded hand-off queue ([backpressure](#the-backpressure-architecture)); overflow = reject the producer — the one transient, retryable signal                                                                                                                  |
| In-flight request body (transport edge)     | ≤ 64 MiB                                            | aggregate bytes being buffered at the OTLP transports before admission ([ADR 0010](../decisions/0010-transport-edge-in-flight-body-budget.md)); overflow = reject the producer (429 / gRPC `RESOURCE_EXHAUSTED`) — transient, retryable, alongside queue saturation |
| Request body read timeout                   | ≤ 10 s                                              | one OTLP body arriving over either transport; a stuck/drip client is refused (408 / gRPC `DEADLINE_EXCEEDED`) so no single body holds its buffered bytes indefinitely                                                                                               |
| Memory-mode retention ceilings              | 2,000,000 records · 256 MiB accounted · 24 h window | eviction per the [retention law](storage-model.md); first ceiling hit wins                                                                                                                                                                                          |
| File-mode retention ceilings                | 5,000,000 records · 1 GiB accounted · 7 d window    | same law, durable                                                                                                                                                                                                                                                   |
| Drain deadline                              | ≤ 5 s                                               | SIGTERM: bounds shutdown **latency**, not completeness — in-flight drains best-effort; whatever misses is dropped observably                                                                                                                                        |

Byte-bounded queues are why these numbers compose: a queue holds at most
64 MiB regardless of how large any single legal record is, so in-flight
memory is bounded by the queue ceilings — not by a record count multiplied by
a worst case.

The retention ceilings bound **accounted** bytes, and accounted size
deliberately over-counts record content ([telemetry-model.md](telemetry-model.md))
— so real content residency sits under the ceiling by design, not by luck.
The byte ceiling also charges each distinct resident stream's identity
exactly once ([storage-model.md](storage-model.md)), so identity content
cannot hide behind the per-record formula — and a stream identity whose
accounted size alone exceeds the ceiling is refused at keep time,
non-retryable and never evicting into
([mechanism](storage-model.md)), the series-cap refusal's sibling. Every
refusal that inserts nothing is also a hook delivery: the store's hook
carries three kinds — evicted record, stream release, keep refusal — and
the composition root ends the refused record's ledger identity through it
exactly as for eviction
([ADR 0008](../decisions/0008-admission-ledger-design.md)), so the ledger's
overhead tracks what residency and the refusals actually hold. Evictions,
keep refusals and their hook deliveries are separately counted, so a
delivery that does not complete is observable, never silent. On top
of the ceiling stand the admission ledger's per-record entries
([ADR 0008](../decisions/0008-admission-ledger-design.md) — bounded overhead
per retained record, sharing ownership with the record it pins) and the
runtime's fixed overhead; how large the real-to-accounted margin actually is
is a measurement for [../benchmarks/README.md](../benchmarks/README.md) once
Phase 1 lands — never an assertion made here.

## The backpressure architecture

Overload is a designed-for state, not later hardening:

1. **Admission control** — ingestion bounds its in-flight work. The wire
   behaviour is contracted, not implied, and each signal names what kind of
   problem it is:
   - saturated queue → **HTTP 429 + `Retry-After`** (gRPC
     `RESOURCE_EXHAUSTED`). This is retryable outright: saturation is
     transient, so a retry can succeed.
   - aggregate in-flight body budget exceeded → **HTTP 429 + `Retry-After`**
     (gRPC `RESOURCE_EXHAUSTED`) — the same retryable answer, for the
     transport edge ([ADR 0010](../decisions/0010-transport-edge-in-flight-body-budget.md)).
     Both 429s are the transient-overload family: the budget frees as bodies
     complete, so a retry can succeed.
   - a request body that never arrives in time → **HTTP 408** (gRPC
     `DEADLINE_EXCEEDED`) — the read timeout, a non-retryable transport
     refusal: the payload never arrived complete and a retry of the same slow
     body is not the answer.
   - per-signal cap violation → OTLP **`partial_success`** naming the
     rejected records where the transport allows it, otherwise a
     **non-retryable** export reject. Never 429: the payload is the problem,
     and retrying an over-cap payload cannot shrink it.
   - draining (SIGTERM received) → **HTTP 503 / gRPC `UNAVAILABLE`** — an
     explicit closing signal. Spec-conformant emitters may retry it; the
     runtime keeps answering until shutdown completes and admits nothing
     new. What it prevents is an emitter hanging on a silent connection,
     not retry.
   - over-ceiling payload → non-retryable reject at the transport edge,
     before the payload is parsed.
2. **Bounded queues everywhere** — every hand-off (ingestion → store, query →
   storage) runs through a queue bounded in accounted bytes (table above)
   with a defined overflow policy: overflow **rejects the producer** —
   drop-oldest exists only in retention eviction, never in queues.
3. **Refuse or degrade, never grow** — a query that cannot be answered within
   its budget either returns a truthfully-truncated answer or fails with a
   budget error naming the dimension, limit and spend
   ([query-model.md](query-model.md) owns the policy); correctness of the
   refusal beats an OOM.
4. **Persistence never blocks admission** —
   [storage-model.md](storage-model.md) owns this rule; restated here because
   it is a resource guarantee: the hot path never waits on durable I/O.

### The transport-edge gate

The queue ceilings bound what the pipeline holds _after_ admission; the
transport edge bounds what the sockets buffer _before_ admission
([ADR 0010](../decisions/0010-transport-edge-in-flight-body-budget.md)). An
overload answers honestly at whichever edge it hits first: the aggregate
in-flight body budget (429 / gRPC `RESOURCE_EXHAUSTED`), the per-body read
timeout (408 / gRPC `DEADLINE_EXCEEDED`), the queue ceiling (429 /
`RESOURCE_EXHAUSTED`), or a retention ceiling. Both transport edges and the
queue share the retryable-transient 429 family; the wire shapes stay the
contracted ones below.

### Ownership law

Resource numbers live in the table above — one home, no scattered
duplicates. Decision records may restate a number as context; the table is
normative. Contract defaults that are not resource budgets (the shape of a
query budget, relation caps like `max_hops`) live in the documents that own
those contracts. The **mechanism** that enforces each number lives in the
document that owns the concern
([telemetry-model.md](telemetry-model.md) for admission gates, per-signal
caps and accounted size; [storage-model.md](storage-model.md) for retention
and eviction law; [query-model.md](query-model.md) for query budgets;
[system.md](system.md) for lifecycle and drain); measurements live in
[../benchmarks/README.md](../benchmarks/README.md) — the two are kept apart
on purpose. Memory accounting for byte ceilings is the
[model's accounted size](telemetry-model.md).

## Enforcement trajectory

- **Now (foundation):** budgets are documented here; the benchmark harness
  structure exists ([../benchmarks/README.md](../benchmarks/README.md)); the
  skeleton is small enough to inspect by hand.
- **Phase 6:** published scenarios measure RSS, startup and query behaviour
  against these targets, and CI gates regressions on the measurements
  ([../roadmap/phases.md](../roadmap/phases.md)).
- **Always:** any change that moves ingestion, storage or query architecture
  states its effect on these budgets in the PR description. "Unknown" is an
  acceptable answer; "unconsidered" is not.

Adding a heavyweight dependency to the core (a browser runtime, an external
database client, a telemetry pipeline engine) is an architecture decision
requiring a record in [../decisions/](../decisions/) precisely because these
budgets exist.
