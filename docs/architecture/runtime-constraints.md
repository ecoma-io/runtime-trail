# Runtime constraints

> Authoritative home for resource budgets and the backpressure architecture.
> These are **engineering targets**: commitments the architecture is built to
> honour, not measured results. What has actually been measured lives in
> [../benchmarks/README.md](../benchmarks/README.md) — the two are kept apart
> on purpose.

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

| Budget                     | Target                               | Notes                                                                    |
| -------------------------- | ------------------------------------ | ------------------------------------------------------------------------ |
| Idle RSS                   | **< 50 MB**                          | runtime started, nothing ingested, UI closed                             |
| Typical local workload RSS | **< 100 MB**                         | one developer's services emitting during an active investigation session |
| Startup to serving         | **< 1 s**                            | cold start to health endpoint answering                                  |
| External database          | **none**                             | zero install, zero migration step, every mode                            |
| Deployment floor           | **single core / small VPS / laptop** | no multi-node anything                                                   |
| Telemetry retention        | **bounded**                          | explicit ceilings (below) in every mode                                  |
| Query memory               | **bounded**                          | every query carries a [budget](query-model.md); refuse-don't-grow        |
| Ingestion under overload   | **backpressure**                     | signal, never buffer-without-bound, never block on persistence           |
| Overload steady-state RSS  | **< 150 MB**                         | sustained saturation; queue + retention ceilings hold (below)            |

### Numeric limits (targets)

These are the concrete numbers the targets table names. They are
**startup-configurable targets**: the architecture owns these defaults, an
operator may tune them at startup only — never mid-session — and changing a
default is an architecture change (the PR states its effect on the table
above).

| Limit                              | Default                                             | Applies to                                                                    |
| ---------------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------- |
| OTLP payload ceiling               | 4 MiB                                               | one OTLP export request; rejected non-retryably at the transport edge         |
| Attributes per signal              | ≤ 256                                               | per span, log record, data point, resource                                    |
| Attribute value size               | ≤ 4 KiB                                             | one attribute value's accounted size ([model](telemetry-model.md) accounting) |
| Events per span                    | ≤ 128                                               | span events; links ≤ 32 per span                                              |
| Key-value list depth               | ≤ 8                                                 | nested kvlist inside attribute values                                         |
| Data points per export             | ≤ 10,000                                            | one export; overflow rejects the whole export (retryable)                     |
| Series cap (active)                | ≤ 100,000                                           | per runtime session; new series rejected, never evicted, counter observable   |
| In-flight records (hand-off queue) | ≤ 10,000                                            | each bounded queue; overflow = reject the producer (retryable backpressure)   |
| Memory-mode retention ceilings     | 2,000,000 records · 512 MiB accounted · 24 h window | eviction of oldest; first ceiling hit wins                                    |
| File-mode retention ceilings       | 5,000,000 records · 1 GiB accounted · 7 d window    | same eviction law, durable                                                    |
| Drain deadline                     | ≤ 5 s                                               | SIGTERM: admitted in-flight drains to store; past deadline dropped observably |

## The backpressure architecture

Overload is a designed-for state, not later hardening:

1. **Admission control** — ingestion bounds its in-flight work. The wire
   behaviour is contracted, not implied:
   - saturated admission → **HTTP 429 + `Retry-After`** (gRPC
     `RESOURCE_EXHAUSTED`);
   - per-signal cap rejection → OTLP **`partial_success`** naming the
     rejected records where the transport allows it — otherwise the export
     is rejected non-retryably;
   - draining (SIGTERM received) → **HTTP 503 / gRPC `UNAVAILABLE`**
     immediately, so emitters fail over instead of retrying into a closing
     runtime;
   - over-ceiling payload → non-retryable reject at the transport edge,
     before the payload is parsed.
2. **Bounded queues everywhere** — every hand-off (ingestion → store, query →
   storage) runs through a bounded queue with a defined overflow policy:
   overflow **rejects the producer** — drop-oldest exists only in retention
   eviction, never in queues. Queue bounds are in the numeric-limits table.
3. **Refuse or degrade, never grow** — a query that cannot be answered within
   its budget either returns a truthfully-truncated answer or fails with a
   budget error naming the dimension, limit and spend
   ([query-model.md](query-model.md) owns the policy); correctness of the
   refusal beats an OOM.
4. **Persistence never blocks admission** —
   [storage-model.md](storage-model.md) owns this rule; restated here because
   it is a resource guarantee: the hot path never waits on durable I/O.

## Ownership law

Numbers live here — one table, no scattered duplicates. The **mechanism** that
enforces each number lives in the document that owns the concern
([telemetry-model.md](telemetry-model.md) for admission gates, per-signal caps
and accounted size; [storage-model.md](storage-model.md) for retention and
eviction law; [query-model.md](query-model.md) for query budgets;
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
