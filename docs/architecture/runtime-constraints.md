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
| Telemetry retention        | **bounded**                          | explicit ceilings (bytes, records, window) in every mode                 |
| Query memory               | **bounded**                          | every query carries a budget; refuse-don't-grow                          |
| Ingestion under overload   | **backpressure**                     | signal, never buffer-without-bound, never block on persistence           |

## The backpressure architecture

Overload is a designed-for state, not later hardening:

1. **Admission control** — ingestion bounds its in-flight work; emitters see
   standard OTLP retryable failure when admission is saturated.
2. **Bounded queues everywhere** — every hand-off (ingestion → store, query →
   storage) runs through a bounded queue with a defined overflow policy.
3. **Refuse, don't grow** — a query that cannot be answered within its budget
   fails with a budget error, naming the budget. Correctness of the refusal
   beats an OOM.
4. **Persistence never blocks admission** —
   [storage-model.md](storage-model.md) owns this rule; restated here because
   it is a resource guarantee: the hot path never waits on durable I/O.

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
