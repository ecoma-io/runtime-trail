# Benchmarks

> How `runtime-trail`'s [resource targets](../architecture/runtime-constraints.md)
> become measured facts. This page owns the _measurement_ contract; the
> targets themselves live in runtime-constraints and are not restated here.

## Principle: targets are not measurements

[runtime-constraints.md](../architecture/runtime-constraints.md) publishes
engineering **targets** (idle RSS, workload RSS, startup, bounded
retention/query memory). Nothing on this page is evidence that a target is
met. A number only becomes a **result** when it is produced by a registered
scenario below, on a named machine, at a named commit — and until then the
only honest status is "not yet measured". A result becomes a **gated
regression check** when the case asserts its budget: the Phase-6 scenarios
below do, and CI runs the harness on every change
([roadmap](../roadmap/phases.md)). The Phase-1 cases remain advisory
evidence — they measure, they never assert.

## The harness

```text
scripts/bench/
  run-all.sh        # runs every registered case; fails loudly if none exist
  cases/            # one executable case per file (added by the phase that can support it)
  results/          # machine-written result records (git-ignored summaries, kept locally)
```

- `scripts/bench/run-all.sh` executes every `cases/*` script and reports each
  case's verdict. **With zero registered cases it exits non-zero** with an
  explanation — an empty harness must not read as a passing one.
- A case is a script that measures one thing against one target and prints a
  machine-readable record. Cases are added by the phase that ships the
  capability they measure, never before (a benchmark against scaffolding
  measures the scaffolding).
- Results are written to `scripts/bench/results/` with the commit SHA and
  machine description in the record. Summaries may be committed deliberately;
  raw runs are local artefacts (git-ignored).

## Registered cases (Phase 1: the memory path)

The [roadmap](../roadmap/phases.md) starts Phase 1 with the first real
measurement of the memory path — payload → decode → model conversion →
queue → storage. The three cases below execute the probe binaries of
`crates/bench-probes`, which compose exactly
the layers they measure (the OTLP ingestion pipeline, the bounded queue,
the in-memory store and the telemetry model — nothing else in the
workspace). Phase 1 **measures, it does not gate**: every case below
exits zero when it ran and produced its numbers, and non-zero only when it
could not measure. Each record carries the model's _accounted_ bytes next
to the kernel's `VmRSS`/`VmHWM` readings so the real-to-accounted margin
is computable from the record alone — the margin itself is never asserted
here.

- `ingest-overload` — repeated maximum-size legal OTLP gauge exports
  (at-cap point counts, attribute-heavy resources) through the real
  pipeline into the real store at the contract ceilings, until the store's
  accounted ceiling holds under the pump and then `QueueSaturated`
  persists without one. Records residency, eviction and saturation
  counters beside RSS.
- `idle-rss` — the same composition, constructed and left alone: the
  settled resident set of queue + pipeline + store with no traffic. (The
  composition mirrors the crates' own construction APIs; the served
  runtime's idle RSS is the Phase-6 `startup` case, which leaves the real
  server binary at rest and reads its settled `VmRSS`.)
- `retention-bound` — the retention law alone: a small, stated store
  configuration filled past its ceilings, then held under continued load
  while RSS is sampled across the steady state. Plateau samples, eviction
  counters and residency are recorded; the spread of the plateau is the
  evidence, not a passed threshold.

## Registered cases (Phase 6: the served runtime)

Phase 6's scenarios ([roadmap](../roadmap/phases.md)) measure the **served
runtime** — the real `runtime-trail-server` binary, launched by the case
script on a loopback port, driven over OTLP/HTTP and the Investigation API,
and read through `/proc/<pid>/status`. Budgets come from
[runtime-constraints.md](../architecture/runtime-constraints.md) and are
asserted by the case itself: a violation exits non-zero with the target and
the measured value in the message, which is what makes the CI `bench` job a
gate rather than a report.

- `startup` — cold start of the real server binary to its first 200
  `/healthz`: wall time from spawn to answer, then the settled `VmRSS`
  once the health probe has been served. Asserts the startup target
  (< 1 s).
- `workload-rss` — a scripted investigation session against the served
  runtime: an OTLP trace of a few hundred spans, its related logs and a
  handful of metrics posted over HTTP, then the investigation surface's
  subject request read back. Records the server's `VmRSS` during the pump
  and after settle. Asserts the typical-workload RSS target (< 100 MiB).
- `query-budget` — an investigation forced past its chain budget: the
  served surface must answer truthfully (a refusal, or a degraded 200
  naming the dimension and the limit) and keep answering, and the server's
  RSS must not grow under the storm. Asserts a truthful answer on every
  storm request, zero fabricated-complete 200s, and no RSS growth beyond a
  small noise margin.

### Records, machine-attached

Measured locally with `scripts/bench/run-all.sh` on the tree of commit
`c6a9d26`, machine `Linux x86_64, Debian GNU/Linux forky/sid` (kernel
`7.1.8+deb14-amd64`, Intel Core i7-10700K). Every raw run records its own
commit and machine in its header; the table quotes that one named run.

| case           | measured                                                                                      | budget (runtime-constraints.md) |
| -------------- | --------------------------------------------------------------------------------------------- | ------------------------------- |
| `startup`      | 46 ms to first 200 `/healthz`; settled `VmRSS` 6,252 KiB                                      | < 1 s to serving                |
| `workload-rss` | settled `VmRSS` 8,188 KiB (HWM 9,232 KiB) under 250 spans, 50 logs, 16 points                 | < 100 MiB typical session       |
| `query-budget` | `VmRSS` 35,164 → 35,200 KiB across the storm (+36 KiB); 20/20 truthful, 0 fabricated-complete | bounded: refuse-don't-grow      |

CI runner variance (query-budget): the case's noise margin is measurement
tolerance, not the budget — the budget is "bounded: refuse-don't-grow" and
carries no KiB number ([runtime-constraints.md](../architecture/runtime-constraints.md)).
Two margin-exceeding runs on the **identical tree** (no code delta, both
green on rerun / in earlier queue runs), GitHub Actions `ubuntu-latest`:

| run                                                                               | context                                         | `VmRSS` growth across the storm |
| --------------------------------------------------------------------------------- | ----------------------------------------------- | ------------------------------- |
| [35467768518](https://github.com/ecoma-io/runtime-trail/actions/runs/35467768518) | release PR #7, 2026-09-19T20:33Z                | 6,444 KiB                       |
| [35468332667](https://github.com/ecoma-io/runtime-trail/actions/runs/35468332667) | merge-group input `a3c3d7de`, 2026-09-19T20:45Z | 5,428 KiB                       |

Both exceeded the then-5 MiB margin by 308–1,324 KiB; the margin is 8 MiB
since 2026-09-19 ([issue #74](https://github.com/ecoma-io/runtime-trail/issues/74))
so allocator/runner noise on shared runners does not flake the gate. The
local named run above measures +36 KiB on the same tree.

## What CI does with benchmarks

CI runs the harness on every pull request and merge in the `bench` job
(`.github/workflows/ci.yml`), which is part of the required `ci-gate`. The
Phase-6 cases assert their budgets, so the job fails when a budget breaks:

```sh
scripts/bench/run-all.sh   # served-runtime cases assert; Phase-1 cases measure only
```

The Phase-1 cases above still never assert — their records remain evidence
produced by the harness and checked by hand. A budget stops being a target
the day a scenario asserts it under this job.
