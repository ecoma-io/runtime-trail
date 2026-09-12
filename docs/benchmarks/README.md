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
regression check** only from Phase 6 ([roadmap](../roadmap/phases.md));
before that, per-phase case assertions are advisory evidence produced by the
harness and checked by hand — never a CI gate.

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
[`crates/bench-probes`](../../crates/bench-probes/), which compose exactly
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
  composition mirrors the crates' own construction APIs until the server
  wires the path in Phase 1 wave 3; the served runtime's idle RSS is this
  case again once that wiring exists.)
- `retention-bound` — the retention law alone: a small, stated store
  configuration filled past its ceilings, then held under continued load
  while RSS is sampled across the steady state. Plateau samples, eviction
  counters and residency are recorded; the spread of the plateau is the
  evidence, not a passed threshold.

## What CI does with benchmarks

**Nothing yet, deliberately.** Benchmark gating arrives with Phase 6
([roadmap](../roadmap/phases.md)) when the scenarios exist and the numbers
are stable. Until then the harness is run by hand:

```sh
scripts/bench/run-all.sh   # runs the registered cases; no case asserts a budget
```

## Planned scenarios (placeholders in name only — none exist)

Registered by their phases, not before:

- `startup` (Phase 1) — cold start to an answered health probe; measures
  against the startup target.
- `workload-rss` (Phase 2) — a scripted investigation session against a
  generated OTLP stream; measures RSS against the workload target.
- `query-budget` (Phase 2) — a query forced past its budget; asserts the
  refuse-don't-grow contract.

Adding a scenario before its phase's capability exists is a scope violation —
it would measure nothing and report it anyway.
