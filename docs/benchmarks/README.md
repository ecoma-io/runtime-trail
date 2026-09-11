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

## What CI does with benchmarks

**Nothing yet, deliberately.** Benchmark gating arrives with Phase 6
([roadmap](../roadmap/phases.md)) when the scenarios exist and the numbers
are stable. Until then the harness is run by hand:

```sh
scripts/bench/run-all.sh   # today: exits non-zero — no cases registered yet
```

## Planned scenarios (placeholders in name only — none exist)

Registered by their phases, not before:

- `idle-rss` (Phase 1) — runtime started, nothing ingested; measures RSS
  against the idle target.
- `workload-rss` (Phase 2) — a scripted investigation session against a
  generated OTLP stream; measures RSS against the workload target.
- `startup` (Phase 1) — cold start to a answered health probe; measures
  against the 1 s target.
- `retention-bound` (Phase 1) — sustained overload; asserts bounded memory
  and backpressure signalling, not a number.
- `query-budget` (Phase 2) — a query forced past its budget; asserts the
  refuse-don't-grow contract.

Adding a scenario before its phase's capability exists is a scope violation —
it would measure nothing and report it anyway.
