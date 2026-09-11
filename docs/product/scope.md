# Scope

> Authoritative home for the committed product direction.
> Vision: [vision.md](vision.md) · Exclusions: [non-goals.md](non-goals.md) ·
> Delivery order: [../roadmap/phases.md](../roadmap/phases.md)

This document states what `runtime-trail` **commits to provide**. Nothing here
is a claim that a capability exists today; implementation status is tracked in
[the roadmap](../roadmap/README.md), and the repository README always states the
current status honestly.

## Signals

- **OpenTelemetry Traces** — ingest spans via OTLP; preserve parent/child
  structure, attributes, events, links and status.
- **OpenTelemetry Logs** — ingest log records via OTLP; preserve severity,
  body, attributes and trace correlation fields when the emitter provides them.
- **OpenTelemetry Metrics** — ingest metric points via OTLP; preserve name,
  kind, attributes, timestamps and exemplars where present.

The internal [telemetry model](../architecture/telemetry-model.md) is
OTLP-shaped at its core: it must remain faithful to what emitters sent, and it
must remain independent of any presentation concern.

## Investigation (the product's centre of gravity)

- **Cross-signal correlation** — given a trace, find the logs and metrics that
  belong to its time window and context; given a log, find the trace it
  belongs to when correlation fields exist; given a metric anomaly window,
  narrow to the traces and logs inside it.
- **Trace waterfall / span tree** — navigate a trace as a tree and as a
  timeline; inspect any span's attributes, events and linked logs.
- **Logs ↔ Trace navigation** — from a log record to its trace (when trace
  context is present) and from a span to its logs.
- **Metrics ↔ Trace context** — from a metric point or window to the traces and
  logs inside that window, and back.
- **Developer-first UX** — answers in seconds; flows designed for a single
  developer's investigation session, not an on-call war room.

All of the above is exposed through one Investigation API. The Developer UI and
the MCP server are both clients of that API — neither talks to storage or the
query engine directly. See [../architecture/investigation-model.md](../architecture/investigation-model.md)
and [../architecture/boundaries.md](../architecture/boundaries.md).

## Runtime characteristics

- **Extremely low memory footprint** — resource budgets are part of the
  architecture, not later hardening; see
  [../architecture/runtime-constraints.md](../architecture/runtime-constraints.md)
  for the engineering targets (targets, not measured facts).
- **Two first-class storage modes** — in-memory (ephemeral, the default for a
  quick session) and embedded file-backed persistence. See
  [../architecture/storage-model.md](../architecture/storage-model.md).
- **Bounded resources** — telemetry retention, query memory and ingestion
  backpressure are bounded by explicit budgets in every mode.
- **Zero external database requirement** — no separately installed database,
  broker or collector is ever needed to run `runtime-trail`.

## Surfaces

- **Developer UI** — built with Vue 3 and [Loom](https://github.com/ecoma-io/loom),
  the organisation's UI system. The UI is a client of the Investigation API and
  nothing else.
- **MCP interface for AI agents** — an MCP server exposing investigation
  capabilities to agents, backed by the same Investigation API as the UI.
  See [../architecture/mcp-model.md](../architecture/mcp-model.md).

## Distribution

- **Docker** — a container image that starts the same native core and server
  used everywhere else (no separate cloud build).
- **Desktop app** — a desktop shell wrapping the same native core; the shell is
  a presentation surface only and never contains a second implementation of the
  core. See [../architecture/system.md](../architecture/system.md).

## The single invariant behind all of it

Every capability above is reached through the Investigation API. When a new
capability is added, it is added to the telemetry/runtime/investigation core
and surfaced to both UI and MCP — never implemented twice, never implemented on
one surface only.
