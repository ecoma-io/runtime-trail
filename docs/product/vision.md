# Vision

> Authoritative home for what `runtime-trail` is, who it serves, and why it exists.
> Other documents link here instead of restating it.

## What runtime-trail is

`runtime-trail` is a **local developer observability and investigation runtime**.

It runs on a developer's machine, accepts OpenTelemetry signals from the services
and agents that developer is working on, and turns those signals into something a
human — or an AI agent — can actually investigate: correlated logs, traces and
metrics, navigable as one story instead of three disconnected tabs.

It is **not** a production observability platform and **not** a lightweight
Grafana clone. See [scope.md](scope.md) for what that commits us to and
[non-goals.md](non-goals.md) for what it rules out.

## Who it is for

- **Developers** running a service (or several) locally who need to follow one
  request across its logs, its trace, and the metrics around it — without
  standing up a hosted backend or a multi-container observability stack.
- **AI agents** and agent-driven workflows (editors, CLIs, CI debugging) that
  need programmatic access to the same investigation capabilities through MCP.
- **Small teams** who want one shared vocabulary for telemetry during local and
  review-time investigation, before anything reaches production tooling.

## Why it exists

OpenTelemetry solved emission: nearly everything can emit OTLP today. What is
still hard is _investigation at development time_:

- Production-grade backends are built for scale, tenancy and retention — not for
  a developer who wants an answer in the next thirty seconds.
- Logs, traces and metrics land in separate tools, and the developer performs the
  correlation by hand.
- Existing local options are either heavyweight (a full LGTM stack), or
  single-signal toys that cannot follow a trace into its logs.

`runtime-trail` exists to close that gap: one small local runtime that ingests
OTLP, keeps the three signal kinds correlated, and exposes them through a
developer-first UI and an MCP interface.

## The core principle

```text
OpenTelemetry
    ↓
Telemetry Runtime          (ingest, model, keep)
    ↓
Investigation Runtime      (query, correlate, explain)
    ├── Developer UI         (Loom-based, human investigation)
    └── MCP                  (agent investigation, same API)
```

Everything else is subordinate to this chain:

1. **Telemetry Runtime** — receive OTLP, hold it in a faithful internal model,
   keep it within bounded resources.
2. **Investigation Runtime** — query and correlate across signals; this is the
   product's centre of gravity and the only layer the UI and MCP may talk to.
3. **Surfaces** — the Developer UI and the MCP server are two clients of the
   same Investigation API. Neither is privileged; neither bypasses the core.

The [architecture documents](../architecture/README.md) define how this is
enforced; [scope.md](scope.md) defines what the surfaces must eventually do.

## Product commitments

The durable commitments that follow from this vision — OpenTelemetry-native,
investigation-first, low-memory, local — are locked in
[scope.md](scope.md). What is deliberately out of scope lives in
[non-goals.md](non-goals.md). Delivery order lives in
[the roadmap](../roadmap/README.md).
