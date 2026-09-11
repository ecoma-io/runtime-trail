# Investigation model

> Authoritative home for the Investigation API, the Query Engine and the
> Correlation Engine. Signal semantics live in
> [telemetry-model.md](telemetry-model.md); keeping them lives in
> [storage-model.md](storage-model.md).

## The Investigation API is the product's only door

Every capability `runtime-trail` offers reaches its user through one
Investigation API. The Loom UI consumes it over HTTP; the MCP server consumes
the same API in-process. Both are clients; neither is privileged; neither has
a second path into the core. This is the invariant
[boundaries.md](boundaries.md) enforces with the `layer-view` and
`layer-agent` rows.

The API is _investigation-shaped_, not _storage-shaped_: its queries name
traces, logs, metrics and the relations between them — never tables, indexes
or storage modes.

## The three engines

```text
Investigation API  ──shapes and answers investigations──►  surfaces (UI, MCP)
       │
   Query Engine          filtering, search, aggregation — within budget
       │
Correlation Engine       the one place that knows how signals relate
       │
  Telemetry Model        what is being investigated
```

- **Query Engine** (`crates/query`, `layer-query`) — answers questions over
  the telemetry model through the [storage abstraction](storage-model.md):
  find traces by service/time/attributes, list log records around a window,
  series for a metric. Every query carries its budget; the engine refuses
  work that cannot be answered within it rather than growing until the
  machine dies (see [runtime-constraints.md](runtime-constraints.md)).
- **Correlation Engine** (`crates/correlation`, `layer-correlation`) — owns
  cross-signal identity: which log records belong to which trace, which
  metric windows contain which traces, which spans share a request. Every
  correlation strategy lives here and nowhere else; the UI never "just
  filters by trace_id" on its own data because it holds no data.
- **Investigation API** (`crates/investigation`, `layer-api`) — composes the
  two engines into the named investigation flows the surfaces need (trace
  waterfall + related logs + surrounding metrics as one response), defines
  the request/response shapes, and is the _only_ project both surfaces may
  depend on.

## Committed investigation capabilities

The capabilities this stack must eventually serve are locked in
[../product/scope.md](../product/scope.md): trace waterfall/span tree,
logs↔trace navigation, metrics↔trace context, cross-signal correlation,
developer-first latency. This document owns none of their HTTP/MCP shapes —
those arrive with their phases ([../roadmap/phases.md](../roadmap/phases.md))
and are designed against this model when they do.

## Status

**Bootstrap.** The three crates exist as declared boundaries with scaffolding
only; no query, correlation or API surface is implemented. The crates'
structure and dependency directions are real and enforced from this commit on.
