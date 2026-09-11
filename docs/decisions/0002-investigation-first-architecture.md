# 0002: Investigation-first — one Investigation API, two equal surfaces

## Status

Accepted (2026-09-11, foundation commit)

## Context

`runtime-trail` could grow as most observability tools do: an ingestion
pipeline first, a UI bolted on, and — recently — agent access bolted on
behind the UI's private endpoints. Each of those orderings produces a core
that is really "the UI's backend", with agents as second-class clients, or a
scraping surface that breaks on every UI change.

The product's centre of gravity is the _investigation_: trace waterfall,
logs↔trace navigation, metrics↔trace context ([scope](../product/scope.md)).
Those flows are needed identically by a human in a UI and by an agent over
MCP. Any architecture where one surface reaches the data differently from the
other guarantees the two answers diverge.

## Decision

1. The **Investigation API is the only door** to the core. The Loom UI (HTTP
   client) and the MCP server (in-process client) are two equal consumers of
   it; neither touches storage, the query or correlation engines, or
   ingestion. Enforced mechanically by the `layer-view` and `layer-agent`
   rows in [boundaries.md](../architecture/boundaries.md), canary-tested in
   `tools/fixtures/`.
2. **No surface-only capabilities.** A capability that exists on one surface
   and not the other is a defect in the Investigation API — it is fixed
   there, not papered over with a second path
   ([mcp-model.md](../architecture/mcp-model.md)).
3. **Build order follows the dependency direction.** Telemetry model →
   storage/query/correlation → Investigation API → surfaces. UI polish never
   precedes the API it would be pinned to
   ([roadmap phases](../roadmap/phases.md): ingestion and the investigation
   runtime precede investigation UI surfaces).
4. The API is _investigation-shaped_: its operations name traces, logs,
   metrics and their relations — never storage or internal structures.

## Consequences

- UI iteration cannot corrupt the core: the API is a real boundary, compiled
  and enforced, not a convention.
- MCP arrives cheap: by Phase 4 the server is a protocol adapter over
  capabilities that already exist for the UI.
- The UI cannot "quickly" reach into storage for a feature — that shortcut is
  a build failure, which is the point.
- Investigations that only make sense visually (rendering, layout) stay in
  the view layer; anything an agent must reason over belongs in the API.
