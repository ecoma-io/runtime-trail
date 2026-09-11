# Telemetry model

> Authoritative home for the internal representation of logs, traces and
> metrics. Ingestion, storage and query all speak this model; nothing above it
> may redefine it.

## What the model is

The telemetry model is `runtime-trail`'s single, presentation-independent
representation of OpenTelemetry data. It is the currency of the whole core:
[ingestion](system.md) produces it, [storage](storage-model.md) keeps it, the
[query and correlation engines](investigation-model.md) read it. It is defined
once, in the `telemetry-model` crate (tag `layer-model`), which — per
[boundaries.md](boundaries.md) — depends on nothing else in this repository.

## Design rules

1. **OTLP-faithful.** The model preserves what the emitter actually sent:
   span parent/child structure, attributes, events, links, status; log
   severity, body, attributes; metric name, kind, attributes, timestamps,
   exemplars where present. Normalisation happens at the edges of the model,
   never by rewriting the data inside it.
2. **Correlation fields are part of the model.** The fields the Correlation
   Engine needs — trace ID, span ID on log records, timestamps, resource and
   scope identity — are first-class model concerns, not annotations bolted on
   later. Their semantics are owned here.
3. **Independent of presentation.** No rendering hints, no UI shapes, no
   strings formatted for display. The model does not know the UI or MCP
   exist; both receive _views over_ the model produced by the Investigation
   API, never the model's own types.
4. **Independent of storage and transport.** No serialisation format is
   normative inside the model. OTLP (protobuf) is the _ingestion_ wire
   format; the model outlives any particular encoding of itself.
5. **Bounded by construction.** Model types carry the information budgets
   need (attribute counts, sizes) so that
   [runtime-constraints.md](runtime-constraints.md) can be honoured without
   reachability hacks.

## Relationship to OTLP

The model is semantically aligned with the OpenTelemetry data model —
resources, scopes, spans, log records, metric points — because OTLP is the
only ingestion contract ([non-goals](../product/non-goals.md)). It is not a
binding to the OTLP protobuf types: the protobuf shapes are an ingestion
concern (the `telemetry-ingestion` crate's), translated _into_ the model at
admission. Nothing else in the core sees protobuf.

## Status

**Bootstrap.** The `telemetry-model` crate exists as the declared boundary
with scaffolding only; the concrete types land with Phase 1 (telemetry
ingestion) per [the roadmap](../roadmap/phases.md). This document is the
contract they must satisfy.
