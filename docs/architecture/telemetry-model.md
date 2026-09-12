# Telemetry model

> Authoritative home for the internal representation of logs, traces and
> metrics: the semantics every signal carries, the identity by which it is
> known, and the budgets that bound it. Ingestion, storage and query all speak
> this model; nothing above it may redefine it. How signals are investigated
> lives in [investigation-model.md](investigation-model.md); how they are kept
> lives in [storage-model.md](storage-model.md); the numeric budget values live
> in [runtime-constraints.md](runtime-constraints.md).

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
   later. Their semantics are owned here; the relations built from them are
   owned by [correlation-model.md](correlation-model.md).
3. **Independent of presentation.** No rendering hints, no UI shapes, no
   strings formatted for display. The model does not know the UI or MCP
   exist; both receive _views over_ the model produced by the Investigation
   API, never the model's own types.
4. **Independent of storage and transport.** No serialisation format is
   normative inside the model. OTLP (protobuf) is the _ingestion_ wire
   format; the model outlives any particular encoding of itself.
5. **Bounded by construction.** The model types carry the counts and sizes
   budgets need — attribute counts, value sizes, event and link counts — so
   [runtime-constraints.md](runtime-constraints.md) can be honoured without
   reachability hacks. Which budgets exist is specified below; their numbers
   live there. A record that exceeds a budget is refused at admission; the
   model never silently shrinks what it has admitted.

## Conformance language

- **Preserved verbatim** means the value crosses from the wire into the model
  unchanged: the model never invents, synthesises, reorders into meaning, or
  rewrites it. Any loss is either refused or recorded — never silent.
- Where this document and the OpenTelemetry data model or the OTLP
  specification disagree, the OTel specification is the semantics of last
  resort. This document narrows the OTel model for local investigation; it
  never contradicts it.
- "Normalisation at the edges" (rule 1) means decode-time representation
  choices only — never value mutation.

## Trace context

- `trace_id` is 16 bytes; `span_id` and `parent_span_id` are 8 bytes. The
  all-zero encoding is _invalid_, and an invalid value is preserved as what
  the emitter sent — never regenerated, hashed, or coerced. An absent parent
  is distinct from a zero parent.
- `trace_flags` is the full 32-bit field the wire carries (`fixed32`). Only
  the sampled bit (bit 0) is interpreted; every other bit is preserved
  verbatim — including bits 8 and 9, OTLP's remote-parent signalling, and
  everything above them. A reader must not assume the high bits are zero.
- `tracestate` is an ordered list of (vendor, opaque value) entries. The order
  is semantic; entries are never merged, deduplicated, or sorted.

## Spans

A span carries its trace context, its `span_id`, an optional parent, its name
verbatim, its kind (all six OTLP values, unspecified included — kinds are never
collapsed), and start/end timestamps in nanoseconds on the emitter's clock.
`end == start` is a valid zero-length span; an unset end means the span is
unfinished and is distinct from any ended value.

A span also carries its **resource and scope identity** — OTLP attaches them
at the envelope (`ResourceSpans`/`ScopeSpans`) level; the model flattens them
onto every record, because correlation fields are first-class (rule 2) and
the resource-equality rule below must apply to all three signal kinds. The
same object is shared, not copied, between records from one batch
([ADR 0008](../decisions/0008-admission-ledger-design.md)).

- **Events** carry their own timestamp, name and attributes; the order the
  emitter sent them is semantic and is preserved.
- **Links** carry the linked trace context (including flags and, where sent,
  tracestate) and attributes.
- **Status** is two fields: the code (unset/ok/error) and the message. Both
  are preserved even when the code is unset, and the code is never derived
  from the kind or the events.
- **Emitter-reported loss is data.** The dropped-attribute, dropped-event and
  dropped-link counts the emitter sent are preserved — they are part of
  faithfulness and part of eviction observability
  ([storage-model.md](storage-model.md)).
- The runtime's own admission time is separate metadata; it never overwrites
  or substitutes for an emitter timestamp.

## Log records

- `timestamp` (event time) and `observed_timestamp` (observation time) are
  two distinct fields with two distinct meanings. Neither is synthesised from
  the other at the model level; any query-time fallback between them is a
  Phase 2 policy _over_ the model, not a model mutation.
- `severity_number` (integer 1–24) and `severity_text` (free-form emitter
  text) are independent preserved fields. No coercion happens in either
  direction; displaying a mapped severity name is a view concern (rule 3).
- The body is a full [value](#values-and-attributes) — not necessarily a
  string. Attributes are a value map. Trace context is optional and comes as
  **three independent fields** — `trace_id`, `span_id`, `flags` — each
  present or absent on its own, exactly as the emitter sent them: a record
  that sent a span id without a trace id keeps exactly that. Nothing
  fabricates a zero id to fill an absent half; the
  `(trace_id, span_id)` correlation pair exists only when both are present.
- The event name (OTLP `LogRecord.event_name`) is a preserved field, present
  or absent. A record also carries the resource and instrumentation scope it
  was emitted under, flattened exactly as on spans.
- The dropped-attribute count is preserved, as on spans. The
  [depth budget](#information-budgets) applies to the body like to any
  value the record carries.

## Metrics

Every metric carries its name, description and unit verbatim (absent is not
empty; the unit is opaque at the model level — no unit grammar is interpreted
here), its `metadata` (preserved as an attribute map; duplicate keys refused
as everywhere), its scope and resource (below), and its points.

- **The five OTLP kinds are distinct model-level shapes**: gauge; sum (with
  its monotonicity flag preserved — it changes what the number means, and it
  is never inferred); histogram; exponential histogram (scale, zero count and
  threshold, bucket layout preserved exactly — never re-bucketed); summary
  (quantiles, count, sum). No kind is collapsed into another.
- **The kind imposes a shape law.** A point offered under a kind whose shape
  it does not carry — a histogram-shaped point under a sum, an interval
  point without a `start_time` — is refused at admission as invalid, never
  coerced. Admission re-applies the same law a stream construction enforces,
  so a point cannot bypass it by arriving without its stream.
- **Temporality is per data point stream.** A delta stream and a cumulative
  stream with the same name are different series — never merged, split, or
  converted. Conversion is a transformation pipeline, which
  [non-goals](../product/non-goals.md) excludes.
- A data point carries its attribute set plus `start_time` (interval start)
  and `time` (interval end or measurement time). A gauge point that _did_
  send a `start_time` has it preserved verbatim — outside gauge identity,
  never refused, and never a conflict when a re-delivery differs only there
  — because the spec's gauge semantics ignore it while producers are
  encouraged to send it. Every data point also carries its flags as sent;
  the staleness marker (`DATA_POINT_FLAG_NO_RECORDED_VALUE`) is the one
  interpreted bit, all 32 bits are preserved, and a flagged point is a
  different point from an unflagged one.
- **Exemplars** carry their value, timestamp, filtered attributes, and two
  independently optional trace ids — `trace_id` and `span_id`, exactly as
  the wire carries them (no flags, no tracestate). The exemplar's ids are
  the model-level hook for metrics↔trace correlation — a committed
  capability ([scope](../product/scope.md)) — and neither is ever
  fabricated when absent.

## Values and attributes

- A value is exactly one of: string, boolean, 64-bit integer, 64-bit float,
  bytes, array, or key-value list (an ordered map). Integers and floats are
  never interconverted; NaN and the infinities are preserved values.
- Array elements are of one value kind — **any single one of the seven
  kinds**, including arrays of arrays and arrays of key-value lists, which
  OTLP carries (`ArrayValue` is repeated `AnyValue`) and structured log
  bodies use. A mixed-kind array is invalid input: it is rejected, not
  coerced.
- **Duplicate keys are refused.** A key repeated within one key-value list —
  attributes, metric metadata, exemplar filtered attributes — is invalid
  input: the record is rejected at admission. Silent keep-first or keep-last
  dropping is never model behavior; the emitter that sends a key twice has
  sent something the model cannot represent faithfully.
- **Empty is a value.** An empty string, array, key-value list or byte
  string, zero, and false are each distinct from the key being absent. The
  distinction is load-bearing for identity (below).
- Attribute count per entity, per-value size, and key-value-list depth are
  model-level facts — [runtime-constraints.md](runtime-constraints.md) owns
  their numbers, and [information budgets](#information-budgets) owns what
  happens when one is exceeded.

## Resource and scope identity

- A resource is an attribute map. Two records share a resource exactly when
  their attribute maps are equal under this model's comparison — regardless
  of how the emitter batched its exports, and regardless of `schema_url`,
  which never participates in resource identity (it is preserved
  provenance). The comparison is over the whole map — full-field, never
  hash-based. Resources are never merged; resource merging is a collector
  processor feature, and this runtime is a destination, not a relay.
- A scope is identified by (name, version, attributes, schema URL) — the
  full field set. The same name with a different version is a different
  scope. An empty scope name is valid. The resource keeps OTel's own
  identity rule (the attribute map); the scope's wider key is this model's
  choice — it is the identity metric stream identity is built from, so no
  two scopes a stream could distinguish are ever equalised. Both levels
  carry an emitter-reported `dropped_attributes_count`: preserved data
  outside identity at the resource level, part of the scope's full-field
  identity at the scope level.
- `schema_url` exists at two levels — resource and scope — and both are
  preserved as distinct fields. At the resource level it is provenance
  outside identity; at the scope level it participates in the scope's
  full-field identity.
- OTLP attaches resources and scopes at the envelope level
  (`ResourceSpans`/`ScopeSpans`, `ResourceLogs`/`ScopeLogs`,
  `ResourceMetrics`/`ScopeMetrics`). The model flattens them onto every
  record — spans and log records carry resource and scope identity exactly
  like metric points ([Spans](#spans)) — and preserves the envelope-level
  `dropped_attributes_count` fields on resource and scope alongside them.
- **"Service" is not a typed model field.** Service identity is the resource
  attribute `service.name`; anything first-class built on it is a view that
  query or correlation builds over the model. It is stated here so that no
  layer invents a typed Service.

## Record identity and duplicate delivery

OTLP delivery is at-least-once in practice: SDKs retry on transport timeouts
and transient saturation
([runtime-constraints.md](runtime-constraints.md) contracts exactly which
admission signal is retryable).
Idempotent admission is therefore a model requirement, not an optimisation.

- **Admission assigns every record an entity id** — an opaque, typed
  identifier, unique within the runtime's session (one process lifetime; ids
  are not persisted — a reopened file-backed session reassigns them, and no
  handle built on them survives a restart). A span whose `trace_id` and
  `span_id` are both valid keeps its natural identity (`trace_id`,
  `span_id`) as its entity id; every other record — log records, metric
  points, and spans carrying an invalid (all-zero) id — receives an
  admission-assigned id, because nothing in its wire data distinguishes it
  from any other record. Entity ids are added metadata: they never replace
  or rewrite emitter data. Cursors, relation endpoints and investigation
  references name entity ids — nothing else does.
- **Collapse happens only where the OTel data model itself defines
  identity.** A re-delivered span (same `trace_id` + `span_id`) is the same
  span: it collapses onto the one already admitted. A re-delivered metric
  point (same stream identity — resource, scope, name, kind, temporality —
  plus the point's attribute set, `start_time`, `time`, and flags; for
  gauges the point is (stream, `time`, flags) and a sent `start_time` is
  normalised out of the identity) is the same data point: it collapses.
- **Log records are never collapsed.** OTLP defines no log-record identity,
  and this model refuses to invent a destructive one: two byte-identical log
  records are two admitted records — the emitter sent two. Duplicate
  suppression at the source is the emitter SDK's job, not admission's.
- **Admitted data is immutable.** A delivery that conflicts with an
  already-admitted record under the same natural identity (a span re-sent
  with a different payload) does not overwrite it — the first admitted
  record stands, and the conflict is **recorded** as an admission anomaly:
  an observable runtime counter surfaced in investigation
  [coverage](investigation-model.md). No collapse or conflict resolution
  ever silently destroys or rewrites a record.
- **Comparison is byte-exact and total.** Two values, attribute maps or
  records are equal only when their wire content is equal: floats compare by
  bit pattern (`-0.0` and `0.0` are different values; a NaN equals only a
  NaN with the same bit pattern), maps compare key sets and per-key values,
  and structural equality never falls back to hashing or string forms. This
  is the equality the resource rule and the duplicate rules above rest on;
  the [admission ledger](../decisions/0008-admission-ledger-design.md)
  implements it with no approximations.
- **Identity lives as long as the record does.** The ledger answering "have
  I seen this natural identity?" shares ownership of each retained record's
  comparison payload with the store; evicting a record also forgets its
  identity entry, and a re-delivery after eviction is admitted as a fresh
  record — first-stands applies within residency, not across it
  ([ADR 0008](../decisions/0008-admission-ledger-design.md)). The same
  law bounds **stream identities**: the ledger drops a stream's interned
  identity when the stream's last resident point leaves, so interning
  never pins content for a session longer than the points that justify
  it.
- **Record identity is a model concern; adjacency is not.** That two signals
  are related is a derived, strategy-owned fact
  ([correlation-model.md](correlation-model.md)), which is built on the
  identity above and never rewrites it.

## Information budgets

- The model owns the budget **taxonomy**: attribute count per span, log
  record, data point, resource and scope; attribute count per span event,
  link and exemplar; attribute value size — with attribute keys and
  structure names counted; events per span; links per span; exemplars per
  point; data points per export; key-value-list depth. The numbers live in
  [runtime-constraints.md](runtime-constraints.md), which owns them; the
  model carries them as one startup-configurable limit set that every
  admission gate consumes.
- **The nesting budget is about every recursive structure a record
  carries.** Key-value lists and arrays alike count toward the depth, at
  every level, in every value a record holds — attribute values, log
  bodies, event, link and exemplar attributes included. No distinguished
  field is exempt: a record any of whose values exceeds the depth budget is
  refused at admission. The check is an iterative walk that stops at the
  first level past the limit, so an arbitrarily deep payload is refused
  without ever recursing through it — and the post-admission invariant,
  every value in every admitted record within budget, is what makes
  traversal of admitted data safe without depth tricks.
- The model defines every record's **accounted size** — the bytes of its
  value payloads, its attribute keys and structure names, plus measured
  per-container and per-structure overhead — so that everything
  variable-length is counted and byte ceilings have a single definition no
  matter which storage mode enforces them. Accounted size is a deliberate
  **over-approximation** of real heap cost, sized from a counting-allocator
  measurement over the legal adversarial shapes (many tiny attributes
  dominate: their B-tree nodes, not their payloads, are the cost).
  Over-counting is the honest direction; a ceiling that under-counts would
  be a lie about the machine. How tight the bound is (real ÷ accounted) is
  a measured fact for [docs/benchmarks/README.md](../benchmarks/README.md)
  when measurements land — never a claim made here.
- **Byte ceilings count what residency pins — stream identities included.**
  A metric point references a stream identity (resource, scope, name,
  kind, temporality) whose content the point's own accounted size does not
  carry. The store therefore charges each distinct resident stream's
  identity accounted size exactly once — added when the stream's first
  point enters residency, released when its last point leaves — so a
  session of single-point streams cannot park unbounded identity content
  under a ceiling that only saw the points. Admission's per-record gates
  are unchanged (they judge the record as sent); the residency charge is
  the store's, owned by
  [storage-model.md](storage-model.md).
- **Budgets are admission gates, not mutation triggers.** A record that
  exceeds a budget is refused at admission — as a **non-retryable**
  rejection or a `partial_success` naming the budget
  ([runtime-constraints.md](runtime-constraints.md) contracts which wire
  signal each case carries; an over-cap payload cannot be fixed by
  retrying) — never truncated into something the emitter did not send.
  Everything admitted is complete and
  immutable. This is how rule 1 and rule 5 are reconciled: a span with ten
  thousand attributes cannot be both faithfully kept and bounded, so it is
  refused, observably.
- Whose loss it was is always visible: emitter-side loss through the
  preserved dropped-counts, runtime-side refusal through budget errors,
  identity conflicts through admission-anomaly counters
  ([recorded](#record-identity-and-duplicate-delivery) in coverage), and
  post-admission shrinking only through eviction
  ([storage-model.md](storage-model.md)).

## Relationship to OTLP

The model is semantically aligned with the OpenTelemetry data model —
resources, scopes, spans, log records, metric points — because OTLP is the
only ingestion contract ([non-goals](../product/non-goals.md)). It is not a
binding to the OTLP protobuf types: the protobuf shapes are an ingestion
concern (the `telemetry-ingestion` crate's), translated _into_ the model at
admission. Nothing else in the core sees protobuf.

## Status

**Contracts pinned (M0), Phase 1 implements them.** This document is the
normative contract the Phase 1 types are written against and checked
against: the value and signal semantics, the identity and
duplicate-delivery rules, and the budget taxonomy. The `telemetry-model`
crate's concrete types land with Phase 1 (telemetry ingestion) per
[the roadmap](../roadmap/phases.md); admission is where every rule above
becomes behavior on real bytes.
