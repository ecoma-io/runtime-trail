# Investigation model

> Authoritative home for the Investigation API — the product's only door —
> and the shape of its answers. The query contract the API composes lives in
> [query-model.md](query-model.md); relation semantics live in
> [correlation-model.md](correlation-model.md); signal semantics live in
> [telemetry-model.md](telemetry-model.md); how signals are kept lives in
> [storage-model.md](storage-model.md).

## The Investigation API is the product's only door

Every capability `runtime-trail` offers reaches its user through one
Investigation API. Both surfaces — the Loom UI and the MCP server — consume
the same API; both are clients; neither is privileged; neither has a second
path into the core. This is the invariant [boundaries.md](boundaries.md)
enforces with the `layer-view` and `layer-agent` rows.

**The contract is defined without a transport.** The request/response shapes
are wire shapes: transport-independent data descriptions that an adapter
renders. HTTP arrives with the server (layer-app) as one adapter; MCP
consumes the API in-process as another. No capability is defined in terms of
"HTTP semantics" or "MCP primitives", and no capability requires one
transport's machinery to describe. This is what keeps one API serving both
surfaces without drift: the UI cannot ask a question MCP cannot, or vice
versa, because the contract contains no per-surface vocabulary.

The API is _investigation-shaped_, not _storage-shaped_: its queries name
traces, logs, metrics and the relations between them — never tables, indexes
or storage modes.

## The three engines

```text
                 ┌──────────────────┐   ┌───────────────────┐
 surfaces ──────►│ Investigation API│──►│    Query Engine   │
 (UI, MCP)       │    (layer-api)   │   │   (layer-query)   │
                 │                  │   └───────────────────┘
                 │ composes both    │   ┌───────────────────┐
                 │ engines          │──►│ Correlation Engine│
                 └──────────────────┘   │ (layer-correlation)│
                                        └───────────────────┘
                         both engines read only──►┌────────────────┐
                                                  │ Telemetry Model│
                                                  │  (layer-model) │
                                                  └────────────────┘
```

The API **composes** the two engines as siblings — the dependency direction
[boundaries.md](boundaries.md) enforces is `layer-api → {layer-query,
layer-correlation}`, and neither engine depends on the other. (The earlier
stacked diagram in this document implied a Query → Correlation edge that the
boundary law forbids; fixed here.)

- **Query Engine** (`crates/query`, `layer-query`) — answers questions over
  the [telemetry model](telemetry-model.md) through the
  [storage abstraction](storage-model.md): find traces by service/time/attributes,
  list log records around a window, series for a metric. Every query carries
  its [budget](query-model.md); the engine refuses or degrades within it
  (the policy is [query-model.md](query-model.md)'s to own), never growing
  until the machine dies. Storage drivers report scan work in the
  driver-symmetric [scan unit](query-model.md).
- **Correlation Engine** (`crates/correlation`, `layer-correlation`) — owns
  cross-signal identity as [typed, evidenced relations](correlation-model.md):
  which log records belong to which trace, which metric points attach to
  which spans, which records share a resource. Every correlation strategy
  (name, version) lives here and nowhere else; the UI never "just filters by
  trace_id" on its own data because it holds no data.
- **Investigation API** (`crates/investigation`, `layer-api`) — composes the
  two engines into the named investigation flows the surfaces need (trace
  waterfall + related logs + surrounding metrics as one response), defines
  the request/response shapes, and is the _only_ project both surfaces may
  depend on.

## The Investigation envelope

Every response is an **`Investigation`** — one envelope with five parts:

- **subject** — what was investigated: the requested subject as sent by the
  caller, plus the **effective** subject the runtime actually investigated
  (resolutions and normalisations the runtime applied), so a caller can
  compare request against effect. The requested and effective subjects are
  stated separately, never merged — if they differ, the difference is
  inspectable.
- **execution** — what the work cost and what it covered: per-dimension
  budget spend (deadline, results, bytes, scan, aggregation memory) against
  the caller's budget; the truncation list (which dimensions expired and how
  the answer was truncated, per the [refuse-or-degrade
  policy](query-model.md)); opaque cursors for continuation; and **coverage**
  — what was and was not covered (residency gaps under
  [eviction](storage-model.md), suppressed relation evidence under
  [strongest-evidence-wins](correlation-model.md), relation shrinkage under
  eviction, incomplete coverage of a metric's window). Coverage is what
  makes truncation truthful.
- **correlated** — the [Relations](correlation-model.md) discovered between
  the subject and the resident signals: typed, tiered, evidenced
  ([correlation-model.md](correlation-model.md)), ordered deterministically,
  returned only when all endpoints are resident. Relations are never a
  boolean `related = true`.
- **evidence** — the record views the answer is grounded in: inline views of
  the records themselves (spans, logs, points — model-shaped, view-ready but
  view-agnostic per [the model's rule 3](telemetry-model.md)) plus
  `SignalRef` handles — opaque entity-id handles a surface may hand back to
  navigate further (e.g. "jump to logs" affordances). **No dangling
  references**: every `SignalRef` in a response names a resident record and
  every record view carries its entity id.
- **limits** — machine-checkable statements of what the answer is subject
  to: the budget dimensions with their limits and observed spend, the
  coverage statement, strategy versions
  ([correlation-model.md](correlation-model.md)) and
  [eviction state](storage-model.md). A surface may verify the envelope's
  honesty mechanically — a machine-checkable limits block is what makes
  "the answer is subject to X" a fact, not a promise.

### Envelope invariants

1. Requested subject and effective subject are stated separately; any
   difference is inspectable, never merged.
2. Every truncation is named (dimension, truncation point, cursor/coverage),
   and coverage reports residency gaps, suppressed evidence and relation
   shrinkage.
3. Every `SignalRef` is resolvable to a resident record; no dangling
   references.
4. Every record view carries its entity id, so relations, evidence and
   navigation all name the same identity the model assigns.
5. A response never contains a partial aggregate presented as complete
   ([query-model.md](query-model.md) pins this at the engine; the envelope
   reports it).
6. Same resident set + same request + same strategy versions ⇒ byte-identical response (deterministic
   output is what makes pagination stable and reproduction possible).
7. Same investigation on the memory mode and the SQLite mode produces the
   same envelope shape (mode symmetry): the contract contains no mode
   vocabulary, so the envelope cannot differ by mode.

### The investigation-shaped line

A response is investigation-shaped, not storage-shaped: it names
investigation concepts — subjects, views, relations, coverage, limits —
and storage/transport vocabulary never appears in the envelope. That line is
mechanically checkable: any envelope field named in storage or transport
vocabulary fails this contract.

## Committed investigation capabilities

The capabilities this stack must eventually serve are locked in
[scope.md](../product/scope.md): trace waterfall/span tree, logs↔trace
navigation, committed correlation strategies (per
[the taxonomy](correlation-model.md)), metrics↔trace context, cross-signal
correlation, developer-first latency. This document owns none of their wire
shapes — those arrive with their phases
([../roadmap/phases.md](../roadmap/phases.md)) and are designed against this
model when they do.

## Naming

The envelope is named `Investigation`. The API is the Investigation API, its
responses are Investigations, and a surface asks for one by naming a flow.
The name is the product's own word for "an answer with its honesty attached"
— one word, no acronym, no per-surface variant.

## Status

**Contracts pinned (M0).** The envelope, the three-engine composition and
the committed capabilities are the contract Phase 1–2 implement. The three
crates (`investigation`, `query`, `correlation`) exist as declared
boundaries with scaffolding only; the crates' structure and dependency
directions are real and enforced from this commit on, and
[boundaries.md](boundaries.md) is the mechanical form of that law.
