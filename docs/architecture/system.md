# System architecture

> Authoritative home for the component map, runtime modes, lifecycle and
> distribution surfaces. Dependency _rules_ live in [boundaries.md](boundaries.md).

## Component map

```text
 Dependency directions (A → B: "A may depend on B"). The executable law:
 docs/architecture/boundaries.md + module-boundaries.config.mjs.

 surfaces (Loom UI over HTTP via layer-app · MCP in-process)
   └─► Investigation API (layer-api)
         ├─► Query Engine (layer-query)
         │     ├─► Telemetry Model (layer-model)
         │     └─► Storage Abstraction (layer-storage)
         └─► Correlation Engine (layer-correlation)
               ├─► Telemetry Model (layer-model)
               └─► Storage Abstraction (layer-storage)

 Telemetry Model (layer-model)           → nothing internal (leaf)
 Storage Abstraction (layer-storage)     → model
 Memory / SQLite (layer-storage-driver)  → storage, model   named only by layer-app
 OTLP Ingestion (layer-ingest)           → model, storage   writes through the abstraction
 Server / Desktop (layer-app)            → api, storage-driver, ingest, app
```

The map above states the dependency directions; the law they obey is
[boundaries.md](boundaries.md) (and its executable form,
`module-boundaries.config.mjs`) — when this map and that law disagree, the
law wins and this map is wrong. Reading direction is deliberate: ingestion
writes _down_ through the abstraction; surfaces read _through_
investigation; nothing reads sideways past a layer.

Components, top to bottom:

| Component               | Responsibility                                                    | Owns                                                                                                                       |
| ----------------------- | ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **Loom UI**             | Human investigation surfaces                                      | presentation only; holds no domain logic                                                                                   |
| **Investigation API**   | The single contract both surfaces consume                         | request/response and streaming shapes of investigation                                                                     |
| **Query Engine**        | Answering queries over the telemetry model                        | filtering, search, aggregation within [budget](query-model.md)                                                             |
| **Correlation Engine**  | Cross-signal identity and adjacency                               | [typed, evidenced relations](correlation-model.md); the one place that knows how traces, logs and metrics relate           |
| **Telemetry Model**     | Faithful, presentation-independent representation of OTLP signals | [types and semantics](telemetry-model.md) of logs, spans, metric points and their correlation fields                       |
| **Storage Abstraction** | The storage contract                                              | traits/contracts only — no implementation                                                                                  |
| **Memory / SQLite**     | Concrete storage modes                                            | one mode each; swappable behind the abstraction                                                                            |
| **OTLP Ingestion**      | Receiving OpenTelemetry data                                      | OTLP transport decoding, admission control mechanism; **policy** lives in [runtime-constraints.md](runtime-constraints.md) |
| **MCP Server**          | Agent-facing surface                                              | MCP protocol framing; a client of the Investigation API                                                                    |

## Runtime modes

The same core runs in two storage modes, selected at startup:

- **Memory mode** (default, first-class) — ephemeral, zero-setup, nothing
  touches the filesystem; bounded retention applies.
- **File-backed mode** — the embedded store persists a bounded session to
  local disk for later reopening.

Mode choice changes only the storage implementation behind the abstraction —
never the model, never investigation, never a surface. In both modes,
ingestion is never blocked by persistence: see
[storage-model.md](storage-model.md) and
[runtime-constraints.md](runtime-constraints.md).

## Surfaces and distribution

There are exactly two logical surfaces, and three ways to ship them:

- **Developer UI** (Loom web app) — ships inside the Docker image (served by
  the core) and inside the desktop shell.
- **MCP server** — ships in both as part of the same core process.
- **Docker image** — starts the same native core and server that developers
  run locally; it is packaging, not a fork.
- **Desktop app** — a thin native shell that embeds the _same_ core (started
  in-process) and presents the same UI in a webview. The shell contains no
  second implementation of any core behaviour; its only additions are
  shell-level concerns (window, menus, platform integration).

**Same-core invariant:** every distribution mode (local binary, Docker,
desktop) runs one native core with one Investigation API. Any PR that would
give a distribution its own telemetry, query or storage code violates
[boundaries.md](boundaries.md).

## Process topology

Local development keeps it deliberately boring: **one process** hosts the
telemetry runtime, investigation runtime, HTTP server (UI + API) and MCP
endpoint. There is no always-on daemon separate from the app the developer
started, no sidecar services, and no external database. The topology may only
grow along boundaries documented here — not by accretion.

### Lifecycle

**Target state.** The smoke core's shutdown is a plain graceful exit; the
drain semantics below land with the runtime that admits telemetry
([roadmap](../roadmap/phases.md)). The contract:

The process shuts down honestly:

- **SIGTERM** → stop admitting immediately: emitters get the
  [draining wire signal](runtime-constraints.md) (HTTP 503 / gRPC
  `UNAVAILABLE`) from that moment.
- In-flight admitted work drains to the store up to the
  [drain deadline](runtime-constraints.md); past the deadline, remaining
  in-flight work is dropped **observably** (surfaced, not silent).
- **Memory mode** loses undrained signals by mode contract — that is what
  ephemeral means, and the UI says so honestly; **file-backed mode** persists
  best-effort within the deadline.
- Surfaces see a `draining` state for the duration; no new investigation
  admits after drain completes.

## Status

**Bootstrap.** Mode selection and the full topology are target state; the
smoke core (`/healthz`, `/version`) is real and gated. The component map
above states the dependency directions; the composition law itself is
[boundaries.md](boundaries.md), and any PR that would give a distribution
its own telemetry, query or storage code violates it.
