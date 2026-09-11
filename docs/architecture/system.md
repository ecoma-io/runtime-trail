# System architecture

> Authoritative home for the component map, runtime modes, and distribution
> surfaces. Dependency _rules_ live in [boundaries.md](boundaries.md).

## Component map

```text
                        ┌──────────────────┐
                        │     Loom UI      │
                        └────────┬─────────┘
                                 │
                        ┌────────▼─────────┐
                        │ Investigation API│
                        └────────┬─────────┘
                                 │
                        ┌────────▼─────────┐
                        │   Query Engine   │
                        └────────┬─────────┘
                                 │
                 ┌───────────────▼────────────────┐
                 │       Correlation Engine       │
                 └───────────────┬────────────────┘
                                 │
                        ┌────────▼─────────┐
                        │ Telemetry Model  │
                        └────────┬─────────┘
                                 │
                  ┌──────────────▼──────────────┐
                  │    Storage Abstraction      │
                  └──────────┬───────────┬──────┘
                             │           │
                          Memory      SQLite
                             ▲           ▲
                             └─────┬─────┘
                                   │
                          ┌────────▼────────┐
                          │ OTLP Ingestion  │
                          └─────────────────┘


                         ┌─────────────────┐
                         │   MCP Server    │
                         └────────┬────────┘
                                  │
                                  └──► Investigation API
```

Components, top to bottom:

| Component               | Responsibility                                                    | Owns                                                                                             |
| ----------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| **Loom UI**             | Human investigation surfaces                                      | presentation only; holds no domain logic                                                         |
| **Investigation API**   | The single contract both surfaces consume                         | request/response and streaming shapes of investigation                                           |
| **Query Engine**        | Answering queries over the telemetry model                        | filtering, search, aggregation within budget                                                     |
| **Correlation Engine**  | Cross-signal identity and adjacency                               | the correlation indexes/strategies; the one place that knows how traces, logs and metrics relate |
| **Telemetry Model**     | Faithful, presentation-independent representation of OTLP signals | types and semantics of logs, spans, metric points and their correlation fields                   |
| **Storage Abstraction** | The storage contract                                              | traits/contracts only — no implementation                                                        |
| **Memory / SQLite**     | Concrete storage modes                                            | one mode each; swappable behind the abstraction                                                  |
| **OTLP Ingestion**      | Receiving OpenTelemetry data                                      | OTLP transport decoding, admission control, hand-off to the runtime                              |
| **MCP Server**          | Agent-facing surface                                              | MCP protocol framing; a client of the Investigation API                                          |

Reading direction is deliberate: ingestion writes _down_ into the model;
surfaces read _through_ investigation; nothing reads sideways past a layer.

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
