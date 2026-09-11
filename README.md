# runtime-trail

**A local developer observability and investigation runtime.**
Ingest OpenTelemetry signals from what you are building right now; investigate
logs, traces and metrics as one correlated story — in a developer-first UI, or
through MCP from your AI agents.

`runtime-trail` is part of the [ecoma-io](https://github.com/ecoma-io)
organisation.

## What it is

A single small runtime that runs on your machine. Your locally running services
and agents send OpenTelemetry telemetry to it; `runtime-trail` keeps the three
signal kinds **correlated** and lets you follow one request across all of them:

- **Traces** — span trees and waterfalls for a single request.
- **Logs** — the log records that belong to that request's trace and spans.
- **Metrics** — the measurements around that request's time window.

Investigation is the product's centre of gravity, and it is exposed through one
Investigation API with two clients: a **developer UI** and an **MCP server**.

It is **not** a production observability platform and **not** a lightweight
Grafana clone — see [non-goals](docs/product/non-goals.md).

## Who it is for

Developers running services locally who need to answer "what actually happened
in this request?" in seconds — and the AI agents working alongside them, which
get the same investigation capabilities through MCP. See
[docs/product/vision.md](docs/product/vision.md).

## Why local-development-first

Production backends are built for scale, tenancy and retention. Local
investigation needs the opposite: zero setup, negligible resource use, and
answers now. `runtime-trail` requires **no external database, broker or
collector** — it starts in seconds, runs in-memory by default, and can persist
to an embedded store when a session is worth keeping. Production tooling keeps
doing its job; `runtime-trail` covers the gap between "emit telemetry" and
"understand it", where developers actually live.

## The three signals

| Signal  | Ingestion                                    | Investigation                             |
| ------- | -------------------------------------------- | ----------------------------------------- |
| Traces  | OTLP spans, parent/child structure preserved | waterfall + span tree, span inspection    |
| Logs    | OTLP log records                             | log explorer, logs ↔ trace navigation     |
| Metrics | OTLP metric points                           | metric timelines, metrics ↔ trace context |

Cross-signal correlation — trace→logs, log→trace, metric-window→traces — is a
first-class capability of the query and correlation engines, not a UI trick.

## The UI

The developer UI is built with Vue 3 on
[Loom](https://github.com/ecoma-io/loom), the ecoma-io UI system: design
tokens, accessibility-first components. The UI is a client of the
Investigation API — it never touches storage or ingestion directly.

## AI agents via MCP

An MCP server exposes the same investigation capabilities to AI agents that
the UI gives to humans — same API, same correlation, no separate access path.
See [docs/architecture/mcp-model.md](docs/architecture/mcp-model.md).

## Distribution

- **Docker** — one image that starts the same core and server used everywhere.
- **Desktop app** — a desktop shell wrapping the same native core; the shell
  never contains a second implementation of it.

Both are presentation and packaging around one core. See
[docs/architecture/system.md](docs/architecture/system.md).

## Architecture at a glance

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

The rules behind the arrows — what may depend on what, and which edges are
forbidden — are locked in [docs/architecture/boundaries.md](docs/architecture/boundaries.md)
and enforced mechanically by [Archkeep](https://github.com/ecoma-io/archkeep).

## Current status

**Bootstrap.** The engineering foundation is in place — toolchain,
architecture contracts (mechanically enforced, with canary fixtures proving
the enforcement bites), agent workflow, CI, documentation, governance — and
the core skeleton boots: one binary that binds loopback by default, answers
`/healthz` and `/version`, serves the bootstrap web app statically, and runs
against its in-memory store. There is **no
telemetry capability yet**: no OTLP ingestion, no telemetry model beyond the
skeleton's own types, no query or correlation engine, and no MCP server. The
web and desktop apps are bootstrap shells around a status page, not the
product UI. Delivery status is tracked in
[docs/roadmap/README.md](docs/roadmap/README.md).

## Documentation

- [Product](docs/product/vision.md) — vision, scope, non-goals
- [Architecture](docs/architecture/README.md) — system, boundaries, signal models, runtime constraints
- [Roadmap](docs/roadmap/README.md) — status, 1.0 scope, future direction
- [Decisions](docs/decisions/) — architecture decision records
- [Benchmarks](docs/benchmarks/README.md) — resource budgets and measurement harness
- [Contributing](CONTRIBUTING.md) — how to work in this repository
- [Security](SECURITY.md) — how to report vulnerabilities

## License

See [LICENSE](LICENSE).
