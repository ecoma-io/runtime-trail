# runtime-trail documentation

Documentation is split so that every important fact has **one authoritative
home**. Other documents link to that home instead of restating it; if two
documents disagree, the authoritative home wins and the divergence is a bug.

## Map

| Path                                                                       | Authoritative for                                                       |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| [product/vision.md](product/vision.md)                                     | What runtime-trail is, who it serves, why it exists, the core principle |
| [product/scope.md](product/scope.md)                                       | The committed product direction (capabilities we will provide)          |
| [product/non-goals.md](product/non-goals.md)                               | What we deliberately do not build                                       |
| [roadmap/README.md](roadmap/README.md)                                     | Current status, 1.0 scope vs future direction                           |
| [roadmap/phases.md](roadmap/phases.md)                                     | Delivery phases and their acceptance criteria                           |
| [architecture/README.md](architecture/README.md)                           | Index into the architecture documents                                   |
| [architecture/system.md](architecture/system.md)                           | Components, runtime modes, lifecycle, distribution surfaces             |
| [architecture/boundaries.md](architecture/boundaries.md)                   | Allowed dependency directions, forbidden edges, enforcement mapping     |
| [architecture/telemetry-model.md](architecture/telemetry-model.md)         | The internal telemetry model (OTLP-faithful)                            |
| [architecture/investigation-model.md](architecture/investigation-model.md) | Investigation API and the `Investigation` envelope                      |
| [architecture/query-model.md](architecture/query-model.md)                 | Query budgets, ordering, cursors, refuse-or-degrade policy              |
| [architecture/correlation-model.md](architecture/correlation-model.md)     | Relation types, tiers, evidence and provenance                          |
| [architecture/storage-model.md](architecture/storage-model.md)             | Storage abstraction, memory + embedded modes, retention                 |
| [architecture/mcp-model.md](architecture/mcp-model.md)                     | MCP server as a peer client of the Investigation API                    |
| [architecture/runtime-constraints.md](architecture/runtime-constraints.md) | Resource budgets (targets) and backpressure architecture                |
| [benchmarks/README.md](benchmarks/README.md)                               | How resource targets are measured; benchmark harness                    |
| [decisions/](decisions/)                                                   | Architecture decision records (ADRs)                                    |

## Conventions

- Documentation is English-first, like all public artefacts in ecoma-io.
- Statements about the **current** repository must match reality; statements
  about **intended** behaviour must be visibly marked as direction (roadmap,
  decisions) rather than mixed silently into descriptive text.
- When a decision changes, write a new ADR (or supersede an existing one) —
  do not silently rewrite the old one.
