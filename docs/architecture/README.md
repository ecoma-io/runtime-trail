# Architecture

`runtime-trail` is organised around one chain:

```text
OpenTelemetry → Telemetry Runtime → Investigation Runtime → { Developer UI, MCP }
```

The architecture documents define that chain precisely. Each fact lives in
exactly one document; this file is only the index.

## Documents

| Document                                         | Owns                                                                                          |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| [system.md](system.md)                           | The component map, runtime modes, and the distribution surfaces (Docker, desktop)             |
| [boundaries.md](boundaries.md)                   | Dependency rules: allowed directions, forbidden edges, and how they are mechanically enforced |
| [telemetry-model.md](telemetry-model.md)         | The internal representation of logs, traces, metrics and their correlation fields             |
| [investigation-model.md](investigation-model.md) | The Investigation API, the query engine and the correlation engine                            |
| [storage-model.md](storage-model.md)             | The storage abstraction, the two runtime storage modes, retention and backpressure            |
| [mcp-model.md](mcp-model.md)                     | The MCP boundary: what it shares with the UI and what it must never do                        |
| [runtime-constraints.md](runtime-constraints.md) | Resource budgets (engineering targets) and how the architecture honours them                  |

Product-level intent (what we build and why) lives in
[../product/](../product/); delivery order in [../roadmap/](../roadmap/);
point-in-time choices in [../decisions/](../decisions/).

## Enforcement

The dependency rules in [boundaries.md](boundaries.md) are not prose-only:
[Archkeep](https://github.com/ecoma-io/archkeep) checks them mechanically
against the actual source tree on every commit and in CI. The enforcement
configuration (`archkeep.json`, `module-boundaries.config.mjs` at the
repository root) must stay in sync with `boundaries.md`; the configuration is
the mechanically enforced **subset**, the document is the **normative
description**. If the two disagree, that is a defect to fix — either the code
or the rule is wrong.

Before changing anything architectural, read the relevant document, inspect
the enforcement configuration, and run the architecture check — the exact
workflow is defined in the repository's `AGENTS.md` at the root.
