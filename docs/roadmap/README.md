# Roadmap

> Authoritative home for delivery status and the distinction between committed
> direction, 1.0 scope, and future direction.
> Phase detail: [phases.md](phases.md) · Product direction: [../product/scope.md](../product/scope.md)

## How to read this

Three tiers, and the difference matters:

1. **Committed product direction** — what `runtime-trail` is and must
   eventually provide. Defined in [../product/scope.md](../product/scope.md).
   Architecture work may rely on it; documents may state it as intent.
2. **1.0 scope** — the subset of the direction that defines the first
   complete product, listed below. Features outside it are _not_ blocked from
   landing, but 1.0 does not ship without them.
3. **Future direction** — candidate capabilities beyond 1.0. These are
   explicitly _not_ commitments, must not appear in the current architecture
   contract, and must not drive design decisions today.

## Current status

**Bootstrap.** The repository contains the engineering foundation only:
toolchain, architecture contracts, agent workflow, CI/CD gates, documentation
and governance. **No telemetry capability is implemented yet** — no OTLP
ingestion, no storage, no query/correlation, no investigation UI, no MCP tools.
Anything stating otherwise is wrong; this section is the source of truth.

## 1.0 scope

`runtime-trail` 1.0 is a local investigation runtime that can:

- ingest OTLP traces, logs and metrics over the standard OTLP protocols;
- keep the three signal kinds correlated and bounded in memory/disk;
- expose investigation — trace waterfall/span tree, logs↔trace navigation,
  metrics↔trace context — through one Investigation API;
- serve that API to a Loom-based Developer UI **and** to an MCP server;
- run in-memory or with embedded file-backed persistence, with resource
  budgets enforced in both modes;
- ship as a Docker image and as a desktop app, both wrapping the same native
  core.

Release mechanics (versioning, tags, artefact publishing) follow the org
release tooling and stay separate from ordinary CI; see the release decision
record once published in [../decisions/](../decisions/).

## Future direction (not committed)

None are listed yet, by design. When a candidate appears, it is added here
with the problem it would solve — and nowhere else. Candidates must not leak
into [../product/scope.md](../product/scope.md) or the architecture documents.
