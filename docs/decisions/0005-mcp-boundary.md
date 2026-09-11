# 0005: MCP is a peer surface over the Investigation API — never a second back door

## Status

Accepted (2026-09-11, foundation commit)

## Context

AI agents are a first-class audience for `runtime-trail`
([vision](../product/vision.md)): an agent correlating a failing test's trace
with its logs performs the same investigation a developer does by hand. The
MCP (Model Context Protocol) server is how agents reach that investigation.

The tempting shortcuts, and why they are refused:

- _Wrap the UI's HTTP endpoints._ Couples agents to presentation-shaped
  responses that change with UI iterations; makes the UI an accidental API.
- _Give MCP its own path into storage/query._ Answers an agent gets would
  diverge from answers a human gets — two code paths, two sets of budgets,
  two sets of bugs.
- _Design MCP tools now, against an unimplemented core._ Tool schemas frozen
  before the Investigation API exists would be guesses wearing contracts.

Precedent: the org's `archkeep` ships an MCP server as a separate published
crate over the same engine its CLI uses — one engine, multiple surfaces is
established org practice.

## Decision

1. `crates/mcp` (`layer-agent`) is a **peer of the UI**: it depends on the
   Investigation API and nothing else in the core
   ([boundaries](../architecture/boundaries.md) — the `agent → storage` edge
   is forbidden and canary-tested exactly like `view → storage`).
2. **No agent-only capabilities and no agent-only data paths.** Anything MCP
   exposes exists as an Investigation API capability; where an agent-shaped
   gap appears, the API grows the capability and both surfaces get it
   ([mcp-model](../architecture/mcp-model.md)).
3. MCP conversations inherit the same budgets, pagination and eviction
   visibility as UI traffic. An agent gets no unbounded window a human
   cannot get.
4. Protocol details (stdio transport, tool schemas, capability negotiation)
   stay inside `crates/mcp`; the Investigation API stays protocol-agnostic.
5. **The crate ships as a declared boundary with scaffolding only in the
   foundation commit.** Real tools land in Phase 4
   ([roadmap](../roadmap/phases.md)), designed against the then-existing
   Investigation API — no tool schemas are frozen before that.

## Consequences

- Agent workflows and human workflows cannot silently disagree; there is one
  implementation of every investigation.
- The MCP crate stays small forever — protocol adapter only — which keeps
  its dependency surface (and audit surface) minimal.
- The cost is discipline: a capability the UI wants but MCP does not yet
  still goes through the API; there is no "UI-only field" escape hatch.
