# MCP model

> Authoritative home for the MCP boundary: what the MCP server shares with
> the UI, and what it must never do.

## Position in the architecture

The MCP server (`crates/mcp`, tag `layer-agent`) is the AI-agent surface of
`runtime-trail`. It speaks the Model Context Protocol over stdio, translating
agent requests into calls on the **same Investigation API the Loom UI uses**
([investigation-model.md](investigation-model.md)). It is a peer of the UI,
not a privileged backdoor:

```text
Loom UI ──HTTP──► Investigation API ◄──in-process── MCP server ◄──stdio── AI agent
```

## The rules

1. **One door.** MCP depends on the Investigation API and nothing else in the
   core ([boundaries.md](boundaries.md), `layer-agent` row). It never touches
   storage, the query or correlation engines directly, or ingestion — the
   `agent → storage` edge is forbidden by the row and enforced mechanically
   by `pnpm arch`; the canary fixtures prove the law bites where the
   invariants are sharpest (`view → storage`, `driver → correlation`).
2. **No agent-only data paths.** Any capability MCP exposes must be the same
   capability the UI exposes, through the same API. An answer an agent can
   get and a human cannot (or vice versa) is a defect in the Investigation
   API — fix it there, not by adding a second path.
3. **Trust model = the user's machine.** MCP is served to whichever agent the
   developer configured. It grants no network reach, reads no data beyond
   what the Investigation API serves from the runtime's own store, and
   exposes no operation that the local user could not already perform in the
   UI. There is no auth layer to design around
   ([non-goals](../product/non-goals.md)) because there is nothing to
   authenticate to.
4. **Bounded like everything else.** MCP responses are Investigation API
   responses; they inherit the same budgets and pagination as UI traffic. An
   agent asking for "all the logs" gets the same bounded window a human
   would.
5. **Protocol concern stays in the crate.** MCP framing, tool schemas and
   capability negotiation are transport details owned by `crates/mcp`. They
   must not leak _into_ the Investigation API (whose contract is defined
   without a transport — [wire shapes](investigation-model.md)) and
   Investigation types must not leak _out_ as raw MCP content without
   going through the API's response shapes.

## Why MCP is a product surface, not an integration

Agents are first-class investigators here — the same way the UI's user is.
A debugging loop where an agent correlates a failing test's trace with its
logs is the same investigation a developer performs by hand; serving it
through MCP means the organisation's agent workflows get identical semantics
to the human workflow, from one implementation.

## Status

**Bootstrap.** No MCP capability exists; the crate is the declared boundary
only. Tools land in Phase 4 ([../roadmap/phases.md](../roadmap/phases.md)),
designed against this document and [../product/scope.md](../product/scope.md).
