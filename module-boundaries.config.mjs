// Architecture law for runtime-trail, judged by @ecoma-io/archkeep (`pnpm arch`).
//
// This repository is a Moonrepo workspace, so archkeep reads the project graph
// through its Moon provider — the project map lives in `.moon/workspace.yml`
// and each project's vocabulary lives in its `moon.yml` `tags`. A root
// `archkeep.json` cannot coexist with `.moon/` (archkeep refuses the pair),
// which is why this file is the boundary policy the provider loads by
// convention.
//
// Moon tags cannot contain colons, so the constraint vocabulary's `layer:`
// is spelled `layer-` in moon.yml and here alike.
//
// Exit codes archkeep can produce: 0 clean · 1 findings · 2 usage · 3
// no-verdict. A run that ends in anything other than 0 must fail the build —
// a verdict that could not be reached is never a clean tree. `tools/check-arch-canary.mjs`
// proves both directions of this file on every `pnpm arch:canary` run.
//
// The prose law is `docs/architecture/boundaries.md`; this table is its
// executable form. Change them in the same commit or not at all.

/**
 * The dependency constraints, in the `@nx/enforce-module-boundaries` option
 * shape archkeep consumes. One row per layer tag — kept exhaustive on
 * purpose: an unlisted tag would be unconstrained, so a new tag must arrive
 * together with the row that judges it.
 *
 * The direction law this table states (`docs/architecture/boundaries.md` is
 * its source of truth):
 *
 *   layer-view            →  layer-view only        ❌  the UI imports no workspace module;
 *                                                       it reaches the core over HTTP only
 *   layer-agent           →  layer-api              ❌  the MCP surface reads the one door, never beneath it
 *   layer-api             →  query/correlation/model ✅ the Investigation API composes the engines
 *   layer-query           →  model, storage         ✅  the query engine reads through the abstraction
 *   layer-correlation     →  model                  ✅  the correlation engine reasons over the model only
 *   layer-model           →  layer-model only       ❌  the telemetry model depends on nothing internal
 *   layer-storage         →  layer-model            ✅  the storage abstraction knows the model, no driver
 *   layer-storage-driver  →  storage, model         ✅  a driver implements the abstraction it belongs under
 *   layer-ingest          →  model, storage         ✅  ingestion writes through the abstraction
 *   layer-app             →  api/driver/ingest/app  ✅  the composition roots wire everything; app may
 *                                                       compose app (desktop shells server's contract, not
 *                                                       its crate — the edge under test stays server→api)
 *
 * The forbidden edges this makes unreachable, mechanically: view → anything
 * internal, agent → query/correlation/storage/ingest, api → storage or
 * storage-driver, query → correlation (query reads facts; correlation builds
 * them), anything → app, driver → driver of a different layer row, storage →
 * driver.
 *
 * @type {Array<{ sourceTag: string, onlyDependOnLibsWithTags: string[], bannedExternalImports?: string[] }>}
 */
export const depConstraints = [
  // The UI is a peer surface, not a layer of the core: it may import Loom and
  // its own modules, but no workspace project. Every fact it shows arrives
  // over the Investigation API at runtime — an import would be a second,
  // compile-time door around the API.
  { sourceTag: "layer-view", onlyDependOnLibsWithTags: ["layer-view"] },

  // The MCP surface reads the one door. query, correlation, storage and
  // ingestion are deliberately absent: an agent-only path beneath the
  // Investigation API would make the agent a privileged user of the core,
  // which `docs/architecture/mcp-model.md` forbids.
  { sourceTag: "layer-agent", onlyDependOnLibsWithTags: ["layer-api"] },

  // The Investigation API composes the engines and the model — and nothing
  // else. storage is absent on purpose: the API never touches storage
  // directly; queries reach facts through layer-query, which is what makes
  // the storage strategy swappable (ADR 0003).
  {
    sourceTag: "layer-api",
    onlyDependOnLibsWithTags: [
      "layer-query",
      "layer-correlation",
      "layer-model",
    ],
  },

  // The query engine reads facts through the storage abstraction. It may not
  // reach correlation — correlation's output is investigation material, and
  // only layer-api composes the two.
  {
    sourceTag: "layer-query",
    onlyDependOnLibsWithTags: ["layer-model", "layer-storage"],
  },

  // The correlation engine reasons over the telemetry model and nothing
  // below it: correlation is model-shaped, not storage-shaped.
  { sourceTag: "layer-correlation", onlyDependOnLibsWithTags: ["layer-model"] },

  // The telemetry model is the bottom of the internal graph: it imports no
  // other workspace project. OpenTelemetry fidelity is a data property, not
  // a dependency — the model does not (yet) even carry serde; when Phase 1
  // adds it, this row still holds, because serialization types are external.
  { sourceTag: "layer-model", onlyDependOnLibsWithTags: ["layer-model"] },

  // The storage abstraction knows the telemetry model and no driver: a
  // backend that named its own driver would invert the relationship ADR 0003
  // depends on (drivers are named only by the composition roots).
  { sourceTag: "layer-storage", onlyDependOnLibsWithTags: ["layer-model"] },

  // A driver implements the abstraction and models the same facts. Drivers
  // never import each other — memory mode and file-backed mode share nothing
  // but the contract above them.
  {
    sourceTag: "layer-storage-driver",
    onlyDependOnLibsWithTags: ["layer-storage", "layer-model"],
  },

  // Ingestion parses spans/logs/metrics off the wire and writes them through
  // the storage abstraction. It may not read: querying is not its job.
  {
    sourceTag: "layer-ingest",
    onlyDependOnLibsWithTags: ["layer-model", "layer-storage"],
  },

  // The composition roots wire everything: they are the only places allowed
  // to name a concrete driver (ADR 0003) and the only places that bind the
  // network. layer-app → layer-app is allowed so the desktop shell can share
  // composition helpers with the server without a fourth crate; the edge
  // under test stays desktop → server → investigation, which the compiler
  // sees because the desktop project root is the crate root.
  {
    sourceTag: "layer-app",
    onlyDependOnLibsWithTags: [
      "layer-api",
      "layer-storage-driver",
      "layer-ingest",
      "layer-app",
    ],
  },
];

/**
 * The plugin options archkeep judges boundaries with, all written at their
 * defaults so a reader sees the whole policy surface in one place and a future
 * change is a visible diff rather than an inherited assumption.
 *
 * @type {{
 *   allow: string[];
 *   buildTargets: string[];
 *   enforceBuildableLibDependency: boolean;
 *   allowCircularSelfDependency: boolean;
 *   checkDynamicDependenciesExceptions: string[];
 *   ignoredCircularDependencies: string[][];
 *   banTransitiveDependencies: boolean;
 *   checkNestedExternalImports: boolean;
 * }}
 */
export const moduleBoundaryOptions = {
  allow: [],
  buildTargets: ["build"],
  enforceBuildableLibDependency: false,
  allowCircularSelfDependency: false,
  checkDynamicDependenciesExceptions: [],
  ignoredCircularDependencies: [],
  banTransitiveDependencies: false,
  checkNestedExternalImports: false,
};

/**
 * Suppressions are where a boundary rule goes to die quietly, so the list
 * starts empty and every entry that ever lands here must carry a reason —
 * archkeep's loader rejects an entry without one.
 *
 * @type {Array<{ path: string, messageId: string, reason: string }>}
 */
export const boundarySuppressions = [];

/**
 * Files that carry no project tag and therefore no boundary verdict. Every
 * row is an accepted coverage hole with its reason on record — an
 * unaccepted analyzable file refuses the run (exit 3) instead of passing
 * silently. Keep this table in step with the tree: a new unowned directory
 * must land together with the row that accepts it.
 *
 * @type {{ unowned: Array<{ path: string, reason: string }> }}
 */
export const coverage = {
  unowned: [
    {
      path: "scripts/**",
      reason:
        "Repository gates and tooling. Scripts stand outside the product graph on purpose — a gate that imported what it judged would stop being a gate.",
    },
    {
      path: "tools/**",
      reason:
        "The architecture canary runner and its deliberate-violation fixtures. The fixtures are judged in their own archkeep workspaces by tools/check-arch-canary.mjs, never as part of this tree — their Rust sources must be accepted here because the Moon graph refuses unowned Rust files outright.",
    },
    {
      path: "eslint.config.mjs",
      reason:
        "Lint law at the repository root — configuration, not product source.",
    },
    {
      path: "commitlint.config.mjs",
      reason:
        "Commit-message law at the repository root — configuration, not product source.",
    },
    {
      path: ".opencode/**",
      reason:
        "Editor-host plugins, copied verbatim from ecoma-io/action-agents. They execute inside the editor's process to gate agent edits — tooling around the product, never part of its import graph.",
    },
  ],
};
