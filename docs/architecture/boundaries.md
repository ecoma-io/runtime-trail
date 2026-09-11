# Boundaries

> Authoritative home for dependency rules: allowed directions, forbidden edges,
> and how they are enforced. Component responsibilities live in
> [system.md](system.md); this document owns the _rules between_ them.

## The one rule

Everything a user or agent can reach — the Loom UI, the MCP server — talks to
the **Investigation API** and to nothing else below it. Ingestion writes down;
surfaces read through investigation; nothing reads sideways past a layer.

## Projects and tags

The repository is a Cargo workspace (`crates/*`) plus a pnpm workspace
(`apps/*`), orchestrated by [moon](https://moonrepo.dev). Each project carries
exactly one `layer-*` tag, and the law keys on those tags:

| Tag                    | Projects                                                                                    |
| ---------------------- | ------------------------------------------------------------------------------------------- |
| `layer-view`           | `web` (apps/web) — the Loom UI                                                              |
| `layer-agent`          | `mcp` (crates/mcp) — the MCP server                                                         |
| `layer-api`            | `investigation` (crates/investigation) — the Investigation API                              |
| `layer-query`          | `query` (crates/query) — the Query Engine                                                   |
| `layer-correlation`    | `correlation` (crates/correlation) — the Correlation Engine                                 |
| `layer-model`          | `telemetry-model` (crates/telemetry-model) — the Telemetry Model                            |
| `layer-storage`        | `storage` (crates/storage) — the Storage Abstraction                                        |
| `layer-storage-driver` | `storage-memory`, `storage-sqlite` — concrete storage modes                                 |
| `layer-ingest`         | `telemetry-ingestion` (crates/telemetry-ingestion) — OTLP ingestion                         |
| `layer-app`            | `server` (crates/server), `desktop` (apps/desktop/src-tauri) — composition and distribution |

## Allowed dependency directions

`A → B` means "A may depend on B". Every project may always depend on itself;
that is omitted below.

```text
layer-model            → (nothing)                      # leaf
layer-storage          → model
layer-storage-driver   → storage, model
layer-ingest           → model, storage
layer-query            → model, storage
layer-correlation      → model
layer-api              → query, correlation, model
layer-view             → (nothing internal)             # HTTP via the server (layer-app)
layer-agent            → api
layer-app              → api, storage-driver, ingest, app
```

Notes on the two rows that look unusual:

- **`layer-view` depends on nothing internal.** The UI's only access to the
  core is the Investigation API over HTTP. A compile-time dependency from the
  UI onto any Rust crate is a violation — there is no legitimate one.
- **`layer-app` is the composition root.** Only it may name concrete storage
  drivers and ingestion, because choosing them at startup is precisely its job.
  `server` and `desktop` may depend on each other: the desktop shell starts the
  same server core in-process (the same-core invariant in
  [system.md](system.md)).

## Forbidden edges

Anything not reachable through the chains above is forbidden. The ones worth
naming, because they are the invariants this architecture exists to keep:

- `view → storage`, `view → query`, `view → correlation`, `view → model`,
  `view → ingest` — the UI never bypasses the Investigation API.
- `agent → storage`, `agent → query`, `agent → correlation`,
  `agent → model`, `agent → ingest` — MCP has exactly one upstream: the
  Investigation API, the same one the UI uses.
- `api → storage`, `api → storage-driver`, `api → ingest`, `api → view`,
  `api → agent`, `api → app` — the Investigation layer never depends on
  storage (concrete or abstract), ingestion, or any presentation/distribution
  surface.
- `model → anything` — the telemetry model stays independent of storage,
  presentation and transport.
- `storage-driver → query`, `storage-driver → correlation`, … — a driver
  implements the storage abstraction; it never reaches upward.
- **Cross-language edges**: a `.ts`/`.vue` file in `layer-view` importing from
  any Rust crate project, or any internal TS module outside `layer-view`.

## Enforcement

[Archkeep](https://github.com/ecoma-io/archkeep) checks these rules
mechanically against the real source tree — Rust edges from `Cargo.toml` path
dependencies, TS/Vue edges resolved through the TypeScript config. This
repository uses the **Moon provider**: the project graph comes from
`.moon/workspace.yml`, and the law lives in
`module-boundaries.config.mjs` at the repository root. A root `archkeep.json`
must **not** be created — beside `.moon/` it is a hard error for the Moon
provider.

- Run it with `pnpm arch` (whole workspace, uncached). Exit `0` = clean,
  `1` = violations, `2` = usage error, `3` = no verdict. Anything but `0`
  fails.
- CI runs it unconditionally on the whole tree (see the `architecture` job in
  `.github/workflows/ci.yml`) — it is deliberately not a moon task and never
  affected-scoped, so no change can dodge it.
- **Negative fixtures** live in `tools/fixtures/`: small workspaces with their
  own Archkeep configs containing deliberate violations, asserted by
  `node tools/check-arch-canary.mjs` to fail with _exactly_ the expected
  violation. A law that only ever sees legal trees may be approving nothing;
  the canaries prove the law still bites. Run them with `pnpm arch:canary`.

**What Archkeep owns vs what other checks own:** Archkeep owns _dependency
edges between projects_ (imports, path dependencies) and the acyclicity of the
project graph. It does not own formatting, types, lint rules, test coverage or
resource budgets — those belong to the toolchain checks (`lint`, `typecheck`,
`test`) and to [runtime-constraints.md](runtime-constraints.md). A rule that
is neither an edge nor a cycle does not belong in
`module-boundaries.config.mjs`.

When this document and `module-boundaries.config.mjs` disagree, one of them is
wrong — fix whichever it is in the same change; do not loosen the rule to make
a check green.

## Changing the boundaries

Before any change that adds, moves or retags a project — or adds a dependency
that crosses a layer:

1. Read the relevant architecture document
   ([system.md](system.md), this file, and the model doc for the area).
2. Inspect `module-boundaries.config.mjs` and the fixture canaries.
3. Run `pnpm exec archkeep context <project>` for the project's boundary facts,
   then make the change, then run `pnpm arch` and `pnpm arch:canary`.
4. A new allowed direction needs a paragraph here **and** the constraint row
   to match; a new forbidden edge needs a canary that proves it fails.
5. Architecture facts are verified from the repository, never from memory —
   re-read the config; do not trust a recollection of it.
