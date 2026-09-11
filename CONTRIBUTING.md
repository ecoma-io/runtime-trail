# Contributing to Runtime Trail

Thanks for your interest. Runtime Trail is an [ecoma-io](https://github.com/ecoma-io)
repository — read `AGENTS.md` first if you work with an AI coding agent; it is
the authority for how changes are made here, and everything below is its
human-facing restatement for contribution mechanics.

## Getting started

Requirements (see `.node-version`, `package.json#packageManager`, and the
Rust MSRV in the workspace `Cargo.toml`):

- Node 24.x and pnpm 12.x (corepack or your manager of choice)
- A stable Rust toolchain ≥ 1.85
- moon comes from `@moonrepo/cli` through `pnpm install` — no global install;
  every command below runs through the root `pnpm` scripts

```sh
pnpm install          # installs npm dependencies and the git hooks
pnpm verify           # the whole local gate — the definition of "done"
```

The desktop crate additionally needs the platform webview libraries (see
`apps/desktop/README.md`). Without them, `pnpm verify` prints an explicit
skip for the desktop checks; CI always runs them.

## How changes land

1. **Issue first.** File an issue describing the defect or the change; search
   for duplicates. Security issues never go through issues — see `SECURITY.md`.
2. **Branch** from `main`.
3. **Draft PR** early, linking the issue. Keep PRs small enough to review in
   one sitting; split along moon project lines when a change spans crates.
4. **Green CI**, then mark the draft ready. Every PR lands through the merge
   queue — no direct pushes to `main`.

Every commit must be signed and must satisfy Conventional Commits with the
scopes declared in `commitlint.config.mjs`; the pre-commit and pre-push hooks
(`lefthook.yml`) run the fast gates and the full suite respectively. Never
bypass them.

## Architecture changes are document changes

The dependency law lives twice on purpose: as prose in
`docs/architecture/boundaries.md` and as an executable table in
`module-boundaries.config.mjs` (`pnpm arch`). Changing the architecture means
changing both in the same commit, with the reasoning in the PR description —
and, where the decision is durable, an ADR under `docs/decisions/`. The
canary harness (`pnpm arch:canary`) proves the gate still bites; if your
change needs the canaries weakened, that is not your change's call.

New modules bring three things in one commit: the `moon.yml` with its tags, a
constraint row that judges those tags, and the commit scope.

## Roadmap discipline

`docs/roadmap/phases.md` owns sequencing. Please don't pull a future phase's
work into the present one — the value of the early phases is that they stay
small enough to verify completely.

## Reporting bugs

Open an issue with what you observed, what you expected, and how to reproduce
— behaviour without interpretation. Anything security-relevant goes to
`SECURITY.md`'s private channel instead.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0 (`LICENSE`).
