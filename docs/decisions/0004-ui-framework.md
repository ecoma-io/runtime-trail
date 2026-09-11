# 0004: The UI is Vue 3 + Loom (Tailwind CSS 4) from the first commit

## Status

Accepted (2026-09-11, foundation commit)

## Context

The organisation's UI system is [Loom](https://github.com/ecoma-io/loom)
(Vue 3, TypeScript, Tailwind, design tokens, accessibility-first), and the
org's stated direction is that Loom builds every product surface. At the time
of this decision Loom is published and consumable:

- `@ecoma-io/loom@^0.5.0` on the npm registry, peer range `vue >= 3.5`,
  built for **Tailwind CSS 4** (CSS-first: `@theme` tokens, no
  `tailwind.config.js`).
- Integration contract (from Loom's own docs): install `@ecoma-io/loom`,
  `@import "@ecoma-io/loom/styles/global.css";` once, import components by
  name (`import { Button } from "@ecoma-io/loom"`); theming via
  `useTheme` from `@ecoma-io/loom/theme`. Icons come from `@lucide/vue`,
  which pnpm's strict layout does not expose transitively — declare it if
  imported directly.
- Loom's app templates deliberately ship **no router, no state library, no
  backend glue** — those belong to the consumer.

Alternatives: any non-Loom component system would fork the org's UI investment
and duplicate accessibility work; raw Tailwind without Loom would drift from
the org's design tokens from day one. Neither is defensible for an
ecoma-io product.

## Decision

1. `apps/web` is **Vue 3 + Vite + TypeScript + Tailwind CSS 4 + Loom**,
   consuming `@ecoma-io/loom` from the npm registry — real dependency from
   the foundation commit, not a plan.
2. The UI consumes **only the Investigation API** (HTTP). No Rust crate may
   appear in its import graph
   ([boundaries](../architecture/boundaries.md), `layer-view` row — enforced,
   canary-tested).
3. TypeScript strictness matches the org baseline (`tsconfig.base.json`):
   `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`,
   `verbatimModuleSyntax`, `moduleResolution: "bundler"`.
4. **No router and no state library at bootstrap.** The foundation app is a
   single smoke page proving the toolchain and the Loom integration. Router
   arrives with the first real multi-view investigation UI (Phase 2), chosen
   then — deliberately not pre-decided here.

## Consequences

- UI work starts from accessible, token-driven components instead of raw
  primitives; gaps in Loom get fixed in Loom (an explicit
  [non-goal](../product/non-goals.md) to fork it here).
- Tailwind must stay on v4 and CSS-first; a v3 config would fight Loom's
  stylesheet contract.
- The `@lucide/vue` transitivity gotcha is documented here because pnpm's
  strict `node_modules` will hide it exactly once, at the worst time.
