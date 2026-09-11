# apps/desktop

The desktop distribution: a thin [Tauri 2](https://tauri.app) shell around the
same native core — **not** a second implementation of it. The moon project
lives in `src-tauri/` (project id `desktop`), because Archkeep models Rust
edges at `<projectRoot>/Cargo.toml`; nesting the project higher would hide the
desktop→core dependency from the boundary law.

The shell starts `runtime-trail-server` **in-process** on startup
(see `src-tauri/src/lib.rs`) and presents the web app in a webview: dev mode
against the Vite dev server (`devUrl`), packaged mode against
`apps/web/dist` (`frontendDist`). The same-core invariant is
`docs/architecture/system.md`.

## Building locally

The Tauri crate needs the platform webview libraries (on Debian/Ubuntu:
`libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libayatana-appindicator3-dev`
plus `pkg-config`). CI installs them; a machine without them cannot check this
crate, and that is expected — everything else in the repository builds without
them.

```sh
pnpm exec tauri dev    # from apps/desktop/src-tauri, or: pnpm exec moon run desktop:lint
```
