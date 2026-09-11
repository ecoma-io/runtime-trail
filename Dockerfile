# The core, containerised: one static-serving process that boots with no
# database and answers /healthz on 8599. This image exists to make that
# contract runnable anywhere — the smoke test in ci.yml builds it and boots it
# on every pull request, so "the core starts and answers" is a continuously
# tested fact, not a release-time hope.
#
# Base images are pinned tag@digest, for the same reason every `uses:` in
# .github/workflows is pinned by SHA: the thing that resolves the pointer must
# not be the thing that decides what runs. Renovate bumps tag and digest
# together (renovate.json5 pins the docker manager). The builder pins are
# deliberate choices, not defaults:
#
#   - rust:1.85-slim — the workspace MSRV exactly (rust-version in
#     Cargo.toml). Building the shipped image on the floor the workspace
#     declares is the cheapest MSRV regression test there is: if a dependency
#     bump silently raises the real floor, THIS build turns red, not a
#     contributor's next `cargo build`.
#   - node:24-slim — matches .node-version's major; pnpm itself comes from
#     corepack reading `packageManager`, so no version is restated here.
#   - debian:bookworm-slim runtime — the same Debian release both builders
#     are based on (verified: both images are bookworm), so the binary's
#     glibc expectations and the runtime's glibc are the same series. No
#     distroless/alpine swap without re-proving that pairing.
#
# Security posture: the runtime stage runs as its own non-root user, carries
# no shell tools beyond what bookworm-slim ships, and holds nothing but the
# binary and the built UI. There is no HEALTHCHECK — the smoke test in CI is
# the liveness proof, and HEALTHCHECK tooling would add packages this image
# does not otherwise need.

# --- Stage 1: build the UI -------------------------------------------------
FROM node:24-slim@sha256:2fe369e969550cde8e867afc3fe370b260140cab4a23d467074295b42163d553 AS web
WORKDIR /src

# corepack is the pinned-by-packageManager path to pnpm; a `pnpm@x` literal
# here would be a second place a version lives and drifts.
RUN corepack enable

# Manifests first, sources second: the dependency layer rebuilds only when a
# manifest or the lockfile changes. tsconfig.base.json comes with the
# manifests because apps/web/tsconfig.json extends it (ADR 0004: shared
# strict base, no path aliases) — without it the vite build cannot resolve
# the compilerOptions it inherits.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json ./
COPY apps/web/package.json apps/web/package.json
RUN pnpm install --frozen-lockfile

COPY apps/web/ apps/web/
RUN pnpm --filter @runtime-trail/web build

# --- Stage 2: build the core -----------------------------------------------
FROM rust:1.85-slim@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36 AS rust
WORKDIR /src

# Manifests first, sources second — same layering as the UI stage. --locked
# makes Cargo.lock the authority: the build refuses if a manifest and the
# lockfile disagree, exactly like everywhere else in this repository. The
# workspace's every member manifest travels (apps/ included): `cargo build
# --package` still resolves the whole workspace graph, and a member the
# context forgot is a resolution error, not an optimisation.
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY apps/ apps/
RUN cargo build --release --locked --package runtime-trail-server

# --- Stage 3: the runtime image ---------------------------------------------
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

# The image's own user: no entry in /etc/passwd means no home, no shell, and
# no name an attacker can look up. UID 10001 avoids the low UID range that
# host users sometimes collide with.
RUN useradd --system --uid 10001 --no-create-home runtime-trail

COPY --from=rust /src/target/release/runtime-trail-server /usr/local/bin/runtime-trail-server
COPY --from=web --chown=runtime-trail:runtime-trail /src/apps/web/dist /srv/runtime-trail/web

USER runtime-trail
EXPOSE 8599

# The bind is 0.0.0.0 HERE, and that is not the loopback commitment bending:
# the server's default (no arguments) is still 127.0.0.1 — the commitment
# SECURITY.md records. Inside a container, "loopback only" would make the
# process unreachable even through the port mapping, because the container
# itself is the trust boundary; 0.0.0.0 inside it is how a loopback-only
# program is published, not an exception to it. The UI directory is passed
# for the same reason the image ships it: an empty-bundle core is honest, a
# core that has the UI and does not serve it is not.
ENTRYPOINT ["/usr/local/bin/runtime-trail-server"]
CMD ["--bind", "0.0.0.0:8599", "--web-dist", "/srv/runtime-trail/web"]
