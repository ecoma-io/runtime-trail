#!/usr/bin/env bash
# Regenerates the committed OTLP prost types in src/gen/ from the vendored,
# pinned .proto sources in protos/ (see protos/PROVENANCE.md).
#
# This is a maintenance step, NOT part of the build: the generated files are
# committed, so building and testing this crate never needs protoc. Run this
# only when bumping the pinned upstream tag — then update PROVENANCE.md
# (tag, commit, per-file hashes) and review the generated diff in the same
# commit. Never hand-edit anything under src/gen/.
#
# Requirements: cargo (any stable), network for the two pinned codegen
# crates, and a `protoc` binary — either on PATH or provided by the
# `protoc-bin-vendored` crate as below.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/out"

# A throwaway generator, pinned to the same prost line the crate builds with.
# prost-build shells out to protoc; protoc-bin-vendored supplies one so no
# system package is required.
cat >"$work/Cargo.toml" <<'TOML'
[package]
name = "otlp-regen"
version = "0.0.0"
edition = "2021"

[dependencies]
prost-build = "=0.14.1"
prost-types = "=0.14.1"
prost = "=0.14.1"
protoc-bin-vendored = "3"
TOML

cat >"$work/src.rs" <<'RS'
fn main() {
    let out = std::path::PathBuf::from(std::env::args().nth(1).expect("out dir"));
    let protos = std::path::PathBuf::from(std::env::args().nth(2).expect("protos dir"));
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    let files: Vec<std::path::PathBuf> = [
        "opentelemetry/proto/common/v1/common.proto",
        "opentelemetry/proto/resource/v1/resource.proto",
        "opentelemetry/proto/trace/v1/trace.proto",
        "opentelemetry/proto/logs/v1/logs.proto",
        "opentelemetry/proto/metrics/v1/metrics.proto",
        "opentelemetry/proto/collector/trace/v1/trace_service.proto",
        "opentelemetry/proto/collector/logs/v1/logs_service.proto",
        "opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
    ]
    .iter()
    .map(|p| protos.join(p))
    .collect();
    let mut cfg = prost_build::Config::new();
    cfg.protoc_executable(&protoc);
    cfg.out_dir(&out);
    cfg.compile_protos(&files, &[&protos]).expect("codegen");
    println!("generated {}", out.display());
}
RS

mkdir -p "$work/src"
cp "$work/src.rs" "$work/src/main.rs"
(cd "$work" && cargo run -q --release -- "$work/out" "$here/protos")

# The five outputs replace the committed artifacts in place. Their names and
# the module tree in src/gen/mod.rs must stay in step with the proto packages.
for f in "$work/out"/*.rs; do
  cp "$f" "$here/src/gen/$(basename "$f")"
done

echo "regenerated into $here/src/gen/ — now: update protos/PROVENANCE.md if the tag moved,"
echo "review the diff, and never edit the generated files by hand."
