#!/usr/bin/env bash
# Regenerates the committed OTLP prost types in src/otlp/ from the vendored,
# pinned .proto sources in protos/ (see protos/PROVENANCE.md).
#
# What this script writes: the eight `opentelemetry.proto.*.rs` generated
# files, over the committed ones in src/otlp/. What it does NOT write:
# src/otlp/mod.rs — the module tree there is hand-arranged (it exists so
# prost's `super::super::…` cross-references resolve) and must be kept in
# step with the proto packages by hand.
#
# This is a maintenance step, NOT part of the build: the generated files are
# committed, so building and testing this crate never needs protoc and never
# runs this script. Run it only when bumping the pinned upstream tag — then
# update PROVENANCE.md (tag, commit, per-file hashes) and review the
# generated diff (`git diff -- src/otlp`) in the same commit. Never
# hand-edit the generated files.
#
# Requirements: cargo (any stable) with network access once, to fetch the
# pinned generator crates. A `protoc` binary is NOT required on this
# machine: `protoc-bin-vendored` supplies one to the generator, so no
# system package is needed.
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

# The eight outputs replace the committed artifacts in place, in the layout
# the crate actually builds from. Their names must stay in step with the
# proto packages, and src/otlp/mod.rs's include! list with them.
expected=(
  opentelemetry.proto.common.v1.rs
  opentelemetry.proto.resource.v1.rs
  opentelemetry.proto.trace.v1.rs
  opentelemetry.proto.logs.v1.rs
  opentelemetry.proto.metrics.v1.rs
  opentelemetry.proto.collector.trace.v1.rs
  opentelemetry.proto.collector.logs.v1.rs
  opentelemetry.proto.collector.metrics.v1.rs
)
for name in "${expected[@]}"; do
  if [ ! -f "$work/out/$name" ]; then
    echo "✗ codegen did not produce $name — the pinned protos and this script disagree" >&2
    exit 1
  fi
  cp "$work/out/$name" "$here/src/otlp/$name"
done
unexpected="$(find "$work/out" -maxdepth 1 -name '*.rs' -exec basename {} \; | sort)"
for name in "${expected[@]}"; do
  unexpected="$(printf '%s\n' "$unexpected" | grep -vxF "$name" || true)"
done
if [ -n "$unexpected" ]; then
  echo "✗ codegen produced files this script does not install: $unexpected" >&2
  exit 1
fi

echo "regenerated into $here/src/otlp/ — now: update protos/PROVENANCE.md if the tag moved,"
echo "review the diff (git diff -- src/otlp), and never edit the generated files by hand."
echo "src/otlp/mod.rs was NOT touched: it is hand-arranged, not generated."
