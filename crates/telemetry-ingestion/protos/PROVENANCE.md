# Provenance of the vendored OTLP `.proto` sources

These files are **byte-identical copies** of the OpenTelemetry Protocol
file-layout sources, vendored so the OTLP message types this crate compiles
are pinned and reviewable. They are not modified in any way — the only
runtime-trail-authored files in this directory tree are this note and the
regeneration script one level up.

| Fact                | Value                                                                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Upstream repository | `https://github.com/open-telemetry/opentelemetry-proto`                                                                                           |
| Pinned tag          | `v1.11.0` (latest stable release, 2026-07-21)                                                                                                     |
| Pinned commit       | `790608c4d51e6ffc12210b541e8514cbed9e91a4`                                                                                                        |
| Upstream license    | Apache-2.0 (`LICENSE` in the upstream repository; the `.proto` files carry the upstream copyright/SPDX headers)                                   |
| Files               | `opentelemetry/proto/{common,resource,trace,logs,metrics}/v1/*.proto` and `opentelemetry/proto/collector/{trace,logs,metrics}/v1/*_service.proto` |

## Integrity hashes (SHA-256, per vendored file)

```text
620560f3ad4c45d606f8a9c455f2b98089f0d511c5859c3eadd3ee630ae0d4d8  common/v1/common.proto
6b2e1eba0c01ae2da47927c63eabfaf3f151ee7ad846f2cdf3b2b71fb61004fe  logs/v1/logs.proto
68cf07fe34bc5111d7873b5547ca41f0312dc8976d2635d81fac553d5ba98757  metrics/v1/metrics.proto
e0a7cdc0ffcfeffaa2606e8611839735ebffaa2d6acdf33e9356f2c48ae692d3  resource/v1/resource.proto
c3fb1385c90b8bc08a2a462e28b5d0c422c7b524a839f75f75e3cd9f64f36956  trace/v1/trace.proto
0252687fc8f59d139ced709d383e83678e9bd4957bdbe76077242f1c32363fa5  collector/logs/v1/logs_service.proto
2bb3c6adee5f7609c8a32c5e4b1ec57057320015e298e9cf136699a42abc24d7  collector/metrics/v1/metrics_service.proto
03c8cc4e3e101087d884392d6eda32152ad5cd696e6344f50deaa59804a75c7a  collector/trace/v1/trace_service.proto
```

## How these are used

`../regenerate.sh` feeds exactly these files to `prost-build` and writes the
generated modules over the committed artifacts in `../src/otlp/` (the
hand-arranged `../src/otlp/mod.rs` is not regenerated). The generated files
are committed, so nothing in this directory is read at build time; it is the
pinned source of truth the committed artifacts can be reproduced from.
Bumping the pinned tag is an architecture-visible change: regenerate, review
the generated diff, update this note (tag, commit, hashes) in the same PR.
