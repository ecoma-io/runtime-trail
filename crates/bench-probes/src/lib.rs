//! The Phase 1 memory-path benchmark probes.
//!
//! This crate is [`layer-bench`] in the boundary table: a measurement
//! harness that composes the real layers it measures — the telemetry
//! model, the storage contract, the in-memory driver, the OTLP ingestion
//! pipeline — and nothing else in the workspace. It is not a product
//! surface: no transport, no API, no UI, it ships nowhere, and nothing
//! imports it. `scripts/bench/cases/*` execute its binaries; the harness
//! (`scripts/bench/run-all.sh`, owner
//! [docs/benchmarks/README.md](../../docs/benchmarks/README.md)) files
//! their records beside the commit and machine that produced them.
//!
//! [`layer-bench`]: ../../module-boundaries.config.mjs
//!
//! # What the probes measure, and what they refuse to say
//!
//! Phase 1's job is the first real measurement of the memory path —
//! payload → decode → model conversion → queue → storage — with numbers
//! attached to machines, per the law that targets and measurements live
//! in separate places (`docs/architecture/runtime-constraints.md` owns
//! the targets, `docs/benchmarks/README.md` owns the measurement
//! contract). So every probe:
//!
//! - sends **real OTLP wire bytes** through the **real pipeline** into
//!   the **real in-memory store** — never shortcut structs, never a
//!   stand-in for the decode step (the wire writer and its semantic
//!   fixtures live in [`wire`]);
//! - reads memory from the kernel (`/proc/self/status` → `VmRSS`,
//!   `VmHWM`) and reports it **with units**, next to the model's
//!   *accounted* bytes, so the real-to-accounted margin the contracts
//!   name is computable from the raw record;
//! - **measures and exits zero when it ran** — no probe here gates a
//!   number against a target. Gating is Phase 6
//!   (`runtime-constraints.md`, "Enforcement trajectory"); a probe that
//!   cannot measure exits non-zero, loudly.
//!
//! # What is deliberately not here
//!
//! - **A counting allocator.** The honest instrument for separating real
//!   heap from the accounted formula would be a `GlobalAlloc` wrapper —
//!   and that requires `unsafe impl GlobalAlloc`, which this workspace
//!   forbids (`unsafe_code = "forbid"`, workspace lints; the model
//!   already recorded the same trade-off where its accounting constants
//!   are pinned). The probes measure real memory the only honest way
//!   left: the kernel's page accounting, beside the accounted totals.
//! - **The admission-ledger release path.** The pipeline owns its ledger
//!   and releases entries only through the composition root's eviction
//!   hook (ADR 0008, wired in Phase 1 wave 3). A probe outside the
//!   composition root cannot wire that hook, so metric/span loads
//!   measure ledger growth on top of residency — the overload probe
//!   reports that state explicitly instead of hiding it, and the
//!   retention probe uses log records, the record kind whose residency
//!   the retention law alone bounds.

pub mod json;
pub mod probes;
pub mod rss;
pub mod served;
pub mod shapes;
pub mod wire;

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    /// The probes compose the four layers the boundary table allows them,
    /// and the composition is real: a store is constructed, a pipeline is
    /// constructed over a contract queue, and one log record walks the
    /// whole path — wire bytes in, resident record out. This is the
    /// memory-path integration the probes exist to measure, pinned as a
    /// test so it cannot quietly stop being the real path.
    #[test]
    fn the_whole_memory_path_walks_wire_to_residency() {
        // A ceiling above one export's load, so the equality below is the
        // point: every record the pipeline admitted is resident — nothing
        // was evicted in between.
        let config = super::probes::RetentionConfig {
            fill_exports: 1,
            plateau_exports: 0,
            store_config: runtime_trail_storage_memory::MemoryConfig {
                max_records: 10_000,
                max_accounted_bytes: 64 * 1024 * 1024,
                admission_window: std::time::Duration::from_secs(3_600),
                series_cap: 10,
            },
            ..Default::default()
        };
        let report =
            super::probes::run_retention(&config).expect("one export through the whole path");
        assert_eq!(report.exports_ingested, 1);
        assert!(report.admitted_records > 0);
        assert_eq!(report.store_stats.resident_records, report.admitted_records);
        assert!(report.store_stats.accounted_bytes > 0);
    }
}
