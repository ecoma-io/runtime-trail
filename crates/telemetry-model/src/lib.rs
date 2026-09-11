//! The Telemetry Model: `runtime-trail`'s single, presentation-independent
//! representation of OpenTelemetry logs, traces and metrics.
//!
//! This crate is the leaf of the core ([`layer-model`] in the boundary law):
//! it depends on nothing else in this repository, and nothing above it may
//! redefine what telemetry is. The contract this crate must grow into is
//! `docs/architecture/telemetry-model.md`; the dependency law is
//! `docs/architecture/boundaries.md`.
//!
//! [`layer-model`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! Scaffolding only. The concrete model types land with Phase 1 (telemetry
//! ingestion) — see `docs/roadmap/phases.md`.

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }
}
