//! The Correlation Engine: cross-signal identity and adjacency.
//!
//! Which log records belong to which trace, which metric windows contain
//! which traces, which spans share a request — every correlation strategy
//! lives here and nowhere else ([`layer-correlation`]). The contract is
//! `docs/architecture/investigation-model.md`; the dependency law is
//! `docs/architecture/boundaries.md`.
//!
//! [`layer-correlation`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! Scaffolding only; strategies land with Phase 2 —
//! `docs/roadmap/phases.md`.

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    #[test]
    fn depends_on_the_telemetry_model() {
        assert!(!runtime_trail_telemetry_model::VERSION.is_empty());
    }
}
