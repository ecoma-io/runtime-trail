//! The Investigation API: the single contract both surfaces consume.
//!
//! The Loom UI (over HTTP, via the server) and the MCP server (in-process)
//! are two equal clients of this crate and of nothing else in the core —
//! [`layer-api`] depends on the query and correlation engines and the
//! telemetry model, and never on storage drivers, ingestion, or any
//! presentation or distribution surface. The contract is
//! `docs/architecture/investigation-model.md`; the dependency law is
//! `docs/architecture/boundaries.md`; the two-surface decision is
//! `docs/decisions/0002-investigation-first-architecture.md`.
//!
//! [`layer-api`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! Scaffolding only; the API surface lands with Phase 2 —
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
    fn depends_on_the_engines_and_the_model() {
        assert!(!runtime_trail_query::VERSION.is_empty());
        assert!(!runtime_trail_correlation::VERSION.is_empty());
        assert!(!runtime_trail_telemetry_model::VERSION.is_empty());
    }
}
