//! The Query Engine: filtering, search and aggregation over the telemetry
//! model, through the storage abstraction.
//!
//! Every query carries its budget and the engine refuses work it cannot
//! answer within one — [`layer-query`] sees the storage *contract*, never a
//! concrete driver. The contract is
//! `docs/architecture/investigation-model.md`; budgets are
//! `docs/architecture/runtime-constraints.md`; the dependency law is
//! `docs/architecture/boundaries.md`.
//!
//! [`layer-query`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! Scaffolding only; the engine lands with Phase 2 —
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
    fn depends_on_the_storage_contract_and_the_model() {
        assert!(!runtime_trail_storage::VERSION.is_empty());
        assert!(!runtime_trail_telemetry_model::VERSION.is_empty());
    }
}
