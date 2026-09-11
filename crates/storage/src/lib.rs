//! The Storage Abstraction: the contract for keeping telemetry and answering
//! the query engine against it.
//!
//! This crate owns the *contract only* — it knows the telemetry model and
//! nothing else in this repository ([`layer-storage`]). Concrete modes live
//! behind it, one crate each (`storage-memory`, `storage-sqlite`); only the
//! composition root may name a driver. The rules this contract must honour —
//! bounded retention in every mode, persistence never on the ingestion
//! critical path, memory mode first-class — are
//! `docs/architecture/storage-model.md`; the dependency law is
//! `docs/architecture/boundaries.md`.
//!
//! [`layer-storage`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! The trait below is the placeholder shape of the contract, present so the
//! drivers and the boundary law have something real to bind to. Its real
//! surface lands with Phase 1 (memory mode) — see `docs/roadmap/phases.md`.

/// The storage contract every backend implements.
///
/// Deliberately empty at bootstrap: each capability added here (put, window
/// scans, eviction visibility) must be specified in
/// `docs/architecture/storage-model.md` before it is coded.
pub trait StorageBackend {}

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    /// The declared dependency edge on the telemetry model is a real
    /// compile-time fact: the manifest names it and this test makes the
    /// compiler agree. Archkeep judges the same edge statically; this is the
    /// build's side of that proof.
    #[test]
    fn depends_on_the_telemetry_model() {
        assert!(!runtime_trail_telemetry_model::VERSION.is_empty());
    }
}
