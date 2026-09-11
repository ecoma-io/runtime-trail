//! The in-memory storage mode: ephemeral, zero-setup, and the default way
//! `runtime-trail` runs.
//!
//! Memory mode is first-class, not a fallback —
//! `docs/architecture/storage-model.md` owns that rule and the bounded
//! retention this driver must enforce once it keeps real telemetry. This
//! crate is a [`layer-storage-driver`]: it implements the storage
//! abstraction and depends on nothing above it.
//!
//! [`layer-storage-driver`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! Scaffolding only; real keeping lands with Phase 1 —
//! `docs/roadmap/phases.md`.

use runtime_trail_storage::StorageBackend;

/// The in-memory backend.
#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryStore;

impl MemoryStore {
    /// The mode name surfaces report for this driver.
    pub const NAME: &'static str = "memory";

    /// The mode name for this instance, so composition roots report the mode
    /// of the concrete store they actually hold.
    #[must_use]
    pub const fn mode_name(&self) -> &'static str {
        Self::NAME
    }
}

impl StorageBackend for MemoryStore {}

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    #[test]
    fn is_a_storage_backend() {
        fn assert_backend<B: runtime_trail_storage::StorageBackend>() {}
        assert_backend::<super::MemoryStore>();
    }

    #[test]
    fn depends_on_the_storage_contract() {
        assert!(!runtime_trail_storage::VERSION.is_empty());
    }
}
