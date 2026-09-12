//! The embedded file-backed storage mode, powered by `SQLite`.
//!
//! Decision record: `docs/decisions/0003-storage-strategy.md` (why `SQLite`,
//! why never an external database). Rules this driver must honour:
//! `docs/architecture/storage-model.md`. This crate is a
//! [`layer-storage-driver`] — it implements the storage abstraction and
//! depends on nothing above it.
//!
//! [`layer-storage-driver`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! Declared boundary only — no `SQLite` dependency, no implementation. The
//! driver lands with Phase 3 (persistence), per `docs/roadmap/phases.md`.
//! Until then the only thing committed here is the boundary itself, so the
//! dependency law and the canaries see the shape the product will have.
//! The driver it names will implement
//! [`TelemetryStore`](runtime_trail_storage::TelemetryStore) — the one
//! storage contract, like every mode before it.

/// The `SQLite`-backed backend.
///
/// Not constructible at bootstrap: constructing a file-backed store before
/// the persistence phase exists would be a fake success. The type is the
/// boundary; `NAME` is what a composition root will report for this mode.
#[derive(Debug, Default, Clone, Copy)]
pub struct FileBackedStore {
    _private: (),
}

impl FileBackedStore {
    /// The mode name surfaces report for this driver.
    pub const NAME: &'static str = "file-backed";
}

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    #[test]
    fn depends_on_the_storage_contract() {
        assert!(!runtime_trail_storage::VERSION.is_empty());
    }
}
