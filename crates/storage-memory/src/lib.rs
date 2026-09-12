//! The in-memory storage mode: ephemeral, zero-setup, and the default way
//! `runtime-trail` runs.
//!
//! Memory mode is first-class, not a fallback —
//! [`docs/architecture/storage-model.md`](../../docs/architecture/storage-model.md)
//! owns that rule and the bounded retention this driver enforces: the
//! memory-mode ceilings of
//! [`docs/architecture/runtime-constraints.md`](../../docs/architecture/runtime-constraints.md)
//! (2,000,000 records, 256 MiB accounted, a 24 h window, 100,000
//! distinct streams by default;
//! [`MemoryConfig`] starts a store with them or with what an operator
//! tuned — never changed afterwards), enforced in-process after every keep
//! and on every retention pass. This crate is a [`layer-storage-driver`]:
//! it implements the storage abstraction and depends on nothing above it.
//!
//! [`layer-storage-driver`]: ../../docs/architecture/boundaries.md
//!
//! # What this driver promises
//!
//! - **Oldest is admission time**, never emitter event time: the record
//!   with the smallest [`AdmissionKey`](runtime_trail_storage::AdmissionKey)
//!   is evicted first, and scans walk the same order. Ties break by entity
//!   id — the order is total and documented there.
//! - **First ceiling hit wins**: whichever ceiling is exceeded first drives
//!   eviction, one record at a time until every ceiling is satisfied again,
//!   each eviction attributed to the ceiling that triggered it and counted
//!   in [`StoreStats`](runtime_trail_storage::StoreStats).
//! - **The byte ceiling counts what residency pins**: a resident record's
//!   accounted size charges the ceiling once, and so does a resident
//!   stream's identity — charged exactly once per distinct stream
//!   ([`StreamIdentity::accounted_size`](runtime_trail_telemetry_model::StreamIdentity::accounted_size)
//!   in the telemetry model), no
//!   matter how many of its points stand, and released with its last
//!   point. `StoreStats` reports the split (records against identities),
//!   so an operator sees what the ceiling actually held.
//! - **The series cap bounds distinct resident streams** (100,000 by
//!   default, zero legal): a keep that would establish a new stream
//!   beyond the cap is refused by name
//!   ([`KeepOutcome::SeriesCapReached`](runtime_trail_storage::KeepOutcome)),
//!   never answered by eviction. The slot frees exactly when the stream's
//!   last resident point leaves.
//! - **Eviction ends identity**: for each evicted record the wired
//!   [`EvictionHook`](runtime_trail_storage::EvictionHook) fires with the
//!   entity id — the composition root's hook calls the admission ledger's
//!   `forget`, so a re-delivery after eviction is admitted fresh (ADR 0008).
//!   The hook also receives each stream identity whose residency just
//!   ended (`stream_released`), so the ledger drops its interning exactly
//!   when the store stops holding it. This crate never names the ledger's
//!   type; the inversion is the hook, which must not panic — the store
//!   completes its own removal first, and a panicking hook is visible as
//!   the gap between total evictions and hook deliveries.
//! - **Admission never blocks on I/O** — trivially, in memory mode: a keep
//!   is map inserts plus the retention pass, no filesystem, no network, no
//!   locks beyond the caller's own.
//! - **Records are shared, never copied**: a record is stored exactly as
//!   the `Arc` the admission ledger handed over, and retrieval hands the
//!   same allocation back (ADR 0008).
//!
//! [`layer-storage-driver`]: ../../docs/architecture/boundaries.md

mod config;
mod shelf;
mod store;

pub use config::{
    DEFAULT_ADMISSION_WINDOW, DEFAULT_MAX_ACCOUNTED_BYTES, DEFAULT_MAX_RECORDS, MemoryConfig,
};
pub use store::InMemoryStore;

/// The bootstrap mode marker, kept from the foundation commit.
///
/// This unit struct is what the smoke surfaces (`crates/server`) still hold
/// while no telemetry flows; it is **not** the memory-mode driver — the
/// driver is [`InMemoryStore`]. The marker disappears when the composition
/// root is wired to hold a real `Box<dyn TelemetryStore>`; nothing new may
/// depend on it.
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

impl runtime_trail_storage::StorageBackend for MemoryStore {}

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

    /// The real driver implements the real contract, through the trait —
    /// the same path every consumer above the abstraction takes.
    #[test]
    fn the_in_memory_store_implements_the_contract() {
        use runtime_trail_storage::TelemetryStore;

        let mut store = super::InMemoryStore::new(super::MemoryConfig::default(), None);
        store.observe_admission_anomalies(0);
        assert_eq!(store.stats().resident_records, 0);
        assert_eq!(store.config().max_records, 2_000_000);
        assert_eq!(store.mode_name(), "memory");
    }
}
