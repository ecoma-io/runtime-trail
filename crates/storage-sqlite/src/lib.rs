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
//! # What this driver is
//!
//! [`FileBackedStore`] implements the one storage contract —
//! [`TelemetryStore`](runtime_trail_storage::TelemetryStore) — with the
//! memory mode's bounded-retention law, running the same code shapes the
//! in-memory driver runs: shelves ordered by
//! [`AdmissionKey`](runtime_trail_storage::AdmissionKey), the series table
//! charging identity to the byte ceiling once per distinct stream, the
//! ceilings checked after every keep, "first ceiling hit wins", the
//! series cap refusing instead of evicting, and the hook law — a keep
//! that inserted nothing is reported, identity ends where residency does
//! ([ADR 0008](../../docs/decisions/0008-admission-ledger-design.md)).
//!
//! What makes it file-backed: every residency change is mirrored to a
//! `SQLite` file — one transaction per keep (and per retention pass), so
//! on-disk and in-memory residency never disagree by half a keep — and a
//! reopened session starts from what a closed one kept, rehydrated into
//! the same shelves. The parity test suite at
//! `crates/storage/tests/shared/` compiles once and runs against both
//! drivers, so the file-backed mode is parity-by-construction, and the
//! driver's own tests pin the file-specific contract: reopen durability,
//! crash recovery, corrupt-file behaviour, and memory-vs-file symmetry.
//!
//! # Durability, in short
//!
//! Memory stays authoritative on the hot path: a keep is the same map
//! work the memory store does, with the write-through transaction staged
//! from the very changes residency made. `journal_mode=WAL` with
//! `synchronous=FULL` makes every committed change durable against
//! process crash and power loss; a graceful close checkpoints
//! (`PRAGMA wal_checkpoint(TRUNCATE)`) so "copy one file, reopen the
//! session" holds when the session ends cleanly; after a crash the next
//! open recovers the committed tail. An on-disk failure never stalls
//! admission — the change stays resident, the store logs the degradation
//! and keeps running, and the reopen-time divergence (a missing row, a
//! resurrected eviction) is the log's to surface. See the crate docs of
//! [`crate::store`] and `docs/architecture/storage-model.md` for the full
//! contract, including what reopen re-checks (capacity ceilings, never
//! the admission window — the store owns no clock).

mod config;
mod db;
mod shelf;
mod store;

pub use config::{
    DEFAULT_ADMISSION_WINDOW, DEFAULT_MAX_ACCOUNTED_BYTES, DEFAULT_MAX_RECORDS, DEFAULT_SERIES_CAP,
    FileBackedConfig,
};
pub use store::{FileBackedStore, OpenError};

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
