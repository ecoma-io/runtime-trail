//! The Query Engine: budgeted, deterministic reads over the telemetry
//! model, through the storage abstraction.
//!
//! Every query admits with a budget and the engine refuses or degrades
//! within it; ordering is total and deterministic; cursors are opaque and
//! bound to their query's fingerprint. The contract is
//! [query-model.md](../../docs/architecture/query-model.md); the envelope
//! the API layer later composes from results is
//! [investigation-model.md](../../docs/architecture/investigation-model.md);
//! reads flow through the
//! [storage contract](../../docs/architecture/storage-model.md) - the
//! engine sees the *contract*, never a concrete driver (ADR 0003).
//!
//! # Status
//!
//! Implemented. This crate is the records-flow engine: the budgeted,
//! deterministic, cursor-bound reads over the storage contract that
//! Phase 2's M2 shipped and the adversarial review hardened. The
//! contract is owned by
//! [query-model.md](../../docs/architecture/query-model.md); the
//! envelope the API layer composes from the engine's pages lives in
//! [investigation-model.md](../../docs/architecture/investigation-model.md).

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod budget;
pub mod cursor;
pub mod engine;
pub mod filters;
pub mod lease;
pub mod order;
pub mod result;
pub mod spend;

#[cfg(test)]
mod probe;

// The storage contract, re-exported as the facade (ADR 0011, decision 4):
// this crate is the only reader of the storage contract among the core
// crates, so layer-api names the store trait as
// `runtime_trail_query::TelemetryStore` — and the storage-owned types the
// trait's signature mentions — and never declares a storage edge of its
// own. `Admitted` is the model's wrapper (ADR 0006) and comes from the
// model root. The re-export is the contract surface; the trait itself
// stays owned by `runtime-trail-storage` (ADR 0003: only layer-app names
// concrete drivers, and the engine and its callers program against this
// facade).
pub use runtime_trail_storage::{
    AdmissionKey, KeepOutcome, PointView, ScanItem, ScanPage, StoreStats, TelemetryStore,
};
