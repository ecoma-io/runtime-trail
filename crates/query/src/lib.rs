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
//! In progress - the machinery lands with Phase 2's M2 milestone (issue #6).

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod budget;
pub mod cursor;
pub mod engine;
pub mod filters;
pub mod order;
pub mod result;
pub mod spend;
