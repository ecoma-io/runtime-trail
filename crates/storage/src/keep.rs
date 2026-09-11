//! The keep hand-off: what it means for a store to take an admitted record.
//!
//! Ingestion admits a record (the model's ledger) and then hands it to the
//! active store — the admitted-then-kept pipeline of
//! `docs/architecture/storage-model.md`. The store sits on the ingestion hot
//! path, so the hand-off is a plain function call with an outcome, never an
//! I/O wait: a store that cannot keep a record under its ceilings says so in
//! the outcome and its counters, and never stalls.

use runtime_trail_telemetry_model::EntityId;

/// Why a store removed a record from residency.
///
/// The retention ceilings
/// ([runtime-constraints.md](../../docs/architecture/runtime-constraints.md)
/// owns the numbers): when any ceiling is exceeded, the store evicts the
/// oldest record — smallest [`AdmissionKey`](crate::AdmissionKey) — until
/// every ceiling is satisfied again. Each eviction is attributed to the
/// first ceiling found violated at the moment of that eviction, so "first
/// ceiling hit wins" is observable per record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictionCause {
    /// The resident-records ceiling: too many records resident.
    RecordCeiling,
    /// The accounted-byte ceiling: too much accounted content resident.
    AccountedBytesCeiling,
    /// The admission window: the record's admission time is older than the
    /// configured window behind the reference reading (`now`).
    AdmissionWindow,
}

/// What one keep hand-off produced.
///
/// There is no silent outcome. A kept record reports how much the retention
/// law removed to make room; a refusal names itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum KeepOutcome {
    /// The record entered residency. `evicted` counts the records the
    /// retention law removed to restore the ceilings — which, under a
    /// degenerate configuration (a zero ceiling) or a non-monotonic caller
    /// clock, can include the record this call kept; residency is the
    /// store's law, not a per-call promise. Count it in
    /// [`StoreStats`](crate::StoreStats) after the call when the difference
    /// matters.
    Kept { evicted: u64 },
    /// An entity id already resident was kept again. The resident record
    /// stands — admitted data is immutable — and the attempt is counted
    /// (`StoreStats::duplicate_keeps`) but nothing else changes.
    Duplicate,
    /// Refused before anything was evicted: the record's accounted size
    /// alone exceeds the accounted-byte ceiling, so no amount of eviction
    /// could keep it. Counted as `StoreStats::oversized_refusals`.
    Oversized,
}

impl KeepOutcome {
    /// How many records the retention law removed during this keep; zero
    /// for a duplicate or an oversized refusal.
    #[must_use]
    pub const fn evicted(&self) -> u64 {
        match self {
            Self::Kept { evicted } => *evicted,
            Self::Duplicate | Self::Oversized => 0,
        }
    }
}

/// The removal hook: what runs when one record leaves residency.
///
/// This is the inverted dependency the eviction lifecycle needs
/// ([ADR 0008](../../docs/decisions/0008-admission-ledger-design.md)):
/// evicting a record must also end its ledger identity — a re-delivery
/// afterwards is admitted fresh — and the contract must say so without
/// naming the admission ledger's type. The composition root implements this
/// trait over the ledger (`forget`) and hands the store the implementation;
/// storage speaks only "an entity id left residency". The hook fires once
/// per evicted record, after the record left residency, in eviction order.
pub trait EvictionHook {
    /// The record named by `entity` was just removed from residency.
    fn evicted(&mut self, entity: EntityId);
}
