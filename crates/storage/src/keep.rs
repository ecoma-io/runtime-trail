//! The keep hand-off: what it means for a store to take an admitted record.
//!
//! Ingestion admits a record (the model's ledger) and then hands it to the
//! active store — the admitted-then-kept pipeline of
//! `docs/architecture/storage-model.md`. The store sits on the ingestion hot
//! path, so the hand-off is a plain function call with an outcome, never an
//! I/O wait: a store that cannot keep a record under its ceilings says so in
//! the outcome and its counters, and never stalls.

use std::sync::Arc;

use runtime_trail_telemetry_model::{EntityId, StreamIdentity};

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
    /// could keep it. Counted as `StoreStats::oversized_refusals`. Nothing
    /// was inserted, so the store reports the refused record through
    /// [`EvictionHook::keep_refused`] — its ledger identity ends with the
    /// refusal and a re-delivery re-admits fresh; the ceiling refuses this
    /// delivery, it does not blacklist the record.
    Oversized,
    /// Refused before anything was inserted or evicted: keeping the point
    /// would establish a **new distinct stream** while the store already
    /// holds the series cap's worth of resident streams
    /// ([runtime-constraints.md](../../docs/architecture/runtime-constraints.md)
    /// owns the number; the in-memory store starts with it as
    /// `MemoryConfig::series_cap`). The refusal is non-retryable — the
    /// session is at its series ceiling and retrying cannot shrink it —
    /// and it never evicts: the store removes nothing to make room for a
    /// new series. Counted as `StoreStats::kept_out_series_cap`. Nothing
    /// was inserted, so the store reports the refused record through
    /// [`EvictionHook::keep_refused`] and its ledger identity ends with the
    /// refusal. That is what keeps the documented re-attempt reachable: a
    /// slot still frees only when a stream's last resident point is
    /// evicted, but the refused delivery's ledger entry is gone, so a
    /// re-delivery re-admits as a fresh admission — not a collapse onto an
    /// identity whose record never entered residency — and the re-attempt
    /// is measured against the cap as it stands then.
    SeriesCapReached,
    /// Refused before anything was inserted or evicted: the stream
    /// identity the point arrives with carries an accounted size that
    /// alone exceeds the accounted-byte ceiling. The store charges each
    /// distinct resident stream's identity to the ceiling exactly once —
    /// so keeping the point would add a charge already over the cap, and
    /// no amount of eviction could ever satisfy the ceiling. The refusal
    /// names the ceiling it was measured against and the identity's
    /// accounted size. Non-retryable — the identity is payload content;
    /// retrying cannot shrink it — and it never evicts. Counted as
    /// `StoreStats::identity_over_ceiling_refusals`. Nothing was inserted,
    /// so the store reports the refused record through
    /// [`EvictionHook::keep_refused`]: the refusal is final for THIS
    /// delivery, never for the identity forever — the ledger identity ends
    /// with the refusal, so a re-delivery re-admits fresh (and, while the
    /// ceiling stands, refuses again — honestly, and leaving nothing
    /// behind).
    IdentityOverCeiling {
        /// The accounted-byte ceiling the keep was measured against.
        ceiling: u64,
        /// The stream identity's accounted size.
        identity_bytes: u64,
    },
}

impl KeepOutcome {
    /// How many records the retention law removed during this keep; zero
    /// for every refusal.
    #[must_use]
    pub const fn evicted(&self) -> u64 {
        match self {
            Self::Kept { evicted } => *evicted,
            Self::Duplicate
            | Self::Oversized
            | Self::SeriesCapReached
            | Self::IdentityOverCeiling { .. } => 0,
        }
    }
}

/// The lifecycle hook: what runs when a record ends its residency — or
/// never enters it.
///
/// This is the inverted dependency the identity lifecycle needs
/// ([ADR 0008](../../docs/decisions/0008-admission-ledger-design.md)):
/// a record's ledger identity must end wherever the record's residency
/// story ends — a re-delivery afterwards is admitted fresh — and the
/// contract must say so without naming the admission ledger's type. The
/// composition root implements this trait over the ledger (`forget`,
/// `release_stream`) and hands the store the implementation; storage
/// speaks only "an entity id left residency", "a stream's last resident
/// point is gone", and "a keep was refused with nothing inserted".
///
/// # Ordering and the must-not-panic rule
///
/// Every hook delivery fires **after the store's own state has settled**:
/// for an eviction, indexes, shelves, the stream-residency table and the
/// store's counters are all updated before the first hook method runs; for
/// a keep refusal, the refusal's counter is incremented and residency is
/// exactly as it was — nothing was inserted — before the report goes out.
/// A misbehaving hook can therefore never corrupt the store — but it runs
/// where the composition root releases a record's ledger identity, so a
/// panicking hook would leave identity behind after its record's story
/// ended. **A hook must not panic.** Fallibility is the composition root's
/// to wrap: the store does not catch, and a panic propagates out of the
/// keep after the store's own bookkeeping is complete. A caller that
/// catches the unwind finds a consistent store whose
/// [`StoreStats::total_evictions`](crate::StoreStats::total_evictions)
/// exceeds [`StoreStats::hook_deliveries`](crate::StoreStats::hook_deliveries)
/// or whose keep-refusal counters exceed
/// [`StoreStats::refused_hook_deliveries`](crate::StoreStats::refused_hook_deliveries)
/// — the divergence is observable, never silent.
///
/// Per removed record the store calls [`EvictionHook::evicted`] once, in
/// eviction order (oldest first); when that removal was the stream's last
/// resident point, it then calls [`EvictionHook::stream_released`] once for
/// the stream — the record's hook before its stream's. Per refused keep it
/// calls [`EvictionHook::keep_refused`] once.
///
/// The hook bounds are also the store's: [`TelemetryStore`](crate::TelemetryStore)
/// promises `Send + Sync`, which the hook field rides along with, so the
/// trait requires it here rather than at every wiring site.
pub trait EvictionHook: Send + Sync {
    /// The record named by `entity` was just removed from residency.
    fn evicted(&mut self, entity: EntityId);

    /// The stream whose identity is handed over had its **last resident
    /// point** removed: the store's per-stream residency count reached
    /// zero. The identity is shared as stored — the same interned
    /// allocation every resident point of the stream referenced (ADR 0008)
    /// — so the composition root can release the ledger's interning by
    /// content without a copy.
    fn stream_released(&mut self, stream: &Arc<StreamIdentity>);

    /// A keep was refused with **nothing inserted** —
    /// [`KeepOutcome::Oversized`], [`KeepOutcome::SeriesCapReached`] or
    /// [`KeepOutcome::IdentityOverCeiling`] — for the record admission had
    /// handed over as `entity`. The refused record is nowhere in the store,
    /// but admission already gave it a ledger identity (and interned its
    /// stream); this report is what lets the composition root end that
    /// identity, so the pipeline's next delivery of the same natural
    /// identity re-admits fresh instead of collapsing onto an entry whose
    /// record never entered residency (ADR 0008).
    ///
    /// `stream` is the interned identity the refused record arrived with,
    /// when it carried one: a metric point's refusal always does, a span's
    /// or log record's never does — neither kind has a stream identity.
    /// [`KeepOutcome::Duplicate`] is never reported: the record a duplicate
    /// names **is** resident, and its identity must stand.
    fn keep_refused(&mut self, entity: EntityId, stream: Option<&Arc<StreamIdentity>>);
}
