//! The storage contract: the trait every storage mode implements.
//!
//! Storage stays storage. The contract's retrieval primitives are
//! deliberately narrow — get by entity id, ordered scans, count/size
//! introspection — because everything that looks like a *question about the
//! data* (find the slow trace, the related logs, the error rates of a
//! service, the relations between signals) belongs to query and correlation
//! (`docs/architecture/investigation-model.md`), which read through this
//! abstraction. Nothing here filters by content, name, time window or
//! anything else a question would ask; a driver that grew such a primitive
//! would grow a second door around the engines.

use std::sync::Arc;

use runtime_trail_telemetry_model::{
    AdmissionTime, Admitted, EntityId, LogRecord, MetricPoint, Span, StreamIdentity,
};

use crate::keep::KeepOutcome;
use crate::order::{AdmissionKey, ScanPage};
use crate::stats::StoreStats;

/// One resident metric point and the interned stream identity it belongs
/// to.
///
/// Admission hands the stream's one shared [`StreamIdentity`] allocation to
/// the store together with each point (the ledger interns it — ADR 0008);
/// retrieval hands both back, shared as stored.
#[derive(Clone, Debug)]
pub struct PointView {
    /// The point, exactly as admitted.
    pub point: Arc<MetricPoint>,
    /// The interned stream identity the point was admitted under.
    pub stream: Arc<StreamIdentity>,
}

/// The storage contract every storage mode implements.
///
/// One instance per runtime session. Modes are picked once at startup by
/// the composition root ([ADR 0003](../../docs/decisions/0003-storage-strategy.md)),
/// never mid-session: retention ceilings are startup configuration, so the
/// trait has no reconfiguration surface. Implementations must preserve:
///
/// - **the residency order** ([`AdmissionKey`]) in both eviction ("oldest"
///   first) and scan order — one deterministic sequence;
/// - **bounded retention** in every mode
///   ([storage-model.md](../../docs/architecture/storage-model.md)): no
///   unbounded mode, no grow-then-evict;
/// - **the keep hand-off is never on I/O**: persistence never blocks
///   admission (memory mode trivially; a file-backed mode by design);
/// - **the eviction hook** ([`EvictionHook`](crate::EvictionHook)) fires
///   once per evicted record, so identity ends exactly with residency
///   (ADR 0008).
///
/// The trait is object-safe on purpose: the composition root holds
/// `Box<dyn TelemetryStore>` and names the concrete driver only where it
/// wires one.
pub trait TelemetryStore {
    /// Hands one admitted span to the store: the admitted-then-kept
    /// pipeline's write side. The record is shared as handed in — a driver
    /// must not copy the payload the ledger already owns (ADR 0008).
    ///
    /// Never blocks on I/O. The outcome reports the residency decision;
    /// see [`KeepOutcome`].
    fn keep_span(&mut self, admitted: Admitted<Arc<Span>>) -> KeepOutcome;

    /// Hands one admitted log record to the store. Log records have no
    /// natural identity, so every keep is a distinct record; see
    /// [`KeepOutcome::Duplicate`] for what a repeated entity id means.
    fn keep_log_record(&mut self, admitted: Admitted<Arc<LogRecord>>) -> KeepOutcome;

    /// Hands one admitted metric point to the store, together with the
    /// interned stream identity admission resolved for it. The store keeps
    /// the shared stream allocation as given; it does not intern — that is
    /// the ledger's job (ADR 0008).
    fn keep_metric_point(
        &mut self,
        admitted: Admitted<Arc<MetricPoint>>,
        stream: Arc<StreamIdentity>,
    ) -> KeepOutcome;

    /// The resident span known by `entity`, shared as stored; `None` when
    /// no such span is resident — including after its eviction. Absence is
    /// a normal answer, never an error and never a stale record.
    #[must_use]
    fn span(&self, entity: EntityId) -> Option<Arc<Span>>;

    /// The resident log record known by `entity`, shared as stored.
    #[must_use]
    fn log_record(&self, entity: EntityId) -> Option<Arc<LogRecord>>;

    /// The resident metric point known by `entity`, with its stream.
    #[must_use]
    fn metric_point(&self, entity: EntityId) -> Option<PointView>;

    /// Scans resident spans in residency order, starting strictly after
    /// `after` (from the oldest record when `None`), yielding at most
    /// `limit` records — a location primitive over the resident set, never
    /// a content filter. A `limit` of zero yields an empty page and no
    /// cursor.
    #[must_use]
    fn scan_spans(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<Span>>;

    /// Scans resident log records, exactly like [`TelemetryStore::scan_spans`].
    #[must_use]
    fn scan_log_records(
        &self,
        after: Option<AdmissionKey>,
        limit: usize,
    ) -> ScanPage<Arc<LogRecord>>;

    /// Scans resident metric points, exactly like
    /// [`TelemetryStore::scan_spans`].
    #[must_use]
    fn scan_metric_points(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<PointView>;

    /// Runs the retention law now, against the reference reading `now`:
    /// expires records whose admission time fell out of the window, and
    /// restores any ceiling already exceeded. Returns how many records the
    /// pass evicted (counted in [`StoreStats`] as always). The composition
    /// root calls this when it alone knows time has passed — on a timer or
    /// before answering — because the store owns no clock.
    fn enforce_retention(&mut self, now: AdmissionTime) -> u64;

    /// Pushes the current admission-anomaly total through: the pass-through
    /// that surfaces admission's recorded conflicts in the store's stats
    /// without this contract naming the ledger. Latest value wins; the
    /// composition root re-pushes as the ledger records more.
    fn observe_admission_anomalies(&mut self, total: u64);

    /// The store's residency and retention history.
    #[must_use]
    fn stats(&self) -> StoreStats;

    /// The mode name a surface reports for this store — `"memory"` for the
    /// in-memory mode, matching the driver's own `NAME`.
    #[must_use]
    fn mode_name(&self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The composition root holds the contract as a trait object. This
    /// compiles only while the trait stays object-safe: a non-object-safe
    /// addition would break the one way `layer-app` is allowed to hold a
    /// store, so it is a contract break, caught here.
    #[test]
    fn the_contract_is_held_as_a_trait_object() {
        fn stats_of(store: &dyn TelemetryStore) -> StoreStats {
            store.stats()
        }
        fn stats_of_boxed(store: Box<dyn TelemetryStore>) -> StoreStats {
            let stats = stats_of(store.as_ref());
            drop(store); // the box is owned and dropped: the full trait-object shape
            stats
        }
        let _ = stats_of_boxed; // referenced, so the trait-object shapes are checked
    }
}
