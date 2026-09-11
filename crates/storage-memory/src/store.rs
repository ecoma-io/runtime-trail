//! The in-memory store: the default mode, bounded retention enforced
//! in-process.
//!
//! Memory mode is first-class, not a fallback
//! (`docs/architecture/storage-model.md`): instant start, no filesystem
//! writes, and the retention ceilings of
//! `docs/architecture/runtime-constraints.md` enforced by this
//! implementation. "Never blocks admission on I/O" holds trivially here —
//! there is no I/O: a keep is a map insert plus the retention pass, plain
//! memory work on the caller's thread.

use std::sync::Arc;

use runtime_trail_storage::{
    AdmissionKey, EvictionCause, EvictionHook, KeepOutcome, PointView, ScanPage, StoreStats,
    TelemetryStore,
};
use runtime_trail_telemetry_model::{
    Accounted, AdmissionTime, Admitted, EntityId, LogRecord, MetricPoint, Span, StreamIdentity,
};

use crate::config::MemoryConfig;
use crate::shelf::Shelf;

/// A metric point as resident: the point and its interned stream identity,
/// both shared as handed in.
#[derive(Debug)]
struct PointSlot {
    point: Arc<MetricPoint>,
    stream: Arc<StreamIdentity>,
}

impl Accounted for PointSlot {
    /// The point's accounted size. The stream identity is interned — one
    /// allocation shared by the ledger, the store and every point of the
    /// stream (ADR 0008) — so counting it per point would multiply it by
    /// residency; it is interning overhead outside the accounted ceilings,
    /// exactly like the ledger's per-record entries.
    fn accounted_size(&self) -> usize {
        self.point.accounted_size()
    }
}

/// The cumulative counters behind [`StoreStats`].
#[derive(Debug, Default)]
struct Counters {
    evicted_for_record_ceiling: u64,
    evicted_for_accounted_bytes_ceiling: u64,
    evicted_for_admission_window: u64,
    oversized_refusals: u64,
    duplicate_keeps: u64,
}

/// The in-memory store.
///
/// Built once with its [`MemoryConfig`] and — optionally — the
/// [`EvictionHook`] the composition root wires to the admission ledger's
/// `forget`, so a record's identity ends exactly when its residency does
/// (ADR 0008). Ceilings are checked after every keep and on every
/// [`TelemetryStore::enforce_retention`] pass; when any is exceeded the
/// oldest record is evicted — smallest [`AdmissionKey`] — until all are
/// satisfied again. Each eviction is attributed to the first ceiling found
/// violated at that moment, so "first ceiling hit wins" is observable per
/// record, and the hook fires for it.
pub struct InMemoryStore {
    config: MemoryConfig,
    window_nanos: u64,
    hook: Option<Box<dyn EvictionHook>>,
    spans: Shelf<Span>,
    logs: Shelf<LogRecord>,
    points: Shelf<PointSlot>,
    counters: Counters,
    anomalies: u64,
}

impl InMemoryStore {
    /// The mode name surfaces report for this driver.
    pub const NAME: &'static str = "memory";

    /// An empty store bounded by `config`, reporting evictions through
    /// `hook` when one is wired.
    #[must_use]
    pub fn new(config: MemoryConfig, hook: Option<Box<dyn EvictionHook>>) -> Self {
        let window_nanos = u64::try_from(config.admission_window.as_nanos()).unwrap_or(u64::MAX);
        Self {
            config,
            window_nanos,
            hook,
            spans: Shelf::new(),
            logs: Shelf::new(),
            points: Shelf::new(),
            counters: Counters::default(),
            anomalies: 0,
        }
    }

    /// The ceilings this store was started with: startup configuration,
    /// never mid-session state.
    #[must_use]
    pub const fn config(&self) -> &MemoryConfig {
        &self.config
    }

    /// The reference reading for one retention pass. The store owns no
    /// clock (`docs/architecture/telemetry-model.md` keeps the runtime's
    /// time out of the model): a keep passes against the kept record's own
    /// admission time — the newest fact the caller has supplied about now —
    /// and [`TelemetryStore::enforce_retention`] takes the composition
    /// root's current reading.
    fn first_violation(&self, now: AdmissionTime) -> Option<EvictionCause> {
        if self.resident_records() > self.config.max_records {
            return Some(EvictionCause::RecordCeiling);
        }
        if self.accounted_bytes() > self.config.max_accounted_bytes {
            return Some(EvictionCause::AccountedBytesCeiling);
        }
        let oldest = [
            self.spans.smallest_key(),
            self.logs.smallest_key(),
            self.points.smallest_key(),
        ]
        .into_iter()
        .flatten()
        .min();
        // The window compares admission times only: the oldest resident
        // record is expired when `now` is at least one window past its
        // admission.
        let expired = oldest?.admitted_at().as_unix_nano();
        let now_nanos = now.as_unix_nano();
        (now_nanos.saturating_sub(expired) >= self.window_nanos)
            .then_some(EvictionCause::AdmissionWindow)
    }

    /// Runs the retention law to completion against `now`, evicting the
    /// oldest record for as long as any ceiling is violated. Each eviction
    /// is counted under the cause that triggered it and reported through
    /// the hook, so identity-keeping wiring sees exactly what residency
    /// ended.
    fn enforce_against(&mut self, now: AdmissionTime) -> u64 {
        let mut evicted = 0;
        while let Some(cause) = self.first_violation(now) {
            let Some(key) = self.pop_oldest() else {
                break;
            };
            match cause {
                EvictionCause::RecordCeiling => self.counters.evicted_for_record_ceiling += 1,
                EvictionCause::AccountedBytesCeiling => {
                    self.counters.evicted_for_accounted_bytes_ceiling += 1;
                }
                EvictionCause::AdmissionWindow => self.counters.evicted_for_admission_window += 1,
            }
            if let Some(hook) = self.hook.as_mut() {
                hook.evicted(key.entity());
            }
            evicted += 1;
        }
        evicted
    }

    /// Removes the oldest record from whichever shelf holds it and returns
    /// its key. The three shelves cannot hold equal keys — an entity id
    /// names exactly one resident record — so the minimum picks exactly
    /// one shelf.
    fn pop_oldest(&mut self) -> Option<AdmissionKey> {
        let oldest = [
            self.spans.smallest_key(),
            self.logs.smallest_key(),
            self.points.smallest_key(),
        ]
        .into_iter()
        .flatten()
        .min()?;
        if Some(oldest) == self.spans.smallest_key() {
            return self.spans.pop_smallest().map(|(key, _)| key);
        }
        if Some(oldest) == self.logs.smallest_key() {
            return self.logs.pop_smallest().map(|(key, _)| key);
        }
        self.points.pop_smallest().map(|(key, _)| key)
    }

    /// Records currently resident, all shelves together.
    fn resident_records(&self) -> u64 {
        let total = self.spans.len() + self.logs.len() + self.points.len();
        u64::try_from(total).unwrap_or(u64::MAX)
    }

    /// Accounted bytes currently resident, summed over the shelves. A
    /// point's interned stream identity is shared, not copied, so it is
    /// interning overhead outside the ceilings (see [`PointSlot`]).
    fn accounted_bytes(&self) -> u64 {
        self.spans
            .accounted_bytes()
            .saturating_add(self.logs.accounted_bytes())
            .saturating_add(self.points.accounted_bytes())
    }

    fn stats_from(&self) -> StoreStats {
        StoreStats {
            resident_records: self.resident_records(),
            resident_spans: u64::try_from(self.spans.len()).unwrap_or(u64::MAX),
            resident_log_records: u64::try_from(self.logs.len()).unwrap_or(u64::MAX),
            resident_metric_points: u64::try_from(self.points.len()).unwrap_or(u64::MAX),
            accounted_bytes: self.accounted_bytes(),
            evicted_for_record_ceiling: self.counters.evicted_for_record_ceiling,
            evicted_for_accounted_bytes_ceiling: self.counters.evicted_for_accounted_bytes_ceiling,
            evicted_for_admission_window: self.counters.evicted_for_admission_window,
            oversized_refusals: self.counters.oversized_refusals,
            duplicate_keeps: self.counters.duplicate_keeps,
            admission_anomalies: self.anomalies,
        }
    }
}

/// The accounted size of one record as a `u64`, saturating: ceilings are
/// `u64` and a record at `usize::MAX` bytes refuses anyway.
fn accounted_u64(record: &(impl Accounted + ?Sized)) -> u64 {
    u64::try_from(record.accounted_size()).unwrap_or(u64::MAX)
}

/// The shared keep sequence: the size refusal, the duplicate refusal, the
/// insert. Returns `Ok(the reference now for the retention pass)` when the
/// record entered residency, `Err(the counted outcome)` when it was
/// refused; the caller runs the retention pass itself, so every kind's
/// keep is this one sequence.
fn keep_on_shelf<R: Accounted>(
    shelf: &mut Shelf<R>,
    admitted: Admitted<Arc<R>>,
    size: u64,
    max_accounted_bytes: u64,
    counters: &mut Counters,
) -> Result<AdmissionTime, KeepOutcome> {
    if size > max_accounted_bytes {
        counters.oversized_refusals += 1;
        return Err(KeepOutcome::Oversized);
    }
    if shelf.contains(admitted.entity) {
        counters.duplicate_keeps += 1;
        return Err(KeepOutcome::Duplicate);
    }
    let now = admitted.admitted_at;
    shelf.insert(admitted);
    Ok(now)
}

impl TelemetryStore for InMemoryStore {
    fn keep_span(&mut self, admitted: Admitted<Arc<Span>>) -> KeepOutcome {
        let size = accounted_u64(admitted.record.as_ref());
        match keep_on_shelf(
            &mut self.spans,
            admitted,
            size,
            self.config.max_accounted_bytes,
            &mut self.counters,
        ) {
            Ok(now) => KeepOutcome::Kept {
                evicted: self.enforce_against(now),
            },
            Err(outcome) => outcome,
        }
    }

    fn keep_log_record(&mut self, admitted: Admitted<Arc<LogRecord>>) -> KeepOutcome {
        let size = accounted_u64(admitted.record.as_ref());
        match keep_on_shelf(
            &mut self.logs,
            admitted,
            size,
            self.config.max_accounted_bytes,
            &mut self.counters,
        ) {
            Ok(now) => KeepOutcome::Kept {
                evicted: self.enforce_against(now),
            },
            Err(outcome) => outcome,
        }
    }

    fn keep_metric_point(
        &mut self,
        admitted: Admitted<Arc<MetricPoint>>,
        stream: Arc<StreamIdentity>,
    ) -> KeepOutcome {
        let size = accounted_u64(admitted.record.as_ref());
        let slot = PointSlot {
            point: admitted.record,
            stream,
        };
        let admitted = Admitted {
            entity: admitted.entity,
            admitted_at: admitted.admitted_at,
            record: Arc::new(slot),
        };
        match keep_on_shelf(
            &mut self.points,
            admitted,
            size,
            self.config.max_accounted_bytes,
            &mut self.counters,
        ) {
            Ok(now) => KeepOutcome::Kept {
                evicted: self.enforce_against(now),
            },
            Err(outcome) => outcome,
        }
    }

    fn span(&self, entity: EntityId) -> Option<Arc<Span>> {
        self.spans.get(entity)
    }

    fn log_record(&self, entity: EntityId) -> Option<Arc<LogRecord>> {
        self.logs.get(entity)
    }

    fn metric_point(&self, entity: EntityId) -> Option<PointView> {
        let slot = self.points.get(entity)?;
        Some(PointView {
            point: Arc::clone(&slot.point),
            stream: Arc::clone(&slot.stream),
        })
    }

    fn scan_spans(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<Span>> {
        self.spans.scan_after(after, limit)
    }

    fn scan_log_records(
        &self,
        after: Option<AdmissionKey>,
        limit: usize,
    ) -> ScanPage<Arc<LogRecord>> {
        self.logs.scan_after(after, limit)
    }

    fn scan_metric_points(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<PointView> {
        let page = self.points.scan_after(after, limit);
        ScanPage {
            cursor: page.cursor,
            items: page
                .items
                .into_iter()
                .map(|slot| PointView {
                    point: Arc::clone(&slot.point),
                    stream: Arc::clone(&slot.stream),
                })
                .collect(),
        }
    }

    fn enforce_retention(&mut self, now: AdmissionTime) -> u64 {
        self.enforce_against(now)
    }

    fn observe_admission_anomalies(&mut self, total: u64) {
        self.anomalies = total;
    }

    fn stats(&self) -> StoreStats {
        self.stats_from()
    }

    fn mode_name(&self) -> &'static str {
        Self::NAME
    }
}
