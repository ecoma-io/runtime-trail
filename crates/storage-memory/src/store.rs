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

use std::collections::HashMap;
use std::sync::Arc;

use runtime_trail_storage::{
    AdmissionKey, EvictionCause, EvictionHook, KeepOutcome, PointView, ScanItem, ScanPage,
    StoreStats, TelemetryStore,
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
    /// The point's accounted size only. The stream identity is charged to
    /// the byte ceiling by the store's series table — exactly once per
    /// distinct resident stream, never per point — so counting it here
    /// would multiply it by the stream's residency; counting it nowhere
    /// would let a session of single-point streams park identity content
    /// under a ceiling that only saw the points.
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
    kept_out_series_cap: u64,
    identity_over_ceiling_refusals: u64,
    hook_deliveries: u64,
    refused_hook_deliveries: u64,
}

/// The in-memory store.
///
/// Built once with its [`MemoryConfig`] and — optionally — the
/// [`EvictionHook`] the composition root wires to the admission ledger's
/// `forget` and `release_stream`, so identity ends exactly when a record's
/// residency story ends — eviction **and** keep refusal (ADR 0008).
/// Ceilings are checked after every keep and on every
/// [`TelemetryStore::enforce_retention`] pass; when any is exceeded the
/// oldest record is evicted — smallest [`AdmissionKey`] — until all are
/// satisfied again. Each eviction is attributed to the first ceiling found
/// violated at that moment, so "first ceiling hit wins" is observable per
/// record, and the hook fires for it.
///
/// The byte ceiling bounds **what residency pins**: the shelves' record
/// sums plus each distinct resident stream's identity accounted size,
/// charged once by the [`InMemoryStore`] `series` table when the stream's
/// first point enters residency and released when its last point leaves —
/// the same removal that reports [`EvictionHook::stream_released`]. The
/// series cap bounds how many streams may be resident at all: a keep
/// establishing a new stream beyond it is refused
/// (`KeepOutcome::SeriesCapReached`), never evicting for it. And an
/// identity the ceiling cannot hold on its own is refused before the
/// insert (`KeepOutcome::IdentityOverCeiling`, naming the ceiling and the
/// identity's size): evicting into a charge that is over the cap by
/// itself could never satisfy the ceiling. Every refusal that inserted
/// nothing is reported through [`EvictionHook::keep_refused`] — a refused
/// record is nowhere in the store, so its ledger identity must not stay
/// behind (ADR 0008); a [`KeepOutcome::Duplicate`] reports nothing, the
/// record it names being resident.
pub struct InMemoryStore {
    config: MemoryConfig,
    window_nanos: u64,
    hook: Option<Box<dyn EvictionHook>>,
    spans: Shelf<Span>,
    logs: Shelf<LogRecord>,
    points: Shelf<PointSlot>,
    /// Distinct resident streams and how many resident points reference
    /// each: the series residency count. Keys are the interned identities
    /// the points arrived with, compared by content (the model's identity
    /// law) — a stream enters when its first point is charged and leaves
    /// when its last point is released.
    series: HashMap<Arc<StreamIdentity>, u64>,
    /// The identities' share of the byte ceiling: each series entry's
    /// accounted size, counted once.
    identity_accounted_bytes: u64,
    counters: Counters,
    anomalies: u64,
}

impl InMemoryStore {
    /// The mode name surfaces report for this driver.
    pub const NAME: &'static str = "memory";

    /// An empty store bounded by `config`, reporting evictions and stream
    /// releases through `hook` when one is wired.
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
            series: HashMap::new(),
            identity_accounted_bytes: 0,
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
        // admission. A reference reading *behind* the record's admission
        // is a non-monotonic caller clock; the age has no meaning there,
        // so it expires nothing rather than collapsing through a
        // saturating subtraction into a bogus full-window age.
        let expired = oldest?.admitted_at().as_unix_nano();
        now.as_unix_nano()
            .checked_sub(expired)
            .is_some_and(|age| age >= self.window_nanos)
            .then_some(EvictionCause::AdmissionWindow)
    }

    /// Runs the retention law to completion against `now`, evicting the
    /// oldest record for as long as any ceiling is violated. Each eviction
    /// is counted under the cause that triggered it and reported through
    /// the hook, so identity-keeping wiring sees exactly what residency
    /// ended — including streams whose last resident point went with it.
    fn enforce_against(&mut self, now: AdmissionTime) -> u64 {
        let mut evicted = 0;
        while let Some(cause) = self.first_violation(now) {
            let Some((key, released)) = self.pop_oldest() else {
                break;
            };
            match cause {
                EvictionCause::RecordCeiling => self.counters.evicted_for_record_ceiling += 1,
                EvictionCause::AccountedBytesCeiling => {
                    self.counters.evicted_for_accounted_bytes_ceiling += 1;
                }
                EvictionCause::AdmissionWindow => self.counters.evicted_for_admission_window += 1,
            }
            evicted += 1;
            // The store's own removal — shelves, entity index, stream
            // table, counters — is complete above. The hook runs after it
            // and must not panic (`EvictionHook` owns that law): the
            // record is reported first, then the stream its removal
            // retired, if that removal was the stream's last resident
            // point. A delivery is counted only once its call has
            // returned, so a panicking hook shows up as the gap between
            // `total_evictions()` and `hook_deliveries`.
            if let Some(hook) = self.hook.as_mut() {
                hook.evicted(key.entity());
                self.counters.hook_deliveries += 1;
            }
            if let (Some(hook), Some(stream)) = (self.hook.as_mut(), released.as_ref()) {
                hook.stream_released(stream);
            }
        }
        evicted
    }

    /// Reports one keep refusal through the hook, under the same ordering
    /// law the eviction hook runs under: the refusal's own counter is
    /// already incremented and the store's residency is exactly as it was —
    /// nothing was inserted — before the report goes out. The delivery is
    /// counted only once its call has returned, so a panicking hook shows
    /// up as the gap between
    /// [`StoreStats::total_keep_refusals`](runtime_trail_storage::StoreStats::total_keep_refusals)
    /// and
    /// [`StoreStats::refused_hook_deliveries`](runtime_trail_storage::StoreStats::refused_hook_deliveries).
    fn report_refusal(&mut self, entity: EntityId, stream: Option<&Arc<StreamIdentity>>) {
        if let Some(hook) = self.hook.as_mut() {
            hook.keep_refused(entity, stream);
            self.counters.refused_hook_deliveries += 1;
        }
    }

    /// Removes the oldest record from whichever shelf holds it and returns
    /// its key together with the stream the removal retired — `Some` when
    /// the record was a metric point whose stream just lost its last
    /// resident point. The three shelves cannot hold equal keys — an
    /// entity id names exactly one resident record — so the minimum picks
    /// exactly one shelf.
    fn pop_oldest(&mut self) -> Option<(AdmissionKey, Option<Arc<StreamIdentity>>)> {
        let oldest = [
            self.spans.smallest_key(),
            self.logs.smallest_key(),
            self.points.smallest_key(),
        ]
        .into_iter()
        .flatten()
        .min()?;
        if Some(oldest) == self.spans.smallest_key() {
            return self.spans.pop_smallest().map(|(key, _)| (key, None));
        }
        if Some(oldest) == self.logs.smallest_key() {
            return self.logs.pop_smallest().map(|(key, _)| (key, None));
        }
        let (key, slot) = self.points.pop_smallest()?;
        let retired = self.release_stream_ref(&slot.stream);
        Some((key, retired.then(|| Arc::clone(&slot.stream))))
    }

    /// Charges one entering point to its stream: the stream's residency
    /// count grows by one, and the identity's accounted size joins the
    /// byte ceiling exactly when that count leaves zero — the stream's
    /// first resident point.
    fn charge_stream(&mut self, stream: &Arc<StreamIdentity>) {
        let count = self.series.entry(Arc::clone(stream)).or_insert(0);
        if *count == 0 {
            self.identity_accounted_bytes = self
                .identity_accounted_bytes
                .saturating_add(accounted_u64(stream.as_ref()));
        }
        *count += 1;
    }

    /// Releases one removed point's residency of its stream: the count
    /// drops by one, and when it reaches zero — the stream's last resident
    /// point is gone — the identity's charge leaves the ceiling and the
    /// caller is told to report the release. Returns `false` (changing
    /// nothing) for a stream the store is not tracking, which cannot
    /// happen for a slot the store built.
    fn release_stream_ref(&mut self, stream: &Arc<StreamIdentity>) -> bool {
        let retired = match self.series.get_mut(stream) {
            Some(count) => {
                *count -= 1;
                *count == 0
            }
            None => false,
        };
        if retired {
            self.series.remove(stream);
            self.identity_accounted_bytes = self
                .identity_accounted_bytes
                .saturating_sub(accounted_u64(stream.as_ref()));
        }
        retired
    }

    /// Distinct streams with resident points — the number the series cap
    /// bounds.
    fn resident_streams(&self) -> u64 {
        u64::try_from(self.series.len()).unwrap_or(u64::MAX)
    }

    /// Records currently resident, all shelves together.
    fn resident_records(&self) -> u64 {
        let total = self.spans.len() + self.logs.len() + self.points.len();
        u64::try_from(total).unwrap_or(u64::MAX)
    }

    /// Accounted bytes currently resident — what the byte ceiling bounds:
    /// the shelves' record sums plus each distinct resident stream's
    /// identity, charged once by the series table.
    fn accounted_bytes(&self) -> u64 {
        self.record_accounted_bytes()
            .saturating_add(self.identity_accounted_bytes)
    }

    /// The shelves' record sums, without the identity charge.
    fn record_accounted_bytes(&self) -> u64 {
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
            resident_streams: self.resident_streams(),
            accounted_bytes: self.accounted_bytes(),
            record_accounted_bytes: self.record_accounted_bytes(),
            identity_accounted_bytes: self.identity_accounted_bytes,
            evicted_for_record_ceiling: self.counters.evicted_for_record_ceiling,
            evicted_for_accounted_bytes_ceiling: self.counters.evicted_for_accounted_bytes_ceiling,
            evicted_for_admission_window: self.counters.evicted_for_admission_window,
            oversized_refusals: self.counters.oversized_refusals,
            duplicate_keeps: self.counters.duplicate_keeps,
            kept_out_series_cap: self.counters.kept_out_series_cap,
            identity_over_ceiling_refusals: self.counters.identity_over_ceiling_refusals,
            hook_deliveries: self.counters.hook_deliveries,
            refused_hook_deliveries: self.counters.refused_hook_deliveries,
            admission_anomalies: self.anomalies,
        }
    }
}

/// The accounted size of one record as a `u64`, saturating: ceilings are
/// `u64` and a record at `usize::MAX` bytes refuses anyway.
fn accounted_u64(record: &(impl Accounted + ?Sized)) -> u64 {
    u64::try_from(record.accounted_size()).unwrap_or(u64::MAX)
}

/// The refusals every keep runs before anything is inserted: the record's
/// own accounted size against the byte ceiling (no amount of eviction
/// could keep it), then the duplicate check (the resident record stands).
/// The identity-over-ceiling refusal and the series-cap refusal are the
/// metric-point keep's alone; they run after these, also before any
/// insert.
fn refuse_early<R: Accounted>(
    shelf: &Shelf<R>,
    entity: EntityId,
    size: u64,
    max_accounted_bytes: u64,
    counters: &mut Counters,
) -> Result<(), KeepOutcome> {
    if size > max_accounted_bytes {
        counters.oversized_refusals += 1;
        return Err(KeepOutcome::Oversized);
    }
    if shelf.contains(entity) {
        counters.duplicate_keeps += 1;
        return Err(KeepOutcome::Duplicate);
    }
    Ok(())
}

/// The shared keep sequence for spans and logs: the early refusals, the
/// insert. Returns `Ok(the reference now for the retention pass)` when the
/// record entered residency, `Err(the counted outcome)` when it was
/// refused; the caller runs the retention pass itself, so every kind's
/// keep is this one sequence. A refused outcome that inserted nothing is
/// the caller's to report through the hook — the entity id is copied out
/// first, because the `Admitted` hand-off moves into the insert.
fn keep_on_shelf<R: Accounted>(
    shelf: &mut Shelf<R>,
    admitted: Admitted<Arc<R>>,
    size: u64,
    max_accounted_bytes: u64,
    counters: &mut Counters,
) -> Result<AdmissionTime, KeepOutcome> {
    refuse_early(shelf, admitted.entity, size, max_accounted_bytes, counters)?;
    let now = admitted.admitted_at;
    shelf.insert(admitted);
    Ok(now)
}

impl TelemetryStore for InMemoryStore {
    fn keep_span(&mut self, admitted: Admitted<Arc<Span>>) -> KeepOutcome {
        let entity = admitted.entity;
        let size = accounted_u64(admitted.record.as_ref());
        let result = keep_on_shelf(
            &mut self.spans,
            admitted,
            size,
            self.config.max_accounted_bytes,
            &mut self.counters,
        );
        // The one span refusal that inserted nothing reports through the
        // hook, so the refused record's ledger identity ends with the
        // refusal (a span carries no stream identity — nothing to hand
        // over). A Duplicate reports nothing: the record it names is
        // resident.
        if matches!(result, Err(KeepOutcome::Oversized)) {
            self.report_refusal(entity, None);
        }
        match result {
            Ok(now) => KeepOutcome::Kept {
                evicted: self.enforce_against(now),
            },
            Err(outcome) => outcome,
        }
    }

    fn keep_log_record(&mut self, admitted: Admitted<Arc<LogRecord>>) -> KeepOutcome {
        let entity = admitted.entity;
        let size = accounted_u64(admitted.record.as_ref());
        let result = keep_on_shelf(
            &mut self.logs,
            admitted,
            size,
            self.config.max_accounted_bytes,
            &mut self.counters,
        );
        // The one log-record refusal that inserted nothing reports through
        // the hook. (The ledger keeps no entry for log records at all, so
        // the composition root's release is a no-op there — the report is
        // still owed, the hook's law being per refusal, not per kind.) A
        // Duplicate reports nothing: the record it names is resident.
        if matches!(result, Err(KeepOutcome::Oversized)) {
            self.report_refusal(entity, None);
        }
        match result {
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
        // The refusals run before anything is built: oversized and
        // duplicate like every kind, then the series cap — a keep that
        // would establish a NEW distinct stream while the cap is already
        // full is refused without evicting anything. Only after all of
        // them is the slot allocated, so a refused keep costs no heap.
        // Every refusal here inserted nothing, so each one reports through
        // the hook once the store's own state has settled — a refused
        // record's ledger identity ends with the refusal (ADR 0008); a
        // Duplicate reports nothing, the record it names being resident.
        let entity = admitted.entity;
        let size = accounted_u64(admitted.record.as_ref());
        if let Err(outcome) = refuse_early(
            &self.points,
            admitted.entity,
            size,
            self.config.max_accounted_bytes,
            &mut self.counters,
        ) {
            if KeepOutcome::Oversized == outcome {
                self.report_refusal(entity, Some(&stream));
            }
            return outcome;
        }
        // The identity charge a keep would owe: when the stream's first
        // resident point enters, the series table adds the identity's
        // accounted size to the byte ceiling. An identity already over
        // the ceiling on its own can never be kept — no amount of
        // eviction shrinks a charge that is over the cap by itself — so
        // the keep is refused before anything is inserted, evicting
        // nothing, naming the ceiling and the identity's size.
        // Non-retryable: the identity is payload content.
        let identity_bytes = accounted_u64(stream.as_ref());
        if identity_bytes > self.config.max_accounted_bytes {
            self.counters.identity_over_ceiling_refusals += 1;
            let outcome = KeepOutcome::IdentityOverCeiling {
                ceiling: self.config.max_accounted_bytes,
                identity_bytes,
            };
            self.report_refusal(entity, Some(&stream));
            return outcome;
        }
        if !self.series.contains_key(stream.as_ref())
            && self.resident_streams() >= self.config.series_cap
        {
            self.counters.kept_out_series_cap += 1;
            self.report_refusal(entity, Some(&stream));
            return KeepOutcome::SeriesCapReached;
        }
        let slot = Arc::new(PointSlot {
            point: admitted.record,
            stream: Arc::clone(&stream),
        });
        let now = admitted.admitted_at;
        self.points.insert(Admitted {
            entity: admitted.entity,
            admitted_at: now,
            record: slot,
        });
        self.charge_stream(&stream);
        KeepOutcome::Kept {
            evicted: self.enforce_against(now),
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
                .map(|item| ScanItem {
                    key: item.key,
                    record: PointView {
                        point: Arc::clone(&item.record.point),
                        stream: Arc::clone(&item.record.stream),
                    },
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
