//! The file-backed store: the memory mode's bounded-retention law, with
//! every residency change mirrored to a `SQLite` file for reopen durability.
//!
//! Memory stays authoritative on the hot path (`docs/architecture/
//! storage-model.md`): a keep is the same map work the in-memory store
//! does on the caller's thread — shelves, series table, counters — with a
//! write-through transaction staged from the very changes that residency
//! made, committed only after the store's own state has settled and before
//! the eviction hook fires. An on-disk failure (a full disk, a wedged
//! file) never stalls admission: the change stays resident, the store
//! logs the degradation and keeps running, and the divergence —
//! a missing row after reopen, or an evicted record resurrecting — is
//! surfaced by the log, never silent.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::Connection;

use runtime_trail_storage::{
    AdmissionKey, EvictionCause, EvictionHook, KeepOutcome, PointView, ScanItem, ScanPage,
    StoreStats, TelemetryStore,
};
use runtime_trail_telemetry_model::{
    Accounted, AdmissionTime, Admitted, EntityId, LogRecord, MetricPoint, Span, StreamIdentity,
};

use crate::config::FileBackedConfig;
use crate::db::{self, DbOp};
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

/// A hook delivery staged during a pass and fired afterwards — the same
/// deliveries, in the same order, as the memory driver fires inline:
/// evictions oldest-first, each record's `evicted` before its stream's
/// `stream_released`, refusals before the pass they pre-empted. The store
/// stages them so the `SQLite` transaction can commit before the first hook
/// call runs — a panicking hook can then never leave a dangling
/// transaction, while the delivery order the identity-keeping wiring sees
/// is unchanged.
#[derive(Debug)]
enum Delivery {
    Evicted(EntityId),
    StreamReleased(Arc<StreamIdentity>),
    Refused {
        entity: EntityId,
        stream: Option<Arc<StreamIdentity>>,
    },
}

/// The file-backed store.
///
/// Built by [`FileBackedStore::open`] with its [`FileBackedConfig`] and —
/// optionally — the [`EvictionHook`] the composition root wires to the
/// admission ledger's `forget` and `release_stream`. The retention law is
/// the memory mode's, unchanged and enforced by the same code shapes
/// (`docs/architecture/storage-model.md`): ceilings checked after every
/// keep and on every [`TelemetryStore::enforce_retention`] pass; the
/// oldest record — smallest [`AdmissionKey`] — evicted until all ceilings
/// are satisfied again, each eviction attributed to the first ceiling hit
/// at that moment; the series cap refusing the establishing keep, never
/// evicting; identity charged to the byte ceiling once per distinct
/// stream; a keep that inserted nothing reported through the hook.
///
/// The state (shelves, series, counters, connection) lives behind a
/// mutation lock: the keep and retention methods take `&mut self` like
/// the memory mode's, but the retrieval methods take `&self`, and the
/// lock keeps concurrent readers coherent while a writer passes. A
/// panicking hook — which the hook law forbids — poisons the lock
/// instead of corrupting the store; every acquisition recovers the
/// poison, matching the memory mode's "a panicking hook leaves a
/// consistent store" guarantee.
///
/// # Durability
///
/// Each keep (and each retention pass) commits **one transaction**
/// carrying exactly that pass's residency changes: the kept record's
/// insert and every eviction it caused, so on-disk and in-memory
/// residency can never disagree by half a keep. `WAL` + `synchronous =
/// FULL` (`crates/storage-sqlite/src/db.rs` owns the details) makes a
/// committed change durable against process crash and power loss.
/// A graceful drop runs a `TRUNCATE` checkpoint, folding the committed
/// tail into the single database file, so "copy one file, reopen the
/// session" ([ADR 0003](../../docs/decisions/0003-storage-strategy.md))
/// holds when the session ends cleanly. After a crash, the WAL holds the
/// committed tail and the next open recovers it.
///
/// An on-disk failure — the transaction cannot commit — never touches
/// residency: the record stays resident in memory (memory authoritative),
/// the store logs the error and keeps running (degraded durability), and
/// the reopen-time divergence is the log's to surface: the failed record
/// may be missing after a reopen, and a record whose eviction failed to
/// commit may be resurrected by one.
pub struct FileBackedStore {
    config: FileBackedConfig,
    inner: Mutex<Inner>,
}

struct Inner {
    conn: Connection,
    /// The ceilings this session runs under, copied at open: retention
    /// passes and the open-time re-check read them from here, so `Inner`
    /// is the single authority for "what does the current session bound
    /// to".
    config: FileBackedConfig,
    /// The admission window in nanoseconds, as the keep path compares it
    /// (the `Duration` itself is configuration; the nanos are the clock
    /// arithmetic).
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

/// Why [`FileBackedStore::open`] failed.
///
/// Failure never deletes, truncates, or rewrites the named file: a store
/// that cannot open leaves the operator's data exactly where it was.
#[derive(Debug)]
pub enum OpenError {
    /// The database file could not be opened at all: a missing parent
    /// directory, a path naming a directory, a permission problem —
    /// whatever `SQLite` reported.
    Open {
        path: PathBuf,
        source: rusqlite::Error,
    },
    /// The file opened but is not a valid `SQLite` database — its content is
    /// not something this driver can read. The file is left as found.
    NotADatabase {
        path: PathBuf,
        source: rusqlite::Error,
    },
    /// The file is a real database but holds rows this build cannot
    /// decode — content written by a build this one cannot read.
    CorruptData { path: PathBuf, detail: String },
    /// The file opened and read, but a residency change needed to bring
    /// the reopened session under its ceilings could not be written
    /// (typically a full disk). Opening fails rather than serving a
    /// session that begins out of sync with its file.
    Write {
        path: PathBuf,
        source: rusqlite::Error,
    },
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Open { path, source } => {
                write!(
                    f,
                    "could not open the database at {}: {source}",
                    path.display()
                )
            }
            OpenError::NotADatabase { path, source } => write!(
                f,
                "{} is not a valid SQLite database this build can read: {source}",
                path.display()
            ),
            OpenError::CorruptData { path, detail } => write!(
                f,
                "{} holds rows this build cannot decode: {detail}",
                path.display()
            ),
            OpenError::Write { path, source } => {
                write!(
                    f,
                    "could not write the database at {}: {source}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for OpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OpenError::Open { source, .. }
            | OpenError::NotADatabase { source, .. }
            | OpenError::Write { source, .. } => Some(source),
            OpenError::CorruptData { .. } => None,
        }
    }
}

impl FileBackedStore {
    /// The mode name surfaces report for this driver.
    pub const NAME: &'static str = "file-backed";

    /// Opens (or creates) the database at `path` and starts a store over
    /// it: the file's rows rehydrate the shelves (streams re-interned by
    /// content, so one stream's rehydrated points share one identity
    /// Arc), the series table and identity charge rebuild from the
    /// rehydrated points, and the reopened session is bounded the moment
    /// it opens — rehydrated residency is measured against the ceilings
    /// now, and evicted down to fit. The admission *window* is not part of
    /// the open-time re-check: the store owns no clock, and window expiry
    /// waits for the composition root's first
    /// [`TelemetryStore::enforce_retention`] reading.
    ///
    /// The rehydrated store's session counters start at zero (they count
    /// this session's evictions and refusals, like the memory mode's), and
    /// open-time evictions are reported through `hook` like any other
    /// eviction: the hook law makes no exception for a session boundary.
    /// # Errors
    ///
    /// [`OpenError::NotADatabase`] when the file at `path` exists but is
    /// not a readable `SQLite` database (the file is left untouched);
    /// [`OpenError::CorruptData`] when the database is real but holds rows
    /// this build cannot decode; [`OpenError::Open`] for every other
    /// failure to open — a missing parent directory, a path naming a
    /// directory, or a permission problem.
    pub fn open(
        path: impl AsRef<Path>,
        config: FileBackedConfig,
        hook: Option<Box<dyn EvictionHook>>,
    ) -> Result<Self, OpenError> {
        let path = path.as_ref();
        let mut conn = db::open(path).map_err(|source| classify_open_error(path, source))?;
        let loaded = db::load(&mut conn).map_err(|error| OpenError::CorruptData {
            path: path.to_owned(),
            detail: error.to_string(),
        })?;
        let Rehydrated {
            spans,
            logs,
            points,
            series,
            identity_accounted_bytes,
        } = rehydrate(loaded);
        let window_nanos = u64::try_from(config.admission_window.as_nanos()).unwrap_or(u64::MAX);
        let store = Self {
            config,
            inner: Mutex::new(Inner {
                conn,
                config,
                window_nanos,
                hook,
                spans,
                logs,
                points,
                series,
                identity_accounted_bytes,
                counters: Counters::default(),
                anomalies: 0,
            }),
        };
        // Bounded from construction: the rehydrated session is a resident
        // set the ceilings must already fit, so evict down to them now —
        // the same pass retention runs, without the window (store owns no
        // clock), committed to the file and delivered to the hook.
        let mut ops = Vec::new();
        let mut deliveries = Vec::new();
        {
            let mut inner = store.lock();
            inner.enforce_no_window(&mut ops, &mut deliveries);
            db::commit(&mut inner.conn, &ops).map_err(|source| OpenError::Write {
                path: path.to_owned(),
                source,
            })?;
            inner.deliver(deliveries);
        }
        Ok(store)
    }

    /// The ceilings this store was started with: startup configuration,
    /// never mid-session state.
    #[must_use]
    pub const fn config(&self) -> &FileBackedConfig {
        &self.config
    }

    /// The state lock, poison-tolerant: a panicking hook (which the hook
    /// law forbids) poisons the mutex but must never wedge the store — the
    /// memory driver's guarantee is "a panicking hook leaves a consistent
    /// store", and recovery here is that same consistency, observable only
    /// as the counters' documented divergence.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Inner {
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

    /// The open-time variant of [`Inner::first_violation`]: the capacity
    /// ceilings only, never the window — a reopened session has no clock
    /// of its own, and its age is the composition root's to read.
    fn first_violation_no_window(&self) -> Option<EvictionCause> {
        if self.resident_records() > self.config.max_records {
            return Some(EvictionCause::RecordCeiling);
        }
        if self.accounted_bytes() > self.config.max_accounted_bytes {
            return Some(EvictionCause::AccountedBytesCeiling);
        }
        None
    }

    /// Runs the capacity ceilings down to compliance — the open-time
    /// re-check. Staged like [`Inner::enforce_against`]: deletes collected
    /// for one transaction, deliveries collected for the hook.
    fn enforce_no_window(&mut self, ops: &mut Vec<DbOp>, deliveries: &mut Vec<Delivery>) {
        while let Some(cause) = self.first_violation_no_window() {
            let Some((key, retired)) = self.pop_oldest(ops) else {
                break;
            };
            self.count_eviction(cause);
            deliveries.push(Delivery::Evicted(key.entity()));
            if let Some(stream) = retired {
                deliveries.push(Delivery::StreamReleased(stream));
            }
        }
    }

    /// Runs the retention law to completion against `now`, evicting the
    /// oldest record for as long as any ceiling is violated. Each eviction
    /// is counted under the cause that triggered it, staged as a delete
    /// for the transaction, and staged as a delivery for the hook — so
    /// identity-keeping wiring sees exactly what residency ended,
    /// including streams whose last resident point went with it.
    fn enforce_against(
        &mut self,
        now: AdmissionTime,
        ops: &mut Vec<DbOp>,
        deliveries: &mut Vec<Delivery>,
    ) -> u64 {
        let mut evicted = 0;
        while let Some(cause) = self.first_violation(now) {
            let Some((key, retired)) = self.pop_oldest(ops) else {
                break;
            };
            self.count_eviction(cause);
            evicted += 1;
            // The store's own removal — shelves, entity index, stream
            // table, counters — is complete above, and the SQLite commit
            // happens before any hook call (the caller commits the staged
            // ops before delivering). The deliveries preserve the memory
            // driver's order: the record's evicted before its stream's
            // stream_released.
            deliveries.push(Delivery::Evicted(key.entity()));
            if let Some(stream) = retired {
                deliveries.push(Delivery::StreamReleased(stream));
            }
        }
        evicted
    }

    fn count_eviction(&mut self, cause: EvictionCause) {
        match cause {
            EvictionCause::RecordCeiling => self.counters.evicted_for_record_ceiling += 1,
            EvictionCause::AccountedBytesCeiling => {
                self.counters.evicted_for_accounted_bytes_ceiling += 1;
            }
            EvictionCause::AdmissionWindow => self.counters.evicted_for_admission_window += 1,
        }
    }

    /// Stages one keep refusal for the hook, under the same ordering law
    /// the eviction hook runs under: the refusal's own counter is already
    /// incremented and the store's residency is exactly as it was —
    /// nothing was inserted — before the report goes out.
    fn stage_refusal(
        entity: EntityId,
        stream: Option<&Arc<StreamIdentity>>,
        deliveries: &mut Vec<Delivery>,
    ) {
        deliveries.push(Delivery::Refused {
            entity,
            stream: stream.map(Arc::clone),
        });
    }

    /// Removes the oldest record from whichever shelf holds it, stages its
    /// delete, and returns its key together with the stream the removal
    /// retired — `Some` when the record was a metric point whose stream
    /// just lost its last resident point. The three shelves cannot hold
    /// equal keys — an entity id names exactly one resident record — so
    /// the minimum picks exactly one shelf.
    fn pop_oldest(
        &mut self,
        ops: &mut Vec<DbOp>,
    ) -> Option<(AdmissionKey, Option<Arc<StreamIdentity>>)> {
        let oldest = [
            self.spans.smallest_key(),
            self.logs.smallest_key(),
            self.points.smallest_key(),
        ]
        .into_iter()
        .flatten()
        .min()?;
        if Some(oldest) == self.spans.smallest_key() {
            let (key, _) = self.spans.pop_smallest()?;
            ops.push(DbOp::DeleteSpan {
                entity: key.entity(),
            });
            return Some((key, None));
        }
        if Some(oldest) == self.logs.smallest_key() {
            let (key, _) = self.logs.pop_smallest()?;
            ops.push(DbOp::DeleteLog {
                entity: key.entity(),
            });
            return Some((key, None));
        }
        let (key, slot) = self.points.pop_smallest()?;
        ops.push(DbOp::DeletePoint {
            entity: key.entity(),
        });
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

    /// Commits a staged transaction; a failure never touches residency —
    /// the change stays in memory, the store degrades its durability and
    /// keeps running. The divergence the log names: the failed keep's row
    /// may be missing after a reopen, and a record whose eviction failed
    /// to commit may be resurrected by one.
    fn commit(&mut self, ops: &[DbOp]) {
        if let Err(error) = db::commit(&mut self.conn, ops) {
            tracing::error!(
                target: "storage_sqlite",
                %error,
                staged_ops = ops.len(),
                "a residency change could not be persisted; the record stays resident in memory \
                 and durability is degraded until a later write succeeds; after a reopen the \
                 file may differ from this session's residency"
            );
        }
    }

    /// Delivers staged hook calls in staged order — the memory driver's
    /// order — counting a delivery only once its call has returned, so a
    /// panicking hook shows up as the gap between the store's totals and
    /// the deliveries (the same observability law, unchanged by the
    /// staging; `stream_released` deliveries are not counted, exactly like
    /// the memory driver).
    fn deliver(&mut self, deliveries: Vec<Delivery>) {
        for delivery in deliveries {
            let Some(hook) = self.hook.as_mut() else {
                continue;
            };
            match delivery {
                Delivery::Evicted(entity) => {
                    hook.evicted(entity);
                    self.counters.hook_deliveries += 1;
                }
                Delivery::StreamReleased(stream) => {
                    hook.stream_released(&stream);
                }
                Delivery::Refused { entity, stream } => {
                    hook.keep_refused(entity, stream.as_ref());
                    self.counters.refused_hook_deliveries += 1;
                }
            }
        }
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

/// The refusals every keep runs before anything is inserted: the duplicate
/// check first — the record a duplicate names is resident, so the resident
/// record stands and the keep reports nothing (a refusal that inserted
/// nothing would report through the hook and end a still-resident
/// identity) — then the record's own accounted size against the byte
/// ceiling (no amount of eviction could keep it). The identity-over-ceiling
/// refusal and the series-cap refusal are the metric-point keep's alone;
/// they run after these, also before any insert.
fn refuse_early<R: Accounted>(
    shelf: &Shelf<R>,
    entity: EntityId,
    size: u64,
    max_accounted_bytes: u64,
    counters: &mut Counters,
) -> Result<(), KeepOutcome> {
    if shelf.contains(entity) {
        counters.duplicate_keeps += 1;
        return Err(KeepOutcome::Duplicate);
    }
    if size > max_accounted_bytes {
        counters.oversized_refusals += 1;
        return Err(KeepOutcome::Oversized);
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

/// Serializes one record for the write-through file. A failure — which
/// cannot happen for the model's JSON, the `Float` bit-pattern encoding
/// being lossless — leaves the record resident in memory and skips its
/// persistence, logging the degradation like any other write failure.
fn encode(what: &'static str, value: &impl serde::Serialize) -> Option<String> {
    match serde_json::to_string(value) {
        Ok(json) => Some(json),
        Err(error) => {
            tracing::error!(
                target: "storage_sqlite",
                %error,
                "a {what} could not be encoded for persistence; residency keeps it in memory \
                 and durability is degraded for this record"
            );
            None
        }
    }
}

/// Rebuilds the shelves, the stream interning, and the series table from a
/// file's rows. The memory driver never rebuilds — its shelves are born
/// empty and grow by the hook-wired keep path — so this is the one
/// reconstruction a reopen owns. Rehydrated records are fresh `Arc`s and
/// fresh `PointSlot`s; identity is re-interned by content
/// ([ADR 0008](../../docs/decisions/0008-admission-ledger-design.md),
/// the model's identity law), so one stream's rehydrated points share the
/// one Arc the reopened session treats as their identity, and the series
/// residency counts rebuild exactly as if the points had been kept in
/// order — the counts are order-independent.
struct Rehydrated {
    spans: Shelf<Span>,
    logs: Shelf<LogRecord>,
    points: Shelf<PointSlot>,
    series: HashMap<Arc<StreamIdentity>, u64>,
    identity_accounted_bytes: u64,
}

fn rehydrate(loaded: db::LoadedRows) -> Rehydrated {
    let mut spans = Shelf::new();
    let mut logs = Shelf::new();
    let mut points = Shelf::new();
    let mut series: HashMap<Arc<StreamIdentity>, u64> = HashMap::new();
    let mut identity_accounted_bytes = 0u64;
    // The intern map: content-equal stream rows resolve to the one Arc
    // (StreamIdentity hashes by content, so a HashMap is well-defined).
    let mut interned: HashMap<Arc<StreamIdentity>, Arc<StreamIdentity>> = HashMap::new();

    for (entity, nano, span) in loaded.spans {
        spans.insert(Admitted {
            entity,
            admitted_at: AdmissionTime::from_unix_nano(nano),
            record: Arc::new(span),
        });
    }
    for (entity, nano, record) in loaded.logs {
        logs.insert(Admitted {
            entity,
            admitted_at: AdmissionTime::from_unix_nano(nano),
            record: Arc::new(record),
        });
    }
    for (entity, nano, point, stream_identity) in loaded.points {
        let stream = if let Some(known) = interned.get(&stream_identity) {
            Arc::clone(known)
        } else {
            let arc = Arc::new(stream_identity);
            interned.insert(Arc::clone(&arc), Arc::clone(&arc));
            arc
        };
        let count = series.entry(Arc::clone(&stream)).or_insert(0);
        if *count == 0 {
            identity_accounted_bytes =
                identity_accounted_bytes.saturating_add(accounted_u64(stream.as_ref()));
        }
        *count += 1;
        points.insert(Admitted {
            entity,
            admitted_at: AdmissionTime::from_unix_nano(nano),
            record: Arc::new(PointSlot {
                point: Arc::new(point),
                stream: Arc::clone(&stream),
            }),
        });
    }

    Rehydrated {
        spans,
        logs,
        points,
        series,
        identity_accounted_bytes,
    }
}

/// Maps an `SQLite` failure during open onto the error taxonomy: "not a
/// database" is its own answer (the file exists but this build cannot read
/// it); everything else is "could not open".
fn classify_open_error(path: &Path, error: rusqlite::Error) -> OpenError {
    use rusqlite::ErrorCode;
    let is_not_a_database = matches!(
        &error,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::NotADatabase,
                ..
            },
            _
        )
    );
    if is_not_a_database {
        OpenError::NotADatabase {
            path: path.to_owned(),
            source: error,
        }
    } else {
        OpenError::Open {
            path: path.to_owned(),
            source: error,
        }
    }
}

impl TelemetryStore for FileBackedStore {
    fn keep_span(&mut self, admitted: Admitted<Arc<Span>>) -> KeepOutcome {
        let mut inner = self.lock();
        let entity = admitted.entity;
        let size = accounted_u64(admitted.record.as_ref());
        let payload = encode("span", admitted.record.as_ref());
        let mut ops = Vec::new();
        let mut deliveries = Vec::new();
        let result = {
            let Inner {
                spans, counters, ..
            } = &mut *inner;
            keep_on_shelf(
                spans,
                admitted,
                size,
                self.config.max_accounted_bytes,
                counters,
            )
        };
        // The one span refusal that inserted nothing reports through the
        // hook, so the refused record's ledger identity ends with the
        // refusal (a span carries no stream identity — nothing to hand
        // over). A Duplicate reports nothing: the record it names is
        // resident.
        let outcome = match result {
            Ok(now) => {
                if let Some(payload) = payload {
                    ops.push(DbOp::PutSpan {
                        entity,
                        admitted_at: now.as_unix_nano(),
                        payload,
                    });
                }
                KeepOutcome::Kept {
                    evicted: inner.enforce_against(now, &mut ops, &mut deliveries),
                }
            }
            Err(outcome) => {
                if matches!(outcome, KeepOutcome::Oversized) {
                    Inner::stage_refusal(entity, None, &mut deliveries);
                }
                outcome
            }
        };
        inner.commit(&ops);
        inner.deliver(deliveries);
        outcome
    }

    fn keep_log_record(&mut self, admitted: Admitted<Arc<LogRecord>>) -> KeepOutcome {
        let mut inner = self.lock();
        let entity = admitted.entity;
        let size = accounted_u64(admitted.record.as_ref());
        let payload = encode("log record", admitted.record.as_ref());
        let mut ops = Vec::new();
        let mut deliveries = Vec::new();
        let result = {
            let Inner { logs, counters, .. } = &mut *inner;
            keep_on_shelf(
                logs,
                admitted,
                size,
                self.config.max_accounted_bytes,
                counters,
            )
        };
        // The one log-record refusal that inserted nothing reports through
        // the hook. (The ledger keeps no entry for log records at all, so
        // the composition root's release is a no-op there — the report is
        // still owed, the hook's law being per refusal, not per kind.) A
        // Duplicate reports nothing: the record it names is resident.
        let outcome = match result {
            Ok(now) => {
                if let Some(payload) = payload {
                    ops.push(DbOp::PutLog {
                        entity,
                        admitted_at: now.as_unix_nano(),
                        payload,
                    });
                }
                KeepOutcome::Kept {
                    evicted: inner.enforce_against(now, &mut ops, &mut deliveries),
                }
            }
            Err(outcome) => {
                if matches!(outcome, KeepOutcome::Oversized) {
                    Inner::stage_refusal(entity, None, &mut deliveries);
                }
                outcome
            }
        };
        inner.commit(&ops);
        inner.deliver(deliveries);
        outcome
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
        let mut inner = self.lock();
        let entity = admitted.entity;
        let size = accounted_u64(admitted.record.as_ref());
        let payload = encode("metric point", admitted.record.as_ref());
        let stream_payload = encode("stream identity", stream.as_ref());
        let mut ops = Vec::new();
        let mut deliveries = Vec::new();
        let refused = {
            let Inner {
                points, counters, ..
            } = &mut *inner;
            refuse_early(
                points,
                entity,
                size,
                self.config.max_accounted_bytes,
                counters,
            )
        };
        let outcome = if let Err(refused) = refused {
            if KeepOutcome::Oversized == refused {
                Inner::stage_refusal(entity, Some(&stream), &mut deliveries);
            }
            refused
        } else {
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
                inner.counters.identity_over_ceiling_refusals += 1;
                Inner::stage_refusal(entity, Some(&stream), &mut deliveries);
                KeepOutcome::IdentityOverCeiling {
                    ceiling: self.config.max_accounted_bytes,
                    identity_bytes,
                }
            } else if !inner.series.contains_key(stream.as_ref())
                && inner.resident_streams() >= self.config.series_cap
            {
                inner.counters.kept_out_series_cap += 1;
                Inner::stage_refusal(entity, Some(&stream), &mut deliveries);
                KeepOutcome::SeriesCapReached
            } else {
                let slot = Arc::new(PointSlot {
                    point: admitted.record,
                    stream: Arc::clone(&stream),
                });
                let now = admitted.admitted_at;
                inner.points.insert(Admitted {
                    entity,
                    admitted_at: now,
                    record: slot,
                });
                inner.charge_stream(&stream);
                if let (Some(payload), Some(stream_payload)) = (payload, stream_payload) {
                    ops.push(DbOp::PutPoint {
                        entity,
                        admitted_at: now.as_unix_nano(),
                        payload,
                        stream_payload,
                    });
                }
                KeepOutcome::Kept {
                    evicted: inner.enforce_against(now, &mut ops, &mut deliveries),
                }
            }
        };
        inner.commit(&ops);
        inner.deliver(deliveries);
        outcome
    }

    fn span(&self, entity: EntityId) -> Option<Arc<Span>> {
        self.lock().spans.get(entity)
    }

    fn log_record(&self, entity: EntityId) -> Option<Arc<LogRecord>> {
        self.lock().logs.get(entity)
    }

    fn metric_point(&self, entity: EntityId) -> Option<PointView> {
        let inner = self.lock();
        let slot = inner.points.get(entity)?;
        Some(PointView {
            point: Arc::clone(&slot.point),
            stream: Arc::clone(&slot.stream),
        })
    }

    fn scan_spans(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<Span>> {
        let inner = self.lock();
        inner.spans.scan_after(after, limit)
    }

    fn scan_log_records(
        &self,
        after: Option<AdmissionKey>,
        limit: usize,
    ) -> ScanPage<Arc<LogRecord>> {
        let inner = self.lock();
        inner.logs.scan_after(after, limit)
    }

    fn scan_metric_points(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<PointView> {
        let inner = self.lock();
        let page = inner.points.scan_after(after, limit);
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
        let mut inner = self.lock();
        let mut ops = Vec::new();
        let mut deliveries = Vec::new();
        let evicted = inner.enforce_against(now, &mut ops, &mut deliveries);
        inner.commit(&ops);
        inner.deliver(deliveries);
        evicted
    }

    fn observe_admission_anomalies(&mut self, total: u64) {
        self.lock().anomalies = total;
    }

    fn stats(&self) -> StoreStats {
        let inner = self.lock();
        inner.stats_from()
    }

    fn mode_name(&self) -> &'static str {
        Self::NAME
    }
}

impl Drop for FileBackedStore {
    fn drop(&mut self) {
        // Graceful close folds the committed tail into the single file
        // (`PRAGMA wal_checkpoint(TRUNCATE)`), so "copy one file, reopen
        // the session" (ADR 0003) holds when the session ends cleanly.
        // A drop that cannot take the lock — another thread mid-keep, or
        // the guard held during a panic unwind — skips the checkpoint
        // rather than blocking or doubling into a deadlock: the WAL then
        // holds the committed tail, and recovery is the crash path — the
        // next open recovers it. Poisoned guards still checkpoint: the
        match self.inner.try_lock() {
            Ok(mut inner) => {
                if let Err(error) = db::checkpoint(&mut inner.conn) {
                    tracing::error!(
                        target: "storage_sqlite",
                        %error,
                        "the final checkpoint failed; the write-ahead log holds the committed \
                         tail and the next open recovers it"
                    );
                }
            }
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                let mut inner = poisoned.into_inner();
                if let Err(error) = db::checkpoint(&mut inner.conn) {
                    tracing::error!(
                        target: "storage_sqlite",
                        %error,
                        "the final checkpoint failed; the write-ahead log holds the committed \
                         tail and the next open recovers it"
                    );
                }
            }
            Err(_) => {}
        }
    }
}
