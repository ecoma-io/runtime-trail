//! The runtime graph: admission wired to a real storage driver.
//!
//! This is the composition root's heart and the only place that names
//! concrete drivers (ADR 0003): the
//! [`AdmissionLedger`](runtime_trail_telemetry_model::AdmissionLedger) is
//! built here, the [`InMemoryStore`] is built here and held as a
//! `Box<dyn TelemetryStore>`, and the one edge the whole architecture
//! turns on — *eviction ends identity* (ADR 0008) — is wired here, as the
//! [`EvictionHook`] that forwards the store's removals to the ledger the
//! pipeline admits through.
//!
//! The graph is shared by every surface: the OTLP/HTTP endpoints and the
//! OTLP/gRPC services admit through the same [`Pipeline`], whose bounded
//! queue is pumped into the same store by one consumer thread. One graph,
//! no per-transport state.
//!
//! # Threads, not tasks
//!
//! The pump blocks on the hand-off queue by design (`BoundedQueue::pop`
//! is std synchronisation — the queue refuses producers, it never blocks
//! them), so it runs on a dedicated OS thread, not a tokio worker: a
//! blocked worker would stall the reactor that admission answers on. The
//! retention timer is the same shape — a plain thread sleeping between
//! ticks. tokio stays where it belongs: serving sockets.
//!
//! # The clock
//!
//! The store owns no clock and neither does the model; the composition
//! root owns the runtime's one reading of time and hands it to ingestion
//! (`ingest_*(now, …)`) and to the store (`enforce_retention(now)`). The
//! reading is monotonic by construction
//! ([`AdmissionClock`]): the residency order is admission time, so a
//! wall-clock step backwards must never reorder the timeline.

use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, PoisonError,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use runtime_trail_storage::{EvictionHook, KeepOutcome, StoreStats, TelemetryStore};
use runtime_trail_storage_memory::{InMemoryStore, MemoryConfig};
use runtime_trail_telemetry_ingestion::{
    BoundedQueue, LedgerReleaser, PIPELINE_QUEUE_NAME, Pipeline, PipelineConfigError, QueuedRecord,
    RecordSink, StoredRecord,
};
use runtime_trail_telemetry_model::{
    AdmissionTime, Admitted, BudgetLimits, EntityId, StreamIdentity,
};

use crate::DRAIN_DEADLINE;
use crate::transport_guard::InflightBodyBudget;

/// The default per-request body read deadline ([ADR 0010]).
///
/// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// How long an idle pump waits before re-checking the drain state.
///
/// The queue's `pop` blocks with no way to observe a drain that begins
/// later, so the idle pump wakes on this cadence to look. It bounds the
/// latency between the shutdown signal and the pump's last bounded wait —
/// small next to the drain deadline, and no work is lost by it: a record
/// arriving mid-poll is picked up on the next iteration.
const PUMP_IDLE_POLL: Duration = Duration::from_millis(200);

/// How often the composition root runs the retention law against the
/// store.
///
/// The store owns no clock (`docs/architecture/storage-model.md`,
/// "The store owns no clock"): the composition root owns the timer and
/// must call it periodically "at a granularity well inside the shortest
/// configured window". The default window is 24 h, so 60 s is well inside
/// it by two orders of magnitude while costing one no-op pass a minute.
const RETENTION_TICK_PERIOD: Duration = Duration::from_secs(60);

/// Where the runtime's time comes from.
///
/// The composition root owns time; this trait is the seam that lets tests
/// hand the graph a reading instead of the wall clock. Production wires
/// [`SystemWallClock`].
pub trait WallClock: Send + Sync + 'static {
    /// One reading, in the model's admission-time domain.
    fn reading(&self) -> AdmissionTime;
}

/// The production clock: the system's wall time, in unix nanoseconds.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn reading(&self) -> AdmissionTime {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since_epoch| {
                u64::try_from(since_epoch.as_nanos()).unwrap_or(u64::MAX)
            });
        AdmissionTime::from_unix_nano(nanos)
    }
}

/// The runtime's admission clock: wall-clock readings, made monotonic.
///
/// The residency order is *admission time* — eviction and scan order are
/// one sequence keyed on it — so the timeline must never step backwards or
/// repeat, whatever the wall clock does (NTP corrections step). Every
/// reading is therefore at least the previous reading plus one nanosecond:
/// a monotonic sequence anchored to real time, not a stopwatch that drifts
/// from it.
pub struct AdmissionClock {
    source: Box<dyn WallClock>,
    last: AtomicU64,
}

impl AdmissionClock {
    /// A monotonic clock over `source`.
    #[must_use]
    pub fn new(source: Box<dyn WallClock>) -> Self {
        Self {
            source,
            last: AtomicU64::new(0),
        }
    }

    /// The next admission time: the source's reading, or one past the last
    /// handed out, whichever is later.
    #[must_use]
    pub fn now(&self) -> AdmissionTime {
        let reading = self.source.reading().as_unix_nano();
        let mut observed = self.last.load(Ordering::Relaxed);
        loop {
            let next = reading.max(observed.saturating_add(1));
            match self.last.compare_exchange_weak(
                observed,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return AdmissionTime::from_unix_nano(next),
                Err(now_observed) => observed = now_observed,
            }
        }
    }
}

impl std::fmt::Debug for AdmissionClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionClock")
            .field("last", &self.last.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// The composition root's startup configuration, fixed for the session.
///
/// The defaults are the contract numbers: the memory driver's retention
/// ceilings ([`MemoryConfig::default`]), the admission budgets, the 4 MiB
/// OTLP payload ceiling, and the 64 MiB queue ceiling. Like every number
/// in `docs/architecture/runtime-constraints.md` these are
/// startup-configurable, never mid-session state.
pub struct RuntimeConfig {
    /// The memory store's retention ceilings.
    pub store: MemoryConfig,
    /// The admission budgets the pipeline gates against.
    pub budgets: BudgetLimits,
    /// One OTLP export request's ceiling, refused at the transport edge
    /// before parsing.
    pub payload_ceiling_bytes: usize,
    /// The hand-off queue's accounted-byte ceiling — the number a
    /// saturation backs up against.
    pub queue_ceiling_bytes: usize,
    /// The transport-edge aggregate: how many request-body bytes may be
    /// buffered at the OTLP transports before admission
    /// ([ADR 0010](../docs/decisions/0010-transport-edge-in-flight-body-budget.md)).
    /// Defaults to one queue ceiling's worth, so the transport edge can never
    /// hold more buffering than the queue it feeds.
    pub inflight_body_ceiling_bytes: usize,
    /// The per-request body read deadline, shared by both OTLP transports. A
    /// body that has not arrived in full within this bound is refused so a
    /// single slow-drip client cannot hold its buffered bytes indefinitely.
    pub body_read_timeout: Duration,
    /// Where admission times come from. Production:
    /// [`SystemWallClock`].
    pub clock: Box<dyn WallClock>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        use runtime_trail_telemetry_ingestion::QUEUE_CEILING_BYTES;
        use runtime_trail_telemetry_model::budgets::OTLP_PAYLOAD_BYTES;
        Self {
            store: MemoryConfig::default(),
            budgets: BudgetLimits::default(),
            payload_ceiling_bytes: OTLP_PAYLOAD_BYTES,
            queue_ceiling_bytes: QUEUE_CEILING_BYTES,
            inflight_body_ceiling_bytes: QUEUE_CEILING_BYTES,
            body_read_timeout: BODY_READ_TIMEOUT,
            clock: Box::new(SystemWallClock),
        }
    }
}

impl std::fmt::Debug for RuntimeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeConfig")
            .field("store", &self.store)
            .field("budgets", &self.budgets)
            .field("payload_ceiling_bytes", &self.payload_ceiling_bytes)
            .field("queue_ceiling_bytes", &self.queue_ceiling_bytes)
            .field(
                "inflight_body_ceiling_bytes",
                &self.inflight_body_ceiling_bytes,
            )
            .field("body_read_timeout", &self.body_read_timeout)
            .finish_non_exhaustive()
    }
}

/// The eviction hook: the store's removals end identities in the ledger.
///
/// This is the ADR 0008 loop, closed: a record evicted from residency
/// ends its identity through [`LedgerReleaser::forget`] and a stream
/// whose last resident point left through [`LedgerReleaser::release_stream`]
/// — on the *same* ledger the pipeline admits through, shared out through
/// the pipeline's narrow releaser handle. A re-delivery after eviction is
/// therefore admitted fresh, and interning memory ends exactly when
/// residency does.
///
/// A keep the store *refused* ends identity the same way: the refused
/// record never entered residency, but admission had already given it a
/// ledger identity — and interned its stream, for a metric point. The
/// store's `keep_refused` report comes here after its state has settled
/// with nothing inserted, and takes the same release path as an eviction
/// (`forget`, then `release_stream` when a stream rode along), so the
/// pipeline's next delivery of the same natural identity re-admits fresh
/// instead of collapsing onto an entry whose record is nowhere in the
/// store. Every forward is counted — the runtime surfaces it in the
/// shutdown summary.
///
/// # Why this hook cannot panic
///
/// The hook runs inside the store's keep, where a panic would leave
/// identity resident after its record is gone — the one divergence the
/// storage contract calls out as never silent. Structurally, there is
/// nothing here that can unwind: no indexing, no arithmetic that can
/// overflow, no allocation, and the releaser's two calls are total map
/// removals. The one fallible step is the ledger mutex, and poisoning is
/// recovered from (`PoisonError::into_inner`, inside
/// [`LedgerReleaser`]) rather than unwound: the data the guard protects
/// is a map whose every entry is independently valid, so a panic
/// *elsewhere* under it does not make a `forget` unsafe to complete.
struct LedgerHook {
    releaser: LedgerReleaser,
    /// Shared with the runtime: refusals whose identity this hook ended.
    refused_forwards: Arc<AtomicU64>,
}

impl EvictionHook for LedgerHook {
    fn evicted(&mut self, entity: EntityId) {
        self.releaser.forget(entity);
    }

    fn stream_released(&mut self, stream: &Arc<StreamIdentity>) {
        self.releaser.release_stream(stream);
    }

    fn keep_refused(&mut self, entity: EntityId, stream: Option<&Arc<StreamIdentity>>) {
        self.releaser.forget(entity);
        if let Some(stream) = stream {
            self.releaser.release_stream(stream);
        }
        self.refused_forwards.fetch_add(1, Ordering::Relaxed);
    }
}

/// The pump's outcome counters, observable at shutdown and in tests.
///
/// Every [`KeepOutcome`] is counted by name; whatever the store contract
/// grows later lands in `unknown_outcomes` loudly instead of silently in a
/// bucket that already exists.
#[derive(Debug, Default)]
pub struct PumpCounters {
    kept: AtomicU64,
    evicted_on_keep: AtomicU64,
    duplicates: AtomicU64,
    oversized_refusals: AtomicU64,
    series_cap_refusals: AtomicU64,
    identity_over_ceiling_refusals: AtomicU64,
    unknown_outcomes: AtomicU64,
    dropped_on_drain: AtomicU64,
}

impl PumpCounters {
    /// Counts one keep outcome, by name.
    fn observe(&self, outcome: &KeepOutcome) {
        match outcome {
            KeepOutcome::Kept { evicted } => {
                self.kept.fetch_add(1, Ordering::Relaxed);
                self.evicted_on_keep.fetch_add(*evicted, Ordering::Relaxed);
            }
            KeepOutcome::Duplicate => {
                self.duplicates.fetch_add(1, Ordering::Relaxed);
            }
            KeepOutcome::Oversized => {
                self.oversized_refusals.fetch_add(1, Ordering::Relaxed);
            }
            KeepOutcome::SeriesCapReached => {
                self.series_cap_refusals.fetch_add(1, Ordering::Relaxed);
            }
            KeepOutcome::IdentityOverCeiling { .. } => {
                self.identity_over_ceiling_refusals
                    .fetch_add(1, Ordering::Relaxed);
            }
            // The documented catch-all: the storage contract may grow
            // outcomes; an unknown one is counted and logged here, so a
            // future merge stays compilable and no outcome is ever
            // silently absorbed. Unreachable today — every current
            // variant is named above — which is exactly what the allow
            // records.
            #[allow(unreachable_patterns)]
            _ => {
                self.unknown_outcomes.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(?outcome, "the pump counted an unknown keep outcome");
            }
        }
    }

    /// A plain read of every counter, for surfaces and tests. The hook's
    /// refusal forwards ride along — the pump never sees them, the ledger
    /// does.
    #[must_use]
    pub fn summary(&self, refused_forwards: u64) -> RunSummary {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        RunSummary {
            refused_forwards,
            kept: load(&self.kept),
            evicted_on_keep: load(&self.evicted_on_keep),
            duplicates: load(&self.duplicates),
            oversized_refusals: load(&self.oversized_refusals),
            series_cap_refusals: load(&self.series_cap_refusals),
            identity_over_ceiling_refusals: load(&self.identity_over_ceiling_refusals),
            unknown_outcomes: load(&self.unknown_outcomes),
            dropped_on_drain: load(&self.dropped_on_drain),
        }
    }
}

/// A plain read of the pump's counters, taken at shutdown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunSummary {
    /// Records handed to the store and resident.
    pub kept: u64,
    /// Records the retention law removed while making room, during keeps.
    pub evicted_on_keep: u64,
    /// Keeps refused because the entity id was already resident.
    pub duplicates: u64,
    /// Keeps refused: the record alone exceeds the byte ceiling.
    pub oversized_refusals: u64,
    /// Keeps refused: the series cap is full.
    pub series_cap_refusals: u64,
    /// Keeps refused: the stream identity alone exceeds the byte ceiling.
    pub identity_over_ceiling_refusals: u64,
    /// Outcomes the pump had no name for — zero while the storage contract
    /// is the version this graph was built against.
    pub unknown_outcomes: u64,
    /// Records still queued when the drain deadline passed: dropped,
    /// observably.
    pub dropped_on_drain: u64,
    /// Keeps the store refused with nothing inserted, whose ledger
    /// identities the hook ended — the refusal side of "eviction ends
    /// identity" (ADR 0008).
    pub refused_forwards: u64,
}

/// The consumer thread's and the retention thread's handles, kept so
/// [`CoreRuntime::shutdown`] can join them. `retention_stop` is the
/// flag-and-signal pair that ends the retention timer's sleep.
struct Workers {
    pump: std::thread::JoinHandle<()>,
    retention: std::thread::JoinHandle<()>,
    retention_stop: Arc<(Mutex<bool>, Condvar)>,
}

/// The runtime graph: one pipeline, one bounded queue, one store, one
/// ledger — shared by the OTLP/HTTP and OTLP/gRPC surfaces alike.
pub struct CoreRuntime {
    pipeline: Arc<Pipeline>,
    queue: Arc<BoundedQueue>,
    store: Arc<Mutex<Box<dyn TelemetryStore>>>,
    clock: Arc<AdmissionClock>,
    counters: PumpCounters,
    /// Shared with the eviction hook: refused keeps whose ledger identity
    /// the hook ended.
    refused_forwards: Arc<AtomicU64>,
    /// The drain deadline, set once: when the session stops admitting and
    /// the pump's remaining time starts being spent.
    drain_deadline: Mutex<Option<Instant>>,
    payload_ceiling_bytes: usize,
    grpc_decoding_ceiling_bytes: usize,
    /// The transport-edge aggregate in-flight body budget and the per-request
    /// read deadline, shared by both OTLP transports ([ADR 0010]).
    ///
    /// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
    transport_guard: Arc<InflightBodyBudget>,
    body_read_timeout: Duration,
    workers: Mutex<Option<Workers>>,
}

impl CoreRuntime {
    /// Builds the graph and starts its workers: the queue pump and the
    /// retention timer.
    ///
    /// # Errors
    ///
    /// [`PipelineConfigError`](runtime_trail_telemetry_ingestion::PipelineConfigError)
    /// when the configured queue ceiling is below the worst-case legal
    /// record bound of the configured budgets — a startup
    /// misconfiguration that must be loud, never a record stranded behind
    /// the retryable saturation signal forever.
    ///
    /// # Panics
    ///
    /// Only in two configuration-guard shapes, both loud on purpose:
    ///
    /// - the payload ceiling plus the gRPC framing slack overflows `usize`
    ///   — a `usize`-arithmetic guard on configuration, unreachable for
    ///   every ceiling a platform this code compiles for can address;
    /// - the two worker threads (`std::thread::Builder::spawn` for the
    ///   ingestion pump and the retention timer) cannot be spawned — an OS
    ///   refusal to start a thread a session cannot run without.
    pub fn build(config: RuntimeConfig) -> Result<Arc<Self>, PipelineConfigError> {
        let grpc_decoding_ceiling_bytes = config
            .payload_ceiling_bytes
            .checked_add(GRPC_DECODING_SLACK_BYTES)
            .expect("a payload ceiling plus framing slack fits a usize");
        let queue = BoundedQueue::new(PIPELINE_QUEUE_NAME, config.queue_ceiling_bytes);
        let pipeline = Arc::new(Pipeline::with_config(
            Arc::clone(&queue) as Arc<dyn RecordSink>,
            config.budgets,
            config.payload_ceiling_bytes,
        )?);
        let refused_forwards = Arc::new(AtomicU64::new(0));
        let hook = LedgerHook {
            releaser: pipeline.ledger_releaser(),
            refused_forwards: Arc::clone(&refused_forwards),
        };
        let store: Box<dyn TelemetryStore> =
            Box::new(InMemoryStore::new(config.store, Some(Box::new(hook))));
        let runtime = Arc::new(Self {
            pipeline,
            queue,
            store: Arc::new(Mutex::new(store)),
            clock: Arc::new(AdmissionClock::new(config.clock)),
            counters: PumpCounters::default(),
            refused_forwards,
            drain_deadline: Mutex::new(None),
            payload_ceiling_bytes: config.payload_ceiling_bytes,
            grpc_decoding_ceiling_bytes,
            transport_guard: Arc::new(InflightBodyBudget::new(config.inflight_body_ceiling_bytes)),
            body_read_timeout: config.body_read_timeout,
            workers: Mutex::new(None),
        });
        runtime.spawn_workers();
        Ok(runtime)
    }

    fn spawn_workers(self: &Arc<Self>) {
        let pump_runtime = Arc::clone(self);
        let pump = std::thread::Builder::new()
            .name("ingestion-pump".to_owned())
            .spawn(move || pump_loop(&pump_runtime))
            .expect("spawn the ingestion pump thread");
        let retention_runtime = Arc::clone(self);
        let retention_stop: Arc<(Mutex<bool>, Condvar)> =
            Arc::new((Mutex::new(false), Condvar::new()));
        let retention = std::thread::Builder::new()
            .name("retention-tick".to_owned())
            .spawn({
                let retention_stop = Arc::clone(&retention_stop);
                move || retention_loop(&retention_runtime, &retention_stop)
            })
            .expect("spawn the retention tick thread");
        *self.lock_workers() = Some(Workers {
            pump,
            retention,
            retention_stop,
        });
    }

    /// Begins draining: the pipeline admits nothing new, and the pump
    /// spends at most [`DRAIN_DEADLINE`] finishing what is queued. The
    /// deadline is set once — whoever calls this first anchors it.
    pub fn begin_drain(&self) {
        self.begin_drain_deadline(Instant::now() + DRAIN_DEADLINE);
    }

    /// The drain path with the deadline supplied — the shutdown wiring
    /// uses [`Self::begin_drain`]; tests anchor the deadline themselves.
    pub(crate) fn begin_drain_deadline(&self, deadline: Instant) {
        self.pipeline.begin_draining();
        let mut drain = self.lock_drain();
        if drain.is_none() {
            *drain = Some(deadline);
        }
    }

    /// Whether the session is draining — the surfaces' prompt closing
    /// answer, checked before a request body is read.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.pipeline.is_draining()
    }

    /// The admission pipeline every surface ingests through.
    #[must_use]
    pub(crate) fn pipeline(&self) -> &Arc<Pipeline> {
        &self.pipeline
    }

    /// The payload ceiling the OTLP/HTTP routes bound buffering with.
    #[must_use]
    pub fn payload_ceiling_bytes(&self) -> usize {
        self.payload_ceiling_bytes
    }

    /// The decoding ceiling the gRPC frame reader allows: the payload
    /// ceiling plus the framing slack, so over-ceiling exports reach the
    /// pipeline's own gate — which names the ceiling — instead of the
    /// generic gRPC limit.
    #[must_use]
    pub(crate) fn grpc_decoding_ceiling_bytes(&self) -> usize {
        self.grpc_decoding_ceiling_bytes
    }

    /// The transport-edge aggregate in-flight body budget — the shared gate
    /// both OTLP transports charge before buffering a request body
    /// ([ADR 0010]).
    ///
    /// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
    #[must_use]
    pub(crate) fn inflight_body_budget(&self) -> &Arc<InflightBodyBudget> {
        &self.transport_guard
    }

    /// The per-request body read deadline — the bound a single body's
    /// buffering window must fit within ([ADR 0010]).
    ///
    /// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
    #[must_use]
    pub(crate) fn body_read_timeout(&self) -> Duration {
        self.body_read_timeout
    }

    /// The admission clock's next reading: the `admitted_at` every
    /// ingested record is stamped with.
    pub fn now(&self) -> AdmissionTime {
        self.clock.now()
    }

    /// The mode name the version surface reports — the real driver's own
    /// answer, through the abstraction.
    #[must_use]
    pub(crate) fn storage_mode(&self) -> &'static str {
        self.lock_store().mode_name()
    }

    /// The store's residency snapshot: the observability the wire gates
    /// and tests read.
    #[must_use]
    pub(crate) fn store_stats(&self) -> StoreStats {
        self.lock_store().stats()
    }

    /// The pump's counters as plain numbers.
    #[must_use]
    pub fn counters(&self) -> RunSummary {
        self.counters.summary(self.refused_keep_forwards())
    }

    /// Keeps the store refused with nothing inserted whose ledger identity
    /// the hook then ended — the refusal half of the ADR 0008 edge, as
    /// this graph actually ran it.
    #[must_use]
    pub fn refused_keep_forwards(&self) -> u64 {
        self.refused_forwards.load(Ordering::Relaxed)
    }

    /// Stops the session: drains the queue to storage within the drain
    /// deadline, joins the workers, and reports. Whatever is still queued
    /// when the deadline passes is dropped observably — counted and
    /// logged, never silent.
    ///
    /// Safe to call once per runtime; a second call reports the final
    /// counters.
    pub fn shutdown(&self) -> RunSummary {
        let workers = self.lock_workers().take();
        if workers.is_some() {
            self.begin_drain();
        }
        if let Some(workers) = workers {
            let Workers {
                pump,
                retention,
                retention_stop,
            } = workers;
            if pump.join().is_err() {
                tracing::error!("the ingestion pump thread panicked");
            }
            let (flag, signal) = &*retention_stop;
            *flag.lock().unwrap_or_else(PoisonError::into_inner) = true;
            signal.notify_all();
            if retention.join().is_err() {
                tracing::error!("the retention tick thread panicked");
            }
        }
        // The pump has exited; an in-flight handler could still have
        // offered after it stopped counting, so the queue's remaining
        // length is read here — after the join — as the dropped total.
        let dropped = self.queue.len();
        self.counters
            .dropped_on_drain
            .fetch_add(dropped as u64, Ordering::Relaxed);
        if dropped > 0 {
            tracing::warn!(
                dropped,
                deadline_secs = DRAIN_DEADLINE.as_secs(),
                "drain deadline reached: records still in flight were dropped, observably"
            );
        }
        let summary = self.counters.summary(self.refused_keep_forwards());
        // The stop's residency snapshot: what the bounded store actually
        // holds when the runtime ends, next to what the pump kept.
        let residency = self.store_stats();
        tracing::info!(?summary, ?residency, "runtime shutdown complete");
        summary
    }

    fn lock_workers(&self) -> MutexGuard<'_, Option<Workers>> {
        self.workers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_drain(&self) -> MutexGuard<'_, Option<Instant>> {
        self.drain_deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_store(&self) -> MutexGuard<'_, Box<dyn TelemetryStore>> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes the store's lock — the seam that lets tests freeze the pump
    /// (its keeps block here) while they fill the queue or observe a drain.
    #[cfg(test)]
    pub(crate) fn lock_store_for_test(&self) -> MutexGuard<'_, Box<dyn TelemetryStore>> {
        self.lock_store()
    }

    /// The hand-off queue's record count — the seam that lets tests
    /// observe a *pop*. The pump pops a record before its keep takes the
    /// store lock, so a frozen store bounds the pump to one further pop
    /// without bounding when that pop lands; queue-length movement is the
    /// only test-visible proof that pop has happened (a keep blocked on
    /// the frozen store counts nothing).
    #[cfg(test)]
    pub(crate) fn queue_len_for_test(&self) -> usize {
        self.queue.len()
    }
}

impl std::fmt::Debug for CoreRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreRuntime")
            .field(
                "counters",
                &self.counters.summary(self.refused_keep_forwards()),
            )
            .field("payload_ceiling_bytes", &self.payload_ceiling_bytes)
            .field("is_draining", &self.pipeline.is_draining())
            .finish_non_exhaustive()
    }
}

/// The framing slack the gRPC reader is allowed above the payload ceiling.
///
/// A gRPC message travels length-prefixed, so a payload at the ceiling is
/// a message a few bytes larger than the ceiling. The slack is sized to
/// cover that framing (5 bytes) with room for encoder overhead, so an
/// over-ceiling export reaches the pipeline's own
/// [`AdmissionSignal::PayloadOverCap`] gate — whose answer names the
/// contract ceiling — instead of the generic gRPC
/// `OUT_OF_RANGE`, which names nothing.
const GRPC_DECODING_SLACK_BYTES: usize = 8 * 1024;

/// The pump: pops one admitted record and hands it to the store, forever,
/// until the drain deadline ends it.
///
/// The drain check leads every iteration: once draining, the pump waits
/// only out the remaining deadline on each pop and exits — through the
/// empty-queue door or the deadline door — never waiting for new work
/// that admission no longer produces.
fn pump_loop(runtime: &CoreRuntime) {
    loop {
        // Read the drain state and let the lock go BEFORE waiting on the
        // queue: the shutdown path takes this same mutex to begin the
        // drain, and a guard held across the pop would make every
        // `begin_drain` wait out the pump's whole bounded wait.
        let drain_deadline = *runtime.lock_drain();
        let record = match drain_deadline {
            Some(deadline) => {
                // Draining: the deadline bounds the drain, so a spent
                // deadline ends the pump before another pop — whatever is
                // still queued is then dropped, and counted, by shutdown.
                // Otherwise a non-blocking pop first, so an emptied queue
                // ends the pump immediately; the bounded wait is only for
                // what is still in flight.
                match deadline.checked_duration_since(Instant::now()) {
                    None => None,
                    Some(remaining) => match runtime.queue.pop_timeout(Duration::ZERO) {
                        Some(record) => Some(record),
                        None => runtime.queue.pop_timeout(remaining),
                    },
                }
            }
            None => match runtime.queue.pop_timeout(PUMP_IDLE_POLL) {
                Some(record) => Some(record),
                None => continue, // woke without work: re-check the drain state
            },
        };
        let Some(record) = record else {
            // The deadline passed with the queue empty (or the drain
            // itself expired): the pump is done. Whatever remains is
            // counted by `CoreRuntime::shutdown`, after this join, so an
            // in-flight handler's last offers are not lost from the count.
            return;
        };
        keep_record(runtime, record);
    }
}

/// Hands one queued record to the store: the admitted-then-kept pipeline's
/// write side. The record travels exactly as admission queued it — the
/// ledger's `Arc`s, the entity id, the admission time.
fn keep_record(runtime: &CoreRuntime, record: QueuedRecord) {
    let outcome = {
        let mut store = runtime.lock_store();
        let entity = record.entity;
        let admitted_at = record.admitted_at;
        match record.record {
            StoredRecord::Span(span) => store.keep_span(Admitted {
                entity,
                admitted_at,
                record: span,
            }),
            StoredRecord::Log(log) => store.keep_log_record(Admitted {
                entity,
                admitted_at,
                record: log,
            }),
            StoredRecord::Point { stream, point } => store.keep_metric_point(
                Admitted {
                    entity,
                    admitted_at,
                    record: point,
                },
                stream,
            ),
        }
    };
    if outcome.evicted() > 0 {
        tracing::debug!(
            evicted = outcome.evicted(),
            "the retention law removed records while keeping an admitted record"
        );
    }
    runtime.counters.observe(&outcome);
}

/// The retention timer's body: one pass of the retention law against the
/// store, at the composition root's reading of time.
///
/// The store owns no clock — this is where the law lands. Exposed as a
/// free function over `&mut dyn TelemetryStore` so tests can tick a store
/// directly, with their own `now`.
pub fn retention_tick(store: &mut dyn TelemetryStore, now: AdmissionTime) -> u64 {
    let evicted = store.enforce_retention(now);
    if evicted > 0 {
        tracing::debug!(
            evicted,
            "the retention tick expired records outside the admission window"
        );
    }
    evicted
}

/// The retention timer: one tick per [`RETENTION_TICK_PERIOD`], until the
/// runtime stops.
fn retention_loop(runtime: &CoreRuntime, stop: &(Mutex<bool>, Condvar)) {
    let (flag, signal) = stop;
    let mut stopping = flag.lock().unwrap_or_else(PoisonError::into_inner);
    while !*stopping {
        let (guard, _timed_out) = signal
            .wait_timeout(stopping, RETENTION_TICK_PERIOD)
            .unwrap_or_else(PoisonError::into_inner);
        stopping = guard;
        if *stopping {
            break;
        }
        let now = runtime.clock.now();
        retention_tick(&mut **runtime.lock_store(), now);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use runtime_trail_storage_memory::{InMemoryStore, MemoryConfig};
    use runtime_trail_telemetry_ingestion::fixtures as fx;
    use runtime_trail_telemetry_ingestion::{RecordOutcome, StoredRecord};
    use runtime_trail_telemetry_model::Admitted;

    use super::*;

    /// Polls until the pump's counters satisfy `pred`, with a hard bound so
    /// a stuck pump fails the test instead of hanging it.
    fn wait_for_pump(runtime: &CoreRuntime, pred: impl Fn(&RunSummary) -> bool) -> RunSummary {
        for _ in 0..5_000 {
            let summary = runtime.counters();
            if pred(&summary) {
                return summary;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!(
            "the pump never reached the expected counters: {:?}",
            runtime.counters()
        );
    }

    /// The encoded metrics export carrying one point of one stream: the
    /// stream is named by the metric descriptor alone, so `name` selects it.
    fn one_point_export(name: &str, at: u64) -> Vec<u8> {
        let mut point = fx::number_point(fx::as_double(1.0));
        point.time_unix_nano = at;
        let metric = fx::described_metric(name, "d", "s", Vec::new(), vec![point]);
        let request = fx::metrics_request(vec![fx::resource_metrics(
            None,
            vec![fx::scope_metrics(None, vec![metric])],
        )]);
        fx::encode(&request)
    }

    /// The encoded traces export carrying one legal span.
    fn one_span_export(trace_id: [u8; 16], span_id: [u8; 8]) -> Vec<u8> {
        let request = fx::traces_request(vec![fx::resource_spans(
            None,
            vec![fx::scope_spans(
                None,
                vec![fx::trace_span("op", trace_id, span_id)],
            )],
        )]);
        fx::encode(&request)
    }

    /// A store whose only tight ceiling is the record count.
    fn two_record_config() -> RuntimeConfig {
        RuntimeConfig {
            store: MemoryConfig {
                max_records: 2,
                max_accounted_bytes: u64::MAX,
                admission_window: Duration::from_secs(3_600),
                series_cap: u64::MAX,
            },
            ..RuntimeConfig::default()
        }
    }

    /// The ADR 0008 loop, through the real wiring: when the store evicts,
    /// the hook ends the evicted record's identity in the ledger the
    /// pipeline admitted through. The behavioral proof is the fresh
    /// re-admission of the exact evicted point; the store-side counters
    /// prove residency and identity ended together; and the ledger-side
    /// counterpart — the interned stream released exactly when its last
    /// point leaves — is asserted here through the pipeline's
    /// fixture-gated releaser window, so a hook whose `stream_released`
    /// forward is a no-op fails this test instead of passing silently.
    #[test]
    fn an_eviction_ends_identity_through_the_hook() {
        let runtime = CoreRuntime::build(two_record_config()).expect("the config is buildable");
        let releaser = runtime.pipeline().ledger_releaser();
        let ingest_point = |name: &str, at: u64| {
            runtime
                .pipeline()
                .ingest_metrics(
                    AdmissionTime::from_unix_nano(at),
                    &one_point_export(name, at),
                )
                .expect("a one-point export is admitted")
        };

        // Keep two points of stream "a", then one of stream "b". The third
        // keep evicts stream "a"'s first point; "a" still has a resident
        // point, so the stream itself stays.
        ingest_point("a", 1);
        ingest_point("a", 2);
        ingest_point("b", 3);
        wait_for_pump(&runtime, |summary| summary.kept == 3);
        assert_eq!(runtime.counters().evicted_on_keep, 1);
        assert_eq!(
            releaser.resident_streams(),
            2,
            "streams a and b are interned while their points are resident"
        );

        // The next keep evicts stream "a"'s last point: the stream leaves
        // residency on the store side with it.
        ingest_point("b", 4);
        wait_for_pump(&runtime, |summary| summary.kept == 4);
        assert_eq!(runtime.counters().evicted_on_keep, 2);
        let stats = runtime.store_stats();
        assert_eq!(stats.resident_records, 2);
        assert_eq!(
            stats.resident_streams, 1,
            "stream a's last point left: it is resident no more"
        );
        assert_eq!(
            releaser.resident_streams(),
            1,
            "the hook released stream a's interned identity when its last \
             point left: the ledger tracks residency, not history"
        );

        // The proof the evictions ended the evicted point's identity: the
        // exact same point, re-delivered, is admitted FRESH — a `Collapsed`
        // outcome would mean the ledger still held the evicted entry
        // behind it.
        //
        // The freeze holds across the re-delivery and its ledger reads on
        // purpose: the re-delivery carries its original admission time, so
        // once the pump keeps it the store's make-room eviction evicts the
        // record it just kept (its key is the smallest resident key) and
        // the hook then releases the stream again. Without the freeze the
        // two asserts below race the pump's keep and can read the release
        // instead of the intern (filed flake #10). With it, the pump cannot
        // keep — and thereby cannot evict or release — until the
        // ledger-side proof is done. Nothing is awaited under the lock: the
        // blocked keep is the test's determinism device, the same one the
        // saturation tests use.
        {
            let _frozen = runtime.lock_store_for_test();
            let again = ingest_point("a", 1);
            assert!(
                matches!(&again.records[0], RecordOutcome::Admitted { .. }),
                "the re-delivery must be fresh, not collapsed onto the evicted \
                 entry: {:?}",
                again.records[0]
            );
            assert_eq!(
                releaser.resident_streams(),
                2,
                "the re-delivery re-interned stream a fresh"
            );
        }
        wait_for_pump(&runtime, |summary| summary.kept == 5);
        // Every removal was reported: identity ended wherever residency
        // did, with no divergence between the store's removals and the
        // hook's deliveries.
        let stats = runtime.store_stats();
        assert_eq!(stats.hook_deliveries, stats.total_evictions());
    }

    /// The refusal side of the ADR 0008 edge, through the same wiring: a
    /// keep the store refuses inserted nothing, and the hook ends the
    /// identity admission had already handed out — so a re-delivery is
    /// admitted **fresh**, not collapsed onto the refused entry's phantom.
    /// The refused point's stream release — the `Some(stream)` arm — is
    /// asserted here at the ledger through the fixture-gated releaser
    /// window, so a hook whose `stream_released` forward is a no-op fails
    /// this test instead of passing silently.
    #[test]
    fn a_refused_keep_ends_identity_through_the_hook() {
        // A byte ceiling no record can fit: every keep is refused with
        // nothing inserted.
        let runtime = CoreRuntime::build(RuntimeConfig {
            store: MemoryConfig {
                max_accounted_bytes: 1,
                ..MemoryConfig::default()
            },
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let payload = one_span_export(fx::T1, fx::S1);

        // Admission has no store-side opinion: the span is admitted,
        // queued, refused by the store, and its identity is then ended by
        // the hook.
        let first = runtime
            .pipeline()
            .ingest_spans(AdmissionTime::from_unix_nano(1), &payload)
            .expect("admission admits the span");
        assert!(
            matches!(&first.records[0], RecordOutcome::Admitted { .. }),
            "the first delivery is admitted: {:?}",
            first.records[0]
        );
        wait_for_pump(&runtime, |summary| summary.refused_forwards == 1);
        assert_eq!(runtime.store_stats().resident_records, 0, "nothing kept");

        // The proof the span's identity really ended: the identical
        // re-delivery is admitted **fresh** — a `Collapsed` outcome would
        // mean the refused entry's identity was stranded in the ledger,
        // and the re-delivery had collapsed onto that phantom.
        let again = runtime
            .pipeline()
            .ingest_spans(AdmissionTime::from_unix_nano(2), &payload)
            .expect("the re-delivery is admitted");
        assert!(
            matches!(&again.records[0], RecordOutcome::Admitted { .. }),
            "the re-delivery must be admitted fresh, not collapsed onto \
             the refused entry's stranded identity: {:?}",
            again.records[0]
        );

        // The `Some(stream)` arm rides the same hook: a refused point's
        // store refusal carries its stream, and the forward is counted
        // like every other.
        runtime
            .pipeline()
            .ingest_metrics(AdmissionTime::from_unix_nano(3), &one_point_export("a", 3))
            .expect("admission admits the point");
        // Three forwards: both span deliveries and the point were all
        // refused by the 1-byte ceiling, and every refusal was reported to
        // the hook — no divergence between refusals and forwards.
        wait_for_pump(&runtime, |summary| summary.refused_forwards == 3);
        let stats = runtime.store_stats();
        assert_eq!(stats.refused_hook_deliveries, stats.total_keep_refusals());
        assert_eq!(stats.resident_records, 0);
        assert_eq!(
            runtime.pipeline().ledger_releaser().resident_streams(),
            0,
            "the refused point's stream must leave the ledger with its \
             refused keep — an interned survivor would pin the identity \
             for the whole session"
        );
    }

    /// The retention tick is the composition root's clock handed to the
    /// store's law: records outside the admission window leave, records
    /// inside it stay.
    #[test]
    fn retention_tick_expires_records_outside_the_window() {
        // Fabricate one real model span through the pipeline's own door.
        let harness = fx::Harness::new();
        let payload = one_span_export(fx::T1, fx::S1);
        let now = AdmissionTime::from_unix_nano(1_000);
        let outcome = harness
            .pipeline
            .ingest_spans(now, &payload)
            .expect("a one-span export is legal");
        let entity = fx::standing_entity(&outcome, 0);
        let record = harness.drain().pop().expect("one record queued").record;
        let span = match record {
            StoredRecord::Span(span) => span,
            StoredRecord::Log(_) | StoredRecord::Point { .. } => {
                panic!("expected a span record")
            }
        };

        let mut store = InMemoryStore::new(
            MemoryConfig {
                admission_window: Duration::from_secs(10),
                ..MemoryConfig::default()
            },
            None,
        );
        let _kept = store.keep_span(Admitted {
            entity,
            admitted_at: now,
            record: span,
        });

        // Five seconds in, the record is inside the window and stays.
        let inside = now.as_unix_nano() + 5_000_000_000;
        assert_eq!(
            retention_tick(&mut store, AdmissionTime::from_unix_nano(inside)),
            0
        );
        assert_eq!(store.stats().resident_records, 1);

        // Eleven seconds in, the record is outside the window and leaves.
        let outside = now.as_unix_nano() + 11_000_000_000;
        assert_eq!(
            retention_tick(&mut store, AdmissionTime::from_unix_nano(outside)),
            1
        );
        assert_eq!(store.stats().resident_records, 0);
    }

    /// The drain path: shutdown refuses new telemetry, drains what it can
    /// within the deadline, and whatever cannot be reached is dropped
    /// observably — counted, not lost silently.
    #[test]
    fn drain_drops_the_unreachable_observably() {
        let runtime =
            CoreRuntime::build(RuntimeConfig::default()).expect("the default config is buildable");
        let at = AdmissionTime::from_unix_nano(1_000);
        let payloads = [
            one_span_export(fx::T1, fx::S1),
            one_span_export(fx::T2, fx::S2),
            one_span_export(fx::T1, fx::S2),
        ];

        // Freeze the pump: its keeps block on the store's lock, so the
        // queue holds what admission offers.
        {
            let _frozen = runtime.lock_store_for_test();
            for payload in &payloads {
                runtime
                    .pipeline()
                    .ingest_spans(at, payload)
                    .expect("each one-span export is admitted");
            }
            // Drain begins while the records are still queued, with the
            // deadline already spent: at most the one record the pump
            // already popped is reachable, the rest miss the drain.
            runtime.begin_drain_deadline(
                Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .expect("a past instant on any running system"),
            );
        }
        assert!(runtime.is_draining());

        let summary = runtime.shutdown();
        assert_eq!(
            summary.kept + summary.dropped_on_drain,
            payloads.len() as u64,
            "every record is either kept or dropped observably"
        );
        assert!(
            summary.dropped_on_drain >= 2,
            "the frozen-out records are dropped and counted: {summary:?}"
        );
    }

    /// The admission clock: a wall clock that stalls or steps backwards
    /// still hands out a strictly increasing admission time, because
    /// residency order is admission time.
    #[test]
    fn admission_clock_never_repeats_or_steps_back() {
        struct SteppingClock(std::sync::Mutex<Vec<u64>>);
        impl WallClock for SteppingClock {
            fn reading(&self) -> AdmissionTime {
                let mut steps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
                AdmissionTime::from_unix_nano(steps.remove(0))
            }
        }
        // Readings 100, 100, 90, 200: the wall clock stalls, steps
        // backwards, then jumps forward.
        let clock = AdmissionClock::new(Box::new(SteppingClock(std::sync::Mutex::new(vec![
            100, 100, 90, 200,
        ]))));
        let first = clock.now().as_unix_nano();
        let second = clock.now().as_unix_nano();
        let third = clock.now().as_unix_nano();
        let fourth = clock.now().as_unix_nano();
        assert_eq!(first, 100);
        assert_eq!(second, 101, "a stalled wall clock still advances");
        assert_eq!(third, 102, "a backwards step never turns the clock back");
        assert_eq!(fourth, 200, "a real step forward is taken");
    }
}
