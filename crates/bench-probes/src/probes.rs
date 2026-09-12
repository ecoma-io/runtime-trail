//! The Phase 1 probes: the real memory path, driven to its bounds and
//! measured from the process.
//!
//! Each probe composes the real runtime the way the composition root will
//! (queue → pipeline → store; `crates/server` does not wire ingestion
//! until Phase 1 wave 3, so the composition here mirrors the crates' own
//! construction APIs and says so in its output), drives it with real
//! wire bytes, and reports what the process and the accounted ceilings
//! actually did:
//!
//! - [`run_overload`] — repeated max-size legal OTLP exports through the
//!   real pipeline into the real in-memory store at the *contract*
//!   ceilings (64 MiB queue, 256 MiB accounted retention): the pump keeps
//!   up until the store's ceiling holds, then ingestion is pushed without
//!   a pump until [`AdmissionSignal::QueueSaturated`] persists — the one
//!   retryable signal, observed at saturation.
//! - [`run_retention`] — the retention law alone: fill a store at a
//!   small, stated configuration until its ceilings evict, then keep
//!   loading and sample RSS across the steady state. Bounded, not fixed:
//!   the samples are the evidence, printed, never gated here (Phase 6 owns
//!   gating — `runtime-constraints.md`, "Enforcement trajectory").
//! - [`run_idle`] — the composition, constructed and left alone: the
//!   process's idle resident set.
//!
//! # What a probe may and may not say
//!
//! A probe that ran and produced its numbers succeeded — Phase 1
//! measures, it does not gate. A probe that could not measure (RSS
//! unreadable, an admission signal its workload cannot explain) fails
//! loudly with [`ProbeError`] and exits non-zero, per AGENTS.md: a check
//! that cannot run says so and exits non-zero.
//!
//! # The margin the records are for
//!
//! Every overload/retention record carries both sides of the gap the
//! benchmarks contract exists to measure: the *accounted* bytes
//! (`accounted_bytes`, `queue_accounted_bytes` — the model's accounting,
//! what the ceilings bound) and the *real* bytes (`vm_rss_kib`,
//! `vm_hwm_kib` — the kernel's page accounting, everything the process
//! holds). The difference is computed from the record, never asserted by
//! the probe: `resident_accounted_bytes` is printed so the margin is a
//! subtraction away, exactly as `runtime-constraints.md` asks ("how large
//! the real-to-accounted margin actually is is a measurement for
//! benchmarks README once Phase 1 lands — never an assertion made here").

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use runtime_trail_storage::{KeepOutcome, StoreStats, TelemetryStore};
use runtime_trail_storage_memory::{InMemoryStore, MemoryConfig};
use runtime_trail_telemetry_ingestion::{
    AdmissionSignal, BoundedQueue, ExportOutcome, PIPELINE_QUEUE_NAME, Pipeline,
    PipelineConfigError, QUEUE_CEILING_BYTES, QueuedRecord, RecordOutcome, RecordSink,
    StoredRecord,
};
use runtime_trail_telemetry_model::{AdmissionTime, Admitted};

use crate::json::{JsonRecord, JsonValue};
use crate::rss;
use crate::shapes::{gauge_point_attributes, gauge_shape, log_record_attributes, log_shape};
use crate::wire::{encode_gauge_export, encode_log_export};

/// Why a probe could not produce its numbers.
#[derive(Debug)]
pub struct ProbeError(String);

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ProbeError {}

/// The runtime's clock, as admission expects it: nanoseconds on the
/// runtime's wall clock. The model owns no clock; neither does this crate.
#[must_use]
pub fn now_unix_nano() -> AdmissionTime {
    let nano = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        });
    AdmissionTime::from_unix_nano(nano)
}

/// Queue + pipeline + store, wired the way the composition root wires
/// them (mirrored from the crates' construction APIs; the server wires
/// this itself in Phase 1 wave 3).
pub struct Composition {
    /// The bounded hand-off queue admission writes into.
    pub queue: Arc<BoundedQueue>,
    /// The admission pipeline over that queue.
    pub pipeline: Arc<Pipeline>,
    /// The in-memory store the pump keeps into.
    pub store: InMemoryStore,
}

impl Composition {
    /// The composition at `store_config` with the contract queue ceiling.
    ///
    /// # Errors
    ///
    /// [`PipelineConfigError`] if the contract queue ceiling were ever
    /// below the pipeline's worst-case legal record — impossible under
    /// the contract numbers, and a startup misconfiguration if it ever
    /// changes.
    pub fn new(store_config: MemoryConfig) -> Result<Self, PipelineConfigError> {
        let queue = BoundedQueue::new(PIPELINE_QUEUE_NAME, QUEUE_CEILING_BYTES);
        let pipeline = Pipeline::with_config(
            Arc::clone(&queue) as Arc<dyn RecordSink>,
            runtime_trail_telemetry_model::BudgetLimits::default(),
            runtime_trail_telemetry_model::budgets::OTLP_PAYLOAD_BYTES,
        )?;
        Ok(Self {
            queue,
            pipeline: Arc::new(pipeline),
            store: InMemoryStore::new(store_config, None),
        })
    }

    /// The consumer side of the hand-off: pops everything queued and
    /// keeps it into the store — the pump the server runs (wave 3), in
    /// its simplest complete form. Never blocks; a probe pumps only when
    /// it intends to drain.
    pub fn pump(&mut self) -> PumpTally {
        let mut tally = PumpTally::default();
        while let Some(record) = self.queue.pop_timeout(Duration::ZERO) {
            tally.tally(self.keep(record));
        }
        tally
    }

    fn keep(&mut self, record: QueuedRecord) -> KeepOutcome {
        let admitted_at = record.admitted_at;
        let entity = record.entity;
        match record.record {
            StoredRecord::Span(span) => self.store.keep_span(Admitted {
                entity,
                admitted_at,
                record: span,
            }),
            StoredRecord::Log(log) => self.store.keep_log_record(Admitted {
                entity,
                admitted_at,
                record: log,
            }),
            StoredRecord::Point { stream, point } => self.store.keep_metric_point(
                Admitted {
                    entity,
                    admitted_at,
                    record: point,
                },
                stream,
            ),
        }
    }
}

/// What one pump drain did, keep outcome by keep outcome.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PumpTally {
    /// Keeps that entered residency.
    pub kept: u64,
    /// Records the retention law removed across those keeps.
    pub evicted: u64,
    /// Keeps refused as individually oversized.
    pub oversized: u64,
    /// Keeps refused as duplicates of a resident entity id.
    pub duplicates: u64,
    /// Keeps refused because the stream identity alone exceeds the byte ceiling.
    pub identity_over_ceiling: u64,
    /// Keeps refused at the series cap.
    pub series_cap: u64,
}

impl PumpTally {
    fn tally(&mut self, outcome: KeepOutcome) {
        match outcome {
            KeepOutcome::Kept { evicted } => {
                self.kept += 1;
                self.evicted += evicted;
            }
            KeepOutcome::Duplicate => self.duplicates += 1,
            KeepOutcome::Oversized => self.oversized += 1,
            KeepOutcome::IdentityOverCeiling { .. } => self.identity_over_ceiling += 1,
            KeepOutcome::SeriesCapReached => self.series_cap += 1,
        }
    }

    fn add(&mut self, other: &Self) {
        self.kept += other.kept;
        self.evicted += other.evicted;
        self.oversized += other.oversized;
        self.duplicates += other.duplicates;
        self.identity_over_ceiling += other.identity_over_ceiling;
        self.series_cap += other.series_cap;
    }
}

/// What one export's outcomes added up to, per-record fate by fate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ExportTally {
    admitted: u64,
    collapsed: u64,
    conflicts: u64,
    rejected: u64,
}

impl ExportTally {
    fn tally(&mut self, outcome: &ExportOutcome) {
        for record in &outcome.records {
            match record {
                RecordOutcome::Admitted { .. } => self.admitted += 1,
                RecordOutcome::Collapsed { .. } => self.collapsed += 1,
                RecordOutcome::Conflict { .. } => self.conflicts += 1,
                RecordOutcome::Rejected { .. } => self.rejected += 1,
            }
        }
    }
}

/// `usize` counts and byte totals as `u64` in the records: the probe runs
/// where `usize` is 64-bit, and a count that could not convert has no
/// honest small number to wear instead.
fn u64_of(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Samples RSS, converting a kernel failure into the probe's own error —
/// a probe never continues without its measurement.
fn rss_sample() -> Result<rss::RssSample, ProbeError> {
    rss::sample().map_err(|error| ProbeError(error.to_string()))
}

/// The overload probe's knobs. Defaults are the published contract load;
/// the tests drive smaller knobs in-process.
#[derive(Clone, Copy, Debug)]
pub struct OverloadConfig {
    /// The store configuration under test — the contract ceilings by
    /// default; tests shrink it to run the whole probe in-process.
    pub store_config: MemoryConfig,
    /// Pumped rounds before the saturation phase (each round is one
    /// max-size export + one full pump).
    pub pumped_rounds: usize,
    /// Consecutive saturated exports that count as saturation persisting.
    pub saturation_streak: usize,
    /// Upper bound on saturation-phase exports, whatever the streak does.
    pub saturation_round_cap: usize,
    /// Sample RSS every N iterations (1 = every export).
    pub sample_every: usize,
    /// Wall-clock budget; a probe that cannot finish in it reports the
    /// partial state honestly rather than running away.
    pub wall_budget: Duration,
}

impl Default for OverloadConfig {
    /// The published contract load: the probes measure the default
    /// configuration, stated in the record next to the numbers.
    fn default() -> Self {
        Self {
            store_config: MemoryConfig::default(),
            pumped_rounds: 48,
            saturation_streak: 3,
            saturation_round_cap: 12,
            sample_every: 4,
            wall_budget: Duration::from_secs(90),
        }
    }
}

/// What the overload probe measured. Every field is a fact the run
/// produced; none is compared against a target here.
#[derive(Clone, Debug)]
pub struct OverloadReport {
    /// The queue ceiling the run used (contract: 64 MiB).
    pub queue_ceiling_bytes: usize,
    /// The store configuration the run used (contract ceilings by
    /// default; printed so the record explains its own workload).
    pub store_config: MemoryConfig,
    /// Export requests actually offered to the pipeline.
    pub exports_ingested: u64,
    /// Exports the pipeline refused with `QueueSaturated` — the one
    /// retryable signal.
    pub queue_saturated_exports: u64,
    /// Bytes of one max-size legal export payload.
    pub payload_bytes: usize,
    /// Per-record fates across every export offered.
    pub admitted_records: u64,
    /// Re-deliveries that collapsed onto standing records.
    pub collapsed_records: u64,
    /// Conflicting re-deliveries (recorded anomalies).
    pub conflict_records: u64,
    /// Records the admission gates refused.
    pub rejected_records: u64,
    /// What the pump kept into the store, and what retention removed.
    pub pump: PumpTally,
    /// The store's counters and residency at the end.
    pub store_stats: StoreStats,
    /// The queue's accounted bytes at the end (its in-flight load).
    pub queue_accounted_bytes: u64,
    /// The queue's record count at the end.
    pub queue_records: u64,
    /// Admission anomalies the pipeline recorded.
    pub anomalies: u64,
    /// RSS at the end of the run.
    pub final_sample: rss::RssSample,
    /// RSS samples along the way (KiB), one per sampled iteration.
    pub rss_samples_kib: Vec<u64>,
    /// Ingest calls the probe made (exports offered).
    pub iterations: u64,
    /// Whether the run finished under its own wall budget.
    pub completed_within_budget: bool,
    /// Whether the saturation phase observed its streak.
    pub saturation_persisted: bool,
    /// Wall seconds the run took.
    pub wall_seconds: f64,
}

/// The overload run's state across its two phases: the composition, the
/// load shape, and every counter the report will print.
struct OverloadRun {
    composition: Composition,
    shape: crate::wire::GaugeShape,
    tally: ExportTally,
    pump_total: PumpTally,
    samples: Vec<u64>,
    export_index: u64,
    exports_ingested: u64,
    saturated: u64,
    payload_bytes: usize,
}

impl OverloadRun {
    fn new(store_config: MemoryConfig) -> Result<Self, ProbeError> {
        Ok(Self {
            composition: Composition::new(store_config)
                .map_err(|error| ProbeError(error.to_string()))?,
            shape: gauge_shape(),
            tally: ExportTally::default(),
            pump_total: PumpTally::default(),
            samples: Vec::new(),
            export_index: 0,
            exports_ingested: 0,
            saturated: 0,
            payload_bytes: 0,
        })
    }

    /// Offers one at-cap gauge export to the real pipeline: every fate it
    /// reports is tallied; a `QueueSaturated` refusal is counted and
    /// returned to the caller (the phases read it differently); any other
    /// signal is one a legal payload cannot explain, so the run stops.
    fn offer(&mut self) -> Result<Option<AdmissionSignal>, ProbeError> {
        let payload = encode_gauge_export(&self.shape, self.export_index, gauge_point_attributes);
        self.export_index += 1;
        self.exports_ingested += 1;
        self.payload_bytes = payload.len();
        match self
            .composition
            .pipeline
            .ingest_metrics(now_unix_nano(), payload.as_slice())
        {
            Ok(outcome) => {
                self.tally.tally(&outcome);
                Ok(None)
            }
            Err(saturated @ AdmissionSignal::QueueSaturated { .. }) => {
                self.saturated += 1;
                Ok(Some(saturated))
            }
            Err(other) => Err(ProbeError(format!(
                "unexpected admission signal: {other:?}"
            ))),
        }
    }

    /// Drains the queue into the store and lets the store observe the
    /// pipeline's admission anomalies — the pump the server runs, in its
    /// simplest complete form.
    fn pump_and_observe(&mut self) {
        self.pump_total.add(&self.composition.pump());
        self.composition
            .store
            .observe_admission_anomalies(self.composition.pipeline.anomalies().total());
        self.composition.store.enforce_retention(now_unix_nano());
    }
}

/// Drives the real pipeline + in-memory store at the contract ceilings to
/// saturation, and reports what the process and the accounting did.
///
/// # Errors
///
/// [`ProbeError`] when the process cannot be measured, or when the
/// pipeline refuses a probe payload with a signal that payload cannot
/// explain (a legal, at-cap, under-ceiling export has no reason to be
/// malformed, over-cap or draining) — a probe stops rather than reports a
/// state it does not understand.
pub fn run_overload(config: &OverloadConfig) -> Result<OverloadReport, ProbeError> {
    let mut run = OverloadRun::new(config.store_config)?;
    let started = Instant::now();
    let mut completed_within_budget = true;

    // Phase A — the pumped path: export, then drain the queue into the
    // store. The store's ceilings start evicting; the queue keeps
    // draining; this is the runtime's steady overload state.
    let mut steady_rounds: usize = 0;
    for round in 0..config.pumped_rounds {
        run.offer()?;
        run.pump_and_observe();
        if round % config.sample_every == 0 {
            run.samples.push(rss_sample()?.vm_rss_kib);
        }

        // Once the ceilings are holding under sustained load (evictions
        // on every round for a while), the pumped path is at steady
        // state; stop and go saturate the queue.
        if run.composition.store.stats().total_evictions() > 0 {
            steady_rounds += 1;
            if steady_rounds >= 8 {
                break;
            }
        } else {
            steady_rounds = 0;
        }
        if started.elapsed() > config.wall_budget {
            completed_within_budget = false;
            break;
        }
    }

    // Phase B — saturation: the producer outruns the consumer. No pump;
    // exports are offered until `QueueSaturated` persists — the one
    // retryable signal, at its contract ceiling. Whatever was admitted
    // before each refusal stays queued (never dropped), which is why the
    // queue's accounted bytes end at its ceiling.
    let mut streak: usize = 0;
    let mut saturation_persisted = false;
    for _ in 0..config.saturation_round_cap {
        if started.elapsed() > config.wall_budget {
            completed_within_budget = false;
            break;
        }
        if run.offer()?.is_some() {
            streak += 1;
            if streak >= config.saturation_streak {
                saturation_persisted = true;
                break;
            }
        } else {
            streak = 0;
        }
    }

    let stats = run.composition.store.stats();
    Ok(OverloadReport {
        queue_ceiling_bytes: run.composition.queue.ceiling_bytes(),
        store_config: config.store_config,
        exports_ingested: run.exports_ingested,
        queue_saturated_exports: run.saturated,
        payload_bytes: run.payload_bytes,
        admitted_records: run.tally.admitted,
        collapsed_records: run.tally.collapsed,
        conflict_records: run.tally.conflicts,
        rejected_records: run.tally.rejected,
        pump: run.pump_total,
        store_stats: stats,
        queue_accounted_bytes: u64_of(run.composition.queue.accounted_bytes()),
        queue_records: u64_of(run.composition.queue.len()),
        anomalies: run.composition.pipeline.anomalies().total(),
        final_sample: rss_sample()?,
        rss_samples_kib: run.samples,
        iterations: run.exports_ingested,
        completed_within_budget,
        saturation_persisted,
        wall_seconds: started.elapsed().as_secs_f64(),
    })
}

/// The retention probe's knobs: a small, stated store configuration (the
/// contract numbers are the *defaults*; config exists so a probe can fill
/// its ceilings in seconds), and how long to hold the plateau.
#[derive(Clone, Copy, Debug)]
pub struct RetentionConfig {
    /// The store configuration under test — printed in the record.
    pub store_config: MemoryConfig,
    /// Log exports to load before the plateau sampling begins.
    pub fill_exports: usize,
    /// Log exports to keep loading while sampling the steady state.
    pub plateau_exports: usize,
    /// Sample RSS every N plateau exports.
    pub sample_every: usize,
    /// Wall-clock budget.
    pub wall_budget: Duration,
}

impl Default for RetentionConfig {
    /// The published retention workload: a store whose ceilings arrive in
    /// seconds, loaded with log records — the record kind whose residency
    /// the retention law alone bounds (log records leave no admission
    /// ledger entry, so the store's ceilings are the whole bound; see the
    /// crate docs for what that means for the metric path at this base).
    fn default() -> Self {
        Self {
            store_config: MemoryConfig {
                max_records: 40_000,
                max_accounted_bytes: 24 * 1024 * 1024,
                admission_window: Duration::from_secs(60 * 60),
                series_cap: 100,
            },
            fill_exports: 10,
            plateau_exports: 30,
            sample_every: 3,
            wall_budget: Duration::from_secs(60),
        }
    }
}

/// What the retention probe measured.
#[derive(Clone, Debug)]
pub struct RetentionReport {
    /// The store configuration the run used (stated, so the record
    /// explains its own workload).
    pub store_config: MemoryConfig,
    /// The queue ceiling the run used.
    pub queue_ceiling_bytes: usize,
    /// Exports offered.
    pub exports_ingested: u64,
    /// Records admitted across those exports.
    pub admitted_records: u64,
    /// What the pump kept and what retention removed.
    pub pump: PumpTally,
    /// The store's counters and residency at the end.
    pub store_stats: StoreStats,
    /// RSS samples across the steady state (KiB).
    pub plateau_vm_rss_kib: Vec<u64>,
    /// The spread (max − min) of the plateau samples: how far the
    /// resident set moved while the ceilings held. A number, not a
    /// verdict — Phase 6 owns verdicts.
    pub plateau_spread_kib: u64,
    /// RSS at the end of the run.
    pub final_sample: rss::RssSample,
    /// Ingest calls made.
    pub iterations: u64,
    /// Whether the run finished under its own wall budget.
    pub completed_within_budget: bool,
    /// Wall seconds the run took.
    pub wall_seconds: f64,
}

/// Fills the store to its retention ceilings and holds it there, sampling
/// RSS across the steady state.
///
/// Log records are the load on purpose: they carry no natural identity,
/// so admission keeps no ledger entry for them and the retention
/// ceilings are the *whole* bound on their residency — the cleanest read
/// of the retention law's own bound. A metric-point load would measure
/// the admission ledger's growth on top (the pipeline owns its ledger and
/// releases it only through the wave-3 eviction hook, which a probe
/// outside the composition root cannot wire); the overload probe reports
/// that effect explicitly instead of hiding it inside this one.
///
/// # Errors
///
/// As [`run_overload`].
pub fn run_retention(config: &RetentionConfig) -> Result<RetentionReport, ProbeError> {
    let mut composition =
        Composition::new(config.store_config).map_err(|error| ProbeError(error.to_string()))?;
    let shape = log_shape();
    let mut tally = ExportTally::default();
    let mut pump_total = PumpTally::default();
    let mut export_index: u64 = 0;
    let mut iterations: u64 = 0;
    let mut plateau_samples: Vec<u64> = Vec::new();
    let started = Instant::now();
    let mut completed_within_budget = true;

    let load_one = |composition: &mut Composition,
                    export_index: &mut u64,
                    iterations: &mut u64,
                    tally: &mut ExportTally,
                    pump_total: &mut PumpTally|
     -> Result<(), ProbeError> {
        let payload = encode_log_export(&shape, *export_index, log_record_attributes);
        *export_index += 1;
        *iterations += 1;
        match composition
            .pipeline
            .ingest_logs(now_unix_nano(), payload.as_slice())
        {
            Ok(outcome) => tally.tally(&outcome),
            Err(other) => {
                return Err(ProbeError(format!(
                    "unexpected admission signal: {other:?}"
                )));
            }
        }
        pump_total.add(&composition.pump());
        composition
            .store
            .observe_admission_anomalies(composition.pipeline.anomalies().total());
        composition.store.enforce_retention(now_unix_nano());
        Ok(())
    };

    // Fill to (and past) the ceilings: evictions begin during the fill.
    for _ in 0..config.fill_exports {
        load_one(
            &mut composition,
            &mut export_index,
            &mut iterations,
            &mut tally,
            &mut pump_total,
        )?;
        if started.elapsed() > config.wall_budget {
            completed_within_budget = false;
            break;
        }
    }

    // The plateau: keep the same load on, sample the resident set. The
    // ceilings are already holding, so every sample is steady-state —
    // bounded, not fixed, is what the samples get checked for later, by a
    // human, against the record.
    for round in 0..config.plateau_exports {
        load_one(
            &mut composition,
            &mut export_index,
            &mut iterations,
            &mut tally,
            &mut pump_total,
        )?;
        if round % config.sample_every == 0 {
            plateau_samples.push(rss_sample()?.vm_rss_kib);
        }
        if started.elapsed() > config.wall_budget {
            completed_within_budget = false;
            break;
        }
    }

    let spread = match plateau_samples.iter().copied().reduce(u64::max) {
        Some(max) => max - plateau_samples.iter().copied().min().unwrap_or(max),
        None => 0,
    };
    let store_stats = composition.store.stats();
    Ok(RetentionReport {
        store_config: config.store_config,
        queue_ceiling_bytes: composition.queue.ceiling_bytes(),
        exports_ingested: iterations,
        admitted_records: tally.admitted,
        pump: pump_total,
        store_stats,
        plateau_vm_rss_kib: plateau_samples,
        plateau_spread_kib: spread,
        final_sample: rss_sample()?,
        iterations,
        completed_within_budget,
        wall_seconds: started.elapsed().as_secs_f64(),
    })
}

/// What the idle probe measured.
#[derive(Clone, Debug)]
pub struct IdleReport {
    /// The store configuration the composition was built with (contract
    /// ceilings).
    pub store_config: MemoryConfig,
    /// The queue ceiling the composition was built with (contract).
    pub queue_ceiling_bytes: usize,
    /// RSS once settled.
    pub settled_sample: rss::RssSample,
    /// Every settle sample, in order.
    pub settle_samples_kib: Vec<u64>,
}

/// Constructs the composition and leaves it alone: the process's idle
/// resident set. This is the number "Idle RSS < 50 MiB" is *about* — this
/// probe reports it, and asserts nothing about it (Phase 1 measures).
///
/// # Errors
///
/// As [`run_overload`].
pub fn run_idle() -> Result<IdleReport, ProbeError> {
    let store_config = MemoryConfig::default();
    let composition =
        Composition::new(store_config).map_err(|error| ProbeError(error.to_string()))?;
    let mut samples = Vec::new();
    // Settle: give the allocator a few beats to return the construction
    // scratch, sampling as it settles. A probe measures the process as it
    // will idle — after the settling, not during the first instruction.
    for _ in 0..8 {
        std::thread::sleep(Duration::from_millis(250));
        samples.push(rss_sample()?.vm_rss_kib);
    }
    let settled_sample = rss_sample()?;
    drop(composition);
    Ok(IdleReport {
        store_config,
        queue_ceiling_bytes: QUEUE_CEILING_BYTES,
        settled_sample,
        settle_samples_kib: samples,
    })
}

/// The overload record: the JSON the harness files. Field order is
/// stable, so two runs diff line by line.
#[must_use]
pub fn overload_record(report: &OverloadReport) -> JsonRecord {
    let mut record = JsonRecord::new();
    overload_load_fields(&mut record, report);
    overload_residency_fields(&mut record, report);
    overload_process_fields(&mut record, report);
    record
}

/// The overload record's identity and workload fields: what ran, under
/// which ceilings, and what the admission gates did with it.
fn overload_load_fields(record: &mut JsonRecord, report: &OverloadReport) {
    record
        .field("probe", JsonValue::Text("probe-ingest-overload".to_owned()))
        .field(
            "composition",
            JsonValue::Text(
                "queue+pipeline+store mirrored from crate APIs (server wiring: wave 3)".to_owned(),
            ),
        )
        .field(
            "signal",
            JsonValue::Text(
                "OTLP/gauge export, at-cap points, attribute-heavy resource".to_owned(),
            ),
        )
        .field(
            "queue_ceiling_bytes",
            JsonValue::U64(u64_of(report.queue_ceiling_bytes)),
        )
        .field(
            "store_max_accounted_bytes",
            JsonValue::U64(report.store_config.max_accounted_bytes),
        )
        .field(
            "store_max_records",
            JsonValue::U64(report.store_config.max_records),
        )
        .field(
            "store_series_cap",
            JsonValue::U64(report.store_config.series_cap),
        )
        .field(
            "payload_bytes",
            JsonValue::U64(u64_of(report.payload_bytes)),
        )
        .field("exports_ingested", JsonValue::U64(report.exports_ingested))
        .field(
            "queue_saturated_exports",
            JsonValue::U64(report.queue_saturated_exports),
        )
        .field(
            "saturation_persisted",
            JsonValue::Flag(report.saturation_persisted),
        )
        .field("admitted_records", JsonValue::U64(report.admitted_records))
        .field(
            "collapsed_records",
            JsonValue::U64(report.collapsed_records),
        )
        .field("conflict_records", JsonValue::U64(report.conflict_records))
        .field("rejected_records", JsonValue::U64(report.rejected_records))
        .field("records_kept", JsonValue::U64(report.pump.kept))
        .field("keeps_evicted", JsonValue::U64(report.pump.evicted));
}

/// The overload record's residency fields: the store and queue
/// accounting, both sides of the real-to-accounted margin.
fn overload_residency_fields(record: &mut JsonRecord, report: &OverloadReport) {
    let stats = &report.store_stats;
    record
        .field(
            "oversized_refusals",
            JsonValue::U64(stats.oversized_refusals),
        )
        .field("duplicate_keeps", JsonValue::U64(stats.duplicate_keeps))
        .field(
            "series_cap_refusals",
            JsonValue::U64(stats.kept_out_series_cap),
        )
        .field("resident_records", JsonValue::U64(stats.resident_records))
        .field(
            "resident_metric_points",
            JsonValue::U64(stats.resident_metric_points),
        )
        .field("resident_streams", JsonValue::U64(stats.resident_streams))
        .field("accounted_bytes", JsonValue::U64(stats.accounted_bytes))
        .field(
            "record_accounted_bytes",
            JsonValue::U64(stats.record_accounted_bytes),
        )
        .field(
            "identity_accounted_bytes",
            JsonValue::U64(stats.identity_accounted_bytes),
        )
        .field("evicted_total", JsonValue::U64(stats.total_evictions()))
        .field(
            "evicted_record_ceiling",
            JsonValue::U64(stats.evicted_for_record_ceiling),
        )
        .field(
            "evicted_accounted_bytes_ceiling",
            JsonValue::U64(stats.evicted_for_accounted_bytes_ceiling),
        )
        .field(
            "queue_accounted_bytes",
            JsonValue::U64(report.queue_accounted_bytes),
        )
        .field("queue_records", JsonValue::U64(report.queue_records))
        .field(
            "resident_accounted_bytes",
            JsonValue::U64(stats.accounted_bytes + report.queue_accounted_bytes),
        )
        .field("anomaly_conflicts", JsonValue::U64(report.anomalies));
}

/// The overload record's process fields: the kernel's readings beside
/// the run's own accounting of itself.
fn overload_process_fields(record: &mut JsonRecord, report: &OverloadReport) {
    record
        .field("vm_rss_kib", JsonValue::U64(report.final_sample.vm_rss_kib))
        .field("vm_hwm_kib", JsonValue::U64(report.final_sample.vm_hwm_kib))
        .field(
            "rss_samples_kib",
            JsonValue::U64List(report.rss_samples_kib.clone()),
        )
        .field("iterations", JsonValue::U64(report.iterations))
        .field(
            "completed_within_budget",
            JsonValue::Flag(report.completed_within_budget),
        )
        .field("wall_seconds", JsonValue::F64(report.wall_seconds));
}

/// The retention record.
#[must_use]
pub fn retention_record(report: &RetentionReport) -> JsonRecord {
    let stats = &report.store_stats;
    let mut record = JsonRecord::new();
    record
        .field("probe", JsonValue::Text("probe-retention-bound".to_owned()))
        .field(
            "signal",
            JsonValue::Text("OTLP/log export; log records leave no ledger entry".to_owned()),
        )
        .field(
            "store_max_accounted_bytes",
            JsonValue::U64(report.store_config.max_accounted_bytes),
        )
        .field(
            "store_max_records",
            JsonValue::U64(report.store_config.max_records),
        )
        .field(
            "store_window_seconds",
            JsonValue::U64(report.store_config.admission_window.as_secs()),
        )
        .field(
            "store_series_cap",
            JsonValue::U64(report.store_config.series_cap),
        )
        .field(
            "queue_ceiling_bytes",
            JsonValue::U64(u64_of(report.queue_ceiling_bytes)),
        )
        .field("exports_ingested", JsonValue::U64(report.exports_ingested))
        .field("admitted_records", JsonValue::U64(report.admitted_records))
        .field("records_kept", JsonValue::U64(report.pump.kept))
        .field("keeps_evicted", JsonValue::U64(report.pump.evicted))
        .field("evicted_total", JsonValue::U64(stats.total_evictions()))
        .field(
            "evicted_record_ceiling",
            JsonValue::U64(stats.evicted_for_record_ceiling),
        )
        .field(
            "evicted_accounted_bytes_ceiling",
            JsonValue::U64(stats.evicted_for_accounted_bytes_ceiling),
        )
        .field(
            "evicted_admission_window",
            JsonValue::U64(stats.evicted_for_admission_window),
        )
        .field("resident_records", JsonValue::U64(stats.resident_records))
        .field(
            "resident_log_records",
            JsonValue::U64(stats.resident_log_records),
        )
        .field("resident_streams", JsonValue::U64(stats.resident_streams))
        .field("accounted_bytes", JsonValue::U64(stats.accounted_bytes))
        .field(
            "record_accounted_bytes",
            JsonValue::U64(stats.record_accounted_bytes),
        )
        .field(
            "plateau_vm_rss_kib",
            JsonValue::U64List(report.plateau_vm_rss_kib.clone()),
        )
        .field(
            "plateau_spread_kib",
            JsonValue::U64(report.plateau_spread_kib),
        )
        .field("vm_rss_kib", JsonValue::U64(report.final_sample.vm_rss_kib))
        .field("vm_hwm_kib", JsonValue::U64(report.final_sample.vm_hwm_kib))
        .field("iterations", JsonValue::U64(report.iterations))
        .field(
            "completed_within_budget",
            JsonValue::Flag(report.completed_within_budget),
        )
        .field("wall_seconds", JsonValue::F64(report.wall_seconds));
    record
}

/// The idle record.
#[must_use]
pub fn idle_record(report: &IdleReport) -> JsonRecord {
    let mut record = JsonRecord::new();
    record
        .field("probe", JsonValue::Text("probe-idle".to_owned()))
        .field(
            "composition",
            JsonValue::Text(
                "queue+pipeline+store mirrored from crate APIs (server wiring: wave 3)".to_owned(),
            ),
        )
        .field(
            "queue_ceiling_bytes",
            JsonValue::U64(u64_of(report.queue_ceiling_bytes)),
        )
        .field(
            "store_max_accounted_bytes",
            JsonValue::U64(report.store_config.max_accounted_bytes),
        )
        .field(
            "store_max_records",
            JsonValue::U64(report.store_config.max_records),
        )
        .field(
            "settled_vm_rss_kib",
            JsonValue::U64(report.settled_sample.vm_rss_kib),
        )
        .field(
            "vm_hwm_kib",
            JsonValue::U64(report.settled_sample.vm_hwm_kib),
        )
        .field(
            "settle_samples_kib",
            JsonValue::U64List(report.settle_samples_kib.clone()),
        );
    record
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small overload run through the real pipeline and store, shrunk
    /// to seconds: contract-shaped knobs, tiny ceilings. This is the
    /// "run the smallest probe against a tiny config in-process" test —
    /// it asserts the run's *facts* (records moved, the pump kept them,
    /// the store counts residency), never a budget.
    #[test]
    fn the_overload_run_produces_its_numbers_on_a_small_configuration() {
        let config = OverloadConfig {
            store_config: MemoryConfig {
                max_records: 50_000,
                max_accounted_bytes: 48 * 1024 * 1024,
                admission_window: Duration::from_secs(3_600),
                series_cap: 10_000,
            },
            pumped_rounds: 6,
            saturation_streak: 2,
            saturation_round_cap: 8,
            sample_every: 2,
            wall_budget: Duration::from_secs(120),
        };
        let report = run_overload(&config).expect("the small overload run measures");
        assert_eq!(
            report.exports_ingested, report.iterations,
            "every iteration is one export offered"
        );
        assert!(
            report.admitted_records > 0,
            "legal at-cap payloads admit records"
        );
        assert_eq!(report.rejected_records, 0);
        assert!(report.payload_bytes > 0);
        assert!(
            report.payload_bytes <= 4 * 1024 * 1024,
            "the probe's own payload stays under the wire ceiling"
        );
        assert!(
            report.store_stats.resident_records > 0,
            "the pump kept records into the store"
        );
        assert!(
            report.store_stats.accounted_bytes > 0,
            "the store accounts residency in bytes"
        );
        assert_eq!(
            report.store_stats.resident_streams > 0,
            report.store_stats.resident_metric_points > 0,
            "a resident point implies a resident stream"
        );
        assert!(
            report.final_sample.vm_rss_kib > 0,
            "the process is measured"
        );
        assert!(report.final_sample.vm_hwm_kib >= report.final_sample.vm_rss_kib);
        assert!(!report.rss_samples_kib.is_empty(), "the run sampled RSS");
    }

    /// The retention run on its small, stated configuration.
    #[test]
    fn the_retention_run_reaches_its_ceilings_and_samples_the_plateau() {
        // A shorter run for test time: fewer exports, same law.
        let config = RetentionConfig {
            fill_exports: 6,
            plateau_exports: 9,
            sample_every: 3,
            ..Default::default()
        };
        let report = run_retention(&config).expect("the small retention run measures");
        assert!(
            report.store_stats.total_evictions() > 0,
            "a load past the ceilings must show evictions: {report:?}"
        );
        assert_eq!(
            report.store_stats.resident_records, report.store_stats.resident_log_records,
            "a log-record load residents only log records"
        );
        assert_eq!(
            report.store_stats.resident_streams, 0,
            "logs are not streams"
        );
        assert!(
            report.plateau_vm_rss_kib.len() >= 2,
            "the plateau is sampled more than once"
        );
        assert!(
            report.store_stats.accounted_bytes <= config.store_config.max_accounted_bytes,
            "the ceilings hold: accounted {} stays under {}",
            report.store_stats.accounted_bytes,
            config.store_config.max_accounted_bytes
        );
    }

    #[test]
    fn the_idle_composition_is_measured() {
        let report = run_idle().expect("the idle composition is measured");
        assert!(report.settled_sample.vm_rss_kib > 0);
        assert_eq!(report.settle_samples_kib.len(), 8);
        assert_eq!(report.queue_ceiling_bytes, 64 * 1024 * 1024);
    }
}
