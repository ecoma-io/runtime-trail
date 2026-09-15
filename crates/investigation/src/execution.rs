//! The execution part of the [`Investigation`](crate::Investigation) envelope.
//!
//! The execution part reports, per evidence part, the run facts of every
//! budgeted engine walk the flow performed: each walk's outcome, its
//! coverage entries, and the opaque cursor left behind for continuation.
//! The flow NEVER re-computes or swallows these facts — it mirrors what the
//! engine reported, 1:1, in order (one outcome and one coverage list per
//! page). A fabricated `Complete` is a contract violation; every
//! truncation, refusal and stall the engine reports must surface here.
//!
//! The vocabulary is the envelope's own transport-independent mirror of the
//! engine's [`result`](runtime_trail_query::result) vocabulary: the request
//! and response shapes never leak engine types.

use std::time::Duration;

use runtime_trail_correlation::bounds::StoppedAt;
use runtime_trail_telemetry_model::{AdmissionTime, EntityId};

/// Which budget dimension a run fact refers to. The five dimensions mirror
/// the query engine's per-page budget contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Dimension {
    /// The deadline dimension: how long a page may take.
    Deadline,
    /// The results dimension: how many records a page may return.
    Results,
    /// The bytes dimension: how many accounted record bytes a page may hold.
    Bytes,
    /// The scan dimension: how many residency positions a page may examine.
    Scan,
    /// The aggregation-memory dimension: how much bookkeeping memory a page
    /// may use.
    AggregationMemory,
}

/// A measured quantity on a budget dimension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Magnitude {
    /// A duration, on the deadline dimension.
    Duration(Duration),
    /// A count of units, on the results, scan or aggregation-memory
    /// dimensions.
    Units(u64),
    /// A byte count, on the bytes or aggregation-memory dimensions.
    Bytes(u64),
}

/// Where a truncation happened: the point the walk reached when it stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TruncationPoint {
    /// The opaque continuation cursor of the truncated page: a caller with
    /// that cursor can continue the walk from exactly this point.
    Cursor(Vec<u8>),
    /// The last record examined by the truncated page, by entity id. Chosen
    /// when the walk has no continuation cursor (the engine stopped before
    /// minting one).
    LastExamined(EntityId),
}

/// A named truncation: which dimension expired, where the walk stopped,
/// and how many records were omitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Truncation {
    /// The dimension that expired.
    pub dimension: Dimension,
    /// Where the walk stopped.
    pub position: TruncationPoint,
    /// How many records the expired dimension omitted.
    pub omitted: u64,
}

/// A budget refusal: the engine refused the requested page before
/// examining anything, naming the dimension, the limit it enforced and
/// what the page would have needed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// The dimension that refused the page.
    pub dimension: Dimension,
    /// The limit the engine enforced.
    pub limit: Magnitude,
    /// What the page would have needed, as the engine measured it.
    pub observed: Magnitude,
}

/// The outcome of one budgeted engine walk, mirrored verbatim from the
/// engine's `PartOutcome`. Never fabricated: every walk the flow performs
/// reports exactly the outcome the engine gave it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The engine walked the whole resident surface of the kind within the
    /// page budget.
    Complete,
    /// The engine stopped early on a budget dimension, naming the
    /// truncation.
    Degraded {
        /// The named truncation.
        truncation: Truncation,
    },
    /// The engine refused the page outright, naming the refusal.
    Refused(Refusal),
    /// The engine stalled: consecutive empty pages from the store. The
    /// stall's coverage entry (`DriverStall`) rides with the run.
    Stalled,
}

/// A residency-coverage fact about one engine walk, mirrored from the
/// engine's coverage entries. Each entry names a gap or boundary the walk
/// had to account for — nothing is smoothed over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoverageEntry {
    /// The store reported an eviction gap between two residency positions:
    /// records that left residency between `after` and `before` (exclusive
    /// of both) were not examined.
    EvictionGap {
        /// The last position the walk saw before the gap.
        after: EntityId,
        /// The first position the walk saw after the gap.
        before: EntityId,
    },
    /// A snapshot boundary: the walk's residency snapshot starts at this
    /// admission; records admitted earlier were not examined.
    SnapshotBoundary {
        /// When the boundary record was admitted.
        admission_time: AdmissionTime,
        /// The boundary record.
        entity: EntityId,
    },
    /// An uncounted tail: records past this position on the dimension were
    /// not counted by the walk (only their existence is known, or their
    /// dimension could not be tallied).
    UncountedTail {
        /// The last counted position.
        after: EntityId,
        /// The dimension the tail was uncounted on.
        dimension: Dimension,
    },
    /// The store returned consecutive empty pages with continuation
    /// cursors; the engine stalled rather than looping forever.
    DriverStall {
        /// The position where the stall began.
        after: EntityId,
    },
}

/// Which evidence part a run belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartName {
    /// The trace waterfall (spans).
    Spans,
    /// The trace's related logs.
    RelatedLogs,
    /// The metrics surrounding the waterfall (surrounding metrics).
    SurroundingMetrics,
}

/// An opaque engine continuation cursor, base64-blob-neutral. The flow
/// hands it to the engine between pages and reports it verbatim; it is
/// never interpreted by the envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpaqueCursor(pub Vec<u8>);

/// The run facts of one budgeted engine walk: its outcome, its coverage,
/// and the continuation cursor it left behind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunFacts {
    /// The walk's outcome, exactly as the engine reported it.
    pub outcome: Outcome,
    /// The walk's coverage entries, exactly as the engine reported them,
    /// in order.
    pub coverage: Vec<CoverageEntry>,
    /// The continuation cursor the walk left, when it left one.
    pub next_cursor: Option<OpaqueCursor>,
}

/// The run facts of one evidence part: every budgeted walk the flow
/// performed for that part, in execution order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunGroup {
    /// Which part these runs fed.
    pub part: PartName,
    /// The runs, in execution order (one per engine page).
    pub runs: Vec<RunFacts>,
}

/// A half-open time window `[from, to)` over `time_unix_nano`, the metric
/// surface's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeWindow {
    /// Window start, inclusive.
    pub from: u64,
    /// Window end, exclusive.
    pub to: u64,
}

/// Where the correlation engine's walk stopped, when it degraded rather
/// than finishing the resident read. The envelope's own mirror of the
/// engine's stop verdict — the execution part never leaks engine types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorrelationStop {
    /// The run was bounded to zero hop depth.
    MaxDepth {
        /// The depth the run was bounded to.
        depth: u8,
    },
    /// The scan-work allowance ran out.
    ScanExhausted {
        /// The resident positions examined before the allowance ran out.
        examined: u64,
    },
    /// The relation ceiling cut the answer.
    MaxRelations {
        /// The ceiling the relations were cut at.
        count: u64,
    },
}

impl From<StoppedAt> for CorrelationStop {
    /// The engine's stop verdict, mirrored verbatim into the envelope's
    /// vocabulary.
    fn from(stopped: StoppedAt) -> Self {
        match stopped {
            StoppedAt::MaxDepth { depth } => Self::MaxDepth { depth },
            StoppedAt::ScanExhausted { examined } => Self::ScanExhausted { examined },
            StoppedAt::MaxRelations { count } => Self::MaxRelations { count },
        }
    }
}

/// A flow-level coverage fact: something the FLOW had to account for that
/// is not an engine walk fact. Kept distinct from [`CoverageEntry`] so the
/// engine's voice and the flow's voice never blur.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlowCoverageEntry {
    /// The metric part's window statement: the window the flow asked for
    /// against the window the resident points actually span. The gap
    /// between the two is the flow's honest bound on the part's coverage.
    MetricWindow {
        /// The window asked for (the waterfall's time extent).
        asked: TimeWindow,
        /// The window the resident points span.
        resident: TimeWindow,
    },
    /// Admission anomalies (identity conflicts) the store reported at flow
    /// admission.
    AdmissionAnomalies {
        /// The store's anomaly counter.
        total: u64,
    },
    /// Selected records that left residency between selection and identity
    /// naming: they were seen, are not part of the evidence, and the hole
    /// is named rather than hidden.
    ResidencyHole {
        /// The part the records belonged to.
        part: PartName,
        /// How many records were lost.
        count: u64,
    },
    /// A correlated relation the engine accounted and the flow held: a log
    /// record attached to its exact span, whose trace-identity relations
    /// were suppressed in that relation's favor. Accounting is scoped to
    /// the log record and complete regardless of any run bound.
    SuppressedEvidence {
        /// The log record whose trace-identity relations were suppressed.
        log: EntityId,
        /// The exact span the log attached to.
        span: EntityId,
        /// The suppressed relations, counted.
        suppressed: u64,
    },
    /// Completeness coverage: a log record whose trace has no resident
    /// span, so no identity relation can name one of its members. Named,
    /// never invented into a relation.
    AbsentTraceSpans {
        /// The log record whose trace has no resident span.
        log: EntityId,
    },
    /// Correlated relations the flow dropped because an endpoint left the
    /// evidence — the store reported it resident at scan time, but the
    /// evidence part does not carry it. Named rather than hidden; the
    /// envelope never emits a dangling signal ref (invariant 3).
    RelationShrinkage {
        /// How many relations were dropped.
        count: u64,
    },
    /// Where the correlated part degraded, when the engine stopped before
    /// finishing the resident read: the answer holds what it holds, and
    /// the stop is stated.
    CorrelationDegradation {
        /// The stop the engine reported, mirrored.
        at: CorrelationStop,
    },
    /// A committed strategy that did not run, named with the reason: the
    /// temporal co-activity strategy requires the caller's explicit
    /// window, and the runtime picks no window by default. The skip is a
    /// coverage fact, never a silent absence.
    TemporalStrategySkipped {
        /// Why the strategy did not run.
        reason: &'static str,
    },
}

/// The envelope's execution part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Execution {
    /// The run facts of every evidence part, in part order.
    pub run_groups: Vec<RunGroup>,
    /// The flow-level coverage facts, in the order the flow recorded them.
    pub flow_coverage: Vec<FlowCoverageEntry>,
}

impl Execution {
    /// Builds the execution part from run groups and flow coverage.
    #[must_use]
    pub const fn new(run_groups: Vec<RunGroup>, flow_coverage: Vec<FlowCoverageEntry>) -> Self {
        Self {
            run_groups,
            flow_coverage,
        }
    }
}

impl RunFacts {
    /// The run's facts.
    #[must_use]
    pub fn new(
        outcome: Outcome,
        coverage: Vec<CoverageEntry>,
        next_cursor: Option<OpaqueCursor>,
    ) -> Self {
        Self {
            outcome,
            coverage,
            next_cursor,
        }
    }
}

impl TimeWindow {
    /// A window over `time_unix_nano`, half-open.
    #[must_use]
    pub const fn new(from: u64, to: u64) -> Self {
        Self { from, to }
    }
}

impl OpaqueCursor {
    /// An opaque cursor, verbatim.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The cursor's bytes, uninterpreted.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}
