//! The bounds a correlation run is subject to, and the truth it reports.
//!
//! A run never refuses the read: when a bound is hit it degrades truthfully
//! — it returns the relations found so far and names, in the truth, where
//! it stopped (the hop depth, the scan-work allowance, or the relation
//! ceiling). The flow turns that degradation into coverage entries, so the
//! envelope always states what the correlated part does and does not hold.

use runtime_trail_telemetry_model::{EntityId, TraceId};

use crate::relations::{Relation, RelationType, SignalRef, StrategyVersion, Window};

/// A correlation strategy the engine can run. The taxonomy's other relation
/// types are pinned contract — a strategy is the committed, versioned
/// procedure that produces relations of its type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// Attaches a log record to the exact span its trace context names.
    SpanIdentity,
    /// Attaches a log record to the resident spans of its trace, unless
    /// the exact span is resident (then those relations are suppressed in
    /// its favor).
    TraceIdentity,
    /// Relates the subject's resident spans to the data points co-active in
    /// a caller-supplied window.
    TemporalCoActivity,
}

impl Strategy {
    /// The strategy's name, as bound into relations and the limits
    /// statement.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::SpanIdentity => "span_identity",
            Self::TraceIdentity => "trace_identity",
            Self::TemporalCoActivity => "temporal_co_activity",
        }
    }

    /// The strategy's version.
    #[must_use]
    pub const fn version(self) -> &'static str {
        match self {
            Self::SpanIdentity | Self::TraceIdentity | Self::TemporalCoActivity => "1.0.0",
        }
    }

    /// The taxonomy type the strategy's relations carry.
    #[must_use]
    pub const fn relation_type(self) -> RelationType {
        match self {
            Self::SpanIdentity => RelationType::SpanIdentity,
            Self::TraceIdentity => RelationType::TraceIdentity,
            Self::TemporalCoActivity => RelationType::TemporalCoActivity,
        }
    }
}

/// The bounds a correlation run is produced under.
pub struct CorrelationBounds {
    /// The strategies to run, in run order. Versions in effect are the
    /// versions of exactly these strategies, stated in the same order.
    pub strategies: Vec<Strategy>,
    /// The interval the temporal strategy grounds its pairs on. The
    /// identity strategies need none; `None` disables temporal relations.
    pub window: Option<Window>,
    /// The maximum relations the run returns before degrading.
    pub max_relations: u64,
    /// The maximum hop depth the run expands to. The committed strategies
    /// are single-hop; `0` names the depth and yields no relations.
    pub max_hops: u8,
    /// The maximum resident positions the run may examine across its scans.
    /// Same unit as the query engine's per-page examinations: one resident
    /// record examined by a scan.
    pub max_scan: u64,
    /// The trace the investigated subject belongs to. The identity
    /// strategies relate the subject's logs to its resident spans, so they
    /// run only when a trace is named; `None` leaves them silent.
    pub subject_trace: Option<TraceId>,
}

/// One suppression the run accounted: a log record attached to its exact
/// span, whose trace-identity relations were suppressed in that relation's
/// favor. Accounting is scoped to the log record and is complete regardless
/// of any bound the run degraded under — it never waits on the budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Suppression {
    /// The log record whose trace-identity relations were suppressed.
    pub log: EntityId,
    /// The span the log attached to, exactly.
    pub span: EntityId,
    /// The number of trace-identity relations suppressed for the log.
    pub suppressed: u64,
}

/// One completeness absence: a log record whose trace has no resident
/// span, so no identity relation can name one of its members. Completeness
/// coverage, reported against the resident set as scanned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbsentTrace {
    /// The log record whose trace has no resident span.
    pub log: EntityId,
}

/// Where a run stopped, when it did not finish the whole resident read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoppedAt {
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

/// The truth of one correlation run: what ran, what it spent, and where it
/// degraded or found the store holding less than the subject implies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Truth {
    /// The strategies the answer was produced under, in run order.
    pub strategy_versions: Vec<StrategyVersion>,
    /// Every suppression the run accounted, complete regardless of bounds.
    pub suppressions: Vec<Suppression>,
    /// Every log record whose trace has no resident span.
    pub absent_traces: Vec<AbsentTrace>,
    /// Relations dropped because an endpoint left residency mid-run: the
    /// resident-endpoint re-check happened after the relation was formed.
    pub shrunken: u64,
    /// Where the run stopped, when it did not finish.
    pub stopped_at: Option<StoppedAt>,
    /// The resident positions the run examined.
    pub scan_spent: u64,
}

/// The outcome of one correlation run: its relations and its truth.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrelationOutcome {
    /// The relations the run produced, deterministically ordered.
    pub relations: Vec<Relation<SignalRef>>,
    /// The run's truth.
    pub truth: Truth,
}
