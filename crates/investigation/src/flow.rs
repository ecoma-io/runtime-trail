//! The committed flows: the trace investigation composing the query engine.
//!
//! The one flow this crate ships (M3, issue #12) is the **trace
//! investigation**: given a root span and a caller budget, it composes the
//! query engine into ONE [`Investigation`](crate::Investigation) envelope —
//! the trace waterfall, the trace's related logs and the metrics
//! surrounding the waterfall — as evidence, with every engine run mirrored
//! into the execution part and every limit stated in the limits part.
//!
//! # Budget decomposition
//!
//! The caller admits one budget ([`InvestigationBudget`]) for the whole
//! investigation. The flow slices it into **fresh per-page engine budgets**:
//! every engine call re-admits the caller's five ceilings, because a
//! continuation page is a NEW execution and the engine binds no chain (it
//! enforces only the budget of the page it is given). The engine stays the
//! sole enforcer of each per-page budget; the chain-level ceilings — total
//! pages, total evidence entities, identity-recovery examinations — are the
//! flow's own ([`ChainBudget`]) and are reported in the envelope's limits
//! part when the flow stops on them, never silently.
//!
//! Every engine outcome surfaces in the execution part, mirrored 1:1:
//! `Complete` stays complete, `Degraded` carries its truncation, `Refused`
//! its refusal, `Stalled` its driver-stall coverage entry; coverage entries
//! are mirrored verbatim and cursors verbatim. The flow never fabricates a
//! completion.
//!
//! # Identity recovery
//!
//! The engine's [`RecordView`]s carry no entity id by design. Spans have
//! their natural identity; log records and metric points are
//! admission-assigned. The flow re-names selected records by scanning the
//! store's scan surface and matching by payload pointer ([`Arc::ptr_eq`]:
//! ADR 0008 — the driver shares the payload Arcs). A selected record whose
//! identity the recovery walk could not name — evicted or re-admitted
//! between selection and naming — is excluded from evidence and reported as
//! a `ResidencyHole` in flow coverage, never emitted unkeyed.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use runtime_trail_correlation::bounds::{CorrelationBounds, Strategy};
use runtime_trail_query::budget::QueryBudget;
use runtime_trail_query::engine::{
    RecordView, RecordsQuery, SignalKind as EngineSignalKind, records,
};
use runtime_trail_query::result::{
    BudgetRefusal as EngineRefusal, CoverageEntry as EngineCoverageEntry,
    Dimension as EngineDimension, Magnitude as EngineMagnitude, Page, PartOutcome,
    Truncation as EngineTruncation, TruncationPoint as EngineTruncationPoint,
};
use runtime_trail_query::{AdmissionKey, TelemetryStore};
use runtime_trail_telemetry_model::{
    EntityId, LogRecord, MetricPoint, Span, SpanId, StreamIdentity, TraceId,
};

use crate::correlated::{Correlated, Relation};
use crate::envelope::Investigation;
use crate::evidence::{Evidence, LogEvidence, PointEvidence, SpanEvidence};
use crate::execution::{
    CorrelationStop, CoverageEntry, Dimension, Execution, FlowCoverageEntry, Magnitude,
    OpaqueCursor, Outcome, PartName, Refusal, RunFacts, RunGroup, TimeWindow, Truncation,
    TruncationPoint,
};
use crate::limits::{BudgetLimits, ChainBasis, ChainLimits, EvictionState, Limits};
use crate::subject::{EffectiveRoot, EffectiveSubject, RequestedSubject, ResolutionNote, Subject};

/// The slice size of one identity-recovery scan page. The recovery walk is
/// bounded by the chain's examination ceiling, not by page size; this only
/// sets the chunking of the store's scan surface.
const RECOVERY_SCAN_SLICE: usize = 256;

/// The caller's budget for ONE investigation: one set of five ceilings
/// ([`QueryBudget`] dimensions) admitted for the whole flow. The flow
/// re-admits these ceilings fresh on every engine page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvestigationBudget {
    /// The wall-clock deadline of every engine page of this investigation.
    pub deadline: Duration,
    /// The maximum items one engine page may return.
    pub max_results: u64,
    /// The maximum accounted bytes one engine page may examine.
    pub max_bytes: u64,
    /// The maximum resident records one engine page may examine (the
    /// engine's scan-work ceiling).
    pub max_scan: u64,
    /// The maximum aggregation memory one engine page may spend.
    pub max_aggregation_memory: u64,
}

impl InvestigationBudget {
    /// Builds an investigation budget from the five engine dimensions.
    #[must_use]
    pub const fn new(
        deadline: Duration,
        max_results: u64,
        max_bytes: u64,
        max_scan: u64,
        max_aggregation_memory: u64,
    ) -> Self {
        Self {
            deadline,
            max_results,
            max_bytes,
            max_scan,
            max_aggregation_memory,
        }
    }

    /// The per-page engine budget: the caller's five ceilings, re-admitted
    /// fresh for every page (a continuation page is a new execution; the
    /// engine binds no chain).
    fn per_page(&self) -> QueryBudget {
        QueryBudget::new(
            self.deadline,
            self.max_results,
            self.max_bytes,
            self.max_scan,
            self.max_aggregation_memory,
        )
    }
}

/// A trace investigation request: the root span to investigate and the
/// budget the caller admits for the whole flow. An optional correlation
/// window enables the temporal co-activity strategy; without one the
/// runtime picks no window and the identity strategies run alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceInvestigationRequest {
    /// The subject the caller asks about: the root span's entity id. The
    /// span must be resident; a span never admitted (or evicted) makes the
    /// investigation fail with [`FlowError::SubjectUnresolved`].
    pub root_span: EntityId,
    /// The caller's budget for the whole investigation.
    pub budget: InvestigationBudget,
    /// An optional correlation window: the half-open time range the
    /// temporal co-activity strategy scans inside. `Some(window)` enables
    /// temporal co-activity within `window`; `None` skips the temporal
    /// strategy — the runtime picks no window by default — and the
    /// identity strategies run alone.
    pub correlation_window: Option<TimeWindow>,
}

impl TraceInvestigationRequest {
    /// Builds a trace investigation request.
    #[must_use]
    pub const fn new(
        root_span: EntityId,
        budget: InvestigationBudget,
        correlation_window: Option<TimeWindow>,
    ) -> Self {
        Self {
            root_span,
            budget,
            correlation_window,
        }
    }
}

/// Why a trace investigation failed outright. The flow fails only when it
/// cannot even start — the subject is not resident, a continuation cursor
/// was rejected, or the engine answered a page without any part outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlowError {
    /// The requested root span is not resident: never admitted, or evicted
    /// before the investigation reached it.
    SubjectUnresolved {
        /// The requested root span, unfulfilled.
        requested: EntityId,
    },
    /// The engine rejected a continuation cursor (a malformed or foreign
    /// cursor; the flow only presents cursors it was handed).
    CursorRejected,
    /// The engine returned a page whose execution names no part outcome — a
    /// contract violation the flow refuses to guess at (it never fabricates
    /// a completion, not even a degraded one).
    EngineContract,
}

impl fmt::Display for FlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubjectUnresolved { requested } => {
                write!(f, "the requested root span {requested:?} is not resident")
            }
            Self::CursorRejected => write!(f, "an engine continuation cursor was rejected"),
            Self::EngineContract => {
                write!(f, "the engine returned a page without a part outcome")
            }
        }
    }
}

impl std::error::Error for FlowError {}

/// The flow's chain-level ceilings: the limits the FLOW owns across all
/// engine calls, beyond the engine's per-page budgets. The engine never
/// sees these; the flow enforces them between pages and reports a stop in
/// the envelope's `limits.chain`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainBudget {
    /// The maximum total engine pages across all parts. Pages are atomic to
    /// the chain: a stop on this ceiling is reported as
    /// [`ChainBasis::TotalPages`].
    pub max_total_pages: u64,
    /// The maximum total evidence entities across all parts (selected
    /// waterfall spans, related logs, surrounding points — and any
    /// residency holes). Selection is post-hoc per part, so the ceiling
    /// gates between walks; one walk's selection may overshoot it by a
    /// page, reported via [`ChainBasis::TotalEntities`].
    pub max_total_entities: u64,
    /// The maximum scan examinations the identity-recovery walk may spend
    /// naming admission-assigned records. Selected records left unnamed are
    /// residency holes, counted as entities.
    pub max_identity_examinations: u64,
}

impl ChainBudget {
    /// Builds a chain budget from the three flow-owned ceilings.
    #[must_use]
    pub const fn new(
        max_total_pages: u64,
        max_total_entities: u64,
        max_identity_examinations: u64,
    ) -> Self {
        Self {
            max_total_pages,
            max_total_entities,
            max_identity_examinations,
        }
    }

    /// The flow's default chain ceilings. Every ceiling is at least one:
    /// a flow that has not run a single page claims nothing (the
    /// `no_partial_content_as_complete` invariant forbids a stopped
    /// envelope with zero recorded pages).
    #[must_use]
    pub const fn defaults() -> Self {
        Self::new(16, 10_000, 100_000)
    }
}

/// The flow's walking state, shared across the three parts.
struct WalkState {
    /// The caller's budget, re-admitted per page.
    budget: InvestigationBudget,
    /// The chain ceilings that bound the whole walk.
    chain: ChainBudget,
    /// Engine pages run so far, all parts.
    pages: u64,
    /// Evidence entities selected so far (views plus holes).
    entities: u64,
    /// Scan examinations spent on identity recovery.
    examinations: u64,
    /// The chain ceiling the walk stopped on, if any.
    stopped: Option<ChainBasis>,
}

impl WalkState {
    /// The fresh per-page engine budget.
    fn per_page(&self) -> QueryBudget {
        self.budget.per_page()
    }
}

/// Investigates the trace of a root span under the caller's budget and the
/// flow's default chain limits.
///
/// The one flow composing the query engine (ADR 0011): it walks the
/// resident spans into the trace waterfall, the trace's related logs, and
/// the metrics surrounding the waterfall, and returns ONE envelope — the
/// subject resolved, the execution mirroring every engine run, the evidence
/// named, the limits stated.
///
/// # Errors
///
/// [`FlowError::SubjectUnresolved`] when the root span is not resident;
/// [`FlowError::CursorRejected`] when the engine rejects a continuation
/// cursor; [`FlowError::EngineContract`] when the engine answers a page
/// with no part outcome.
pub fn investigate_trace(
    store: &dyn TelemetryStore,
    request: TraceInvestigationRequest,
) -> Result<Investigation, FlowError> {
    investigate_trace_bounded(store, &request, ChainBudget::defaults())
}

/// Investigates the trace of a root span under the caller's budget and the
/// given chain ceilings.
///
/// The chain ceilings bound the flow's OWN work across all parts: total
/// engine pages, total evidence entities, and identity-recovery
/// examinations. A stop at a chain ceiling is reported in `limits.chain`;
/// everything collected before the stop is honest.
///
/// # Errors
///
/// Same as [`investigate_trace`].
#[allow(clippy::too_many_lines)] // one phase-composing flow function
pub fn investigate_trace_bounded(
    store: &dyn TelemetryStore,
    request: &TraceInvestigationRequest,
    chain: ChainBudget,
) -> Result<Investigation, FlowError> {
    let root_span = request.root_span;
    let budget = request.budget;

    // The eviction state and admission anomalies are read once, at flow
    // admission, and stated in the limits part. The flow holds the store
    // read-only (`&dyn TelemetryStore`): nothing here mutates residency.
    let stats = store.stats();

    // The requested subject is the caller's own words (invariant 1); it is
    // never rewritten.
    let requested = RequestedSubject::new(root_span);

    // The root span must be resident: it is the subject of the whole
    // investigation, never synthesised from a page.
    let root = store.span(root_span).ok_or(FlowError::SubjectUnresolved {
        requested: root_span,
    })?;

    let mut walk = WalkState {
        budget,
        chain,
        pages: 0,
        entities: 0,
        examinations: 0,
        stopped: None,
    };

    // Part 1: every resident span, walked page by page. Selection (the
    // trace) is a separate step after the walk.
    let spans_query = RecordsQuery::new(EngineSignalKind::Spans);
    let (span_views, spans_group) = walk_pages(store, &spans_query, PartName::Spans, &mut walk)?;
    let spans: Vec<Arc<Span>> = span_views_of(span_views);

    // The effective subject: the trace's parentless root and the note that
    // says how the runtime got there.
    let effective = resolve_subject(&root, root_span, &spans);
    let relates_to = root.natural_identity().map(|(trace_id, _)| trace_id);

    // The waterfall: breadth-first levels from the effective root, then the
    // trace members the walk could not reach through a parent chain, in
    // engine order.
    let waterfall = waterfall(&spans, &effective, &root);
    walk.entities = walk.entities.saturating_add(waterfall.len() as u64);

    // Part 2: every resident log record; the trace's are selected and
    // named.
    let logs_query = RecordsQuery::new(EngineSignalKind::LogRecords);
    let (log_views, logs_group) = walk_pages(store, &logs_query, PartName::RelatedLogs, &mut walk)?;
    let (log_evidence, log_holes) = related_logs(store, log_views, relates_to, &mut walk);

    // Part 3: every resident metric point; those inside the waterfall's
    // window are selected and named.
    let points_query = RecordsQuery::new(EngineSignalKind::MetricPoints);
    let (point_views, points_group) = walk_pages(
        store,
        &points_query,
        PartName::SurroundingMetrics,
        &mut walk,
    )?;
    let (min_start, max_end) = extent_of(&waterfall);
    let asked_window = TimeWindow::new(min_start, max_end);
    let (point_evidence, point_holes, resident_window) =
        surrounding_metrics(store, point_views, asked_window, relates_to, &mut walk);

    let evidence = Evidence::new(
        span_evidence(&waterfall, &effective),
        log_evidence,
        point_evidence,
    );

    // Execution: the three run groups in part order, then the flow's own
    // coverage — the metric window statement, the admission anomalies read
    // at flow admission, and any residency holes.
    let mut flow_coverage = Vec::new();
    if !points_group.runs.is_empty() {
        flow_coverage.push(FlowCoverageEntry::MetricWindow {
            asked: asked_window,
            resident: resident_window,
        });
    }
    flow_coverage.push(FlowCoverageEntry::AdmissionAnomalies {
        total: stats.admission_anomalies,
    });
    if log_holes > 0 {
        flow_coverage.push(FlowCoverageEntry::ResidencyHole {
            part: PartName::RelatedLogs,
            count: log_holes,
        });
    }
    if point_holes > 0 {
        flow_coverage.push(FlowCoverageEntry::ResidencyHole {
            part: PartName::SurroundingMetrics,
            count: point_holes,
        });
    }
    // Correlated: run the committed strategies within the store, narrow
    // the relations to evidence-resident endpoints (invariant 3), and
    // account the engine's truth in the flow coverage and limits.
    //
    // The temporal strategy needs the caller's explicit window; without
    // one the runtime picks no window (correlation-model.md), runs the
    // identity strategies only, and names the skip in the coverage.
    let (strategies, window) = if let Some(asked) = request.correlation_window {
        (
            vec![
                Strategy::SpanIdentity,
                Strategy::TraceIdentity,
                Strategy::TemporalCoActivity,
            ],
            Some(Into::into(asked)),
        )
    } else {
        flow_coverage.push(FlowCoverageEntry::TemporalStrategySkipped {
            reason: "no_correlation_window",
        });
        (vec![Strategy::SpanIdentity, Strategy::TraceIdentity], None)
    };
    let bounds = CorrelationBounds {
        strategies,
        window,
        max_relations: 10_000,
        max_hops: 2,
        max_scan: budget.max_scan,
        subject_trace: relates_to,
    };
    let outcome = runtime_trail_correlation::correlate(store, &bounds);
    let mut relations: Vec<Relation> = outcome
        .relations
        .into_iter()
        .map(|relation| {
            Relation::new(
                relation.relation_type,
                relation.from.into(),
                relation.to.into(),
                relation.facts,
                relation.strategy,
                relation.window,
            )
        })
        .collect();
    let before_narrow = relations.len();
    relations.retain(|relation| {
        evidence.entity_of(&relation.from.kind, &relation.from.entity)
            && evidence.entity_of(&relation.to.kind, &relation.to.entity)
    });
    let shrunk = (before_narrow - relations.len()) as u64;
    let truth = outcome.truth;

    // Flow coverage: the engine's truth — suppressions, absent traces,
    // any relation shrinkage, and where the run degraded.
    for suppression in &truth.suppressions {
        flow_coverage.push(FlowCoverageEntry::SuppressedEvidence {
            log: suppression.log,
            span: suppression.span,
            suppressed: suppression.suppressed,
        });
    }
    for absent in &truth.absent_traces {
        flow_coverage.push(FlowCoverageEntry::AbsentTraceSpans { log: absent.log });
    }
    if shrunk > 0 {
        flow_coverage.push(FlowCoverageEntry::RelationShrinkage { count: shrunk });
    }
    if let Some(at) = truth.stopped_at {
        flow_coverage.push(FlowCoverageEntry::CorrelationDegradation {
            at: CorrelationStop::from(at),
        });
    }

    let correlated = Correlated::new(relations);

    // Limits: the caller's budget mirrored, the chain's stated spend and
    // stop, the strategy versions in effect, the eviction state.
    let limits = Limits::new(
        BudgetLimits::new(
            budget.deadline,
            budget.max_results,
            budget.max_bytes,
            budget.max_scan,
            budget.max_aggregation_memory,
        ),
        ChainLimits::new(
            chain.max_total_entities,
            chain.max_total_pages,
            walk.entities,
            walk.pages,
            walk.examinations,
            truth.scan_spent,
            walk.stopped,
        ),
        truth.strategy_versions,
        EvictionState::new(stats.resident_records, stats.total_evictions()),
    );

    let execution = Execution::new(vec![spans_group, logs_group, points_group], flow_coverage);

    Ok(Investigation::new(
        Subject::new(requested, effective),
        execution,
        correlated,
        evidence,
        limits,
    ))
}

/// The span evidence of a waterfall: every span named by its natural
/// identity; the degenerate subject (no valid identity) is named by the
/// identity the caller used to reach it, which is also the one the
/// effective root carries.
fn span_evidence(waterfall: &[Arc<Span>], effective: &EffectiveSubject) -> Vec<SpanEvidence> {
    waterfall
        .iter()
        .map(|span| {
            let entity = span
                .natural_identity()
                .map_or(effective.root.entity, |(trace_id, span_id)| {
                    EntityId::Span { trace_id, span_id }
                });
            SpanEvidence::new(Some(entity), Arc::clone(span))
        })
        .collect()
}

/// The trace's related logs: select the walked views whose trace identity
/// is the subject's, name them via the recovery walk, and assemble the
/// evidence. Selected records the recovery could not name become holes
/// (counted here and stated in flow coverage); the entity total counts
/// selected views and holes alike.
fn related_logs(
    store: &dyn TelemetryStore,
    log_views: Vec<RecordView>,
    relates_to: Option<TraceId>,
    walk: &mut WalkState,
) -> (Vec<LogEvidence>, u64) {
    let log_records: Vec<Arc<LogRecord>> = log_views
        .into_iter()
        .filter_map(|view| match view {
            RecordView::LogRecord(record) => Some(record),
            _ => None,
        })
        .collect();
    let selected: Vec<Arc<LogRecord>> = match relates_to {
        Some(trace_id) => log_records
            .into_iter()
            .filter(|record| record.trace_id == Some(trace_id))
            .collect(),
        // The subject carries no valid trace identity: the investigation
        // is the span alone; nothing is selected as related.
        None => Vec::new(),
    };
    let ids = name_log_identities(store, &selected, walk);
    let holes = ids.iter().filter(|id| id.is_none()).count() as u64;
    walk.entities = walk.entities.saturating_add(selected.len() as u64);
    let mut evidence = Vec::with_capacity(selected.len());
    for (index, record) in selected.iter().enumerate() {
        if let Some(entity) = ids[index] {
            evidence.push(LogEvidence::new(Some(entity), Arc::clone(record)));
        }
    }
    (evidence, holes)
}

/// The metrics surrounding the waterfall: select the walked points inside
/// the asked window, name them via the recovery walk, and assemble the
/// evidence plus the resident window. A window with no selected point is
/// stated as empty at the asked boundary.
fn surrounding_metrics(
    store: &dyn TelemetryStore,
    point_views: Vec<RecordView>,
    asked: TimeWindow,
    relates_to: Option<TraceId>,
    walk: &mut WalkState,
) -> (Vec<PointEvidence>, u64, TimeWindow) {
    let points: Vec<(Arc<MetricPoint>, Arc<StreamIdentity>)> = point_views
        .into_iter()
        .filter_map(|view| match view {
            RecordView::MetricPoint { point, stream } => Some((point, stream)),
            _ => None,
        })
        .collect();
    let selected: Vec<(Arc<MetricPoint>, Arc<StreamIdentity>)> = match relates_to {
        Some(_) => points
            .into_iter()
            .filter(|(point, _)| {
                let time = point.time_unix_nano();
                time >= asked.from && time < asked.to
            })
            .collect(),
        None => Vec::new(),
    };
    let point_records: Vec<Arc<MetricPoint>> = selected
        .iter()
        .map(|(point, _)| Arc::clone(point))
        .collect();
    let ids = name_point_identities(store, &point_records, walk);
    let holes = ids.iter().filter(|id| id.is_none()).count() as u64;
    walk.entities = walk.entities.saturating_add(selected.len() as u64);
    let mut evidence = Vec::with_capacity(selected.len());
    let mut resident_from = u64::MAX;
    let mut resident_to = 0_u64;
    for (index, (point, stream)) in selected.iter().enumerate() {
        if let Some(entity) = ids[index] {
            let time = point.time_unix_nano();
            resident_from = resident_from.min(time);
            resident_to = resident_to.max(time);
            evidence.push(PointEvidence::new(
                Some(entity),
                Arc::clone(point),
                Arc::clone(stream),
            ));
        }
    }
    let resident = if evidence.is_empty() {
        TimeWindow::new(asked.to, asked.to)
    } else {
        TimeWindow::new(resident_from, resident_to.saturating_add(1))
    };
    (evidence, holes, resident)
}

/// Walks one signal kind page by page, chaining the engine's cursors, until
/// the pages are exhausted or the chain stops. Every page is mirrored into
/// a run fact — outcome, coverage, next cursor — exactly as the engine
/// reported it.
fn walk_pages(
    store: &dyn TelemetryStore,
    query: &RecordsQuery,
    part: PartName,
    walk: &mut WalkState,
) -> Result<(Vec<RecordView>, RunGroup), FlowError> {
    let mut items = Vec::new();
    let mut runs = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        // Chain guard, before every page: the walk stops on its own
        // ceilings, and a stop is reported — never silently swallowed.
        if walk.pages >= walk.chain.max_total_pages {
            walk.stopped = Some(ChainBasis::TotalPages);
            break;
        }
        if walk.entities >= walk.chain.max_total_entities {
            walk.stopped = Some(ChainBasis::TotalEntities);
            break;
        }
        // Every continuation page is a NEW execution: fresh budget.
        // A rejected cursor is the flow's only engine-side failure mode.
        let page = records(store, query, walk.per_page(), cursor.as_deref())
            .map_err(|_| FlowError::CursorRejected)?;
        walk.pages += 1;
        runs.push(mirror_page(&page)?);
        let page_cursor = page.next_cursor;
        items.extend(page.items);
        match page_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok((items, RunGroup { part, runs }))
}

/// Mirrors one engine page into a run fact: the part outcome (the engine
/// reports exactly one), the coverage entries 1:1, and the next cursor.
fn mirror_page(page: &Page<RecordView>) -> Result<RunFacts, FlowError> {
    let outcome = page
        .execution
        .parts
        .first()
        .map(mirror_outcome)
        .ok_or(FlowError::EngineContract)?;
    let coverage = page
        .execution
        .coverage
        .entries
        .iter()
        .map(mirror_coverage)
        .collect();
    let next_cursor = page
        .next_cursor
        .as_ref()
        .map(|bytes| OpaqueCursor::new(bytes.clone()));
    Ok(RunFacts::new(outcome, coverage, next_cursor))
}

/// Mirrors an engine part outcome into the envelope's outcome vocabulary.
fn mirror_outcome(part: &PartOutcome) -> Outcome {
    match part {
        PartOutcome::Complete => Outcome::Complete,
        PartOutcome::Degraded { truncation } => Outcome::Degraded {
            truncation: mirror_truncation(truncation),
        },
        PartOutcome::Refused(refusal) => Outcome::Refused(mirror_refusal(refusal)),
        PartOutcome::Stalled => Outcome::Stalled,
    }
}

/// Mirrors an engine truncation into the envelope's vocabulary.
fn mirror_truncation(truncation: &EngineTruncation) -> Truncation {
    Truncation {
        dimension: mirror_dimension(truncation.dimension),
        position: match &truncation.position {
            EngineTruncationPoint::Cursor(bytes) => TruncationPoint::Cursor(bytes.clone()),
            EngineTruncationPoint::LastExamined(entity) => TruncationPoint::LastExamined(*entity),
        },
        omitted: truncation.omitted,
    }
}

/// Mirrors an engine budget refusal into the envelope's vocabulary.
fn mirror_refusal(refusal: &EngineRefusal) -> Refusal {
    Refusal {
        dimension: mirror_dimension(refusal.dimension),
        limit: mirror_magnitude(&refusal.limit),
        observed: mirror_magnitude(&refusal.observed),
    }
}

/// Mirrors an engine dimension into the envelope's dimension.
fn mirror_dimension(dimension: EngineDimension) -> Dimension {
    match dimension {
        EngineDimension::Deadline => Dimension::Deadline,
        EngineDimension::Results => Dimension::Results,
        EngineDimension::Bytes => Dimension::Bytes,
        EngineDimension::Scan => Dimension::Scan,
        EngineDimension::AggregationMemory => Dimension::AggregationMemory,
    }
}

/// Mirrors an engine magnitude into the envelope's magnitude.
fn mirror_magnitude(magnitude: &EngineMagnitude) -> Magnitude {
    match magnitude {
        EngineMagnitude::Duration(duration) => Magnitude::Duration(*duration),
        EngineMagnitude::Units(units) => Magnitude::Units(*units),
        EngineMagnitude::Bytes(bytes) => Magnitude::Bytes(*bytes),
    }
}

/// Mirrors an engine coverage entry into the envelope's vocabulary. The
/// snapshot boundary's admission key is decomposed into its two public
/// halves (the envelope carries transport-independent values).
fn mirror_coverage(entry: &EngineCoverageEntry) -> CoverageEntry {
    match entry {
        EngineCoverageEntry::EvictionGap { after, before } => CoverageEntry::EvictionGap {
            after: *after,
            before: *before,
        },
        EngineCoverageEntry::SnapshotBoundary { admission } => CoverageEntry::SnapshotBoundary {
            admission_time: admission.admitted_at(),
            entity: admission.entity(),
        },
        EngineCoverageEntry::UncountedTail { after, dimension } => CoverageEntry::UncountedTail {
            after: *after,
            dimension: mirror_dimension(*dimension),
        },
        EngineCoverageEntry::DriverStall { after } => CoverageEntry::DriverStall { after: *after },
    }
}

/// The span views of a spans walk. The query kind fixes every view; the
/// impossible other kinds are filtered out by construction.
fn span_views_of(views: Vec<RecordView>) -> Vec<Arc<Span>> {
    views
        .into_iter()
        .filter_map(|view| match view {
            RecordView::Span(span) => Some(span),
            RecordView::LogRecord(_) | RecordView::MetricPoint { .. } => None,
        })
        .collect()
}

/// Resolves the effective subject: the trace's parentless root, with the
/// note that explains how the runtime got there (invariant 1).
///
/// The trace's members are the resident spans whose natural trace id equals
/// the requested root's. The effective root is the FIRST parentless member
/// in engine order; when none is resident, the requested root stands with a
/// `NoParentlessSpanResident` note. A requested root with no valid trace
/// identity (natural identity absent) investigates the span alone.
fn resolve_subject(root: &Arc<Span>, requested: EntityId, spans: &[Arc<Span>]) -> EffectiveSubject {
    let Some((trace_id, _)) = root.natural_identity() else {
        let effective = EffectiveRoot::new(
            requested,
            root.name.clone(),
            root.context.trace_id,
            root.context.span_id,
        );
        return EffectiveSubject::new(effective, vec![ResolutionNote::RootHasNoValidTraceIdentity]);
    };

    // The trace's resident members, carrying the identities they were
    // selected by (a member's natural identity is guaranteed by selection).
    let members: Vec<(&Arc<Span>, TraceId, SpanId)> = spans
        .iter()
        .filter_map(|span| {
            span.natural_identity()
                .and_then(|(member_trace, member_span)| {
                    (member_trace == trace_id).then_some((span, member_trace, member_span))
                })
        })
        .collect();
    let parentless: Vec<(&Arc<Span>, TraceId, SpanId)> = members
        .iter()
        .copied()
        .filter(|(span, _, _)| span.parent_span_id.is_none())
        .collect();

    let (effective, notes) =
        if let Some((candidate, candidate_trace, candidate_span)) = parentless.first() {
            let effective = EffectiveRoot::new(
                EntityId::Span {
                    trace_id: *candidate_trace,
                    span_id: *candidate_span,
                },
                candidate.name.clone(),
                *candidate_trace,
                *candidate_span,
            );
            let notes = if Arc::ptr_eq(candidate, root) {
                // The requested span IS the trace's parentless root (and the
                // first such in residency order): no redirection was needed,
                // only confirmation.
                vec![ResolutionNote::RequestedSpanIsRoot]
            } else {
                // A parentless member earlier in residency order is the trace's
                // actual root; the difference is named.
                vec![ResolutionNote::TraceRootedAt {
                    entity: EntityId::Span {
                        trace_id: *candidate_trace,
                        span_id: *candidate_span,
                    },
                    name: candidate.name.clone(),
                    span_id: *candidate_span,
                }]
            };
            (effective, notes)
        } else {
            // The trace's parentless root(s) are not resident: its chains were
            // evicted or sampled out. The requested span stands.
            let effective = EffectiveRoot::new(
                EntityId::Span {
                    trace_id,
                    span_id: root.context.span_id,
                },
                root.name.clone(),
                trace_id,
                root.context.span_id,
            );
            (effective, vec![ResolutionNote::NoParentlessSpanResident])
        };
    EffectiveSubject::new(effective, notes)
}

/// Orders the trace's spans into the waterfall: breadth-first levels from
/// the effective root over parent links, children in engine order, then the
/// trace members the walk could not reach through a parent chain, in engine
/// order. A subject with no valid trace identity is the span alone.
fn waterfall(
    spans: &[Arc<Span>],
    effective: &EffectiveSubject,
    root: &Arc<Span>,
) -> Vec<Arc<Span>> {
    let trace_id = effective.root.trace_id;
    let root_id = effective.root.span_id;

    // The seed: the effective root by identity.
    let seed_index = spans.iter().position(|span| {
        matches!(
            span.natural_identity(),
            Some((natural_trace, natural_span))
                if natural_trace == trace_id && natural_span == root_id
        )
    });
    let Some(seed_index) = seed_index else {
        // The effective root carries no valid identity: the requested
        // subject is resident but nameless — the waterfall is the span
        // alone.
        return vec![Arc::clone(root)];
    };

    let mut ordered = vec![Arc::clone(&spans[seed_index])];
    let mut visited: Vec<SpanId> = vec![root_id];
    let mut cursor = 0_usize;
    while cursor < ordered.len() {
        let parent_id = ordered[cursor]
            .natural_identity()
            .map(|(_, span_id)| span_id)
            .expect("every ordered span is a trace member with a valid identity");
        for span in spans {
            if span.parent_span_id != Some(parent_id) {
                continue;
            }
            if let Some((natural_trace, natural_span)) = span.natural_identity() {
                if natural_trace == trace_id && !visited.contains(&natural_span) {
                    visited.push(natural_span);
                    ordered.push(Arc::clone(span));
                }
            }
        }
        cursor += 1;
    }

    // The members no parent chain reached: broken links and missing
    // parents. They follow the reached waterfall, in engine order.
    for span in spans {
        if let Some((natural_trace, natural_span)) = span.natural_identity() {
            if natural_trace == trace_id && !visited.contains(&natural_span) {
                visited.push(natural_span);
                ordered.push(Arc::clone(span));
            }
        }
    }
    ordered
}

/// The waterfall's time extent: the minimum start to the maximum end (a
/// span without an end time ends at its start) across the waterfall spans.
fn extent_of(spans: &[Arc<Span>]) -> (u64, u64) {
    let mut min_start = u64::MAX;
    let mut max_end = 0_u64;
    for span in spans {
        let start = span.start_time_unix_nano;
        let end = span.end_time_unix_nano.unwrap_or(start);
        min_start = min_start.min(start);
        max_end = max_end.max(end);
    }
    (min_start, max_end)
}

/// Names selected log records by matching their payload Arcs against the
/// store's scan surface (`Arc::ptr_eq`: the driver shares payload Arcs,
/// ADR 0008). The recovery walk is chain-bounded: a selected record the
/// walk could not name stays `None` here — a residency hole, counted by
/// the caller.
fn name_log_identities(
    store: &dyn TelemetryStore,
    selected: &[Arc<LogRecord>],
    walk: &mut WalkState,
) -> Vec<Option<EntityId>> {
    let mut wanted: HashSet<*const LogRecord> = selected.iter().map(Arc::as_ptr).collect();
    let mut names = vec![None; selected.len()];
    let mut after: Option<AdmissionKey> = None;
    while !wanted.is_empty() && walk.examinations < walk.chain.max_identity_examinations {
        let page = store.scan_log_records(after, RECOVERY_SCAN_SLICE);
        for item in page.items {
            if wanted.is_empty() || walk.examinations >= walk.chain.max_identity_examinations {
                break;
            }
            walk.examinations += 1;
            let pointer = Arc::as_ptr(&item.record);
            if wanted.remove(&pointer) {
                let index = selected
                    .iter()
                    .position(|arc| Arc::ptr_eq(arc, &item.record))
                    .expect("a matched pointer is one of the selected records");
                names[index] = Some(item.key.entity());
            }
        }
        match page.cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    names
}

/// Names selected metric points by matching their payload Arcs against the
/// store's scan surface, exactly as [`name_log_identities`] does for logs.
fn name_point_identities(
    store: &dyn TelemetryStore,
    selected: &[Arc<MetricPoint>],
    walk: &mut WalkState,
) -> Vec<Option<EntityId>> {
    let mut wanted: HashSet<*const MetricPoint> = selected.iter().map(Arc::as_ptr).collect();
    let mut names = vec![None; selected.len()];
    let mut after: Option<AdmissionKey> = None;
    while !wanted.is_empty() && walk.examinations < walk.chain.max_identity_examinations {
        let page = store.scan_metric_points(after, RECOVERY_SCAN_SLICE);
        for item in page.items {
            if wanted.is_empty() || walk.examinations >= walk.chain.max_identity_examinations {
                break;
            }
            walk.examinations += 1;
            let pointer = Arc::as_ptr(&item.record.point);
            if wanted.remove(&pointer) {
                let index = selected
                    .iter()
                    .position(|arc| Arc::ptr_eq(arc, &item.record.point))
                    .expect("a matched pointer is one of the selected records");
                names[index] = Some(item.key.entity());
            }
        }
        match page.cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    names
}
