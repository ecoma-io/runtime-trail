//! The correlation engine: runs the committed strategies over a store
//! within the caller's bounds, and reports the relations and the truth.
//!
//! The engine reads the resident set through the storage contract only —
//! it never names a driver, and no strategy reaches past
//! [`TelemetryStore`]. A run examines at most `max_scan` resident
//! positions, never refuses the read, and reports every degradation in the
//! truth.
//!
//! Strategy semantics:
//!
//! * **SpanIdentity** — a log record whose trace context names the exact
//!   span of its trace is related to that span.
//! * **TraceIdentity** — a log record is related to every resident span of
//!   its trace, *unless* the exact span is resident: then those relations
//!   are suppressed in the exact relation's favor (the suppression is
//!   accounted scoped to the log record, complete regardless of bounds).
//!   A log whose trace has no resident span gets no relation, and the
//!   absence is accounted as completeness coverage — the trace is named by
//!   the log but absent from the resident set.
//! * **TemporalCoActivity** — the subject's resident spans and the data
//!   points co-active in the caller-supplied window are related. A record
//!   participates iff it falls inside the window; two in-window records are
//!   co-active when they overlap or when the gap between their time values
//!   (the interval edges for spans) is at most the window's length. The
//!   window is recorded on every relation, and the cited times make
//!   overlap vs. proximity derivable from the facts.
//!
//! Every relation's endpoints are re-checked against the store after the
//! run: a relation whose endpoint left residency mid-run is dropped and the
//! drop is counted in the truth (`shrunken`). Relations are returned in a
//! deterministic order (type, then endpoints), and the relation ceiling is
//! applied after that ordering, so a capped answer is a stable prefix of
//! the full one.

use std::cmp::Ordering;
use std::sync::Arc;

use runtime_trail_storage::TelemetryStore;
use runtime_trail_telemetry_model::{
    AssignedId, EntityId, LogRecord, MetricPoint, Span, SpanId, TraceId, Value,
};

use crate::bounds::{
    AbsentTrace, CorrelationBounds, CorrelationOutcome, StoppedAt, Strategy, Suppression, Truth,
};
use crate::relations::{
    EvidenceFact, Relation, RelationType, SignalKind, SignalRef, StrategyVersion, Window,
};

/// The page width of the engine's resident scans.
const SCAN_PAGE: usize = 256;

/// Runs the requested strategies over the store within the bounds, and
/// returns the relations and the run's truth.
#[must_use]
pub fn correlate(store: &dyn TelemetryStore, bounds: &CorrelationBounds) -> CorrelationOutcome {
    // The strategies' versions in effect: those of exactly the strategies
    // requested, in run order. A run bounded to zero depth never starts any.
    let strategy_versions = if bounds.max_hops == 0 {
        Vec::new()
    } else {
        bounds
            .strategies
            .iter()
            .map(|strategy| {
                StrategyVersion::new(strategy.name().to_owned(), strategy.version().to_owned())
            })
            .collect()
    };

    if bounds.max_hops == 0 {
        return CorrelationOutcome {
            relations: Vec::new(),
            truth: Truth {
                strategy_versions,
                suppressions: Vec::new(),
                absent_traces: Vec::new(),
                shrunken: 0,
                stopped_at: Some(StoppedAt::MaxDepth { depth: 0 }),
                scan_spent: 0,
            },
        };
    }

    let mut spent: u64 = 0;
    let mut stopped_at: Option<StoppedAt> = None;

    // The resident scans, in a fixed order: spans, then logs, then points.
    // Every scanned position is examined (counted into the spend) whether
    // or not the run selects it.
    let spans = scan_spans(store, bounds, &mut spent, &mut stopped_at);
    let logs = scan_logs(store, bounds, &mut spent, &mut stopped_at);
    let points = scan_points(store, bounds, &mut spent, &mut stopped_at);

    // The subject's resident spans: the members the strategies relate the
    // subject to. Identity strategies are silent when no trace is named.
    let members = subject_spans(&spans, bounds.subject_trace);

    let mut relations: Vec<Relation<SignalRef>> = Vec::new();
    let mut suppressions: Vec<Suppression> = Vec::new();
    let mut absent_traces: Vec<AbsentTrace> = Vec::new();

    for strategy in &bounds.strategies {
        match strategy {
            Strategy::SpanIdentity => {
                span_identity(bounds.subject_trace, &members, &logs, &mut relations);
            }
            Strategy::TraceIdentity => {
                trace_identity(
                    bounds.subject_trace,
                    &members,
                    &logs,
                    &mut relations,
                    &mut suppressions,
                    &mut absent_traces,
                );
            }
            Strategy::TemporalCoActivity => {
                temporal_co_activity(&members, &points, bounds.window, &mut relations);
            }
        }
    }

    // The resident-endpoint re-check: a relation whose endpoint left
    // residency after it was formed does not survive.
    let before = relations.len();
    relations.retain(|relation| endpoints_resident(store, relation));
    let shrunken = u64::try_from(before - relations.len()).unwrap_or(u64::MAX);

    // Deterministic order, then the relation ceiling as a stable prefix.
    relations.sort_by(relation_order);
    let total = u64::try_from(relations.len()).unwrap_or(u64::MAX);
    if total > bounds.max_relations {
        let count = bounds.max_relations;
        relations.truncate(usize::try_from(count).unwrap_or(usize::MAX));
        if stopped_at.is_none() {
            stopped_at = Some(StoppedAt::MaxRelations { count });
        }
    }

    CorrelationOutcome {
        relations,
        truth: Truth {
            strategy_versions,
            suppressions,
            absent_traces,
            shrunken,
            stopped_at,
            scan_spent: spent,
        },
    }
}

/// The subject's resident spans: those whose natural identity names the
/// subject trace.
fn subject_spans(
    spans: &[(EntityId, Arc<Span>)],
    subject: Option<TraceId>,
) -> Vec<&(EntityId, Arc<Span>)> {
    let Some(subject) = subject else {
        return Vec::new();
    };
    spans
        .iter()
        .filter(|(_, span)| {
            span.natural_identity()
                .is_some_and(|(trace, _)| trace == subject)
        })
        .collect()
}

/// `SpanIdentity`: attaches a log record to the exact span its trace context
/// names, when that span is resident.
fn span_identity(
    subject: Option<TraceId>,
    members: &[&(EntityId, Arc<Span>)],
    logs: &[(EntityId, Arc<LogRecord>)],
    relations: &mut Vec<Relation<SignalRef>>,
) {
    let Some(subject) = subject else {
        return;
    };
    let version = StrategyVersion::new(
        Strategy::SpanIdentity.name().to_owned(),
        Strategy::SpanIdentity.version().to_owned(),
    );
    for (log_entity, log) in logs {
        let Some((trace, span_id)) = log.correlation_pair() else {
            continue;
        };
        if trace != subject {
            continue;
        }
        let Some(span_entity) = span_with_identity(members, trace, span_id) else {
            continue;
        };
        relations.push(Relation::new(
            RelationType::SpanIdentity,
            SignalRef::new(SignalKind::LogRecords, *log_entity),
            SignalRef::new(SignalKind::Spans, span_entity),
            identity_facts(trace, span_id),
            version.clone(),
            None,
        ));
    }
}

/// `TraceIdentity`: attaches a log record to every resident span of its
/// trace. When the log's exact span is resident, those relations are
/// suppressed in the exact relation's favor (accounted, complete); when
/// the trace has no resident span, the absence is accounted as
/// completeness coverage.
fn trace_identity(
    subject: Option<TraceId>,
    members: &[&(EntityId, Arc<Span>)],
    logs: &[(EntityId, Arc<LogRecord>)],
    relations: &mut Vec<Relation<SignalRef>>,
    suppressions: &mut Vec<Suppression>,
    absent_traces: &mut Vec<AbsentTrace>,
) {
    let Some(subject) = subject else {
        return;
    };
    let version = StrategyVersion::new(
        Strategy::TraceIdentity.name().to_owned(),
        Strategy::TraceIdentity.version().to_owned(),
    );
    for (log_entity, log) in logs {
        let Some(trace) = log.trace_id else {
            continue;
        };
        if trace != subject {
            continue;
        }
        let exact = log
            .correlation_pair()
            .and_then(|(trace, span_id)| span_with_identity(members, trace, span_id));
        match exact {
            Some(exact_entity) => {
                // Attached to its exact span: the sibling relations are
                // suppressed in its favor, accounted complete.
                let siblings = members
                    .iter()
                    .filter(|(entity, _)| *entity != exact_entity)
                    .count();
                suppressions.push(Suppression {
                    log: *log_entity,
                    span: exact_entity,
                    suppressed: u64::try_from(siblings).unwrap_or(u64::MAX),
                });
            }
            None => {
                if members.is_empty() {
                    // The trace is named by the log but absent from the
                    // resident set: completeness coverage, no relation.
                    absent_traces.push(AbsentTrace { log: *log_entity });
                } else {
                    for (span_entity, _) in members {
                        relations.push(Relation::new(
                            RelationType::TraceIdentity,
                            SignalRef::new(SignalKind::LogRecords, *log_entity),
                            SignalRef::new(SignalKind::Spans, *span_entity),
                            trace_facts(trace),
                            version.clone(),
                            None,
                        ));
                    }
                }
            }
        }
    }
}

/// `TemporalCoActivity`: relates the subject's resident spans to the
/// in-window data points, recording the window on every relation.
fn temporal_co_activity(
    members: &[&(EntityId, Arc<Span>)],
    points: &[(EntityId, Arc<MetricPoint>)],
    window: Option<Window>,
    relations: &mut Vec<Relation<SignalRef>>,
) {
    let Some(window) = window else {
        return;
    };
    if window.is_empty() {
        return;
    }
    let version = StrategyVersion::new(
        Strategy::TemporalCoActivity.name().to_owned(),
        Strategy::TemporalCoActivity.version().to_owned(),
    );
    for (span_entity, span) in members {
        // A relation holds between records inside the window: a span that
        // does not intersect it never grounds one, even when it is near.
        let start = span.start_time_unix_nano;
        let end = span.end_time_unix_nano.unwrap_or(start);
        if end <= window.from || start >= window.to {
            continue;
        }
        for (point_entity, point) in points {
            let time = point.time_unix_nano();
            // Co-active when the pair overlaps or the gap between their
            // time values is at most the window's length.
            let gap = if (start..end).contains(&time) {
                0
            } else if time < start {
                start - time
            } else {
                time - end
            };
            if gap > window.len() {
                continue;
            }
            let mut facts = vec![
                EvidenceFact::new("span.start_time_unix_nano".to_owned(), clock_value(start)),
                EvidenceFact::new("point.time_unix_nano".to_owned(), clock_value(time)),
            ];
            if let Some(end) = span.end_time_unix_nano {
                facts.push(EvidenceFact::new(
                    "span.end_time_unix_nano".to_owned(),
                    clock_value(end),
                ));
            }
            relations.push(Relation::new(
                RelationType::TemporalCoActivity,
                SignalRef::new(SignalKind::Spans, *span_entity),
                SignalRef::new(SignalKind::MetricPoints, *point_entity),
                facts,
                version.clone(),
                Some(window),
            ));
        }
    }
}

/// The resident span named by `(trace, span_id)`, if any.
fn span_with_identity(
    members: &[&(EntityId, Arc<Span>)],
    trace: TraceId,
    span_id: SpanId,
) -> Option<EntityId> {
    members
        .iter()
        .find(|(_, span)| {
            span.natural_identity()
                .is_some_and(|(candidate_trace, candidate_span)| {
                    candidate_trace == trace && candidate_span == span_id
                })
        })
        .map(|(entity, _)| *entity)
}

/// The facts of an exact attachment: the trace context cited verbatim.
fn identity_facts(trace: TraceId, span_id: SpanId) -> Vec<EvidenceFact> {
    vec![
        EvidenceFact::new(
            "trace_id".to_owned(),
            Value::Bytes(trace.as_bytes().to_vec()),
        ),
        EvidenceFact::new(
            "span_id".to_owned(),
            Value::Bytes(span_id.as_bytes().to_vec()),
        ),
    ]
}

/// The facts of a trace relation: the trace cited verbatim.
fn trace_facts(trace: TraceId) -> Vec<EvidenceFact> {
    vec![EvidenceFact::new(
        "trace_id".to_owned(),
        Value::Bytes(trace.as_bytes().to_vec()),
    )]
}

/// A model clock value as a fact value. The model clocks are u64 nanos;
/// the value space holds i64, and every sane clock reading fits — a
/// reading beyond `i64::MAX` (year 2262) saturates rather than wraps.
fn clock_value(nano: u64) -> Value {
    Value::Int(i64::try_from(nano).unwrap_or(i64::MAX))
}

/// Scans the resident spans, counting every examined position into the
/// work allowance.
fn scan_spans(
    store: &dyn TelemetryStore,
    bounds: &CorrelationBounds,
    spent: &mut u64,
    stopped_at: &mut Option<StoppedAt>,
) -> Vec<(EntityId, Arc<Span>)> {
    let mut resident = Vec::new();
    let mut after = None;
    loop {
        if *spent >= bounds.max_scan {
            *stopped_at = Some(StoppedAt::ScanExhausted { examined: *spent });
            break;
        }
        let page = store.scan_spans(after, SCAN_PAGE);
        if page.items.is_empty() {
            break;
        }
        for item in &page.items {
            if *spent >= bounds.max_scan {
                *stopped_at = Some(StoppedAt::ScanExhausted { examined: *spent });
                return resident;
            }
            *spent += 1;
            resident.push((item.key.entity(), Arc::clone(&item.record)));
        }
        match page.cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    resident
}

/// Scans the resident log records, counting every examined position into
/// the work allowance.
fn scan_logs(
    store: &dyn TelemetryStore,
    bounds: &CorrelationBounds,
    spent: &mut u64,
    stopped_at: &mut Option<StoppedAt>,
) -> Vec<(EntityId, Arc<LogRecord>)> {
    let mut resident = Vec::new();
    let mut after = None;
    loop {
        if *spent >= bounds.max_scan {
            *stopped_at = Some(StoppedAt::ScanExhausted { examined: *spent });
            break;
        }
        let page = store.scan_log_records(after, SCAN_PAGE);
        if page.items.is_empty() {
            break;
        }
        for item in &page.items {
            if *spent >= bounds.max_scan {
                *stopped_at = Some(StoppedAt::ScanExhausted { examined: *spent });
                return resident;
            }
            *spent += 1;
            resident.push((item.key.entity(), Arc::clone(&item.record)));
        }
        match page.cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    resident
}

/// Scans the resident metric points, counting every examined position into
/// the work allowance; selects only the points inside the run's window,
/// when the run grounds one.
fn scan_points(
    store: &dyn TelemetryStore,
    bounds: &CorrelationBounds,
    spent: &mut u64,
    stopped_at: &mut Option<StoppedAt>,
) -> Vec<(EntityId, Arc<MetricPoint>)> {
    let mut resident = Vec::new();
    let mut after = None;
    loop {
        if *spent >= bounds.max_scan {
            *stopped_at = Some(StoppedAt::ScanExhausted { examined: *spent });
            break;
        }
        let page = store.scan_metric_points(after, SCAN_PAGE);
        if page.items.is_empty() {
            break;
        }
        for item in &page.items {
            if *spent >= bounds.max_scan {
                *stopped_at = Some(StoppedAt::ScanExhausted { examined: *spent });
                return resident;
            }
            *spent += 1;
            let point = Arc::clone(&item.record.point);
            let in_window = bounds.window.is_some_and(|window| {
                let time = point.time_unix_nano();
                time >= window.from && time < window.to
            });
            if in_window {
                resident.push((item.key.entity(), point));
            }
        }
        match page.cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    resident
}

/// Whether both of the relation's endpoints are still resident.
fn endpoints_resident(store: &dyn TelemetryStore, relation: &Relation<SignalRef>) -> bool {
    for endpoint in [&relation.from, &relation.to] {
        let present = match endpoint.kind {
            SignalKind::Spans => store.span(endpoint.entity).is_some(),
            SignalKind::LogRecords => store.log_record(endpoint.entity).is_some(),
            SignalKind::MetricPoints => store.metric_point(endpoint.entity).is_some(),
        };
        if !present {
            return false;
        }
    }
    true
}

/// The deterministic relation order: type, then endpoints.
fn relation_order(left: &Relation<SignalRef>, right: &Relation<SignalRef>) -> Ordering {
    type_rank(left.relation_type)
        .cmp(&type_rank(right.relation_type))
        .then_with(|| signal_ref_order(&left.from, &right.from))
        .then_with(|| signal_ref_order(&left.to, &right.to))
}

/// The taxonomy order of a relation type, exhaustively: a new variant must
/// be placed here, so the ordering can never silently forget one.
fn type_rank(relation_type: RelationType) -> u8 {
    match relation_type {
        RelationType::SpanIdentity => 0,
        RelationType::TraceIdentity => 1,
        RelationType::ParentChild => 2,
        RelationType::ResourceContext => 3,
        RelationType::ExemplarAttachment => 4,
        RelationType::TemporalCoActivity => 5,
        RelationType::Inferred => 6,
    }
}
fn signal_ref_order(left: &SignalRef, right: &SignalRef) -> Ordering {
    kind_rank(left.kind)
        .cmp(&kind_rank(right.kind))
        .then_with(|| entity_order(left.entity, right.entity))
}

fn kind_rank(kind: SignalKind) -> u8 {
    match kind {
        SignalKind::Spans => 0,
        SignalKind::LogRecords => 1,
        SignalKind::MetricPoints => 2,
    }
}

/// A deterministic total order over entities: span identities by trace then
/// span id, then assigned entities by serial.
fn entity_order(left: EntityId, right: EntityId) -> Ordering {
    match (left, right) {
        (
            EntityId::Span {
                trace_id: left_trace,
                span_id: left_span,
            },
            EntityId::Span {
                trace_id: right_trace,
                span_id: right_span,
            },
        ) => left_trace
            .as_bytes()
            .cmp(&right_trace.as_bytes())
            .then_with(|| left_span.as_bytes().cmp(&right_span.as_bytes())),
        (EntityId::Span { .. }, EntityId::Assigned(_)) => Ordering::Less,
        (EntityId::Assigned(_), EntityId::Span { .. }) => Ordering::Greater,
        (EntityId::Assigned(left), EntityId::Assigned(right)) => {
            entity_serial(left).cmp(&entity_serial(right))
        }
    }
}

fn entity_serial(assigned: AssignedId) -> u64 {
    assigned.serial().get()
}
