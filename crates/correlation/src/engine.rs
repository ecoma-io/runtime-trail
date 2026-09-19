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
//! * **ParentChild** — a span is related to the span its `parent_span_id`
//!   names, when that span is resident. Derived from the span's own parent
//!   field; a parent absent from the resident set grounds nothing.
//! * **ResourceContext** — records that share one resource identity are
//!   related every which way, evidenced by the shared resource's own
//!   attributes.
//! * **ExemplarAttachment** — a metric data point is related to the span
//!   an exemplar's trace context names, when that span is resident.
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
//! drop is counted in the truth (`shrunken`). The relation ceiling engages
//! during generation, never after: every push site checks it, keeps the
//! first `max_relations` relations formed in deterministic scan order, and
//! reports the stop through `stopped_at`, so a capped answer is the
//! generation-order prefix and an under-cap answer is byte-identical to
//! the unbound run over the same resident set. Relations are returned in a
//! deterministic order (type, then endpoints).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use runtime_trail_storage::TelemetryStore;
use runtime_trail_telemetry_model::{
    AssignedId, EntityId, LogRecord, MetricPoint, Resource, Span, SpanId, StreamIdentity, TraceId,
    Value,
};

use crate::bounds::{
    AbsentTrace, CorrelationBounds, CorrelationOutcome, StoppedAt, Strategy, Suppression, Truth,
};
use crate::relations::{
    EvidenceFact, Relation, RelationType, SignalKind, SignalRef, StrategyVersion, Window,
};

/// The page width of the engine's resident scans.
const SCAN_PAGE: usize = 256;

/// Appends formed relations under the run's ceiling: every push site
/// checks `max_relations`, keeps the first `limit` relations formed in
/// deterministic generation order, and records `StoppedAt::MaxRelations`
/// exactly once. Pushes past the ceiling are never materialized, so peak
/// memory stays O(limit) instead of a strategy's full growth.
struct RelationSink<'a> {
    relations: &'a mut Vec<Relation<SignalRef>>,
    /// The run's relation ceiling.
    limit: u64,
    /// Relations accepted so far — never exceeds `limit`.
    kept: u64,
    /// Where the run stopped; the ceiling is named here once, and only if
    /// no earlier bound already stopped the run.
    stopped_at: &'a mut Option<StoppedAt>,
}

impl RelationSink<'_> {
    fn new<'a>(
        relations: &'a mut Vec<Relation<SignalRef>>,
        limit: u64,
        stopped_at: &'a mut Option<StoppedAt>,
    ) -> RelationSink<'a> {
        RelationSink {
            relations,
            limit,
            kept: 0,
            stopped_at,
        }
    }

    /// Accepts a formed relation unless the ceiling is exhausted; the stop
    /// is named once, on the first push past the ceiling.
    fn push(&mut self, relation: Relation<SignalRef>) {
        if self.kept >= self.limit {
            if self.stopped_at.is_none() {
                *self.stopped_at = Some(StoppedAt::MaxRelations { count: self.limit });
            }
            return;
        }
        self.relations.push(relation);
        self.kept += 1;
    }
}

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
    {
        let mut sink = RelationSink::new(&mut relations, bounds.max_relations, &mut stopped_at);

        for strategy in &bounds.strategies {
            match strategy {
                Strategy::SpanIdentity => {
                    span_identity(bounds.subject_trace, &members, &logs, &mut sink);
                }
                Strategy::TraceIdentity => {
                    trace_identity(
                        bounds.subject_trace,
                        &members,
                        &logs,
                        &mut sink,
                        &mut suppressions,
                        &mut absent_traces,
                    );
                }
                Strategy::ParentChild => {
                    parent_child(&spans, &mut sink);
                }
                Strategy::ResourceContext => {
                    resource_context(&spans, &logs, &points, &mut sink);
                }
                Strategy::ExemplarAttachment => {
                    exemplar_attachment(&points, &spans, &mut sink);
                }
                Strategy::TemporalCoActivity => {
                    temporal_co_activity(&members, &points, bounds.window, &mut sink);
                }
            }
        }
    }

    // The resident-endpoint re-check: a relation whose endpoint left
    // residency after it was formed does not survive.
    let before = relations.len();
    relations.retain(|relation| endpoints_resident(store, relation));
    let shrunken = u64::try_from(before - relations.len()).unwrap_or(u64::MAX);

    // Deterministic order over the kept set: the ceiling engaged during
    // generation, so the set holds at most `max_relations` already.
    relations.sort_by(relation_order);

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
    sink: &mut RelationSink<'_>,
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
        sink.push(Relation::new(
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
    sink: &mut RelationSink<'_>,
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
                        sink.push(Relation::new(
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
    points: &[(EntityId, Arc<MetricPoint>, Arc<StreamIdentity>)],
    window: Option<Window>,
    sink: &mut RelationSink<'_>,
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
        for (point_entity, point, _) in points {
            // A point outside the window never grounds a relation, even
            // when it is near a span: the window is the strategy's whole
            // domain, so the filter is applied while generating.
            let time = point.time_unix_nano();
            if time < window.from || time >= window.to {
                continue;
            }
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
            sink.push(Relation::new(
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

/// `ParentChild`: relates a span to the span its `parent_span_id` names,
/// when that span is resident. The relation is derived from the span's
/// own parent field, and the evidence is that field cited verbatim. The
/// zero parent (the absent-parent marker emitters send), a parent outside
/// the resident set, and a span naming itself are never grounded — the
/// absent-endpoint and self-loop guards are applied while generating,
/// never by dropping a formed relation.
fn parent_child(spans: &[(EntityId, Arc<Span>)], sink: &mut RelationSink<'_>) {
    let version = StrategyVersion::new(
        Strategy::ParentChild.name().to_owned(),
        Strategy::ParentChild.version().to_owned(),
    );
    for (child_entity, child) in spans {
        let Some(parent_span_id) = child.parent_span_id else {
            continue; // a root span has no parent field to derive from
        };
        if !parent_span_id.is_valid() {
            continue; // the zero parent is the absent-parent marker
        }
        let Some((trace, _)) = child.natural_identity() else {
            continue; // no natural identity, no resolvable parent
        };
        let Some(parent_entity) = resident_span_named(spans, trace, parent_span_id) else {
            continue; // the named parent is not resident
        };
        if *child_entity == parent_entity {
            continue; // the no-self-loop guard
        }
        sink.push(Relation::new(
            RelationType::ParentChild,
            SignalRef::new(SignalKind::Spans, *child_entity),
            SignalRef::new(SignalKind::Spans, parent_entity),
            vec![EvidenceFact::new(
                "parent_span_id".to_owned(),
                Value::Bytes(parent_span_id.as_bytes().to_vec()),
            )],
            version.clone(),
            None,
        ));
    }
}

/// `ResourceContext`: relates records that share one resource identity —
/// every unordered pair within a group of two or more, citing the shared
/// resource's own attributes as the evidence. A singleton group grounds
/// no relation; the identity that relates a pair is the attribute map
/// itself, so the evidence is the map's contents, never fabricated.
fn resource_context(
    spans: &[(EntityId, Arc<Span>)],
    logs: &[(EntityId, Arc<LogRecord>)],
    points: &[(EntityId, Arc<MetricPoint>, Arc<StreamIdentity>)],
    sink: &mut RelationSink<'_>,
) {
    let version = StrategyVersion::new(
        Strategy::ResourceContext.name().to_owned(),
        Strategy::ResourceContext.version().to_owned(),
    );
    // Group the resident records by resource identity — the ordered
    // attribute map — so groups and members run deterministically: groups
    // in map order, members in scan order.
    let mut groups: BTreeMap<Resource, Vec<SignalRef>> = BTreeMap::new();
    for (entity, span) in spans {
        groups
            .entry((*span.resource).clone())
            .or_default()
            .push(SignalRef::new(SignalKind::Spans, *entity));
    }
    for (entity, log) in logs {
        groups
            .entry((*log.resource).clone())
            .or_default()
            .push(SignalRef::new(SignalKind::LogRecords, *entity));
    }
    for (entity, _, stream) in points {
        groups
            .entry(stream.resource.clone())
            .or_default()
            .push(SignalRef::new(SignalKind::MetricPoints, *entity));
    }
    for (resource, members) in groups {
        if members.len() < 2 {
            continue;
        }
        // Any → any: each member relates to every later member — one
        // relation per unordered pair; the inverse direction is the same
        // relation seen backwards and adds no facts.
        for (index, from) in members.iter().enumerate() {
            for to in &members[index + 1..] {
                sink.push(Relation::new(
                    RelationType::ResourceContext,
                    from.clone(),
                    to.clone(),
                    resource_facts(&resource),
                    version.clone(),
                    None,
                ));
            }
        }
    }
}

/// The evidence a resource-context relation stands on: the shared
/// resource's own attributes, each cited verbatim as `(key, value)`. An
/// empty shared identity — a resource with no attributes — is still the
/// identity that relates the pair, cited as one empty attribute list so
/// no relation ships without its evidence.
fn resource_facts(resource: &Resource) -> Vec<EvidenceFact> {
    let mut facts: Vec<EvidenceFact> = resource
        .identity()
        .iter()
        .map(|(key, value)| EvidenceFact::new(key.clone(), value.clone()))
        .collect();
    if facts.is_empty() {
        facts.push(EvidenceFact::new(
            "resource".to_owned(),
            Value::kv_list(Vec::new()).expect("an empty list cannot duplicate keys"),
        ));
    }
    facts
}

/// `ExemplarAttachment`: relates a metric data point to the span an
/// exemplar's trace context names, when that span is resident. The
/// exemplar's two ids are the evidence, cited verbatim; an exemplar that
/// carries only one id — or names a span outside the resident set — is
/// never grounded, so nothing is fabricated and no relation to an absent
/// record is formed.
fn exemplar_attachment(
    points: &[(EntityId, Arc<MetricPoint>, Arc<StreamIdentity>)],
    spans: &[(EntityId, Arc<Span>)],
    sink: &mut RelationSink<'_>,
) {
    let version = StrategyVersion::new(
        Strategy::ExemplarAttachment.name().to_owned(),
        Strategy::ExemplarAttachment.version().to_owned(),
    );
    for (point_entity, point, _) in points {
        for exemplar in point.exemplars() {
            let Some((trace, span_id)) = exemplar.correlation_pair() else {
                continue; // both ids are needed to name a span
            };
            let Some(span_entity) = resident_span_named(spans, trace, span_id) else {
                continue; // the exemplar's span is not resident
            };
            sink.push(Relation::new(
                RelationType::ExemplarAttachment,
                SignalRef::new(SignalKind::MetricPoints, *point_entity),
                SignalRef::new(SignalKind::Spans, span_entity),
                vec![
                    EvidenceFact::new(
                        "exemplar.trace_id".to_owned(),
                        Value::Bytes(trace.as_bytes().to_vec()),
                    ),
                    EvidenceFact::new(
                        "exemplar.span_id".to_owned(),
                        Value::Bytes(span_id.as_bytes().to_vec()),
                    ),
                ],
                version.clone(),
                None,
            ));
        }
    }
}

/// The resident span whose natural identity is `(trace, span_id)`, in
/// scan order.
fn resident_span_named(
    spans: &[(EntityId, Arc<Span>)],
    trace: TraceId,
    span_id: SpanId,
) -> Option<EntityId> {
    spans
        .iter()
        .find(|(_, candidate)| {
            candidate
                .natural_identity()
                .is_some_and(|(candidate_trace, candidate_span)| {
                    candidate_trace == trace && candidate_span == span_id
                })
        })
        .map(|(entity, _)| *entity)
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
/// the work allowance, and returns them with their streams. The window
/// filter is the temporal strategy's own, applied while generating — the
/// other strategies (resource context, exemplar attachment) see every
/// resident point.
fn scan_points(
    store: &dyn TelemetryStore,
    bounds: &CorrelationBounds,
    spent: &mut u64,
    stopped_at: &mut Option<StoppedAt>,
) -> Vec<(EntityId, Arc<MetricPoint>, Arc<StreamIdentity>)> {
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
            resident.push((
                item.key.entity(),
                Arc::clone(&item.record.point),
                Arc::clone(&item.record.stream),
            ));
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
