//! Behavioral tests for the correlation engine's committed strategies:
//! `SpanIdentity`, `TraceIdentity` (with suppression and completeness
//! accounting) and `TemporalCoActivity`, plus the run's bounds, degradation
//! and residency re-check.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;

use runtime_trail_correlation::bounds::{
    CorrelationBounds, CorrelationOutcome, StoppedAt, Strategy,
};
use runtime_trail_correlation::engine;
use runtime_trail_correlation::relations::{Relation, RelationType, SignalKind, SignalRef, Window};
use runtime_trail_storage::{
    AdmissionKey, KeepOutcome, PointView, ScanItem, ScanPage, StoreStats, TelemetryStore,
};
use runtime_trail_telemetry_model::{
    AdmissionTime, Admitted, AssignedId, Attributes, EmitterDroppedCounts, EntityId,
    InstrumentationScope, LogRecord, MetricNumber, MetricPoint, NumberPoint, Resource, Span,
    SpanId, SpanKind, SpanStatus, SpanStatusCode, StreamIdentity, StreamKind, TraceContext,
    TraceFlags, TraceId, TraceState, Value,
};

const TRACE: u8 = 1;

fn at(nano: u64) -> AdmissionTime {
    AdmissionTime::from_unix_nano(nano)
}

fn span_entity(trace: u8, span_byte: u8) -> EntityId {
    EntityId::Span {
        trace_id: TraceId::from_bytes([trace; 16]),
        span_id: span_id_of(span_byte),
    }
}

fn span_id_of(span_byte: u8) -> SpanId {
    SpanId::from_bytes([span_byte; 8])
}

fn trace_id_of(trace: u8) -> TraceId {
    TraceId::from_bytes([trace; 16])
}

fn assigned(serial: u64) -> EntityId {
    EntityId::Assigned(AssignedId::from_serial(
        NonZeroU64::new(serial).expect("fixture serials start at 1"),
    ))
}

fn fixture_resource() -> Resource {
    Resource {
        attributes: Attributes::default(),
        schema_url: None,
        dropped_attributes_count: 0,
    }
}

fn fixture_scope() -> InstrumentationScope {
    InstrumentationScope {
        name: String::new(),
        version: None,
        attributes: Attributes::default(),
        schema_url: None,
        dropped_attributes_count: 0,
    }
}

fn fixture_span(trace: u8, span_byte: u8, name: &str, start: u64) -> Span {
    Span {
        context: TraceContext {
            trace_id: trace_id_of(trace),
            span_id: span_id_of(span_byte),
            flags: TraceFlags::new(1),
            tracestate: TraceState::default(),
        },
        parent_span_id: None,
        name: name.to_owned(),
        kind: SpanKind::Server,
        start_time_unix_nano: start,
        end_time_unix_nano: Some(start + 10),
        resource: Arc::new(fixture_resource()),
        scope: Arc::new(fixture_scope()),
        attributes: Attributes::default(),
        emitter_dropped: EmitterDroppedCounts::default(),
        events: Vec::new(),
        links: Vec::new(),
        status: SpanStatus {
            code: SpanStatusCode::Unset,
            message: String::new(),
        },
    }
}

fn fixture_log(trace: Option<u8>, span_byte: Option<u8>, name: &str) -> LogRecord {
    LogRecord {
        timestamp_unix_nano: Some(1),
        observed_timestamp_unix_nano: Some(2),
        severity_number: None,
        severity_text: None,
        body: Some(Value::String(name.to_owned())),
        resource: Arc::new(fixture_resource()),
        scope: Arc::new(fixture_scope()),
        attributes: Attributes::default(),
        dropped_attribute_count: 0,
        trace_id: trace.map(trace_id_of),
        span_id: span_byte.map(span_id_of),
        trace_flags: None,
        event_name: None,
    }
}

fn fixture_point(time: u64) -> MetricPoint {
    MetricPoint::Number(NumberPoint::measurement(
        time,
        MetricNumber::int(1),
        Attributes::default(),
        Vec::new(),
    ))
}

fn fixture_stream(name: &str) -> StreamIdentity {
    StreamIdentity {
        resource: fixture_resource(),
        scope: fixture_scope(),
        name: name.to_owned(),
        description: None,
        unit: None,
        metadata: Attributes::default(),
        kind: StreamKind::Gauge,
        temporality: None,
    }
}

// ---------------------------------------------------------------------------
// The fixture store: a contract-conforming stub over residency-order maps,
// holding the same Arc payloads across every call.
// ---------------------------------------------------------------------------

struct Shelf<R> {
    by_key: BTreeMap<AdmissionKey, Arc<R>>,
    by_entity: HashMap<EntityId, AdmissionKey>,
}

impl<R> Shelf<R> {
    fn empty() -> Self {
        Self {
            by_key: BTreeMap::new(),
            by_entity: HashMap::new(),
        }
    }

    fn insert(&mut self, admitted: Admitted<Arc<R>>) {
        let key = AdmissionKey::new(admitted.admitted_at, admitted.entity);
        self.by_entity.insert(admitted.entity, key);
        self.by_key.insert(key, admitted.record);
    }

    fn get(&self, entity: EntityId) -> Option<Arc<R>> {
        let key = self.by_entity.get(&entity)?;
        self.by_key.get(key).cloned()
    }

    fn len(&self) -> usize {
        self.by_key.len()
    }

    fn scan(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<R>> {
        if limit == 0 {
            return ScanPage {
                items: Vec::new(),
                cursor: None,
            };
        }
        let records: Box<dyn Iterator<Item = (AdmissionKey, &Arc<R>)> + '_> = match after {
            Some(after) => Box::new(
                self.by_key
                    .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                    .map(|(key, record)| (*key, record)),
            ),
            None => Box::new(self.by_key.iter().map(|(key, record)| (*key, record))),
        };
        let mut items = Vec::new();
        let mut cursor = None;
        let mut last_in_page = None;
        for (key, record) in records {
            if items.len() == limit {
                cursor = last_in_page;
                break;
            }
            items.push(ScanItem {
                key,
                record: Arc::clone(record),
            });
            last_in_page = Some(key);
        }
        ScanPage { items, cursor }
    }
}

struct StoredPoint {
    point: Arc<MetricPoint>,
    stream: Arc<StreamIdentity>,
}

struct FixtureStore {
    spans: Shelf<Span>,
    logs: Shelf<LogRecord>,
    points: Shelf<StoredPoint>,
}

impl FixtureStore {
    fn empty() -> Self {
        Self {
            spans: Shelf::empty(),
            logs: Shelf::empty(),
            points: Shelf::empty(),
        }
    }

    fn admit_span(&mut self, nano: u64, entity: EntityId, span: Span) {
        let _ = self.keep_span(Admitted {
            entity,
            admitted_at: at(nano),
            record: Arc::new(span),
        });
    }

    fn admit_log(&mut self, nano: u64, entity: EntityId, log: LogRecord) {
        let _ = self.keep_log_record(Admitted {
            entity,
            admitted_at: at(nano),
            record: Arc::new(log),
        });
    }

    fn admit_point(&mut self, nano: u64, entity: EntityId, point: MetricPoint) {
        let _ = self.keep_metric_point(
            Admitted {
                entity,
                admitted_at: at(nano),
                record: Arc::new(point),
            },
            Arc::new(fixture_stream("requests")),
        );
    }
}

impl TelemetryStore for FixtureStore {
    fn keep_span(&mut self, admitted: Admitted<Arc<Span>>) -> KeepOutcome {
        if self.spans.get(admitted.entity).is_some() {
            return KeepOutcome::Duplicate;
        }
        self.spans.insert(admitted);
        KeepOutcome::Kept { evicted: 0 }
    }

    fn keep_log_record(&mut self, admitted: Admitted<Arc<LogRecord>>) -> KeepOutcome {
        if self.logs.get(admitted.entity).is_some() {
            return KeepOutcome::Duplicate;
        }
        self.logs.insert(admitted);
        KeepOutcome::Kept { evicted: 0 }
    }

    fn keep_metric_point(
        &mut self,
        admitted: Admitted<Arc<MetricPoint>>,
        stream: Arc<StreamIdentity>,
    ) -> KeepOutcome {
        if self.points.get(admitted.entity).is_some() {
            return KeepOutcome::Duplicate;
        }
        self.points.insert(Admitted {
            entity: admitted.entity,
            admitted_at: admitted.admitted_at,
            record: Arc::new(StoredPoint {
                point: admitted.record,
                stream,
            }),
        });
        KeepOutcome::Kept { evicted: 0 }
    }

    fn span(&self, entity: EntityId) -> Option<Arc<Span>> {
        self.spans.get(entity)
    }

    fn log_record(&self, entity: EntityId) -> Option<Arc<LogRecord>> {
        self.logs.get(entity)
    }

    fn metric_point(&self, entity: EntityId) -> Option<PointView> {
        let stored = self.points.get(entity)?;
        Some(PointView {
            point: Arc::clone(&stored.point),
            stream: Arc::clone(&stored.stream),
        })
    }

    fn scan_spans(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<Span>> {
        self.spans.scan(after, limit)
    }

    fn scan_log_records(
        &self,
        after: Option<AdmissionKey>,
        limit: usize,
    ) -> ScanPage<Arc<LogRecord>> {
        self.logs.scan(after, limit)
    }

    fn scan_metric_points(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<PointView> {
        let page = self.points.scan(after, limit);
        let items = page
            .items
            .into_iter()
            .map(|item| ScanItem {
                key: item.key,
                record: PointView {
                    point: Arc::clone(&item.record.point),
                    stream: Arc::clone(&item.record.stream),
                },
            })
            .collect();
        ScanPage {
            items,
            cursor: page.cursor,
        }
    }

    fn enforce_retention(&mut self, _now: AdmissionTime) -> u64 {
        0
    }

    fn observe_admission_anomalies(&mut self, _total: u64) {}

    fn stats(&self) -> StoreStats {
        StoreStats {
            resident_records: u64::try_from(self.spans.len() + self.logs.len() + self.points.len())
                .unwrap_or(u64::MAX),
            resident_spans: u64::try_from(self.spans.len()).unwrap_or(u64::MAX),
            resident_log_records: u64::try_from(self.logs.len()).unwrap_or(u64::MAX),
            resident_metric_points: u64::try_from(self.points.len()).unwrap_or(u64::MAX),
            ..StoreStats::default()
        }
    }

    fn mode_name(&self) -> &'static str {
        "fixture"
    }
}

/// A store that has evicted a named set of entities: their payloads are
/// gone from the per-entity lookups, though the scans still yield them —
/// the eviction happened mid-run, between the relation being formed and
/// the resident-endpoint re-check.
struct EvictingStore {
    inner: FixtureStore,
    evicted: HashSet<EntityId>,
}

impl TelemetryStore for EvictingStore {
    fn keep_span(&mut self, admitted: Admitted<Arc<Span>>) -> KeepOutcome {
        self.inner.keep_span(admitted)
    }

    fn keep_log_record(&mut self, admitted: Admitted<Arc<LogRecord>>) -> KeepOutcome {
        self.inner.keep_log_record(admitted)
    }

    fn keep_metric_point(
        &mut self,
        admitted: Admitted<Arc<MetricPoint>>,
        stream: Arc<StreamIdentity>,
    ) -> KeepOutcome {
        self.inner.keep_metric_point(admitted, stream)
    }

    fn span(&self, entity: EntityId) -> Option<Arc<Span>> {
        if self.evicted.contains(&entity) {
            return None;
        }
        self.inner.span(entity)
    }

    fn log_record(&self, entity: EntityId) -> Option<Arc<LogRecord>> {
        if self.evicted.contains(&entity) {
            return None;
        }
        self.inner.log_record(entity)
    }

    fn metric_point(&self, entity: EntityId) -> Option<PointView> {
        if self.evicted.contains(&entity) {
            return None;
        }
        self.inner.metric_point(entity)
    }

    fn scan_spans(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<Span>> {
        self.inner.scan_spans(after, limit)
    }

    fn scan_log_records(
        &self,
        after: Option<AdmissionKey>,
        limit: usize,
    ) -> ScanPage<Arc<LogRecord>> {
        self.inner.scan_log_records(after, limit)
    }

    fn scan_metric_points(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<PointView> {
        self.inner.scan_metric_points(after, limit)
    }

    fn enforce_retention(&mut self, now: AdmissionTime) -> u64 {
        self.inner.enforce_retention(now)
    }

    fn observe_admission_anomalies(&mut self, total: u64) {
        self.inner.observe_admission_anomalies(total);
    }

    fn stats(&self) -> StoreStats {
        self.inner.stats()
    }

    fn mode_name(&self) -> &'static str {
        "fixture-evicting"
    }
}

// ---------------------------------------------------------------------------
// The fixture: one trace with three spans, one exact log, one trace-only
// log, and one log of a trace with no resident spans.
// ---------------------------------------------------------------------------

fn identity_store() -> FixtureStore {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, "root", 10),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        fixture_span(TRACE, 2, "child", 20),
    );
    store.admit_span(
        300,
        span_entity(TRACE, 3),
        fixture_span(TRACE, 3, "grandchild", 30),
    );
    store.admit_log(400, assigned(1), fixture_log(Some(TRACE), Some(1), "exact"));
    store.admit_log(
        500,
        assigned(2),
        fixture_log(Some(TRACE), None, "trace-only"),
    );
    store.admit_log(600, assigned(3), fixture_log(Some(2), None, "other-trace"));
    store
}

fn default_bounds() -> CorrelationBounds {
    CorrelationBounds {
        strategies: vec![
            Strategy::SpanIdentity,
            Strategy::TraceIdentity,
            Strategy::TemporalCoActivity,
        ],
        window: None,
        max_relations: 1_000,
        max_hops: 2,
        max_scan: 1_000,
        subject_trace: Some(trace_id_of(TRACE)),
    }
}

fn run(store: &dyn TelemetryStore, bounds: &CorrelationBounds) -> CorrelationOutcome {
    engine::correlate(store, bounds)
}

// ---------------------------------------------------------------------------
// SpanIdentity
// ---------------------------------------------------------------------------

#[test]
fn span_identity_attaches_a_log_to_its_exact_span() {
    let store = identity_store();
    let outcome = run(&store, &default_bounds());

    let span_identity: Vec<&Relation<SignalRef>> = outcome
        .relations
        .iter()
        .filter(|relation| relation.relation_type == RelationType::SpanIdentity)
        .collect();
    assert_eq!(span_identity.len(), 1);
    let relation = span_identity[0];
    assert_eq!(relation.from.kind, SignalKind::LogRecords);
    assert_eq!(relation.from.entity, assigned(1));
    assert_eq!(relation.to.kind, SignalKind::Spans);
    assert_eq!(relation.to.entity, span_entity(TRACE, 1));
    assert_eq!(relation.window, None);
    assert_eq!(relation.strategy.name, "span_identity");
    assert_eq!(relation.strategy.version, "1.0.0");
    // The evidence cites the trace context verbatim.
    let fields: Vec<(&str, &Value)> = relation
        .facts
        .iter()
        .map(|fact| (fact.field.as_str(), &fact.value))
        .collect();
    assert!(fields.contains(&("trace_id", &Value::Bytes(vec![TRACE; 16]))));
    assert!(fields.contains(&("span_id", &Value::Bytes(vec![1; 8]))));
}

#[test]
fn an_exact_attachment_suppresses_the_trace_identity_siblings_complete() {
    let store = identity_store();
    let outcome = run(&store, &default_bounds());

    // The exact log: its trace-identity relations (two siblings) are
    // suppressed in the exact relation's favor, accounted complete.
    assert_eq!(outcome.truth.suppressions.len(), 1);
    let suppression = &outcome.truth.suppressions[0];
    assert_eq!(suppression.log, assigned(1));
    assert_eq!(suppression.span, span_entity(TRACE, 1));
    assert_eq!(suppression.suppressed, 2);

    // The trace-only log: three trace-identity relations, none suppressed.
    let trace_identity: Vec<&Relation<SignalRef>> = outcome
        .relations
        .iter()
        .filter(|relation| relation.relation_type == RelationType::TraceIdentity)
        .collect();
    assert_eq!(trace_identity.len(), 3);
    assert!(
        trace_identity
            .iter()
            .all(|relation| relation.from.entity == assigned(2))
    );
    let spans: HashSet<EntityId> = trace_identity
        .iter()
        .map(|relation| relation.to.entity)
        .collect();
    assert_eq!(
        spans,
        HashSet::from([
            span_entity(TRACE, 1),
            span_entity(TRACE, 2),
            span_entity(TRACE, 3)
        ])
    );
    assert!(trace_identity.iter().all(|relation| {
        relation
            .facts
            .iter()
            .any(|fact| fact.field == "trace_id" && fact.value == Value::Bytes(vec![TRACE; 16]))
    }));

    // The other-trace log is outside the subject: neither related nor
    // accounted — the subject bounds the run.
    assert!(
        !outcome
            .relations
            .iter()
            .any(|relation| relation.from.entity == assigned(3))
    );
    assert!(
        !outcome
            .truth
            .absent_traces
            .iter()
            .any(|absent| absent.log == assigned(3))
    );
}

#[test]
fn a_trace_named_only_by_logs_is_accounted_absent_completeness() {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, "root", 10),
    );
    store.admit_log(400, assigned(1), fixture_log(Some(2), None, "orphan"));
    store.admit_log(
        500,
        assigned(2),
        fixture_log(Some(TRACE), None, "trace-own"),
    );

    let mut bounds = default_bounds();
    bounds.subject_trace = Some(trace_id_of(2));
    let outcome = run(&store, &bounds);

    assert!(outcome.relations.is_empty());
    assert_eq!(outcome.truth.absent_traces.len(), 1);
    assert_eq!(outcome.truth.absent_traces[0].log, assigned(1));
    assert!(outcome.truth.suppressions.is_empty());

    // The trace's own log still relates to its resident member.
    bounds.subject_trace = Some(trace_id_of(TRACE));
    let outcome = run(&store, &bounds);
    assert_eq!(outcome.relations.len(), 1);
    assert_eq!(
        outcome.relations[0].relation_type,
        RelationType::TraceIdentity
    );
    assert!(outcome.truth.absent_traces.is_empty());
}

// ---------------------------------------------------------------------------
// TemporalCoActivity
// ---------------------------------------------------------------------------

fn temporal_store() -> FixtureStore {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, "root", 10),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        fixture_span(TRACE, 2, "child", 20),
    );
    store.admit_span(
        300,
        span_entity(TRACE, 3),
        fixture_span(TRACE, 3, "grandchild", 30),
    );
    store.admit_point(600, assigned(1), fixture_point(15));
    store.admit_point(700, assigned(2), fixture_point(25));
    store.admit_point(800, assigned(3), fixture_point(45));
    store
}

fn temporal_bounds() -> CorrelationBounds {
    CorrelationBounds {
        strategies: vec![Strategy::TemporalCoActivity],
        window: Some(Window::new(10, 40)),
        max_relations: 1_000,
        max_hops: 2,
        max_scan: 1_000,
        subject_trace: Some(trace_id_of(TRACE)),
    }
}

#[test]
fn temporal_co_activity_pairs_the_subject_spans_and_in_window_points() {
    let store = temporal_store();
    let outcome = run(&store, &temporal_bounds());

    // Three spans intersecting [10, 40) times two in-window points: the
    // point outside the window (45) never grounds a relation.
    assert_eq!(outcome.relations.len(), 6);
    for relation in &outcome.relations {
        assert_eq!(relation.relation_type, RelationType::TemporalCoActivity);
        assert_eq!(relation.from.kind, SignalKind::Spans);
        assert_eq!(relation.to.kind, SignalKind::MetricPoints);
        assert_eq!(relation.window, Some(Window::new(10, 40)));
        assert_eq!(relation.strategy.name, "temporal_co_activity");
        // The cited times make overlap vs. proximity derivable.
        let fields: Vec<&str> = relation
            .facts
            .iter()
            .map(|fact| fact.field.as_str())
            .collect();
        assert!(fields.contains(&"span.start_time_unix_nano"));
        assert!(fields.contains(&"point.time_unix_nano"));
    }
    let point_entities: HashSet<EntityId> = outcome
        .relations
        .iter()
        .map(|relation| relation.to.entity)
        .collect();
    assert_eq!(point_entities, HashSet::from([assigned(1), assigned(2)]));
}

#[test]
fn temporal_co_activity_counts_proximity_inside_the_window_and_never_outside() {
    let mut store = FixtureStore::empty();
    // A span [11, 20) and points at 9 (outside), 21 (proximate, inside)
    // and 41 (outside by a hair).
    store.admit_span(
        100,
        span_entity(TRACE, 2),
        fixture_span(TRACE, 2, "tail", 11),
    );
    store.admit_point(600, assigned(1), fixture_point(9));
    store.admit_point(700, assigned(2), fixture_point(21));
    store.admit_point(800, assigned(3), fixture_point(41));

    let outcome = run(&store, &temporal_bounds());

    assert_eq!(outcome.relations.len(), 1);
    let relation = &outcome.relations[0];
    assert_eq!(relation.from.entity, span_entity(TRACE, 2));
    assert_eq!(relation.to.entity, assigned(2));
    let times: Vec<&Value> = relation
        .facts
        .iter()
        .filter(|fact| fact.field == "point.time_unix_nano")
        .map(|fact| &fact.value)
        .collect();
    assert_eq!(times, vec![&Value::Int(21)]);
}

#[test]
fn temporal_co_activity_is_silent_without_a_window() {
    let store = temporal_store();
    let mut bounds = temporal_bounds();
    bounds.window = None;
    let outcome = run(&store, &bounds);
    assert!(outcome.relations.is_empty());
}

// ---------------------------------------------------------------------------
// Determinism and bounds
// ---------------------------------------------------------------------------

#[test]
fn runs_are_deterministic_regardless_of_strategy_order() {
    let store = temporal_store();

    let mut reversed = temporal_bounds();
    reversed.strategies = vec![
        Strategy::TemporalCoActivity,
        Strategy::TraceIdentity,
        Strategy::SpanIdentity,
    ];
    let identity = temporal_store();
    let _ = identity;

    let first = run(&store, &temporal_bounds());
    let second = run(&store, &reversed);
    let third = run(&store, &temporal_bounds());

    assert_eq!(first.relations, second.relations);
    assert_eq!(first.relations, third.relations);
    assert_eq!(first.truth.suppressions, second.truth.suppressions);
}

#[test]
fn every_relation_cites_resident_endpoints_and_no_self_loop() {
    let store = identity_store();
    let outcome = run(&store, &default_bounds());

    for relation in &outcome.relations {
        assert_ne!(relation.from, relation.to);
        assert!(!relation.facts.is_empty());
        let from_present = match relation.from.kind {
            SignalKind::Spans => store.span(relation.from.entity).is_some(),
            SignalKind::LogRecords => store.log_record(relation.from.entity).is_some(),
            SignalKind::MetricPoints => store.metric_point(relation.from.entity).is_some(),
        };
        let to_present = match relation.to.kind {
            SignalKind::Spans => store.span(relation.to.entity).is_some(),
            SignalKind::LogRecords => store.log_record(relation.to.entity).is_some(),
            SignalKind::MetricPoints => store.metric_point(relation.to.entity).is_some(),
        };
        assert!(from_present, "from endpoint must be resident");
        assert!(to_present, "to endpoint must be resident");
        // Only committed strategies produce relations: none of the pinned
        // contract types, and nothing inferred.
        assert!(matches!(
            relation.relation_type,
            RelationType::SpanIdentity
                | RelationType::TraceIdentity
                | RelationType::TemporalCoActivity
        ));
    }
}

#[test]
fn max_relations_caps_the_answer_as_a_stable_prefix_and_reports_the_count() {
    let store = temporal_store();
    let mut bounds = temporal_bounds();
    bounds.max_relations = 3;
    let outcome = run(&store, &bounds);

    assert_eq!(outcome.relations.len(), 3);
    assert_eq!(
        outcome.truth.stopped_at,
        Some(StoppedAt::MaxRelations { count: 3 })
    );
    // A capped answer is the full answer's stable prefix.
    let full = run(&store, &temporal_bounds());
    assert_eq!(
        outcome.relations,
        full.relations[..3],
        "the cap never picks a different subset"
    );
}

#[test]
fn suppression_accounting_never_waits_on_the_relation_budget() {
    let store = identity_store();
    let mut bounds = default_bounds();
    bounds.max_relations = 0;
    let outcome = run(&store, &bounds);

    assert!(outcome.relations.is_empty());
    assert_eq!(
        outcome.truth.stopped_at,
        Some(StoppedAt::MaxRelations { count: 0 })
    );
    // The suppression was still accounted completely.
    assert_eq!(outcome.truth.suppressions.len(), 1);
    assert_eq!(outcome.truth.suppressions[0].suppressed, 2);
}

#[test]
fn zero_hop_bounds_yield_nothing_and_name_the_depth() {
    let store = identity_store();
    let mut bounds = default_bounds();
    bounds.max_hops = 0;
    let outcome = run(&store, &bounds);

    assert!(outcome.relations.is_empty());
    assert_eq!(
        outcome.truth.stopped_at,
        Some(StoppedAt::MaxDepth { depth: 0 })
    );
    assert!(outcome.truth.strategy_versions.is_empty());
    assert_eq!(outcome.truth.scan_spent, 0);
    assert!(outcome.truth.suppressions.is_empty());
    assert!(outcome.truth.absent_traces.is_empty());
}

#[test]
fn scan_allowance_exhaustion_degrades_truthfully() {
    let store = identity_store();
    let mut bounds = default_bounds();
    // One span and one log fit in two examinations; the points never do.
    bounds.max_scan = 2;
    let outcome = run(&store, &bounds);

    assert_eq!(outcome.truth.scan_spent, 2);
    assert_eq!(
        outcome.truth.stopped_at,
        Some(StoppedAt::ScanExhausted { examined: 2 })
    );
    // The trace-only log was examined and related before the allowance ran
    // out; nothing past the scan was invented.
    assert!(
        outcome
            .relations
            .iter()
            .all(|relation| relation.relation_type == RelationType::TraceIdentity)
    );
    assert!(outcome.truth.absent_traces.is_empty());
}

#[test]
fn evicted_endpoints_drop_their_relations_and_are_counted() {
    let mut store = identity_store();
    store.admit_log(700, assigned(4), fixture_log(Some(TRACE), None, "another"));
    let evicted = span_entity(TRACE, 2);
    let store = EvictingStore {
        inner: store,
        evicted: HashSet::from([evicted]),
    };

    let outcome = run(&store, &default_bounds());

    // The two trace-only logs would have related to span 2; the re-check
    // dropped those relations and counted the drop.
    assert_eq!(outcome.truth.shrunken, 2);
    assert!(
        !outcome
            .relations
            .iter()
            .any(|relation| relation.to.entity == evicted)
    );
    // The dropped span's relations are the only ones missing: span 1 and
    // span 3 still hold their relations.
    let spans: HashSet<EntityId> = outcome
        .relations
        .iter()
        .filter(|relation| relation.relation_type == RelationType::TraceIdentity)
        .map(|relation| relation.to.entity)
        .collect();
    assert_eq!(
        spans,
        HashSet::from([span_entity(TRACE, 1), span_entity(TRACE, 3)])
    );
}
