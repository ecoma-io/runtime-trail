//! Behavioral tests for the correlation engine's committed strategies:
//! `SpanIdentity`, `TraceIdentity` (with suppression and completeness
//! accounting), `ParentChild`, `ResourceContext`, `ExemplarAttachment`
//! and `TemporalCoActivity`, plus the run's bounds, degradation, the
//! residency re-check, and the machine-readable, versioned strategy set.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;

use runtime_trail_correlation::bounds::{
    COMMITTED_STRATEGIES, CorrelationBounds, CorrelationOutcome, STRATEGY_SET_VERSION, StoppedAt,
    Strategy, committed_versions,
};
use runtime_trail_correlation::engine;
use runtime_trail_correlation::relations::{
    Relation, RelationType, SignalKind, SignalRef, Tier, Window,
};
use runtime_trail_storage::{
    AdmissionKey, KeepOutcome, PointView, ScanItem, ScanPage, StoreStats, TelemetryStore,
};
use runtime_trail_telemetry_model::{
    AdmissionTime, Admitted, AssignedId, Attributes, EmitterDroppedCounts, EntityId, Exemplar,
    InstrumentationScope, KeyValueList, LogRecord, MetricNumber, MetricPoint, NumberPoint,
    Resource, Span, SpanId, SpanKind, SpanStatus, SpanStatusCode, StreamIdentity, StreamKind,
    TraceContext, TraceFlags, TraceId, TraceState, Value,
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

fn stream_on(name: &str, resource: Resource) -> StreamIdentity {
    let mut stream = fixture_stream(name);
    stream.resource = resource;
    stream
}

fn resource_with(pairs: &[(&str, i64)]) -> Resource {
    Resource {
        attributes: Attributes::from_pairs(
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_owned(), Value::Int(*value)))
                .collect(),
        )
        .expect("fixture attribute keys are unique"),
        schema_url: None,
        dropped_attributes_count: 0,
    }
}

fn fixture_log_with(
    trace: Option<u8>,
    span_byte: Option<u8>,
    name: &str,
    resource: Resource,
) -> LogRecord {
    let mut log = fixture_log(trace, span_byte, name);
    log.resource = Arc::new(resource);
    log
}

/// A span whose parent field and resource the fixture controls, so the
/// taxonomy strategies see realistic shapes.
fn taxonomy_span(
    trace: u8,
    span_byte: u8,
    name: &str,
    start: u64,
    parent: Option<u8>,
    resource: Resource,
) -> Span {
    let mut span = fixture_span(trace, span_byte, name, start);
    span.parent_span_id = parent.map(span_id_of);
    span.resource = Arc::new(resource);
    span
}

fn fixture_exemplar(trace: Option<u8>, span: Option<u8>) -> Exemplar {
    Exemplar {
        value: MetricNumber::int(3),
        time_unix_nano: 12,
        filtered_attributes: Attributes::default(),
        trace_id: trace.map(trace_id_of),
        span_id: span.map(span_id_of),
    }
}

fn fixture_point_with_exemplars(time: u64, exemplars: Vec<Exemplar>) -> MetricPoint {
    MetricPoint::Number(NumberPoint::measurement(
        time,
        MetricNumber::int(1),
        Attributes::default(),
        exemplars,
    ))
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

    fn admit_point_on(
        &mut self,
        nano: u64,
        entity: EntityId,
        point: MetricPoint,
        stream: StreamIdentity,
    ) {
        let _ = self.keep_metric_point(
            Admitted {
                entity,
                admitted_at: at(nano),
                record: Arc::new(point),
            },
            Arc::new(stream),
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
// The taxonomy fixture: the committed set's coverage store — a
// root/child/grandchild chain sharing one resource, an exact log, a
// trace-only log, an other-trace log, and two points inside the window,
// one of them carrying an exemplar that names the child span.
// ---------------------------------------------------------------------------

fn taxonomy_store() -> FixtureStore {
    let mut store = FixtureStore::empty();
    let shared = resource_with(&[("service.name", 1), ("deployment", 2)]);
    let solo = resource_with(&[("service.name", 3)]);
    let solo2 = resource_with(&[("service.name", 4)]);
    let point_a = resource_with(&[("service.name", 5)]);
    let point_b = resource_with(&[("service.name", 6)]);
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        taxonomy_span(TRACE, 1, "root", 10, None, shared.clone()),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        taxonomy_span(TRACE, 2, "child", 20, Some(1), shared.clone()),
    );
    store.admit_span(
        300,
        span_entity(TRACE, 3),
        taxonomy_span(TRACE, 3, "grandchild", 30, Some(2), shared.clone()),
    );
    store.admit_log(
        400,
        assigned(1),
        fixture_log_with(Some(TRACE), Some(1), "exact", shared),
    );
    store.admit_log(
        500,
        assigned(2),
        fixture_log_with(Some(TRACE), None, "trace-only", solo),
    );
    store.admit_log(
        600,
        assigned(3),
        fixture_log_with(Some(2), None, "other-trace", solo2),
    );
    store.admit_point_on(
        700,
        assigned(10),
        fixture_point_with_exemplars(15, vec![fixture_exemplar(Some(TRACE), Some(2))]),
        stream_on("requests", point_a),
    );
    store.admit_point_on(
        800,
        assigned(11),
        fixture_point_with_exemplars(25, Vec::new()),
        stream_on("requests", point_b),
    );
    store
}

fn taxonomy_bounds() -> CorrelationBounds {
    CorrelationBounds {
        strategies: vec![
            Strategy::SpanIdentity,
            Strategy::TraceIdentity,
            Strategy::ParentChild,
            Strategy::ResourceContext,
            Strategy::ExemplarAttachment,
            Strategy::TemporalCoActivity,
        ],
        window: Some(Window::new(10, 40)),
        max_relations: 1_000,
        max_hops: 2,
        max_scan: 1_000,
        subject_trace: Some(trace_id_of(TRACE)),
    }
}

/// One span chain plus the malformed shapes the generation guards refuse:
/// a root, a child, a grandchild, a span naming an absent parent, a span
/// carrying the emitter's absent-parent marker (the zero parent), and a
/// span naming itself as its parent.
fn parent_child_store() -> FixtureStore {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        taxonomy_span(
            TRACE,
            1,
            "root",
            10,
            None,
            resource_with(&[("service.name", 1)]),
        ),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        taxonomy_span(
            TRACE,
            2,
            "child",
            20,
            Some(1),
            resource_with(&[("service.name", 2)]),
        ),
    );
    store.admit_span(
        300,
        span_entity(TRACE, 3),
        taxonomy_span(
            TRACE,
            3,
            "grandchild",
            30,
            Some(2),
            resource_with(&[("service.name", 3)]),
        ),
    );
    store.admit_span(
        400,
        span_entity(TRACE, 4),
        taxonomy_span(
            TRACE,
            4,
            "absent-parent",
            40,
            Some(9),
            resource_with(&[("service.name", 4)]),
        ),
    );
    store.admit_span(
        500,
        span_entity(TRACE, 5),
        taxonomy_span(
            TRACE,
            5,
            "zero-parent",
            50,
            Some(0),
            resource_with(&[("service.name", 5)]),
        ),
    );
    store.admit_span(
        600,
        span_entity(TRACE, 6),
        taxonomy_span(
            TRACE,
            6,
            "self-parent",
            60,
            Some(6),
            resource_with(&[("service.name", 6)]),
        ),
    );
    store
}

fn parent_child_bounds() -> CorrelationBounds {
    CorrelationBounds {
        strategies: vec![Strategy::ParentChild],
        window: None,
        max_relations: 1_000,
        max_hops: 2,
        max_scan: 1_000,
        subject_trace: None,
    }
}

/// Records across five resources: a group of four (two spans, one log) on
/// `a`, a span-and-point pair on `b`, a pair sharing the empty identity on
/// `c`, and singletons that must ground nothing.
fn resource_context_store() -> FixtureStore {
    let mut store = FixtureStore::empty();
    let a = resource_with(&[("service.name", 1), ("deployment", 2)]);
    let b = resource_with(&[("service.name", 3)]);
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        taxonomy_span(TRACE, 1, "a1", 10, None, a.clone()),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        taxonomy_span(TRACE, 2, "a2", 20, None, a.clone()),
    );
    store.admit_log(
        300,
        assigned(1),
        fixture_log_with(Some(TRACE), None, "a-log", a),
    );
    store.admit_span(
        400,
        span_entity(TRACE, 3),
        taxonomy_span(TRACE, 3, "b1", 30, None, b.clone()),
    );
    store.admit_point_on(
        500,
        assigned(10),
        fixture_point(35),
        stream_on("requests", b),
    );
    store.admit_span(
        600,
        span_entity(TRACE, 4),
        taxonomy_span(TRACE, 4, "c1", 40, None, fixture_resource()),
    );
    store.admit_log(
        700,
        assigned(2),
        fixture_log_with(Some(TRACE), None, "c-log", fixture_resource()),
    );
    store.admit_span(
        800,
        span_entity(TRACE, 5),
        taxonomy_span(
            TRACE,
            5,
            "solo",
            50,
            None,
            resource_with(&[("service.name", 9)]),
        ),
    );
    store
}

fn resource_context_bounds() -> CorrelationBounds {
    CorrelationBounds {
        strategies: vec![Strategy::ResourceContext],
        window: None,
        max_relations: 1_000,
        max_hops: 2,
        max_scan: 1_000,
        subject_trace: None,
    }
}

/// One resident target span and four points whose exemplars cover the
/// resolvable case and every refusal: a non-resident span, a trace id
/// without a span id, and a span id without a trace id.
fn exemplar_store() -> FixtureStore {
    let mut store = FixtureStore::empty();
    let r = resource_with(&[("service.name", 1)]);
    store.admit_span(
        100,
        span_entity(TRACE, 2),
        taxonomy_span(TRACE, 2, "target", 10, None, r.clone()),
    );
    store.admit_point_on(
        200,
        assigned(10),
        fixture_point_with_exemplars(15, vec![fixture_exemplar(Some(TRACE), Some(2))]),
        stream_on("m", r.clone()),
    );
    store.admit_point_on(
        300,
        assigned(11),
        fixture_point_with_exemplars(25, vec![fixture_exemplar(Some(9), Some(9))]),
        stream_on("m", r.clone()),
    );
    store.admit_point_on(
        400,
        assigned(12),
        fixture_point_with_exemplars(35, vec![fixture_exemplar(Some(TRACE), None)]),
        stream_on("m", r.clone()),
    );
    store.admit_point_on(
        500,
        assigned(13),
        fixture_point_with_exemplars(45, vec![fixture_exemplar(None, Some(2))]),
        stream_on("m", r),
    );
    store
}

fn exemplar_bounds() -> CorrelationBounds {
    CorrelationBounds {
        strategies: vec![Strategy::ExemplarAttachment],
        window: None,
        max_relations: 1_000,
        max_hops: 2,
        max_scan: 1_000,
        subject_trace: None,
    }
}

/// The bounded-generation fixture, run under the three taxonomy-only
/// strategies: the parent/exemplar guards refuse their malformed shapes
/// while generating, and one intended resource pair survives.
fn bounded_generation_store() -> FixtureStore {
    let mut store = FixtureStore::empty();
    let shared = resource_with(&[("service.name", 1), ("deployment", 2)]);
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        taxonomy_span(TRACE, 1, "root", 10, None, shared.clone()),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        taxonomy_span(
            TRACE,
            2,
            "child",
            20,
            Some(1),
            resource_with(&[("service.name", 2)]),
        ),
    );
    store.admit_span(
        300,
        span_entity(TRACE, 3),
        taxonomy_span(
            TRACE,
            3,
            "grandchild",
            30,
            Some(2),
            resource_with(&[("service.name", 3)]),
        ),
    );
    store.admit_span(
        400,
        span_entity(TRACE, 4),
        taxonomy_span(
            TRACE,
            4,
            "absent-parent",
            40,
            Some(9),
            resource_with(&[("service.name", 4)]),
        ),
    );
    store.admit_span(
        500,
        span_entity(TRACE, 5),
        taxonomy_span(
            TRACE,
            5,
            "zero-parent",
            50,
            Some(0),
            resource_with(&[("service.name", 5)]),
        ),
    );
    store.admit_span(
        600,
        span_entity(TRACE, 6),
        taxonomy_span(
            TRACE,
            6,
            "self-parent",
            60,
            Some(6),
            resource_with(&[("service.name", 6)]),
        ),
    );
    store.admit_log(
        700,
        assigned(1),
        fixture_log_with(Some(TRACE), None, "shared", shared),
    );
    store.admit_point_on(
        800,
        assigned(10),
        fixture_point_with_exemplars(15, vec![fixture_exemplar(Some(TRACE), Some(2))]),
        stream_on("m", resource_with(&[("service.name", 7)])),
    );
    store.admit_point_on(
        900,
        assigned(11),
        fixture_point_with_exemplars(25, vec![fixture_exemplar(Some(9), Some(9))]),
        stream_on("m", resource_with(&[("service.name", 8)])),
    );
    store.admit_point_on(
        1000,
        assigned(12),
        fixture_point_with_exemplars(35, vec![fixture_exemplar(Some(TRACE), None)]),
        stream_on("m", resource_with(&[("service.name", 9)])),
    );
    store.admit_point_on(
        1100,
        assigned(13),
        fixture_point_with_exemplars(45, vec![fixture_exemplar(None, Some(2))]),
        stream_on("m", resource_with(&[("service.name", 10)])),
    );
    store
}

fn bounded_generation_bounds() -> CorrelationBounds {
    CorrelationBounds {
        strategies: vec![
            Strategy::ParentChild,
            Strategy::ResourceContext,
            Strategy::ExemplarAttachment,
        ],
        window: None,
        max_relations: 1_000,
        max_hops: 2,
        max_scan: 1_000,
        subject_trace: None,
    }
}

fn span_id_of_entity(entity: EntityId) -> SpanId {
    match entity {
        EntityId::Span { span_id, .. } => span_id,
        other => unreachable!("parent/child endpoints are spans, got {other:?}"),
    }
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
    let store = taxonomy_store();
    let outcome = run(&store, &taxonomy_bounds());

    for relation in &outcome.relations {
        assert_ne!(relation.from, relation.to);
        assert!(
            !relation.facts.is_empty(),
            "every relation carries its evidence"
        );
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
        // Only committed strategies produce relations: the six committed
        // taxonomy types, never `Inferred`.
        assert!(matches!(
            relation.relation_type,
            RelationType::SpanIdentity
                | RelationType::TraceIdentity
                | RelationType::ParentChild
                | RelationType::ResourceContext
                | RelationType::ExemplarAttachment
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

// ---------------------------------------------------------------------------
// Taxonomy completion (issue #37)
// ---------------------------------------------------------------------------

#[test]
fn parent_child_relates_a_span_to_the_span_its_parent_span_id_names() {
    let store = parent_child_store();
    let outcome = run(&store, &parent_child_bounds());

    let parent_child: Vec<&Relation<SignalRef>> = outcome
        .relations
        .iter()
        .filter(|relation| relation.relation_type == RelationType::ParentChild)
        .collect();
    // child → root and grandchild → child: exactly the direct relations,
    // never a second-order grandchild → root.
    assert_eq!(parent_child.len(), 2);
    let pairs: HashSet<(EntityId, EntityId)> = parent_child
        .iter()
        .map(|relation| (relation.from.entity, relation.to.entity))
        .collect();
    assert_eq!(
        pairs,
        HashSet::from([
            (span_entity(TRACE, 2), span_entity(TRACE, 1)),
            (span_entity(TRACE, 3), span_entity(TRACE, 2)),
        ])
    );
    for relation in &parent_child {
        assert_eq!(relation.from.kind, SignalKind::Spans);
        assert_eq!(relation.to.kind, SignalKind::Spans);
        assert_eq!(relation.window, None);
        assert_eq!(relation.strategy.name, "parent_child");
        assert_eq!(relation.strategy.version, "1.0.0");
        assert_eq!(relation.tier(), Tier::Structural);
        // The evidence is the parent id cited verbatim — it names the
        // relation's own target.
        assert_eq!(relation.facts.len(), 1);
        assert_eq!(relation.facts[0].field, "parent_span_id");
        assert_eq!(
            relation.facts[0].value,
            Value::Bytes(span_id_of_entity(relation.to.entity).as_bytes().to_vec())
        );
    }
    // The generation guards held while generating: none of the malformed
    // spans grounded a relation (bounded, never generate-then-drop).
    let producers: HashSet<EntityId> = outcome
        .relations
        .iter()
        .map(|relation| relation.from.entity)
        .collect();
    assert!(!producers.contains(&span_entity(TRACE, 4)));
    assert!(!producers.contains(&span_entity(TRACE, 5)));
    assert!(!producers.contains(&span_entity(TRACE, 6)));
}

#[test]
fn resource_context_relates_every_pair_sharing_a_resource_evidenced_by_its_attributes() {
    let store = resource_context_store();
    let outcome = run(&store, &resource_context_bounds());

    let resource_context: Vec<&Relation<SignalRef>> = outcome
        .relations
        .iter()
        .filter(|relation| relation.relation_type == RelationType::ResourceContext)
        .collect();
    // Group `a` (two spans + one log): three pairs; group `b` (span +
    // point): one pair; group `c` (the empty identity): one pair; the
    // singleton grounds nothing. Five relations, none across resources.
    assert_eq!(resource_context.len(), 5);
    let pairs: HashSet<(EntityId, EntityId)> = resource_context
        .iter()
        .map(|relation| (relation.from.entity, relation.to.entity))
        .collect();
    assert_eq!(
        pairs,
        HashSet::from([
            (span_entity(TRACE, 1), span_entity(TRACE, 2)),
            (span_entity(TRACE, 1), assigned(1)),
            (span_entity(TRACE, 2), assigned(1)),
            (span_entity(TRACE, 3), assigned(10)),
            (span_entity(TRACE, 4), assigned(2)),
        ])
    );
    // The kinds mix across signals: the log and the point both attach.
    let to_kinds: HashSet<SignalKind> = resource_context
        .iter()
        .map(|relation| relation.to.kind)
        .collect();
    assert_eq!(
        to_kinds,
        HashSet::from([
            SignalKind::Spans,
            SignalKind::LogRecords,
            SignalKind::MetricPoints
        ])
    );
    for relation in &resource_context {
        assert_eq!(relation.window, None);
        assert_eq!(relation.strategy.name, "resource_context");
        assert_eq!(relation.strategy.version, "1.0.0");
        assert_eq!(relation.tier(), Tier::Structural);
    }
    // The evidence is the shared resource's own attributes, verbatim —
    // a map, so the facts come in the attributes' own (sorted) order.
    let facts_of = |from: EntityId, to: EntityId| -> HashMap<&str, &Value> {
        resource_context
            .iter()
            .find(|relation| relation.from.entity == from && relation.to.entity == to)
            .map(|relation| {
                relation
                    .facts
                    .iter()
                    .map(|fact| (fact.field.as_str(), &fact.value))
                    .collect()
            })
            .expect("the pair exists in the fixture")
    };
    let a_facts = facts_of(span_entity(TRACE, 1), span_entity(TRACE, 2));
    assert_eq!(a_facts.len(), 2);
    assert_eq!(a_facts.get("service.name"), Some(&&Value::Int(1)));
    assert_eq!(a_facts.get("deployment"), Some(&&Value::Int(2)));
    let b_facts = facts_of(span_entity(TRACE, 3), assigned(10));
    assert_eq!(b_facts.get("service.name"), Some(&&Value::Int(3)));
    // The empty identity is evidenced by itself: one fact naming an empty
    // attribute list, so no relation is ever left without its evidence.
    let c_facts = facts_of(span_entity(TRACE, 4), assigned(2));
    assert_eq!(c_facts.len(), 1);
    assert_eq!(
        c_facts.get("resource"),
        Some(&&Value::KvList(
            KeyValueList::new(Vec::new()).expect("an empty list cannot duplicate keys")
        ))
    );
    match c_facts.get("resource").copied() {
        Some(Value::KvList(list)) => assert!(list.is_empty()),
        other => panic!("expected an empty attribute list, got {other:?}"),
    }
    // A singleton resource shares nothing: it grounds no relation.
    assert!(!resource_context.iter().any(|relation| {
        relation.from.entity == span_entity(TRACE, 5) || relation.to.entity == span_entity(TRACE, 5)
    }));
}

#[test]
fn exemplar_attachment_relates_a_point_to_the_span_its_exemplar_names() {
    let store = exemplar_store();
    let outcome = run(&store, &exemplar_bounds());

    let exemplar_attachment: Vec<&Relation<SignalRef>> = outcome
        .relations
        .iter()
        .filter(|relation| relation.relation_type == RelationType::ExemplarAttachment)
        .collect();
    // Only the point whose exemplar named the resident span 2 attached.
    assert_eq!(exemplar_attachment.len(), 1);
    let relation = exemplar_attachment[0];
    assert_eq!(relation.from.kind, SignalKind::MetricPoints);
    assert_eq!(relation.from.entity, assigned(10));
    assert_eq!(relation.to.kind, SignalKind::Spans);
    assert_eq!(relation.to.entity, span_entity(TRACE, 2));
    assert_eq!(relation.window, None);
    assert_eq!(relation.strategy.name, "exemplar_attachment");
    assert_eq!(relation.strategy.version, "1.0.0");
    assert_eq!(relation.tier(), Tier::Attachment);
    // The exemplar's own ids, cited verbatim.
    assert_eq!(relation.facts.len(), 2);
    let fields: Vec<(String, &Value)> = relation
        .facts
        .iter()
        .map(|fact| (fact.field.clone(), &fact.value))
        .collect();
    assert!(fields.contains(&(
        "exemplar.trace_id".to_owned(),
        &Value::Bytes(vec![TRACE; 16])
    )));
    assert!(fields.contains(&("exemplar.span_id".to_owned(), &Value::Bytes(vec![2; 8]))));
    // The refusals, held while generating: a non-resident span, a trace id
    // alone, a span id alone — none fabricated, no relation to an absent
    // record.
    let producers: HashSet<EntityId> = outcome
        .relations
        .iter()
        .map(|relation| relation.from.entity)
        .collect();
    assert!(!producers.contains(&assigned(11)));
    assert!(!producers.contains(&assigned(12)));
    assert!(!producers.contains(&assigned(13)));
}

#[test]
fn the_committed_strategy_set_is_machine_readable_and_fully_covered() {
    let store = taxonomy_store();
    let outcome = run(&store, &taxonomy_bounds());

    // The expectations are exhaustive over the strategy enum: a strategy
    // added to the set must earn a match arm here (a compile-time failure
    // otherwise) and must produce relations of its type in the fixture.
    let expectation = |strategy: &Strategy| -> usize {
        match strategy {
            Strategy::SpanIdentity => 1,       // the exact log
            Strategy::TraceIdentity => 3,      // the trace-only log × three spans
            Strategy::ParentChild => 2,        // child → root, grandchild → child
            Strategy::ResourceContext => 6,    // the four shared-resource records, every pair
            Strategy::ExemplarAttachment => 1, // the point whose exemplar names span 2
            Strategy::TemporalCoActivity => 6, // three spans × two in-window points
        }
    };
    let mut by_type: HashMap<RelationType, usize> = HashMap::new();
    for relation in &outcome.relations {
        *by_type.entry(relation.relation_type).or_default() += 1;
    }
    // No strategy emits `Inferred`: its zero instances are invariant.
    assert_eq!(
        by_type.get(&RelationType::Inferred),
        None,
        "Inferred has zero instances"
    );
    let total: usize = COMMITTED_STRATEGIES
        .iter()
        .map(|strategy| {
            let expected = expectation(strategy);
            let observed = by_type
                .get(&strategy.relation_type())
                .copied()
                .unwrap_or_default();
            assert_eq!(
                observed,
                expected,
                "strategy {} is covered by its relations",
                strategy.name()
            );
            observed
        })
        .sum();
    assert_eq!(
        total,
        outcome.relations.len(),
        "no relation type is unaccounted"
    );
    // The set is enumerable, complete (six entries, no duplicates), and
    // versioned — the token the Investigation flow pins per investigation.
    let named: HashSet<&'static str> = COMMITTED_STRATEGIES.iter().map(|s| s.name()).collect();
    assert_eq!(named.len(), 6);
    for strategy in COMMITTED_STRATEGIES {
        assert!(named.contains(strategy.name()));
    }
    assert!(!STRATEGY_SET_VERSION.is_empty());
    assert_eq!(committed_versions().len(), COMMITTED_STRATEGIES.len());
    assert!(
        committed_versions()
            .iter()
            .all(|version| !version.version.is_empty())
    );
}

#[test]
fn equal_inputs_produce_byte_equal_relation_sets() {
    // Two independently built stores with identical admissions: the
    // relation sets — their bytes, not merely their shapes — are equal.
    let first_store = taxonomy_store();
    let second_store = taxonomy_store();

    let first = run(&first_store, &taxonomy_bounds());
    let second = run(&second_store, &taxonomy_bounds());

    assert_eq!(
        format!("{:?}", first.relations),
        format!("{:?}", second.relations),
        "same ingestion yields byte-equal relation sets"
    );
    assert_eq!(first.truth, second.truth);
    // Rerunning on the same store is byte-equal too: a run is an
    // idempotent function of (store, bounds).
    let rerun = run(&first_store, &taxonomy_bounds());
    assert_eq!(
        format!("{:?}", rerun.relations),
        format!("{:?}", first.relations)
    );
}

#[test]
fn bounded_generation_never_forms_relations_to_absent_records_or_second_order() {
    let store = bounded_generation_store();
    let outcome = run(&store, &bounded_generation_bounds());

    // Exactly the intended relations survive: the two direct parent links,
    // the one shared-resource pair, and the one resolvable exemplar. The
    // malformed shapes were refused while generating — the engine never
    // forms a relation first and truncates it later.
    let mut by_type: HashMap<RelationType, usize> = HashMap::new();
    for relation in &outcome.relations {
        *by_type.entry(relation.relation_type).or_default() += 1;
    }
    assert_eq!(
        by_type
            .get(&RelationType::ParentChild)
            .copied()
            .unwrap_or_default(),
        2
    );
    assert_eq!(
        by_type
            .get(&RelationType::ResourceContext)
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        by_type
            .get(&RelationType::ExemplarAttachment)
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        by_type.len(),
        3,
        "only the taxonomy strategies produced relations"
    );

    for relation in &outcome.relations {
        assert_ne!(relation.from, relation.to);
        assert!(
            !relation.facts.is_empty(),
            "every relation carries its evidence"
        );
        // Every endpoint is a resident record: no relation formed to an
        // absent parent or an exemplar's absent span.
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
        assert!(from_present, "from endpoint resident");
        assert!(to_present, "to endpoint resident");
    }

    // The refusals: no absent-parent, zero-parent, or self-parent span
    // produced a relation, and no point with a non-resident or partial
    // exemplar attached.
    let producers: HashSet<EntityId> = outcome.relations.iter().map(|r| r.from.entity).collect();
    for refused in [
        span_entity(TRACE, 4),
        span_entity(TRACE, 5),
        span_entity(TRACE, 6),
        assigned(11),
        assigned(12),
        assigned(13),
    ] {
        assert!(
            !producers.contains(&refused),
            "{refused:?} grounded nothing"
        );
    }
    // The strategy versions reported are the ones in effect — the surface
    // an Investigation pins per run — never invented.
    let versions: Vec<(&str, &str)> = outcome
        .truth
        .strategy_versions
        .iter()
        .map(|version| (version.name.as_str(), version.version.as_str()))
        .collect();
    assert_eq!(
        versions,
        vec![
            ("parent_child", "1.0.0"),
            ("resource_context", "1.0.0"),
            ("exemplar_attachment", "1.0.0"),
        ]
    );
}
