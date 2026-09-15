//! Behavioral tests for the trace investigation flow (ADR 0011, M3): the
//! flow composes the query engine into ONE envelope, decomposing the
//! caller budget into fresh per-page engine budgets and owning the
//! chain-level limits it reports honestly.
//!
//! The store under investigation is a TEST-ONLY stub implementing the
//! storage contract (the facade's `TelemetryStore`) — the same pattern as
//! the query crate's own fixture stores: no storage dependency, the
//! boundary law holds (layer-api programs against the facade, and only
//! layer-app names concrete drivers).

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use runtime_trail_investigation::envelope::invariants;
use runtime_trail_investigation::execution::{
    CoverageEntry, Dimension, FlowCoverageEntry, Magnitude, Outcome, PartName, TimeWindow,
};
use runtime_trail_investigation::limits::ChainBasis;
use runtime_trail_investigation::subject::ResolutionNote;
use runtime_trail_investigation::{
    ChainBudget, FlowError, InvestigationBudget, TraceInvestigationRequest, investigate_trace,
    investigate_trace_bounded,
};
use runtime_trail_query::{
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

fn assigned(serial: u64) -> EntityId {
    EntityId::Assigned(AssignedId::from_serial(
        NonZeroU64::new(serial).expect("fixture serials start at 1"),
    ))
}

fn span_id_of(span_byte: u8) -> SpanId {
    SpanId::from_bytes([span_byte; 8])
}

fn open_budget() -> InvestigationBudget {
    InvestigationBudget::new(
        Duration::from_secs(60),
        1_000,
        4_194_304,
        100_000,
        4_194_304,
    )
}

// ---------------------------------------------------------------------------
// The fixture store: a contract-conforming stub over residency-order maps,
// holding the same Arc payloads across every call (ADR 0008 — identity
// recovery matches by payload pointer).
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

    /// The ordered page: at most `limit` records strictly after `after`,
    /// the cursor exactly the last item's key when a record follows it —
    /// the contract's scan law, the same law the real shelf implements.
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
        let outcome = self.keep_span(Admitted {
            entity,
            admitted_at: at(nano),
            record: Arc::new(span),
        });
        assert!(matches!(outcome, KeepOutcome::Kept { evicted: 0 }));
    }

    fn admit_log(&mut self, nano: u64, entity: EntityId, log: LogRecord) {
        let outcome = self.keep_log_record(Admitted {
            entity,
            admitted_at: at(nano),
            record: Arc::new(log),
        });
        assert!(matches!(outcome, KeepOutcome::Kept { evicted: 0 }));
    }

    fn admit_point(&mut self, nano: u64, entity: EntityId, point: MetricPoint) {
        let outcome = self.keep_metric_point(
            Admitted {
                entity,
                admitted_at: at(nano),
                record: Arc::new(point),
            },
            Arc::new(fixture_stream("fixture.stream")),
        );
        assert!(matches!(outcome, KeepOutcome::Kept { evicted: 0 }));
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

/// A TEST-ONLY, contract-violating store: its span scan yields empty pages
/// while claiming a successor cursor, so the engine's stall guard answers
/// a walked `Stalled` page with a named `DriverStall`.
struct StallingStore {
    inner: FixtureStore,
    stall_cursor: AdmissionKey,
}

impl StallingStore {
    fn new(inner: FixtureStore, stall_cursor: AdmissionKey) -> Self {
        Self {
            inner,
            stall_cursor,
        }
    }
}

impl TelemetryStore for StallingStore {
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
        self.inner.span(entity)
    }

    fn log_record(&self, entity: EntityId) -> Option<Arc<LogRecord>> {
        self.inner.log_record(entity)
    }

    fn metric_point(&self, entity: EntityId) -> Option<PointView> {
        self.inner.metric_point(entity)
    }

    fn scan_spans(&self, _after: Option<AdmissionKey>, _limit: usize) -> ScanPage<Arc<Span>> {
        ScanPage {
            items: Vec::new(),
            cursor: Some(self.stall_cursor),
        }
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
        "test-stalling"
    }
}

// ---------------------------------------------------------------------------
// Fixture record builders (mirroring the engine's own fixture shapes).
// ---------------------------------------------------------------------------

fn fixture_span(trace: u8, span_byte: u8, parent: Option<u8>, name: &str, start: u64) -> Span {
    Span {
        context: TraceContext {
            trace_id: TraceId::from_bytes([trace; 16]),
            span_id: span_id_of(span_byte),
            flags: TraceFlags::new(1),
            tracestate: TraceState::default(),
        },
        parent_span_id: parent.map(span_id_of),
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

fn fixture_log(trace: Option<u8>, name: &str) -> LogRecord {
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
        trace_id: trace.map(|byte| TraceId::from_bytes([byte; 16])),
        span_id: Some(span_id_of(1)),
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

/// The three-span fixture: one trace, root -> child -> grandchild, plus
/// two related logs, two in-window points and one outside the window.
fn trace_store() -> FixtureStore {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, None, "root", 10),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        fixture_span(TRACE, 2, Some(1), "child", 20),
    );
    store.admit_span(
        300,
        span_entity(TRACE, 3),
        fixture_span(TRACE, 3, Some(2), "grandchild", 30),
    );
    store.admit_log(400, assigned(1), fixture_log(Some(TRACE), "related one"));
    store.admit_log(500, assigned(2), fixture_log(Some(TRACE), "related two"));
    store.admit_log(600, assigned(3), fixture_log(Some(2), "other trace"));
    store.admit_point(700, assigned(4), fixture_point(15));
    store.admit_point(800, assigned(5), fixture_point(25));
    store.admit_point(900, assigned(6), fixture_point(45));
    store
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn a_complete_trace_investigation_composes_all_parts() {
    let store = trace_store();
    let root = span_entity(TRACE, 1);

    let envelope = investigate_trace(&store, TraceInvestigationRequest::new(root, open_budget()))
        .expect("the fixture trace investigates");

    // Subject: requested and effective agree; the requested span IS the
    // trace's parentless root.
    assert_eq!(envelope.subject.requested.root_span, root);
    assert_eq!(envelope.subject.effective.root.entity, root);
    assert!(
        matches!(
            &envelope.subject.effective.notes[..],
            [ResolutionNote::RequestedSpanIsRoot]
        ),
        "the effective root is confirmed as the requested root"
    );

    // Evidence: the waterfall in breadth-first order, the trace's logs
    // only, the in-window points only.
    let span_names: Vec<&str> = envelope
        .evidence
        .spans
        .iter()
        .map(|view| view.span.name.as_str())
        .collect();
    assert_eq!(span_names, ["root", "child", "grandchild"]);
    let log_names: Vec<&str> = envelope
        .evidence
        .logs
        .iter()
        .map(|view| match view.log.body.as_ref() {
            Some(Value::String(text)) => text.as_str(),
            _ => "",
        })
        .collect();
    assert_eq!(log_names, ["related one", "related two"]);
    assert_eq!(
        envelope.evidence.points.len(),
        2,
        "the outside point stays out"
    );

    // Execution: three run groups, one complete run each, and the flow's
    // own coverage — the metric window statement plus the admission
    // anomalies read at admission.
    assert_eq!(envelope.execution.run_groups.len(), 3);
    for group in &envelope.execution.run_groups {
        assert_eq!(group.runs.len(), 1);
        assert!(matches!(&group.runs[0].outcome, Outcome::Complete));
    }
    assert!(envelope.execution.flow_coverage.iter().any(|entry| {
        matches!(
            entry,
            FlowCoverageEntry::MetricWindow {
                asked: TimeWindow { from: 10, to: 40 },
                resident: TimeWindow { from: 15, to: 26 },
            }
        )
    }));

    // Limits: the caller budget mirrored, the chain totals honest, the
    // eviction state read at admission.
    assert_eq!(envelope.limits.chain.total_pages, 3);
    assert_eq!(envelope.limits.chain.total_entities, 7);
    assert_eq!(envelope.limits.chain.stopped, None);
    assert_eq!(
        envelope.limits.budget.max_results,
        open_budget().max_results
    );
    assert_eq!(
        envelope.limits.eviction.resident_records,
        store.stats().resident_records
    );
    assert!(
        envelope.correlated.relations.is_empty(),
        "correlation is typed-but-empty in M3"
    );

    // Every invariant holds on the complete envelope.
    assert!(invariants::violations(&envelope).is_empty());
    assert!(invariants::views_carry_entity_ids(&envelope));
    assert!(invariants::run_facts_are_consistent(&envelope));
    assert!(invariants::metric_window_is_stated(&envelope));
    assert!(invariants::no_partial_content_as_complete(&envelope));
}

#[test]
fn a_results_degrade_surfaces_its_truncation_and_the_continuation_completes() {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, None, "root", 10),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        fixture_span(TRACE, 2, Some(1), "child", 20),
    );
    let budget = open_budget();
    let capped = InvestigationBudget {
        deadline: budget.deadline,
        max_results: 1,
        max_bytes: budget.max_bytes,
        max_scan: budget.max_scan,
        max_aggregation_memory: budget.max_aggregation_memory,
    };

    let envelope = investigate_trace(
        &store,
        TraceInvestigationRequest::new(span_entity(TRACE, 1), capped),
    )
    .expect("the fixture trace investigates");

    // The spans walk needs two pages: the first degrades on the results
    // ceiling with a continuation cursor, the second — a NEW execution —
    // completes. The budget is re-admitted per page, never chained.
    let spans_group = &envelope.execution.run_groups[0];
    assert_eq!(spans_group.runs.len(), 2);
    assert!(matches!(
        &spans_group.runs[0].outcome,
        Outcome::Degraded {
            truncation: runtime_trail_investigation::execution::Truncation {
                dimension: Dimension::Results,
                omitted: 0,
                ..
            }
        }
    ));
    assert!(
        spans_group.runs[0].next_cursor.is_some(),
        "the truncated page hands the continuation cursor"
    );
    assert!(matches!(&spans_group.runs[1].outcome, Outcome::Complete));
    assert!(spans_group.runs[1].next_cursor.is_none());

    // Everything collected is honest: both spans, the chain totals
    // agreeing with the recorded runs, no fabricated completion.
    assert_eq!(envelope.evidence.spans.len(), 2);
    assert_eq!(envelope.limits.chain.total_pages, 4);
    assert_eq!(envelope.limits.chain.stopped, None);
    assert!(invariants::no_partial_content_as_complete(&envelope));
    assert!(invariants::run_facts_are_consistent(&envelope));
}

#[test]
fn a_refused_scan_budget_lands_in_the_execution_part() {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, None, "root", 10),
    );
    // Each walk needs a resident record for the zero scan ceiling to bite:
    // an empty walk cannot be refused, it completes empty.
    store.admit_log(200, assigned(1), fixture_log(Some(TRACE), "related"));
    store.admit_point(300, assigned(2), fixture_point(15));
    let budget = open_budget();
    let refused = InvestigationBudget {
        deadline: budget.deadline,
        max_results: budget.max_results,
        max_bytes: budget.max_bytes,
        max_scan: 0,
        max_aggregation_memory: budget.max_aggregation_memory,
    };

    let envelope = investigate_trace(
        &store,
        TraceInvestigationRequest::new(span_entity(TRACE, 1), refused),
    )
    .expect("the refusal is an answer shape, not a failure");

    // Every refused walk is mirrored: three groups, each a single Refused
    // run naming the scan ceiling, its zero limit and the zero granted.
    for group in &envelope.execution.run_groups {
        assert_eq!(group.runs.len(), 1);
        match &group.runs[0].outcome {
            Outcome::Refused(refusal) => {
                assert_eq!(refusal.dimension, Dimension::Scan);
                assert_eq!(refusal.limit, Magnitude::Units(0));
                assert_eq!(refusal.observed, Magnitude::Units(0));
            }
            other => panic!("expected a scan refusal, got {other:?}"),
        }
    }

    // The refused walks select nothing; the subject is still the span
    // alone, honestly. The refusal is never swallowed as a completion.
    assert_eq!(envelope.evidence.spans.len(), 1);
    assert!(envelope.evidence.logs.is_empty());
    assert!(envelope.evidence.points.is_empty());
    assert!(invariants::no_partial_content_as_complete(&envelope));
    assert!(invariants::run_facts_are_consistent(&envelope));
}

#[test]
fn a_stalled_driver_lands_as_a_named_stall() {
    let mut inner = FixtureStore::empty();
    inner.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, None, "root", 10),
    );
    let stall_cursor = AdmissionKey::new(at(99), span_entity(9, 9));
    let store = StallingStore::new(inner, stall_cursor);

    let envelope = investigate_trace(
        &store,
        TraceInvestigationRequest::new(span_entity(TRACE, 1), open_budget()),
    )
    .expect("the stall is an answer shape");

    let spans_group = &envelope.execution.run_groups[0];
    assert_eq!(spans_group.runs.len(), 1);
    assert!(matches!(&spans_group.runs[0].outcome, Outcome::Stalled));
    assert_eq!(
        &spans_group.runs[0].coverage,
        &vec![CoverageEntry::DriverStall {
            after: span_entity(9, 9),
        }],
    );
    assert!(
        spans_group.runs[0].next_cursor.is_none(),
        "a stalled walk mints no cursor"
    );

    // The stall is named, never dressed as a completion; the subject stays
    // the span alone and every invariant holds.
    assert!(invariants::violations(&envelope).is_empty());
    assert!(invariants::no_partial_content_as_complete(&envelope));
    assert!(invariants::run_facts_are_consistent(&envelope));
}

#[test]
fn budget_is_re_admitted_fresh_on_every_continuation_page() {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, None, "root", 10),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        fixture_span(TRACE, 2, Some(1), "child", 20),
    );
    let budget = open_budget();
    let capped = InvestigationBudget {
        deadline: budget.deadline,
        max_results: 1, // one result per page
        max_bytes: budget.max_bytes,
        max_scan: budget.max_scan,
        max_aggregation_memory: budget.max_aggregation_memory,
    };

    let envelope = investigate_trace(
        &store,
        TraceInvestigationRequest::new(span_entity(TRACE, 1), capped),
    )
    .expect("the fixture trace investigates");

    // If the flow chained one budget across pages, the second page would
    // arrive with its results slot already spent and refuse. It completes
    // instead: every page is a new execution with the caller's ceilings
    // re-admitted.
    let spans_group = &envelope.execution.run_groups[0];
    assert_eq!(spans_group.runs.len(), 2);
    assert!(matches!(
        &spans_group.runs[0].outcome,
        Outcome::Degraded { .. }
    ));
    assert!(matches!(&spans_group.runs[1].outcome, Outcome::Complete));
    assert_eq!(envelope.evidence.spans.len(), 2);
    assert_eq!(
        envelope.limits.chain.total_pages, 4,
        "2 + 1 + 1 pages, one per part"
    );
}

#[test]
fn a_chain_total_pages_stop_is_reported_not_swallowed() {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, None, "root", 10),
    );
    store.admit_span(
        200,
        span_entity(TRACE, 2),
        fixture_span(TRACE, 2, Some(1), "child", 20),
    );
    store.admit_span(
        300,
        span_entity(TRACE, 3),
        fixture_span(TRACE, 3, Some(2), "grandchild", 30),
    );
    let budget = open_budget();
    let capped = InvestigationBudget {
        deadline: budget.deadline,
        max_results: 1,
        max_bytes: budget.max_bytes,
        max_scan: budget.max_scan,
        max_aggregation_memory: budget.max_aggregation_memory,
    };
    let chain = ChainBudget::new(2, 10_000, 100_000);

    let envelope = investigate_trace_bounded(
        &store,
        &TraceInvestigationRequest::new(span_entity(TRACE, 1), capped),
        chain,
    )
    .expect("the fixture trace investigates");

    // The spans walk stops at the second page; the later walks never start.
    // The stop is stated in the limits part, and the two collected spans
    // are honestly the whole evidence.
    assert_eq!(envelope.limits.chain.stopped, Some(ChainBasis::TotalPages));
    assert_eq!(envelope.limits.chain.total_pages, 2);
    assert_eq!(envelope.limits.chain.max_total_pages, 2);
    let spans_group = &envelope.execution.run_groups[0];
    assert_eq!(spans_group.runs.len(), 2);
    assert!(envelope.execution.run_groups[1].runs.is_empty());
    assert!(envelope.execution.run_groups[2].runs.is_empty());
    assert_eq!(
        envelope.evidence.spans.len(),
        2,
        "the uncollected span is absent"
    );
    assert!(invariants::no_partial_content_as_complete(&envelope));
    assert!(invariants::run_facts_are_consistent(&envelope));
}

#[test]
fn a_chain_total_entities_stop_is_reported_not_swallowed() {
    let mut store = FixtureStore::empty();
    for (index, name) in ["a", "b", "c", "d"].iter().enumerate() {
        let byte = u8::try_from(index + 1).expect("fixture indices fit");
        let parent = if index == 0 { None } else { Some(byte - 1) };
        store.admit_span(
            u64::from(byte) * 100,
            span_entity(TRACE, byte),
            fixture_span(TRACE, byte, parent, name, u64::from(byte) * 10),
        );
    }
    let chain = ChainBudget::new(16, 3, 100_000);

    let envelope = investigate_trace_bounded(
        &store,
        &TraceInvestigationRequest::new(span_entity(TRACE, 1), open_budget()),
        chain,
    )
    .expect("the fixture trace investigates");

    // The spans walk runs (selection is post-hoc, so the entity ceiling
    // gates between walks); the related walks stop at the ceiling, and the
    // stop is stated — the evidence is complete, the walks honest.
    assert_eq!(
        envelope.limits.chain.stopped,
        Some(ChainBasis::TotalEntities)
    );
    assert_eq!(envelope.limits.chain.total_pages, 1);
    assert_eq!(envelope.evidence.spans.len(), 4);
    assert!(envelope.execution.run_groups[1].runs.is_empty());
    assert!(envelope.execution.run_groups[2].runs.is_empty());
    assert!(invariants::no_partial_content_as_complete(&envelope));
    assert!(invariants::run_facts_are_consistent(&envelope));
}

#[test]
fn deterministic_runs_agree_on_content_and_facts() {
    let store = trace_store();
    let request = TraceInvestigationRequest::new(span_entity(TRACE, 1), open_budget());

    let first = investigate_trace(&store, request).expect("first run");
    let second = investigate_trace(&store, request).expect("second run");

    assert!(invariants::content_parts_equal(&first, &second));
    assert!(invariants::run_facts_equivalent(&first, &second));
}

#[test]
fn identity_recovery_holes_are_counted_and_named() {
    let mut store = FixtureStore::empty();
    store.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, None, "root", 10),
    );
    // The related logs are the first two in residency order, so the
    // recovery walk's single examination names exactly one of them.
    store.admit_log(200, assigned(1), fixture_log(Some(TRACE), "named"));
    store.admit_log(300, assigned(2), fixture_log(Some(TRACE), "unnamed"));
    store.admit_log(400, assigned(3), fixture_log(Some(2), "other trace"));
    let chain = ChainBudget::new(16, 10_000, 1); // one identity examination

    let envelope = investigate_trace_bounded(
        &store,
        &TraceInvestigationRequest::new(span_entity(TRACE, 1), open_budget()),
        chain,
    )
    .expect("the fixture trace investigates");

    // One selected log is named; the other is a stated residency hole —
    // never emitted unkeyed, and never silently dropped.
    assert_eq!(envelope.evidence.logs.len(), 1);
    assert!(
        envelope
            .execution
            .flow_coverage
            .iter()
            .any(|entry| matches!(
                entry,
                FlowCoverageEntry::ResidencyHole {
                    part: PartName::RelatedLogs,
                    count: 1,
                }
            )),
        "the unnamed selected record is a named hole"
    );
    assert_eq!(envelope.limits.chain.identity_examinations, 1);
    assert!(invariants::views_carry_entity_ids(&envelope));
    assert!(invariants::run_facts_are_consistent(&envelope));
}

#[test]
fn a_degenerate_root_investigates_the_span_alone() {
    let mut store = FixtureStore::empty();
    let mut degenerate = fixture_span(0, 0, None, "nameless", 10);
    degenerate.context.trace_id = TraceId::from_bytes([0; 16]);
    degenerate.context.span_id = span_id_of(0);
    let entity = assigned(7);
    store.admit_span(100, entity, degenerate);

    let envelope = investigate_trace(
        &store,
        TraceInvestigationRequest::new(entity, open_budget()),
    )
    .expect("the degenerate span investigates");

    // The subject is the span alone, the note names why, and the evidence
    // is keyed by the identity the caller used (invariant 4 holds).
    assert!(
        envelope
            .subject
            .effective
            .notes
            .iter()
            .any(|note| { matches!(note, ResolutionNote::RootHasNoValidTraceIdentity) })
    );
    assert_eq!(envelope.evidence.spans.len(), 1);
    assert_eq!(envelope.evidence.spans[0].entity, Some(entity));
    assert!(envelope.evidence.logs.is_empty());
    assert!(envelope.evidence.points.is_empty());
    assert!(invariants::violations(&envelope).is_empty());
    assert!(invariants::views_carry_entity_ids(&envelope));
    assert!(invariants::run_facts_are_consistent(&envelope));
}

#[test]
fn an_unresolvable_subject_fails_outright() {
    let store = trace_store();
    let unknown = span_entity(7, 7);

    let error = investigate_trace(
        &store,
        TraceInvestigationRequest::new(unknown, open_budget()),
    )
    .expect_err("a never-admitted span cannot be the subject");
    assert_eq!(error, FlowError::SubjectUnresolved { requested: unknown });
}
