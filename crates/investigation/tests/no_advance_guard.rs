//! Regression tests for issue #58's no-progress guard in `walk_pages`: a
//! continuation that hands back the very cursor it was presented advanced
//! nothing — the engine's cursors are strictly monotonic, so only a
//! contract-violating driver can produce this — and re-presenting the
//! cursor would repeat the identical page forever, burning the chain.
//!
//! The store under test is a TEST-ONLY stub implementing the storage
//! contract (the same pattern as `trace_flow.rs`) plus a deliberately
//! non-advancing span scanner.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use runtime_trail_investigation::envelope::invariants;
use runtime_trail_investigation::execution::{Dimension, Outcome, PartName};
use runtime_trail_investigation::{
    ChainBudget, InvestigationBudget, TraceInvestigationRequest, investigate_trace_bounded,
};
use runtime_trail_query::{
    AdmissionKey, KeepOutcome, PointView, ScanItem, ScanPage, StoreStats, TelemetryStore,
};
use runtime_trail_telemetry_model::{
    AdmissionTime, Admitted, Attributes, EmitterDroppedCounts, EntityId, InstrumentationScope,
    LogRecord, MetricPoint, Resource, Span, SpanId, SpanKind, SpanStatus, SpanStatusCode,
    StreamIdentity, TraceContext, TraceFlags, TraceId, TraceState,
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

// ---------------------------------------------------------------------------
// The fixture store: a contract-conforming stub over residency-order maps,
// holding the same Arc payloads across every call (ADR 0008).
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
    /// the contract's scan law.
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

/// A TEST-ONLY, contract-violating driver: `scan_spans` ignores `after`
/// and always re-yields the one resident span. The engine mints a
/// byte-wall cursor anchored at that span; this driver's next scan starts
/// from the beginning again — the very non-advance the engine's strict
/// cursor monotonicity makes impossible for a conforming driver (issue
/// #58's wedge). `walk_pages` must end the part on the first such page.
struct NonAdvancingStore {
    inner: FixtureStore,
    span: Arc<Span>,
    key: AdmissionKey,
}

impl NonAdvancingStore {
    fn new(inner: FixtureStore, span: Arc<Span>, key: AdmissionKey) -> Self {
        Self { inner, span, key }
    }
}

impl TelemetryStore for NonAdvancingStore {
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
            items: vec![ScanItem {
                key: self.key,
                record: Arc::clone(&self.span),
            }],
            cursor: None,
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
        "test-non-advancing"
    }
}

// ---------------------------------------------------------------------------
// Fixture record builders (mirroring trace_flow.rs's shapes).
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

// ---------------------------------------------------------------------------
// The regression.
// ---------------------------------------------------------------------------

/// Issue #58's no-progress guard: one resident span — the requested root
/// itself — with a name so large its evidence clears the byte ceiling on
/// EVERY page. The engine walls at it and counts it; the violating driver
/// then hands the same record back on the continuation, the engine mints
/// the byte-identical cursor (the same anchor, snapshot and fingerprint),
/// and the guard ends the spans part on that page — mirrored, never
/// swallowed — instead of re-presenting the identical cursor until a
/// chain ceiling burns.
#[test]
fn a_non_advancing_continuation_ends_the_part_instead_of_burning_the_chain() {
    let mut resident = FixtureStore::empty();
    resident.admit_span(
        100,
        span_entity(TRACE, 1),
        fixture_span(TRACE, 1, None, &"huge".repeat(64), 10),
    );
    let span = resident
        .span(span_entity(TRACE, 1))
        .expect("the root is resident");
    let key = AdmissionKey::new(at(100), span_entity(TRACE, 1));
    let store = NonAdvancingStore::new(resident, span, key);

    // A byte ceiling below the huge span's evidence: every page walls at
    // it and counts the span (omitted 1). The chain is the default grant.
    let budget = InvestigationBudget::new(Duration::from_secs(60), 1_000, 8, 100_000, 4_194_304);
    let chain = ChainBudget::new(16, 10_000, 100_000);
    let envelope = investigate_trace_bounded(
        &store,
        &TraceInvestigationRequest::new(span_entity(TRACE, 1), budget, None),
        chain,
    )
    .expect("the subject is resident and investigates");

    // The spans part: page one walls at the huge span and counts it; the
    // non-advancing continuation re-presents it (ignoring `after`), the
    // engine mints the byte-identical cursor, and the guard ends the part
    // there. Two runs, never a third page.
    let spans_group = envelope
        .execution
        .run_groups
        .iter()
        .find(|group| group.part == PartName::Spans)
        .expect("the spans part ran");
    assert_eq!(
        spans_group.runs.len(),
        2,
        "the walk and the caught non-advance — never a third page"
    );
    assert!(
        spans_group.runs[0].next_cursor.is_some(),
        "the byte wall mints a cursor"
    );
    assert_eq!(
        spans_group.runs[0].next_cursor, spans_group.runs[1].next_cursor,
        "the continuation handed back the very cursor it was presented — \
         the non-advance the guard catches"
    );
    for run in &spans_group.runs {
        let Outcome::Degraded { truncation } = &run.outcome else {
            panic!("every byte-wall page degrades");
        };
        assert_eq!(truncation.dimension, Dimension::Bytes);
        assert_eq!(truncation.omitted, 1, "the huge span, counted each page");
    }

    // The chain never burned: two span pages plus one each for logs and
    // points, far below the 16-page grant, and no ceiling was hit — the
    // guard ended the part instead.
    let limits = &envelope.limits.chain;
    assert_eq!(limits.total_pages, 4, "2 + 1 + 1 pages, one per part");
    assert!(
        limits.total_pages < limits.max_total_pages,
        "the guard ends the part instead of burning the chain"
    );
    assert!(
        limits.stopped.is_none(),
        "no chain ceiling was hit — the part simply ended on the non-advance"
    );

    // The envelope is a truthful account of a completed investigation.
    assert!(invariants::violations(&envelope).is_empty());
}
