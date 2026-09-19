//! Regression tests for issue #58: a byte-ceiling wall must advance the
//! continuation past the records it counts, never echo the presented
//! cursor and wedge the caller's chain on the same wall forever.
//!
//! The store under test is a TEST-ONLY stub implementing the storage
//! contract (the same pattern as the engine's own unit fixtures): no
//! storage dependency, the boundary law holds.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use runtime_trail_query::budget::QueryBudget;
use runtime_trail_query::cursor::CursorPayload;
use runtime_trail_query::engine::{RecordView, RecordsQuery, SignalKind, records};
use runtime_trail_query::result::{Dimension, Page, PartOutcome, Truncation};
use runtime_trail_query::{
    AdmissionKey, KeepOutcome, PointView, ScanItem, ScanPage, StoreStats, TelemetryStore,
};
use runtime_trail_telemetry_model::{
    Accounted, AdmissionTime, Admitted, Attributes, EmitterDroppedCounts, EntityId,
    InstrumentationScope, LogRecord, MetricPoint, Resource, Span, SpanId, SpanKind, SpanStatus,
    SpanStatusCode, StreamIdentity, TraceContext, TraceFlags, TraceId, TraceState,
};

fn at(nano: u64) -> AdmissionTime {
    AdmissionTime::from_unix_nano(nano)
}

fn span_entity(trace: u8, span_byte: u8) -> EntityId {
    EntityId::Span {
        trace_id: TraceId::from_bytes([trace; 16]),
        span_id: SpanId::from_bytes([span_byte; 8]),
    }
}

fn span_entity_of(view: &RecordView) -> Option<EntityId> {
    match view {
        RecordView::Span(span) => Some(EntityId::Span {
            trace_id: span.context.trace_id,
            span_id: span.context.span_id,
        }),
        _ => None,
    }
}

/// The page's single degradation — every degraded fixture page has
/// exactly one part.
fn degraded_of(page: &Page<RecordView>) -> &Truncation {
    let [PartOutcome::Degraded { truncation }] = &page.execution.parts[..] else {
        panic!("the page degrades");
    };
    truncation
}

// ---------------------------------------------------------------------------
// The fixture store: a contract-conforming stub over residency-order maps,
// holding the same Arc payloads across every call (ADR 0008).
// ---------------------------------------------------------------------------

struct FixtureShelf<R> {
    by_key: BTreeMap<AdmissionKey, Arc<R>>,
    by_entity: HashMap<EntityId, AdmissionKey>,
}

impl<R> FixtureShelf<R> {
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
                    .range((Bound::Excluded(after), Bound::Unbounded))
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
    spans: FixtureShelf<Span>,
    logs: FixtureShelf<LogRecord>,
    points: FixtureShelf<StoredPoint>,
}

impl FixtureStore {
    fn empty() -> Self {
        Self {
            spans: FixtureShelf {
                by_key: BTreeMap::new(),
                by_entity: HashMap::new(),
            },
            logs: FixtureShelf {
                by_key: BTreeMap::new(),
                by_entity: HashMap::new(),
            },
            points: FixtureShelf {
                by_key: BTreeMap::new(),
                by_entity: HashMap::new(),
            },
        }
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

fn fixture_span(trace: u8, span_byte: u8, name: &str) -> Span {
    Span {
        context: TraceContext {
            trace_id: TraceId::from_bytes([trace; 16]),
            span_id: SpanId::from_bytes([span_byte; 8]),
            flags: TraceFlags::new(1),
            tracestate: TraceState::default(),
        },
        parent_span_id: None,
        name: name.to_owned(),
        kind: SpanKind::Server,
        start_time_unix_nano: 10,
        end_time_unix_nano: Some(20),
        resource: Arc::new(Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }),
        scope: Arc::new(InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }),
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

// ---------------------------------------------------------------------------
// The regression.
// ---------------------------------------------------------------------------

/// Issue #58: a byte-ceiling wall must advance past its counted tail.
/// Seven spans; the ceiling fits the first two and stops one byte short
/// of the third, and the scan burst ends page 1 mid-counting — so the
/// wall counts e3, e4 (which would fit, but the include phase is over)
/// and e5, and anchors the continuation at e5, strictly past everything
/// it counted. The records behind the wall that the wall never reached
/// (e6, e7) stay reachable on the continuation: delivered exactly once.
///
/// Kills both pre-fix lies: the omission was under-counted (2 reported
/// where 3 were deliberately not delivered), and the cursor anchored at
/// the last *included* record (e2) — the continuation re-walked the
/// wall, counted it again, and echoed its own cursor, looping forever
/// and never reaching e6 or e7.
#[test]
fn a_byte_wall_advances_past_its_counted_tail_and_the_records_behind_it_stay_reachable() {
    let (store, sizes) = wedge_store();
    let query = RecordsQuery::new(SignalKind::Spans);
    let ceiling = sizes[0] + sizes[1] + sizes[2] - 1;
    // The scan burst of five ends page 1 while it is still counting: the
    // wall counted e3, e4 and e5, and the page stops at the last counted
    // record instead of consuming the whole kind.
    let budget = || QueryBudget::new(Duration::from_secs(10), 1_000, ceiling, 5, 4_096);

    let first = records(&store, &query, budget(), None).expect("the query answers");
    let truncation = degraded_of(&first);
    assert_eq!(truncation.dimension, Dimension::Bytes);
    assert_eq!(
        truncation.omitted, 3,
        "the wall counts every record it examined and deliberately did \
         not deliver: e3 (does not fit), e4 (would fit, but the include \
         phase is over) and e5 (does not fit)"
    );
    let first_entities: Vec<EntityId> = first
        .items
        .iter()
        .map(|view| span_entity_of(view).expect("a span page"))
        .collect();
    assert_eq!(first_entities, vec![span_entity(1, 1), span_entity(2, 2)]);
    let cursor = first.next_cursor.as_deref().expect("the wall continues");
    let payload = CursorPayload::decode(cursor).expect("the engine's own cursor");
    assert_eq!(
        payload.position(),
        500_u64,
        "the cursor anchors at e5, the last counted record — strictly \
         past the wall"
    );
    assert_eq!(payload.last_entity(), span_entity(5, 5));

    // The continuation resumes strictly past the counted tail: e3, e4 and
    // e5 are never re-presented, and the records behind the wall that the
    // wall never reached (e6, e7) are delivered exactly once. Pre-fix,
    // the cursor anchored at the last *included* record (e2), so the
    // continuation re-walked the wall, counted e3-e5 again, and echoed
    // the e2 cursor — a caller chaining pages would loop on the same wall
    // forever, burning its chain and never reaching e6 or e7.
    let second = records(&store, &query, budget(), Some(cursor)).expect("the query answers");
    let second_entities: Vec<EntityId> = second
        .items
        .iter()
        .map(|view| span_entity_of(view).expect("a span page"))
        .collect();
    assert_eq!(
        second_entities,
        vec![span_entity(6, 6), span_entity(7, 7)],
        "the walk advances past the counted tail and delivers what \
         follows — no repeats, no loss"
    );
    assert_eq!(second.execution.parts, vec![PartOutcome::Complete]);
    assert!(
        second.next_cursor.is_none(),
        "the walk consumed the whole kind — the wall no longer wedges it"
    );
}

/// The wedge fixture: seven spans, one wire identity each, admitted
/// 100..=700 nanoseconds apart, with evidence sizes shaped so a ceiling
/// that fits the first two stops one byte short of the third and counts
/// the remainder. Returns the store and the recorded sizes.
fn wedge_store() -> (FixtureStore, Vec<u64>) {
    let mut store = FixtureStore::empty();
    let mut sizes = Vec::new();
    for index in 1_u8..=7_u8 {
        let name = match index {
            1 => "a".repeat(40),
            2 => "b".repeat(40),
            3 => "c".repeat(80),
            4 => "d".repeat(40),
            5 => "e".repeat(100),
            6 => "f".repeat(40),
            _ => "g".repeat(40),
        };
        let span = fixture_span(index, index, &name);
        sizes.push(u64::try_from(span.accounted_size()).expect("sizes fit"));
        let outcome = store.keep_span(Admitted {
            entity: span_entity(index, index),
            admitted_at: at(u64::from(index) * 100),
            record: Arc::new(span),
        });
        assert!(matches!(outcome, KeepOutcome::Kept { evicted: 0 }));
    }
    (store, sizes)
}
