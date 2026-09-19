//! HTTP-vs-MCP parity for issue #36: the MCP surface must answer the
//! Investigation API's one committed flow — `investigate_trace_bounded`
//! under the HTTP adapter's admitted budget and chain — exactly as the
//! HTTP surface (`crates/server/src/investigation_http.rs`) does.
//!
//! Driving the HTTP surface's axum router here is impossible under the
//! boundary law (the MCP crate may not dev-depend on the server crate),
//! so parity is proven structurally instead: each tool's envelope MUST
//! EQUAL the baseline `investigate_trace_bounded` result computed here
//! under the same constants, and the rendered JSON MUST carry the same
//! field paths and values the server's own HTTP tests assert. The
//! renderer under test is `runtime_trail_mcp::render` — the mirror of the
//! server's renderer — so equality of the core envelope plus these
//! field-path checks pins the wire shape to the HTTP surface's.
//!
//! The store is the same contract-conforming test stub the flow crate's
//! own tests use (no storage dependency — the boundary law holds for
//! dev-scope edges too).
//!
//! The multi-page fixture (1 500 related logs under the 1 000-record
//! page budget) exercises the walk's snapshot law: the first run holds
//! to its page's budget, degrades with `dimension: results`, and hands
//! the chain a continuation cursor; the second run completes the walk.
//! The envelope — evidence, run facts, chain totals — is exactly the
//! one the HTTP surface's identical admission would produce.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use runtime_trail_investigation::telemetry_model::{
    AdmissionTime, Admitted, AssignedId, Attributes, EmitterDroppedCounts, EntityId, Float,
    InstrumentationScope, LogRecord, MetricNumber, MetricPoint, NumberPoint, Resource, Span,
    SpanId, SpanKind, SpanStatus, SpanStatusCode, StreamIdentity, StreamKind, TraceContext,
    TraceFlags, TraceId, TraceState, Value,
};
use runtime_trail_investigation::{
    AdmissionKey, ChainBudget, InvestigationBudget, KeepOutcome, PointView, ScanItem, ScanPage,
    StoreStats, TelemetryStore, TraceInvestigationRequest, investigate_trace_bounded,
};
use runtime_trail_mcp::render;
use runtime_trail_mcp::tools;
use serde_json::{Value as JsonValue, json};

// ---------------------------------------------------------------------------
// The adapter's admitted constants, mirrored verbatim from
// `crates/server/src/investigation_http.rs` (and `crates/mcp/src/tools.rs`).
// ---------------------------------------------------------------------------

fn admitted_budget() -> InvestigationBudget {
    InvestigationBudget::new(
        Duration::from_secs(30),
        1_000,
        4 * 1024 * 1024,
        100_000,
        4_194_304,
    )
}

fn admitted_chain() -> ChainBudget {
    ChainBudget::new(16, 10_000, 100_000)
}

// ---------------------------------------------------------------------------
// Fixture helpers.
// ---------------------------------------------------------------------------

const TRACE: u8 = 1;

fn at(nano: u64) -> AdmissionTime {
    AdmissionTime::from_unix_nano(nano)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn span_id_of(span_byte: u8) -> SpanId {
    SpanId::from_bytes([span_byte; 8])
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

/// One log in the subject trace (trace `TRACE`, child span id), carrying
/// `body` — the same shape the HTTP test's `related_logs_payload` builds.
fn related_log(body: Value) -> LogRecord {
    LogRecord {
        timestamp_unix_nano: Some(1),
        observed_timestamp_unix_nano: Some(2),
        severity_number: None,
        severity_text: None,
        body: Some(body),
        resource: Arc::new(fixture_resource()),
        scope: Arc::new(fixture_scope()),
        attributes: Attributes::default(),
        dropped_attribute_count: 0,
        trace_id: Some(TraceId::from_bytes([TRACE; 16])),
        span_id: Some(span_id_of(2)),
        trace_flags: None,
        event_name: None,
    }
}

fn structured_bodies() -> Vec<Value> {
    vec![
        Value::String("related".to_owned()),
        Value::array(vec![
            Value::String("alpha".to_owned()),
            Value::String("beta".to_owned()),
            Value::String("gamma".to_owned()),
        ])
        .expect("a homogeneous string array is valid"),
        Value::kv_list(vec![
            ("name".to_owned(), Value::String("e2e".to_owned())),
            ("attempts".to_owned(), Value::Int(3)),
            (
                "tags".to_owned(),
                Value::array(vec![
                    Value::String("x".to_owned()),
                    Value::String("y".to_owned()),
                ])
                .expect("a homogeneous string array is valid"),
            ),
        ])
        .expect("keys are unique"),
        Value::array(vec![
            Value::kv_list(vec![("a".to_owned(), Value::Int(1))]).expect("unique keys"),
            Value::kv_list(vec![("b".to_owned(), Value::String("two".to_owned()))])
                .expect("unique keys"),
        ])
        .expect("a homogeneous key-value-list array is valid"),
        Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
        Value::Int(i64::MAX),
        Value::Double(Float::new(3.5)),
        Value::String("x".repeat(600)),
    ]
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
// the same pattern the flow crate's own tests use.
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
            Arc::new(fixture_stream("cpu.seconds")),
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

/// The multi-page fixture: a two-span trace, 1 500 related logs (the
/// first eight carrying the structured-value bodies the HTTP surface's
/// own test asserts), and two in-window points. 1 500 related logs walk
/// two engine runs under the 1 000-record page budget: the first run
/// degrades on the results dimension and reports a continuation cursor;
/// the second run completes the walk.
fn parity_store() -> FixtureStore {
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
    let bodies = structured_bodies();
    for (nano, index) in (1_000..).zip(1..=1_500) {
        let body = match bodies.get(index - 1) {
            Some(body) => body.clone(),
            None => Value::String(format!("filler-{index}")),
        };
        store.admit_log(
            nano,
            assigned(u64::try_from(index).expect("1 500 fits u64")),
            related_log(body),
        );
    }
    store.admit_point(5_000, assigned(1_501), fixture_point(15));
    store.admit_point(5_100, assigned(1_502), fixture_point(25));
    store
}

/// The baseline: the HTTP surface's own request under its own admitted
/// constants, computed directly against the flow.
fn baseline(store: &FixtureStore, subject: EntityId) -> runtime_trail_investigation::Investigation {
    investigate_trace_bounded(
        store,
        &TraceInvestigationRequest::new(subject, admitted_budget(), None),
        admitted_chain(),
    )
    .expect("the fixture trace investigates")
}

fn root_arguments() -> JsonValue {
    json!({
        "root_span": {
            "span": {
                "trace_id": hex(&[TRACE; 16]),
                "span_id": hex(&span_id_of(1).as_bytes()),
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn the_trace_tool_answers_the_http_surfaces_envelope() {
    let store = parity_store();
    let root = span_entity(TRACE, 1);

    let tool = tools::investigate_trace(&store, &root_arguments()).expect("the tool answers");
    let expected = baseline(&store, root);
    assert_eq!(
        tool, expected,
        "the tool and the flow admit the same envelope"
    );

    // The rendered JSON carries the field paths the server's own HTTP
    // round-trip test asserts (mirroring investigation_http.rs tests).
    let json = render::render_investigation(&tool);
    assert!(
        json.get("error").is_none(),
        "no error in the envelope: {json}"
    );
    // Subject: requested and effective agree.
    assert_eq!(
        json["subject"]["requested"]["root_span"]["span"]["trace_id"],
        hex(&[TRACE; 16])
    );
    assert_eq!(json["subject"]["effective"]["root"]["name"], "root");
    // Evidence: the waterfall, the related logs, the in-window points.
    let spans = json["evidence"]["spans"].as_array().expect("span views");
    assert_eq!(spans.len(), 2);
    assert_eq!(spans[0]["span"]["name"], "root");
    assert_eq!(spans[1]["span"]["name"], "child");
    assert_eq!(
        spans[1]["span"]["parent_span_id"],
        hex(&span_id_of(1).as_bytes())
    );
    let logs = json["evidence"]["logs"].as_array().expect("log views");
    assert_eq!(
        logs.len(),
        1_024,
        "the page budget holds the first run to its page's worth — 1 024 of the 1 500 resident logs become evidence"
    );
    assert_eq!(logs[0]["log"]["body"], "related");
    let points = json["evidence"]["points"].as_array().expect("point views");
    assert_eq!(points.len(), 2);
    assert_eq!(points[0]["point"]["time_unix_nano"], 15);
    assert_eq!(points[0]["stream"]["name"], "cpu.seconds");
    // Execution: three groups in part order; the one-page parts run once,
    // the log walk pages (see the multi-page test).
    let groups = json["execution"]["run_groups"]
        .as_array()
        .expect("run groups");
    assert_eq!(groups.len(), 3);
    assert_eq!(groups[0]["part"], "spans");
    assert_eq!(groups[1]["part"], "related_logs");
    assert_eq!(groups[2]["part"], "surrounding_metrics");
    for group in groups.iter().take(1).chain(groups.iter().skip(2)) {
        let runs = group["runs"].as_array().expect("runs");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["outcome"]["kind"], "complete");
    }
    // The metric window is stated.
    assert_eq!(
        json["execution"]["flow_coverage"][0]["kind"],
        "metric_window"
    );
    // Limits: the admitted budget mirrored, the chain totals honest.
    assert_eq!(json["limits"]["chain"]["total_pages"], 4);
    assert_eq!(json["limits"]["chain"]["total_entities"], 1_028);
    assert_eq!(json["limits"]["chain"]["stopped"], JsonValue::Null);
    assert_eq!(json["limits"]["budget"]["max_results"], 1_000);
    assert_eq!(json["limits"]["budget"]["deadline_ms"], 30_000);
    assert_eq!(json["limits"]["budget"]["max_bytes"], 4 * 1024 * 1024);
    assert_eq!(json["limits"]["budget"]["max_scan"], 100_000);
    assert_eq!(
        json["limits"]["budget"]["max_aggregation_memory"],
        4_194_304
    );
}

#[test]
fn structured_model_values_render_verbatim() {
    let store = parity_store();
    let tool = tools::investigate_trace(&store, &root_arguments()).expect("the tool answers");
    let json = render::render_investigation(&tool);
    let logs = json["evidence"]["logs"].as_array().expect("log views");
    let bodies: Vec<&JsonValue> = logs.iter().map(|log| &log["log"]["body"]).collect();
    assert!(
        bodies.contains(&&json!(["alpha", "beta", "gamma"])),
        "an array body renders as the full JSON array: {json}"
    );
    assert!(
        bodies.contains(&&json!({ "name": "e2e", "attempts": 3, "tags": ["x", "y"] })),
        "a key-value-list body renders as the full JSON object: {json}"
    );
    assert!(
        bodies.contains(&&json!([{ "a": 1 }, { "b": "two" }])),
        "nesting through arrays of key-value lists renders intact: {json}"
    );
    assert!(
        bodies.contains(&&json!("deadbeef")),
        "bytes render as hex, deliberately: {json}"
    );
    assert!(
        bodies.contains(&&json!(i64::MAX)),
        "a 64-bit integer renders untruncated: {json}"
    );
    assert!(
        bodies.contains(&&json!(3.5)),
        "a double renders untruncated: {json}"
    );
    let long_scalar = bodies
        .iter()
        .filter_map(|body| body.as_str())
        .max_by_key(|text| text.len())
        .expect("a scalar string body is present");
    assert_eq!(
        long_scalar.len(),
        600,
        "a long scalar body renders whole: {json}"
    );
    assert!(
        long_scalar.chars().all(|char| char == 'x'),
        "the long scalar content is intact: {json}"
    );
}

#[test]
fn a_related_logs_walk_pages_and_reports_continuation_cursors() {
    let store = parity_store();
    let tool = tools::investigate_trace(&store, &root_arguments()).expect("the tool answers");
    let json = render::render_investigation(&tool);
    let groups = json["execution"]["run_groups"]
        .as_array()
        .expect("run groups");
    let log_group = &groups[1];
    assert_eq!(log_group["part"], "related_logs");
    let runs = log_group["runs"].as_array().expect("runs");
    assert_eq!(
        runs.len(),
        2,
        "1 500 related logs walk two engine runs under the page budget: {json}"
    );
    assert_eq!(
        runs[0]["outcome"]["kind"], "degraded",
        "the first page holds to its budget and says so: {json}"
    );
    assert_eq!(
        runs[0]["outcome"]["truncation"]["dimension"], "results",
        "the degradation names its dimension: {json}"
    );
    assert_eq!(runs[0]["outcome"]["truncation"]["omitted"], 0);
    assert_eq!(runs[1]["outcome"]["kind"], "complete");
    let first_cursor = runs[0]["next_cursor"]["hex"]
        .as_str()
        .expect("the first page leaves a continuation cursor");
    assert!(
        !first_cursor.is_empty(),
        "the cursor names the page position"
    );
    assert_eq!(
        runs[1]["next_cursor"],
        JsonValue::Null,
        "the final page has nothing to continue with"
    );
}

#[test]
fn a_resident_log_navigates_to_its_trace() {
    let store = parity_store();
    // The first admitted log (serial 1, body "related") carries the child
    // span's context: the tool resolves it to that span, and the flow
    // answers the same envelope its own child-subject baseline does.
    let child = span_entity(TRACE, 2);
    let tool = tools::investigate_log(&store, &json!({ "log": { "assigned": 1 } }))
        .expect("the resident log resolves to its span");
    let expected = baseline(&store, child);
    assert_eq!(
        tool, expected,
        "log navigation is the flow, identically admitted"
    );

    let json = render::render_investigation(&tool);
    assert_eq!(
        json["subject"]["requested"]["root_span"]["span"]["span_id"],
        hex(&span_id_of(2).as_bytes()),
        "the requested subject is the log's own span"
    );
    assert_eq!(
        json["subject"]["effective"]["root"]["name"], "root",
        "the effective subject is the trace's parentless root"
    );
}

#[test]
fn log_and_metric_refusals_mirror_the_http_vocabulary() {
    let store = parity_store();
    // A non-resident log serial is the HTTP surface's 404, word for word.
    let err = tools::investigate_log(&store, &json!({ "log": { "assigned": 777_777 } }))
        .expect_err("a non-resident log must be refused");
    assert!(matches!(err, tools::ToolError::SubjectUnresolved(_)));
    assert!(
        err.to_string()
            .contains("no resident record carries the requested subject"),
        "the refusal names the reason: {err}"
    );
    assert!(
        err.to_string().contains("assigned 777777"),
        "the subject is named: {err}"
    );
    // A resident point is refused cleanly — the model carries no
    // point-to-trace linkage (the strategy set's exemplar relation is
    // pinned not-implemented), and nothing is fabricated.
    let err = tools::investigate_metric(&store, &json!({ "metric": { "assigned": 1_501 } }))
        .expect_err("a resident point with no trace linkage must be refused");
    assert!(
        err.to_string().contains("no trace linkage"),
        "the refusal names the reason: {err}"
    );
    // A non-resident metric serial is the 404 wording.
    let err = tools::investigate_metric(&store, &json!({ "metric": { "assigned": 777_778 } }))
        .expect_err("a non-resident point must be refused");
    assert!(matches!(err, tools::ToolError::SubjectUnresolved(_)));
    assert!(
        err.to_string()
            .contains("no resident record carries the requested subject"),
        "the refusal names the reason: {err}"
    );
    // A span descriptor under the signal keyword is the same flow.
    let log_as_span = tools::investigate_log(
        &store,
        &json!({
            "log": {
                "span": {
                    "trace_id": hex(&[TRACE; 16]),
                    "span_id": hex(&span_id_of(1).as_bytes()),
                }
            }
        }),
    )
    .expect("the span is resident");
    assert_eq!(log_as_span, baseline(&store, span_entity(TRACE, 1)));
}

#[test]
fn continue_investigation_reports_the_same_deterministic_envelope() {
    let store = parity_store();
    let root = span_entity(TRACE, 1);

    // The cursor this surface reports: the first related-log page's
    // continuation cursor, taken from the rendered envelope.
    let first = tools::investigate_trace(&store, &root_arguments()).expect("the tool answers");
    let json = render::render_investigation(&first);
    let cursor = json["execution"]["run_groups"][1]["runs"][0]["next_cursor"]["hex"]
        .as_str()
        .expect("a continuation cursor was reported")
        .to_owned();

    // Re-investigating from that cursor under a fresh admitted budget
    // answers deterministically: the same envelope, the same flow.
    let continued = tools::continue_investigation(
        &store,
        &json!({ "root_span": root_arguments()["root_span"], "cursor": cursor }),
    )
    .expect("a reported cursor is accepted");
    assert_eq!(continued, baseline(&store, root));
    assert_eq!(render::render_investigation(&continued), json);

    // A malformed cursor is refused cleanly — never fabricated past.
    let bad = tools::continue_investigation(
        &store,
        &json!({ "root_span": root_arguments()["root_span"], "cursor": "zz" }),
    )
    .expect_err("a malformed cursor must be refused");
    assert!(
        bad.to_string().contains("lowercase hex"),
        "the refusal names the problem: {bad}"
    );
}
