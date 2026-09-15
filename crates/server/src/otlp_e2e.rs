//! End-to-end acceptance for the real OTLP producer path (issue #4): the
//! real OpenTelemetry SDK exporting over HTTP/protobuf into the served
//! composition root, through storage-memory's bounded store, and out of
//! the Investigation API.
//!
//! Nothing in this module is mocked. The exporter is the real
//! `opentelemetry-otlp` HTTP client, the receiver is the real router, the
//! store is the real in-memory driver wired by the composition root. The
//! wire fixtures under `tests/fixtures/` are the raw HTTP request bodies a
//! real SDK emission produced once, captured byte-for-byte on a loopback
//! listener ([`capture_otlp_wire_fixtures_when_run`]); the replay test
//! posts those captured bytes verbatim through the receiver and asserts
//! the same admission outcome the live export produced.
//!
//! The store is read through the layer-storage abstraction — the same
//! scan primitives the query engine pages with — because the layer-app
//! boundary rows forbid `crates/server` from importing layer-query
//! directly; the query engine's view of the same records is asserted
//! through the Investigation API envelope, its consumer-visible door.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use opentelemetry::Context;
use opentelemetry::logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity};
use opentelemetry::trace::{Span, SpanKind, TraceContextExt, Tracer, TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use prost::Message;
use runtime_trail_storage::PointView;
use runtime_trail_telemetry_ingestion::{
    ExportLogsServiceRequest, ExportMetricsServiceRequest, ExportTraceServiceRequest,
};
use runtime_trail_telemetry_model::metrics::{MetricNumber, MetricPoint};
use runtime_trail_telemetry_model::values::Value;
use runtime_trail_telemetry_model::{LogRecord as StoredLogRecord, Span as StoredSpan};
use tokio::sync::oneshot;

use crate::runtime::CoreRuntime;
use crate::{ServerConfig, build_router};

/// The three OTLP/HTTP export endpoints, exactly as the server serves them.
const TRACES_PATH: &str = "/v1/traces";
const LOGS_PATH: &str = "/v1/logs";
const METRICS_PATH: &str = "/v1/metrics";
/// The Investigation API's trace endpoint.
const INVESTIGATION_TRACES_PATH: &str = "/v1/investigations/traces";
/// The one content-type this server speaks on the OTLP surfaces.
const PROTOBUF_MEDIA_TYPE: &str = "application/x-protobuf";

/// The fixed names and values the producers emit: the assertions read the
/// store for exactly these, and the committed fixtures were captured from
/// the same fixed emissions — the replay is deterministic over the wire
/// format, whatever the SDK's random trace/span ids happened to be.
const SPAN_NAME: &str = "e2e.root";
const CHILD_SPAN_NAME: &str = "e2e.child";
const LOG_BODY: &str = "the e2e log body";
const METRIC_NAME: &str = "e2e.requests";
const METRIC_VALUE: u64 = 7;

/// The spans' time window, in seconds on each side of the emission
/// instant: wide enough that the metric point collected during the same
/// run is inside `[min_start, max_end)`, the investigation's surrounding
/// window, no matter how the producer and the collection interleave.
const SPAN_WINDOW_SECS: u64 = 30;

/// The committed wire captures and their fixture files.
const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
const FIXTURE_FILES: [(&str, &str); 3] = [
    (TRACES_PATH, "otlp-traces.bin"),
    (LOGS_PATH, "otlp-logs.bin"),
    (METRICS_PATH, "otlp-metrics.bin"),
];

/// The real SDK, run against a served runtime: polls until the pump has
/// made `expected` records resident, with a hard bound so a stuck pump
/// fails the test instead of hanging it.
fn wait_for_resident(runtime: &CoreRuntime, expected: u64) {
    for _ in 0..5_000 {
        if runtime.store_stats().resident_records == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!(
        "residency never reached {expected}: {:?}",
        runtime.store_stats()
    );
}

/// A served composition root on an ephemeral loopback port: the real
/// router, the real accept loop, the real store behind it.
struct Served {
    runtime: Arc<CoreRuntime>,
    addr: SocketAddr,
    trigger: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl Served {
    /// Ends the session: the graceful-shutdown trigger drains the runtime
    /// and the task joins cleanly.
    async fn stop(self) {
        self.trigger
            .send(())
            .expect("the server is still listening");
        self.server
            .await
            .expect("the server task joins")
            .expect("the server serves cleanly");
        self.runtime.shutdown();
    }
}

/// Binds an ephemeral loopback port and serves the real router on it.
async fn serve() -> Served {
    let runtime = crate::test_support::runtime();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("an ephemeral bind");
    let addr = listener.local_addr().expect("an ephemeral address");
    let router = build_router(Arc::clone(&runtime), ServerConfig::default());
    let (trigger, gate) = oneshot::channel::<()>();
    let drain_runtime = Arc::clone(&runtime);
    let serve = axum::serve(listener, router).with_graceful_shutdown(async move {
        let _ = gate.await;
        drain_runtime.begin_drain();
    });
    let server = tokio::spawn(async move { serve.await });
    Served {
        runtime,
        addr,
        trigger,
        server,
    }
}

/// What the real SDK emitted, as the tests address it: the root span's
/// trace and span ids in the lowercase hex the Investigation API parses.
struct Produced {
    trace_id_hex: String,
    root_span_id_hex: String,
}
/// The exporters one real ecosystem probes with, built against `base` URL
/// of the served runtime and speaking OTLP/HTTP protobuf on all three
/// signal paths.
///
/// The OTLP HTTP exporter treats a programmatically supplied endpoint as
/// the full request URL (`resolve_http_endpoint` uses it verbatim; only
/// generic/default endpoints get their signal path appended), so each
/// exporter is pointed at the complete `{base}{signal_path}` URL up
/// front.
#[must_use]
fn build_providers(
    base: &str,
) -> (
    opentelemetry_sdk::trace::SdkTracerProvider,
    opentelemetry_sdk::logs::SdkLoggerProvider,
    opentelemetry_sdk::metrics::SdkMeterProvider,
) {
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}{TRACES_PATH}"))
        .build()
        .expect("the trace exporter builds for HTTP/protobuf");
    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}{LOGS_PATH}"))
        .build()
        .expect("the log exporter builds for HTTP/protobuf");
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}{METRICS_PATH}"))
        .build()
        .expect("the metric exporter builds for HTTP/protobuf");

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .build();
    let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_log_processor(opentelemetry_sdk::logs::SimpleLogProcessor::new(
            log_exporter,
        ))
        .build();
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(
            opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter)
                .with_interval(Duration::from_secs(60))
                .build(),
        )
        .build();

    (tracer_provider, logger_provider, meter_provider)
}

/// One real SDK emission: a root span and its child, a log record riding
/// the root's trace context, and a counter point — flushed to `endpoint`,
/// exactly as a producer would. Returns the root's identity.
fn emit_spans_log_and_point(endpoint: &str) -> Produced {
    use opentelemetry::metrics::MeterProvider;
    let (tracer_provider, logger_provider, meter_provider) = build_providers(endpoint);
    let tracer = tracer_provider.tracer("e2e-producer");
    let logger = logger_provider.logger("e2e-producer");
    let meter = meter_provider.meter("e2e-producer");

    let now = SystemTime::now();
    let start = now - Duration::from_secs(SPAN_WINDOW_SECS);
    let end = now + Duration::from_secs(SPAN_WINDOW_SECS);

    let root = tracer
        .span_builder(SPAN_NAME)
        .with_kind(SpanKind::Server)
        .with_start_time(start)
        .start(&tracer);
    let root_context = root.span_context().clone();
    let produced = Produced {
        trace_id_hex: root_context.trace_id().to_string(),
        root_span_id_hex: root_context.span_id().to_string(),
    };
    let parent_cx = Context::current_with_span(root);
    let guard = parent_cx.clone().attach();

    let mut child = tracer
        .span_builder(CHILD_SPAN_NAME)
        .with_kind(SpanKind::Internal)
        .with_start_time(start)
        .start_with_context(&tracer, &parent_cx);

    // The log record is emitted inside the root span's context: the SDK
    // attaches the trace context it rides with, so the investigation's
    // related-log walk can correlate it to the trace.
    let mut record = logger.create_log_record();
    record.set_severity_number(Severity::Info);
    record.set_body(AnyValue::String(LOG_BODY.into()));
    logger.emit(record);

    child.end_with_timestamp(end);
    drop(guard);
    drop(parent_cx);

    let counter = meter
        .u64_counter(METRIC_NAME)
        .with_description("the e2e request counter")
        .build();
    counter.add(METRIC_VALUE, &[]);

    tracer_provider
        .force_flush()
        .expect("the SDK's trace export is accepted");
    tracer_provider
        .shutdown()
        .expect("the tracer provider shuts down cleanly");
    logger_provider
        .shutdown()
        .expect("the logger provider shuts down cleanly");
    meter_provider
        .shutdown()
        .expect("the meter provider shuts down cleanly");
    produced
}

/// The resident set, read through the storage abstraction: one scan per
/// signal kind, in residency order.
#[must_use]
fn resident_records(
    runtime: &CoreRuntime,
) -> (
    Vec<Arc<StoredSpan>>,
    Vec<Arc<StoredLogRecord>>,
    Vec<PointView>,
) {
    let store = runtime.lock_store_for_test();
    let spans = store
        .scan_spans(None, 64)
        .items
        .into_iter()
        .map(|item| item.record)
        .collect();
    let logs = store
        .scan_log_records(None, 64)
        .items
        .into_iter()
        .map(|item| item.record)
        .collect();
    let points = store
        .scan_metric_points(None, 64)
        .items
        .into_iter()
        .map(|item| item.record)
        .collect();
    (spans, logs, points)
}

/// One HTTP/1.1 request over a real TCP connection; the whole response as
/// bytes (headers plus body).
async fn http_exchange(bind: SocketAddr, request: &str, body: &[u8]) -> std::io::Result<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(bind).await?;
    let head = format!(
        "{request}\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(response)
}

/// Asks the Investigation API about a trace over the served socket and
/// returns the parsed envelope.
#[must_use]
async fn investigate_trace(
    addr: SocketAddr,
    trace_id_hex: &str,
    span_id_hex: &str,
) -> serde_json::Value {
    let body = format!(
        r#"{{"root_span": {{"span": {{"trace_id": "{trace_id_hex}", "span_id": "{span_id_hex}"}}}}}}"#
    );
    let response = http_exchange(
        addr,
        &format!("POST {INVESTIGATION_TRACES_PATH} HTTP/1.1\r\nContent-Type: application/json"),
        body.as_bytes(),
    )
    .await
    .expect("the investigation answers on the served socket");
    let response = String::from_utf8_lossy(&response);
    let (_head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("the answer carries a body: {response}"));
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "the investigation admits the trace: {response}"
    );
    serde_json::from_str(body).unwrap_or_else(|_| panic!("the envelope is JSON: {response}"))
}

/// Lowercase hex, the case the Investigation API parses.
#[must_use]
fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

/// The real SDK's traces, logs AND metrics all admitted into the bounded
/// store over the wire: the store's scan primitives return exactly the
/// records the SDK sent, with the fields that identify them.
#[tokio::test]
async fn the_real_sdk_exports_admit_spans_logs_and_points_over_http() {
    let served = serve().await;
    let produced = tokio::task::spawn_blocking({
        let addr = served.addr;
        move || emit_spans_log_and_point(&format!("http://{addr}"))
    })
    .await
    .expect("the SDK producer completes");

    wait_for_resident(&served.runtime, 4);
    assert_eq!(served.runtime.store_stats().resident_records, 4);

    let (spans, logs, points) = resident_records(&served.runtime);
    assert_eq!(spans.len(), 2, "root and child arrive as stored spans");
    assert_eq!(logs.len(), 1, "one log record arrives");
    assert_eq!(points.len(), 1, "one metric point arrives");

    let root = spans
        .iter()
        .find(|span| span.name == SPAN_NAME)
        .expect("the SDK's root span is resident");
    let child = spans
        .iter()
        .find(|span| span.name == CHILD_SPAN_NAME)
        .expect("the SDK's child span is resident");
    assert_eq!(root.parent_span_id, None, "the root has no parent");
    assert_eq!(
        child.parent_span_id,
        Some(root.context.span_id),
        "the child names the root as its parent"
    );
    assert_eq!(
        hex_encode(&root.context.trace_id.as_bytes()),
        produced.trace_id_hex,
        "the stored trace id is the produced one"
    );

    let log = &logs[0];
    assert_eq!(
        log.body,
        Some(Value::String(LOG_BODY.to_owned())),
        "the SDK's log body is stored verbatim"
    );
    assert_eq!(
        log.trace_id,
        Some(root.context.trace_id),
        "the log rides the root's trace context"
    );

    let point = &points[0];
    assert_eq!(
        point.stream.name, METRIC_NAME,
        "the SDK's metric stream is stored under its name"
    );
    match point.point.as_ref() {
        MetricPoint::Number(number) => assert_eq!(
            number.value,
            MetricNumber::Int(i64::try_from(METRIC_VALUE).expect("7 fits an i64")),
            "the SDK's counter value is stored"
        ),
        other => panic!("the SDK's counter exports a number point, got {other:?}"),
    }

    served.stop().await;
}

/// The capture harness: a plain loopback listener that reads the raw HTTP
/// requests the real SDK emits (headers plus body, byte for byte) and
/// writes the request body of each signal to `tests/fixtures/otlp-*.bin`.
///
/// This is the fixture regeneration path — run explicitly, never in the
/// suite:
///
/// ```text
/// cargo test -p runtime-trail-server -- --ignored capture_otlp_wire_fixtures_when_run
/// ```
#[test]
#[ignore = "regenerates tests/fixtures/otlp-*.bin from one loopback capture; run once per SDK bump"]
fn capture_otlp_wire_fixtures_when_run() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("an ephemeral bind for the capture listener");
    let addr = listener.local_addr().expect("an ephemeral address");
    let capture = std::thread::spawn(
        move || -> std::io::Result<std::collections::BTreeMap<String, Vec<u8>>> {
            let mut captured = std::collections::BTreeMap::new();
            while captured.len() < 3 {
                let (mut stream, _) = listener.accept()?;
                let (request_line, body) = read_http_request(&mut stream)?;
                let path = request_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_owned();
                captured.insert(path, body);
                stream.write_all(otlp_ok_answer())?;
            }
            Ok(captured)
        },
    );

    let _produced = emit_spans_log_and_point(&format!("http://{addr}"));
    let captured = capture
        .join()
        .expect("the capture thread joins")
        .expect("the capture reads every export request");

    std::fs::create_dir_all(FIXTURES_DIR).expect("the fixtures directory is created");
    for (path, file) in FIXTURE_FILES {
        let bytes = captured
            .get(path)
            .unwrap_or_else(|| panic!("the capture received a {path} export: {captured:?}"));
        assert!(!bytes.is_empty(), "a {path} export carries a body");
        std::fs::write(format!("{FIXTURES_DIR}/{file}"), bytes).expect("the fixture is written");
    }
}

/// Reads one HTTP/1.1 request off a raw connection: the head through the
/// blank line, then exactly `Content-Length` body bytes. The request line
/// and the body are returned.
fn read_http_request(stream: &mut std::net::TcpStream) -> std::io::Result<(String, Vec<u8>)> {
    let mut head = Vec::new();
    let mut probe = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let read = stream.read(&mut probe)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the request head was cut short",
            ));
        }
        head.push(probe[0]);
    }
    let head_text = String::from_utf8_lossy(&head);
    let request_line = head_text.lines().next().unwrap_or_default().to_owned();
    let content_length = head_text
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(str::trim)
                .and_then(|value| value.parse::<usize>().ok())
        })
        .unwrap_or(0);
    let mut body = vec![0_u8; content_length];
    stream.read_exact(&mut body)?;
    Ok((request_line, body))
}

/// The minimal answer a real OTLP exporter accepts: 200, the protobuf
/// media type, an empty (default) response message.
#[must_use]
const fn otlp_ok_answer() -> &'static [u8] {
    b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
}

/// The committed fixtures replay through the receiver, verbatim, into the
/// same admission the live export produced: the store carries exactly the
/// records the fixture bytes encode, and those records investigate.
#[tokio::test]
async fn the_committed_wire_fixtures_replay_to_the_same_admission() {
    let mut fixture_bytes = Vec::new();
    for (path, file) in FIXTURE_FILES {
        let bytes = std::fs::read(format!("{FIXTURES_DIR}/{file}"))
            .unwrap_or_else(|error| panic!("the committed fixture {file} exists: {error}"));
        assert!(!bytes.is_empty(), "{file} carries body bytes");
        let path = path.to_owned();
        fixture_bytes.push((path, bytes));
    }

    let served = serve().await;
    for (path, bytes) in &fixture_bytes {
        let response = http_exchange(
            served.addr,
            &format!("POST {path} HTTP/1.1\r\nContent-Type: {PROTOBUF_MEDIA_TYPE}"),
            bytes,
        )
        .await
        .expect("the replay is answered");
        let response = String::from_utf8_lossy(&response);
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "the fixture replays into {path}: {response}"
        );
    }
    wait_for_resident(&served.runtime, 4);
    assert_eq!(served.runtime.store_stats().resident_records, 4);

    // The store's read-back equals the fixtures' own content — the same
    // records the live capture admitted, by the same bytes.
    let (spans, logs, points) = resident_records(&served.runtime);
    let trace_bytes = fixture_bytes
        .iter()
        .find(|(path, _)| path == TRACES_PATH)
        .map(|(_, bytes)| bytes)
        .expect("the trace fixture was loaded");
    let mut fixture_spans = fixture_span_names(trace_bytes);
    fixture_spans.sort();
    let mut stored_spans = spans
        .iter()
        .map(|span| span.name.clone())
        .collect::<Vec<_>>();
    stored_spans.sort();
    assert_eq!(
        stored_spans, fixture_spans,
        "the replayed spans are the captured spans"
    );
    assert_eq!(stored_spans.len(), 2, "root and child are both there");

    let log_bytes = fixture_bytes
        .iter()
        .find(|(path, _)| path == LOGS_PATH)
        .map(|(_, bytes)| bytes)
        .expect("the log fixture was loaded");
    assert_eq!(
        logs[0].body,
        Some(Value::String(fixture_log_body(log_bytes))),
        "the replayed log body is the captured log body"
    );

    let metric_bytes = fixture_bytes
        .iter()
        .find(|(path, _)| path == METRICS_PATH)
        .map(|(_, bytes)| bytes)
        .expect("the metric fixture was loaded");
    let (fixture_metric_name, fixture_metric_value) = fixture_metric(metric_bytes);
    assert_eq!(points[0].stream.name, fixture_metric_name);
    match points[0].point.as_ref() {
        MetricPoint::Number(number) => {
            assert_eq!(number.value, MetricNumber::Int(fixture_metric_value));
        }
        other => panic!("the captured counter replays a number point, got {other:?}"),
    }

    // The captured bytes investigate: the root the SDK actually emitted
    // resolves through the Investigation API end to end.
    let fixture_root = fixture_root_identity(trace_bytes);
    let json = investigate_trace(served.addr, &fixture_root.0, &fixture_root.1).await;
    assert!(
        json.get("error").is_none(),
        "no error in the envelope: {json}"
    );
    assert_eq!(json["subject"]["effective"]["root"]["name"], SPAN_NAME);
    assert_eq!(
        json["evidence"]["spans"]
            .as_array()
            .expect("span views")
            .len(),
        2
    );
    let groups = json["execution"]["run_groups"]
        .as_array()
        .expect("run groups");
    assert_eq!(groups.len(), 3);
    for group in groups {
        let runs = group["runs"].as_array().expect("runs");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["outcome"]["kind"], "complete");
    }
    assert_eq!(json["limits"]["chain"]["total_entities"], 4);

    served.stop().await;
}

/// The real SDK's trace investigates over the served Investigation API:
/// the envelope's subject resolves to the emitted root, its evidence views
/// resolve to the stored spans/log/point, the run groups all complete, and
/// the limits report real spend.
#[tokio::test]
async fn the_real_sdk_trace_investigates_over_http() {
    let served = serve().await;
    let produced = tokio::task::spawn_blocking({
        let addr = served.addr;
        move || emit_spans_log_and_point(&format!("http://{addr}"))
    })
    .await
    .expect("the SDK producer completes");

    wait_for_resident(&served.runtime, 4);
    let json = investigate_trace(
        served.addr,
        &produced.trace_id_hex,
        &produced.root_span_id_hex,
    )
    .await;

    assert!(
        json.get("error").is_none(),
        "no error in the envelope: {json}"
    );
    // Subject: requested and effective agree, and the effective root is
    // the span the SDK actually emitted.
    assert_eq!(
        json["subject"]["requested"]["root_span"]["span"]["trace_id"],
        produced.trace_id_hex.as_str()
    );
    assert_eq!(json["subject"]["effective"]["root"]["name"], SPAN_NAME);
    // Evidence: the waterfall, the related log, the in-window point — all
    // resolving to the stored records.
    let spans = json["evidence"]["spans"].as_array().expect("span views");
    assert_eq!(spans.len(), 2);
    assert!(
        spans.iter().any(|span| span["span"]["name"] == SPAN_NAME),
        "the root view: {spans:?}"
    );
    assert!(
        spans.iter().any(|span| {
            span["span"]["name"] == CHILD_SPAN_NAME
                && span["span"]["parent_span_id"] == produced.root_span_id_hex.as_str()
        }),
        "the child view names the root as parent: {spans:?}"
    );
    let logs = json["evidence"]["logs"].as_array().expect("log views");
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0]["log"]["body"], LOG_BODY);
    let points = json["evidence"]["points"].as_array().expect("point views");
    assert_eq!(points.len(), 1);
    assert_eq!(points[0]["stream"]["name"], METRIC_NAME);
    // Execution: three parts, one complete run each.
    let groups = json["execution"]["run_groups"]
        .as_array()
        .expect("run groups");
    assert_eq!(groups.len(), 3);
    for (index, part) in ["spans", "related_logs", "surrounding_metrics"]
        .iter()
        .enumerate()
    {
        assert_eq!(groups[index]["part"], *part);
        let runs = groups[index]["runs"].as_array().expect("runs");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["outcome"]["kind"], "complete");
    }
    // Limits: the chain totals are the real spend — four resident records
    // across three one-page walks — and nothing was cut off.
    assert_eq!(json["limits"]["chain"]["total_pages"], 3);
    assert_eq!(json["limits"]["chain"]["total_entities"], 4);
    assert_eq!(json["limits"]["chain"]["stopped"], serde_json::Value::Null);
    assert!(
        json["limits"]["budget"]["max_results"]
            .as_u64()
            .is_some_and(|max| max >= 4),
        "the budget admits the real spend: {json}"
    );

    served.stop().await;
}

/// The span names one OTLP trace export carries, as the fixture encodes
/// them.
#[must_use]
fn fixture_span_names(bytes: &[u8]) -> Vec<String> {
    let request =
        ExportTraceServiceRequest::decode(bytes).expect("the trace fixture decodes as OTLP");
    request
        .resource_spans
        .iter()
        .flat_map(|resource| resource.scope_spans.iter())
        .flat_map(|scope| scope.spans.iter())
        .map(|span| span.name.clone())
        .collect()
}

/// The log body one OTLP log export carries, as the fixture encodes it.
#[must_use]
fn fixture_log_body(bytes: &[u8]) -> String {
    let request = ExportLogsServiceRequest::decode(bytes).expect("the log fixture decodes as OTLP");
    let record = request
        .resource_logs
        .iter()
        .flat_map(|resource| resource.scope_logs.iter())
        .flat_map(|scope| scope.log_records.iter())
        .next()
        .expect("the captured log export carries a record");
    // The value's oneof enum (common.v1 `any_value`) lives under the
    // ingestion crate's `pub(crate)` OTLP module, which layer-app may not
    // name (module-boundaries law); the read-back therefore compares the
    // derived Debug representation the fixture carries against the derived
    // representation of the expected string — equal repr if, and only if,
    // the variant and payload match.
    let body = record
        .body
        .as_ref()
        .expect("the captured log record carries a body");
    assert!(
        format!("{body:?}").contains(&format!("StringValue({LOG_BODY:?})")),
        "the captured log body is the e2e string, got {body:?}"
    );
    LOG_BODY.to_owned()
}

/// The (name, value) of the metric one OTLP metric export carries, as the
/// fixture encodes them.
#[must_use]
fn fixture_metric(bytes: &[u8]) -> (String, i64) {
    let request =
        ExportMetricsServiceRequest::decode(bytes).expect("the metric fixture decodes as OTLP");
    let metric = request
        .resource_metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics.iter())
        .flat_map(|scope| scope.metrics.iter())
        .next()
        .expect("the captured metric export carries a metric");
    let data = metric.data.as_ref().expect("the metric carries data");
    // The metric oneof enums (`metric::Data`, `number_data_point::Value`)
    // sit in the same unnameable OTLP module: verify the emitted
    // counter's payload through its derived Debug representation instead.
    let debug = format!("{data:?}");
    assert!(
        debug.contains(&format!("AsInt({METRIC_VALUE})")),
        "the captured counter exports the e2e value as an int, got {debug:?}"
    );
    (
        metric.name.clone(),
        i64::try_from(METRIC_VALUE).expect("the e2e value fits an i64"),
    )
}

/// The root span's (trace id, span id) in lowercase hex, from the trace
/// fixture the SDK actually emitted.
#[must_use]
fn fixture_root_identity(bytes: &[u8]) -> (String, String) {
    let request =
        ExportTraceServiceRequest::decode(bytes).expect("the trace fixture decodes as OTLP");
    let root = request
        .resource_spans
        .iter()
        .flat_map(|resource| resource.scope_spans.iter())
        .flat_map(|scope| scope.spans.iter())
        .find(|span| span.parent_span_id.is_empty())
        .expect("the captured trace has a root span");
    (hex_encode(&root.trace_id), hex_encode(&root.span_id))
}
