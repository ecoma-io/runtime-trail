//! Real OTLP wire bytes, encoded from scratch.
//!
//! The probes measure the real memory path, so their payloads are the real
//! thing: protobuf-encoded `ExportMetricsServiceRequest` /
//! `ExportLogsServiceRequest` messages, byte-valid under the OTLP schemas
//! vendored in `crates/telemetry-ingestion/protos/` — the pipeline's own
//! decoder (`prost`) is what proves it, since every payload a probe sends
//! must decode or the probe dies loudly.
//!
//! Ingestion deliberately exposes its pipeline, not its wire types (the
//! prost-generated `otlp` module and its fixtures are `pub(crate)`), so
//! these probes carry a small writer for exactly the three export requests
//! they emit. Field numbers are pinned by the vendored `.proto` sources,
//! and the semantic tests run every payload shape through the real
//! pipeline and count what came back, because "it decoded" is not the
//! assertion — "it admitted exactly this many records" is. No encoder
//! dependency is added to do this (`AGENTS.md`: every dependency needs an
//! architectural justification; ~150 lines over `std::Vec` is the smaller
//! dependency).
//!
//! # Shapes emitted
//!
//! - **gauge export** — one stream (`Metric{name}` → one
//!   `StreamIdentity`), at-cap data points (10,000 = the per-export cap
//!   exactly), each point carrying distinct attributes and timestamps so
//!   no delivery collapses onto another, under an attribute-heavy
//!   resource. This is the heaviest legal per-export load the wire can
//!   carry.
//! - **log export** — plain log records (severity, body, a few
//!   attributes). Log records have no natural identity, so every record is
//!   a fresh admission — the record kind the retention ceilings alone
//!   bound.
//! - **trace export** — one trace per export: a parentless root span
//!   followed by its children, each span carrying a distinct id and
//!   timestamp so no delivery collapses, under the same attribute-heavy
//!   resource. This is the served-runtime probes' investigation subject,
//!   and the workload the query-budget probe saturates a chain with.

/// The wire type of a protobuf field, as the format states it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireType {
    Varint,
    Fixed64,
    Len,
}

impl WireType {
    const fn tag_value(self) -> u64 {
        match self {
            Self::Varint => 0,
            Self::Fixed64 => 1,
            Self::Len => 2,
        }
    }
}

/// The accounted-byte payload ceiling the pipeline gates exports with —
/// `runtime_trail_telemetry_model::budgets::OTLP_PAYLOAD_BYTES`, the
/// contract's 4 MiB, restated here only as the shape's target.
pub const PAYLOAD_CEILING_BYTES: usize = 4 * 1024 * 1024;

/// The per-export data-point cap the pipeline gates metrics with (the
/// contract's 10,000; exactly at-cap is legal, over refuses the whole
/// export).
pub const POINTS_PER_EXPORT_CAP: usize = 10_000;

/// A protobuf writer for the fields the two export requests use.
///
/// A `Wire` is one message body: fields are appended in order, nested
/// messages are built as their own `Wire` and embedded with
/// [`Wire::message`]. Repeated fields are just fields written more than
/// once, in order — the format's own encoding.
#[derive(Debug, Default)]
pub struct Wire {
    buf: Vec<u8>,
}

impl Wire {
    /// An empty message body.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The encoded bytes so far.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// The encoded length so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether nothing has been encoded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Converts into the raw bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    fn varint(&mut self, mut value: u64) {
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                self.buf.push(byte);
                break;
            }
            self.buf.push(byte | 0x80);
        }
    }

    fn key(&mut self, field: u32, wire_type: WireType) {
        self.varint((u64::from(field) << 3) | wire_type.tag_value());
    }

    /// One length-delimited field: tag, length, payload bytes.
    pub fn bytes(&mut self, field: u32, payload: &[u8]) {
        self.key(field, WireType::Len);
        self.varint(payload.len() as u64);
        self.buf.extend_from_slice(payload);
    }

    /// One `string` field.
    pub fn string(&mut self, field: u32, text: &str) {
        self.bytes(field, text.as_bytes());
    }

    /// One varint scalar (integers, enums, booleans).
    pub fn uint64(&mut self, field: u32, value: u64) {
        self.key(field, WireType::Varint);
        self.varint(value);
    }

    /// One `fixed64`/`sfixed64`/`double` scalar (timestamps, `as_int`).
    pub fn fixed64(&mut self, field: u32, value: u64) {
        self.key(field, WireType::Fixed64);
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    /// One nested message field: `message`'s body is embedded verbatim.
    pub fn message(&mut self, field: u32, body: &Wire) {
        self.bytes(field, body.as_slice());
    }
}

/// Writes one `Resource` body: an attribute-heavy attribute list, field 1
/// repeated `KeyValue`.
pub fn encode_resource(out: &mut Wire, attributes: &[(String, String)]) {
    for (key, value) in attributes {
        let mut pair = Wire::new();
        pair.string(1, key);
        let mut any = Wire::new();
        any.string(1, value);
        pair.message(2, &any);
        out.message(1, &pair);
    }
}

/// Writes one `InstrumentationScope` body: name (field 1), no attributes.
pub fn encode_scope(out: &mut Wire, name: &str) {
    out.string(1, name);
}

/// One gauge data point's fields, as [`encode_gauge_export`] generates
/// them.
#[derive(Clone, Copy, Debug)]
pub struct GeneratedPoint {
    /// `start_time_unix_nano` — a real SDK sends one even on a gauge.
    pub start_time_unix_nano: u64,
    /// `time_unix_nano` — distinct per point, so no delivery collapses.
    pub time_unix_nano: u64,
    /// The `as_int` measurement.
    pub value: i64,
}

/// Encodes one gauge export as an `ExportMetricsServiceRequest`.
///
/// One `Metric` named `metric_name` carrying `shape.point_count` data
/// points under `resource_attributes` and `scope_name`; point attributes
/// are minted by `point_attribute` from the export index and the point's
/// position, so every point identity is distinct and every delivery is a
/// fresh admission, never a collapse.
///
/// # Panics
///
/// When the result would not be a legal export under the contract
/// ceilings (over the payload ceiling, or over the per-export point cap):
/// [`assert_within_ceilings`] fires. A probe that shipped an over-ceiling
/// payload would measure the refuse path and report it as saturation —
/// that is a probe bug, and the probe stops instead of measuring one.
pub fn encode_gauge_export(
    shape: &GaugeShape,
    export_index: u64,
    point_attribute: impl Fn(u64, u64) -> Vec<(String, String)>,
) -> Wire {
    let mut points = Wire::new();
    for k in 0..shape.point_count {
        let k = u64::try_from(k).unwrap_or(u64::MAX);
        let point = GeneratedPoint {
            start_time_unix_nano: shape.base_time_unix_nano + k,
            time_unix_nano: shape.base_time_unix_nano + shape.point_count as u64 + k,
            value: i64::try_from(k).unwrap_or(i64::MAX),
        };
        let mut encoded = Wire::new();
        // NumberDataPoint: start_time = 2, time = 3, as_int = 6, attributes = 7.
        encoded.fixed64(2, point.start_time_unix_nano);
        encoded.fixed64(3, point.time_unix_nano);
        encoded.fixed64(6, u64::try_from(point.value).unwrap_or(u64::MAX));
        for (key, value) in point_attribute(export_index, k) {
            let mut pair = Wire::new();
            pair.string(1, &key);
            let mut any = Wire::new();
            any.string(1, &value);
            pair.message(2, &any);
            encoded.message(7, &pair);
        }
        // Gauge.data_points = 1, repeated.
        points.message(1, &encoded);
    }

    let mut metric = Wire::new();
    metric.string(1, &(shape.metric_name)(export_index));
    // Metric.gauge = 5 (the `data` oneof).
    metric.message(5, &points);

    let mut scope_metrics = Wire::new();
    let mut scope = Wire::new();
    encode_scope(&mut scope, &shape.scope_name);
    scope_metrics.message(1, &scope);
    scope_metrics.message(2, &metric);

    let mut resource_metrics = Wire::new();
    let mut resource = Wire::new();
    encode_resource(&mut resource, &shape.resource_attributes);
    resource_metrics.message(1, &resource);
    resource_metrics.message(2, &scope_metrics);

    let mut request = Wire::new();
    request.message(1, &resource_metrics);

    assert_within_ceilings(request.len(), shape.point_count);
    request
}

/// Encodes one log export as an `ExportLogsServiceRequest`:
/// `shape.record_count` records under one resource and scope, each record
/// carrying a distinct body and timestamp.
///
/// # Panics
///
/// As [`encode_gauge_export`] when the payload is not a legal export.
pub fn encode_log_export(
    shape: &LogShape,
    export_index: u64,
    record_attribute: impl Fn(u64, u64) -> Vec<(String, String)>,
) -> Wire {
    // ScopeLogs carries the scope (field 1, an InstrumentationScope
    // message) and the records as repeated field-2 entries written
    // straight into its body — log_records is a repeated field, so each
    // record is one entry, not a nested wrapper.
    let mut scope_logs = Wire::new();
    let mut scope = Wire::new();
    encode_scope(&mut scope, &shape.scope_name);
    scope_logs.message(1, &scope);
    for k in 0..shape.record_count {
        let k = u64::try_from(k).unwrap_or(u64::MAX);
        let mut record = Wire::new();
        // LogRecord: time = 1, severity_number = 2, severity_text = 3,
        // body = 5, attributes = 6, observed_time = 11.
        record.fixed64(1, shape.base_time_unix_nano + k);
        record.uint64(2, u64::from(shape.severity_number));
        record.string(3, &shape.severity_text);
        let mut body = Wire::new();
        body.string(1, &(shape.body)(export_index, k));
        record.message(5, &body);
        for (key, value) in record_attribute(export_index, k) {
            let mut pair = Wire::new();
            pair.string(1, &key);
            let mut any = Wire::new();
            any.string(1, &value);
            pair.message(2, &any);
            record.message(6, &pair);
        }
        record.fixed64(11, shape.base_time_unix_nano + k);
        if let Some(context) = shape.trace_context {
            record.bytes(9, &context.trace_id);
            record.bytes(10, &context.span_id);
        }
        scope_logs.message(2, &record);
    }

    let mut resource_logs = Wire::new();
    let mut resource = Wire::new();
    encode_resource(&mut resource, &shape.resource_attributes);
    resource_logs.message(1, &resource);
    resource_logs.message(2, &scope_logs);

    let mut request = Wire::new();
    request.message(1, &resource_logs);

    assert_within_ceilings(request.len(), 0);
    request
}
/// Encodes one trace export as an `ExportTraceServiceRequest`:
/// `shape.span_count` spans under one resource and scope.
///
/// The span of record `k` of export `export_index` is minted by
/// `shape.span`; its attributes by `span_attribute`, so every span
/// identity is distinct and every delivery is a fresh admission, never a
/// collapse.
///
/// # Panics
///
/// As [`encode_gauge_export`] when the result would not be a legal export.
pub fn encode_trace_export(
    shape: &TraceShape,
    export_index: u64,
    span_attribute: impl Fn(u64, u64) -> Vec<(String, String)>,
) -> Wire {
    // ScopeSpans: scope = 1 (an InstrumentationScope message), spans = 2
    // (repeated Span fields written straight into its body).
    let mut scope_spans = Wire::new();
    let mut scope = Wire::new();
    encode_scope(&mut scope, &shape.scope_name);
    scope_spans.message(1, &scope);
    for k in 0..shape.span_count {
        let k = u64::try_from(k).unwrap_or(u64::MAX);
        let span = (shape.span)(export_index, k);
        let mut record = Wire::new();
        // Span: trace_id = 1, span_id = 2, parent_span_id = 4, name = 5,
        // kind = 6, start = 7, end = 8, attributes = 9.
        record.bytes(1, &span.trace_id);
        record.bytes(2, &span.span_id);
        if span.parent_span_id != [0; 8] {
            record.bytes(4, &span.parent_span_id);
        }
        record.string(5, &span.name);
        record.uint64(6, 1); // SPAN_KIND_INTERNAL
        record.fixed64(7, span.start_time_unix_nano);
        record.fixed64(8, span.end_time_unix_nano);
        for (key, value) in span_attribute(export_index, k) {
            let mut pair = Wire::new();
            pair.string(1, &key);
            let mut any = Wire::new();
            any.string(1, &value);
            pair.message(2, &any);
            record.message(9, &pair);
        }
        scope_spans.message(2, &record);
    }

    let mut resource_spans = Wire::new();
    let mut resource = Wire::new();
    encode_resource(&mut resource, &shape.resource_attributes);
    resource_spans.message(1, &resource);
    resource_spans.message(2, &scope_spans);

    let mut request = Wire::new();
    request.message(1, &resource_spans);

    assert_within_ceilings(request.len(), shape.span_count);
    request
}

/// Mints the attribute list of one record: keyed by its export index and
/// position, so identities stay distinct across a run.
pub type AttributeMinter = Box<dyn Fn(u64, u64) -> Vec<(String, String)>>;

/// The trace context a log record may carry: the OTLP log record's
/// `trace_id` (field 9) and `span_id` (field 10), which make the record
/// evidence of a specific span — the related-logs evidence the served
/// runtime's investigations surface. Absent by default: the retention
/// probe's records are deliberately context-free.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceContext {
    /// The 16-byte trace id.
    pub trace_id: [u8; 16],
    /// The 8-byte span id.
    pub span_id: [u8; 8],
}

/// The per-record payload shape of the log export.
pub struct LogShape {
    /// Resource attributes, shared by every record.
    pub resource_attributes: Vec<(String, String)>,
    /// The instrumentation scope's name.
    pub scope_name: String,
    /// The severity number every record carries (in 1–24).
    pub severity_number: u8,
    /// The severity text every record carries.
    pub severity_text: String,
    /// Records per export.
    pub record_count: usize,
    /// The body text of record `k` of export `export_index`.
    pub body: Box<dyn Fn(u64, u64) -> String>,
    /// Per-record attributes, minted per record.
    pub record_attribute: AttributeMinter,
    /// The base admission-time-class timestamp records count up from.
    pub base_time_unix_nano: u64,
    /// The trace context every record carries, when the workload's logs
    /// belong to a traced span (absent for the context-free retention
    /// records).
    pub trace_context: Option<TraceContext>,
}

impl std::fmt::Debug for LogShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogShape")
            .field("record_count", &self.record_count)
            .field("severity_number", &self.severity_number)
            .field("base_time_unix_nano", &self.base_time_unix_nano)
            .field("trace_context", &self.trace_context)
            .finish_non_exhaustive()
    }
}

/// The per-export payload shape of the gauge export.
pub struct GaugeShape {
    /// Resource attributes, shared by every point (and charged to every
    /// stream identity in full — the accounting under measurement).
    pub resource_attributes: Vec<(String, String)>,
    /// The instrumentation scope's name.
    pub scope_name: String,
    /// The metric name of export `export_index` — distinct per export, so
    /// each export is its own stream.
    pub metric_name: Box<dyn Fn(u64) -> String>,
    /// Data points per export (at the per-export cap, never over).
    pub point_count: usize,
    /// The base timestamp points count up from.
    pub base_time_unix_nano: u64,
}

impl std::fmt::Debug for GaugeShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GaugeShape")
            .field("point_count", &self.point_count)
            .field("base_time_unix_nano", &self.base_time_unix_nano)
            .finish_non_exhaustive()
    }
}
/// One generated span's fields, as [`encode_trace_export`] emits them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedSpan {
    /// The 16-byte trace id (field 1) — every span of one trace carries
    /// the same, so the exporter can spread one trace across exports.
    pub trace_id: [u8; 16],
    /// The 8-byte span id (field 2) — distinct per span.
    pub span_id: [u8; 8],
    /// The 8-byte parent span id (field 4); all-zero = no parent (the
    /// trace's effective root).
    pub parent_span_id: [u8; 8],
    /// The span's name (field 5).
    pub name: String,
    /// `start_time_unix_nano` (field 7, fixed64).
    pub start_time_unix_nano: u64,
    /// `end_time_unix_nano` (field 8, fixed64).
    pub end_time_unix_nano: u64,
}

/// The per-export payload shape of the trace export.
pub struct TraceShape {
    /// Resource attributes, shared by every span.
    pub resource_attributes: Vec<(String, String)>,
    /// The instrumentation scope's name.
    pub scope_name: String,
    /// Spans per export.
    pub span_count: usize,
    /// The span of record `k` of export `export_index`.
    pub span: Box<dyn Fn(u64, u64) -> GeneratedSpan>,
    /// Per-span attributes, minted per span.
    pub span_attribute: AttributeMinter,
    /// The base timestamp spans count up from.
    pub base_time_unix_nano: u64,
}

impl std::fmt::Debug for TraceShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceShape")
            .field("span_count", &self.span_count)
            .field("base_time_unix_nano", &self.base_time_unix_nano)
            .finish_non_exhaustive()
    }
}

/// Fails loudly when an export would not be legal under the contract
/// ceilings: the wire bytes over the payload ceiling, or the point count
/// over the per-export cap.
///
/// # Panics
///
/// As documented on [`encode_gauge_export`]: the probe stops instead of
/// measuring a payload the pipeline is contract-bound to refuse.
pub fn assert_within_ceilings(payload_bytes: usize, points: usize) {
    assert!(
        payload_bytes <= PAYLOAD_CEILING_BYTES,
        "generated payload is {payload_bytes} bytes, over the {PAYLOAD_CEILING_BYTES}-byte \
         OTLP payload ceiling: this probe would measure the refuse path, not the memory path"
    );
    assert!(
        points <= POINTS_PER_EXPORT_CAP,
        "generated export carries {points} points, over the {POINTS_PER_EXPORT_CAP} per-export \
         cap: the pipeline would refuse the whole export"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shapes::{GAUGE_VALUE_CHARS, gauge_shape, log_shape};
    use runtime_trail_telemetry_ingestion::{
        AdmissionSignal, BoundedQueue, ExportOutcome, PIPELINE_QUEUE_NAME, Pipeline,
        QUEUE_CEILING_BYTES, RecordSink,
    };
    use runtime_trail_telemetry_model::AdmissionTime;

    /// The contract harness: a contract-ceiling queue under a
    /// contract-budget pipeline.
    fn harness() -> (std::sync::Arc<Pipeline>, std::sync::Arc<BoundedQueue>) {
        let queue = BoundedQueue::new(PIPELINE_QUEUE_NAME, QUEUE_CEILING_BYTES);
        let pipeline = std::sync::Arc::new(Pipeline::new(
            std::sync::Arc::clone(&queue) as std::sync::Arc<dyn RecordSink>
        ));
        (pipeline, queue)
    }

    fn now() -> AdmissionTime {
        AdmissionTime::from_unix_nano(1)
    }

    #[test]
    fn varint_encoding_is_minimal() {
        let mut wire = Wire::new();
        wire.uint64(1, 0);
        assert_eq!(wire.as_slice(), &[0x08, 0x00]);
        wire.uint64(1, 300);
        // 300 = 0xAC 0x02 in varint, tag 0x08 for field 1 varint.
        assert_eq!(wire.as_slice(), &[0x08, 0x00, 0x08, 0xAC, 0x02]);
    }

    /// Semantic fixture law: the generated gauge payload is not merely
    /// decodable — the real pipeline admits exactly the points it carries.
    #[test]
    fn the_pipeline_admits_every_generated_gauge_point() {
        let shape = gauge_shape();
        let payload = encode_gauge_export(&shape, 0, crate::shapes::gauge_point_attributes);
        assert_within_ceilings(payload.len(), shape.point_count);

        let (pipeline, queue) = harness();
        let outcome: ExportOutcome = pipeline
            .ingest_metrics(now(), payload.as_slice())
            .expect("a legal payload is admitted");
        assert_eq!(outcome.len(), shape.point_count, "one outcome per point");
        assert_eq!(
            outcome.admitted(),
            shape.point_count,
            "every point admitted"
        );
        assert_eq!(outcome.rejected(), 0);
        assert_eq!(queue.len(), shape.point_count, "every admission queued");
        assert!(queue.accounted_bytes() > 0, "the queue accounts in bytes");
    }

    /// Semantic fixture law for the log path.
    #[test]
    fn the_pipeline_admits_every_generated_log_record() {
        let shape = log_shape();
        let payload = encode_log_export(&shape, 0, crate::shapes::log_record_attributes);

        let (pipeline, queue) = harness();
        let outcome = pipeline
            .ingest_logs(now(), payload.as_slice())
            .expect("a legal payload is admitted");
        assert_eq!(outcome.len(), shape.record_count);
        assert_eq!(outcome.admitted(), shape.record_count);
        assert_eq!(outcome.rejected(), 0);
        assert_eq!(queue.len(), shape.record_count);
    }

    #[test]
    fn the_at_cap_gauge_payload_stays_under_the_payload_ceiling() {
        let shape = gauge_shape();
        assert_eq!(
            shape.point_count, POINTS_PER_EXPORT_CAP,
            "the probe shape is at the per-export cap, never over"
        );
        let payload = encode_gauge_export(&shape, 0, crate::shapes::gauge_point_attributes);
        assert!(
            payload.len() <= PAYLOAD_CEILING_BYTES,
            "payload {} bytes is over the {}-byte ceiling",
            payload.len(),
            PAYLOAD_CEILING_BYTES
        );
    }

    /// A second, distinct export never collapses onto the first — and a
    /// second *full* at-cap export cannot even fit beside the first in
    /// the contract queue: the ceiling refuses it with `QueueSaturated`,
    /// which is the boundedness under measurement, not a fixture defect.
    #[test]
    fn export_index_distinguishes_payloads_byte_for_byte() {
        let shape = gauge_shape();
        let first = encode_gauge_export(&shape, 0, crate::shapes::gauge_point_attributes);
        let second = encode_gauge_export(&shape, 1, crate::shapes::gauge_point_attributes);
        assert_ne!(first.as_slice(), second.as_slice());

        let (pipeline, queue) = harness();
        pipeline
            .ingest_metrics(now(), first.as_slice())
            .expect("the first export admits");
        assert_eq!(queue.len(), shape.point_count);
        assert!(queue.accounted_bytes() <= QUEUE_CEILING_BYTES);

        let error = pipeline
            .ingest_metrics(now(), second.as_slice())
            .expect_err("a second full export cannot fit beside the first in a bounded queue");
        assert!(
            matches!(error, AdmissionSignal::QueueSaturated { .. }),
            "the queue ceiling refuses the overload: {error:?}"
        );
        assert!(
            queue.accounted_bytes() <= QUEUE_CEILING_BYTES,
            "the ceiling holds after the refusal"
        );
    }

    #[test]
    fn attribute_values_differ_per_point_and_per_export() {
        let first = crate::shapes::gauge_point_attributes(0, 7);
        let second = crate::shapes::gauge_point_attributes(0, 8);
        let other_export = crate::shapes::gauge_point_attributes(1, 7);
        assert_ne!(first, second);
        assert_ne!(first, other_export);
        assert!(
            first
                .iter()
                .all(|(key, value)| key.starts_with("bench.attr.")
                    && value.len() >= GAUGE_VALUE_CHARS),
            "generated attributes are fixed-width and namespaced: {first:?}"
        );
    }

    #[test]
    fn malformed_bytes_are_refused_not_panicked_on() {
        let (pipeline, _queue) = harness();
        let error = pipeline
            .ingest_metrics(now(), &[0xFF, 0xFF, 0xFF])
            .expect_err("garbage is not an export");
        assert!(matches!(error, AdmissionSignal::MalformedRequest { .. }));
    }

    #[test]
    fn the_ceiling_guard_rejects_an_illegal_shape() {
        assert!(
            std::panic::catch_unwind(|| assert_within_ceilings(PAYLOAD_CEILING_BYTES + 1, 1))
                .is_err(),
            "an over-ceiling payload must stop the probe"
        );
        assert!(
            std::panic::catch_unwind(|| assert_within_ceilings(1, POINTS_PER_EXPORT_CAP + 1))
                .is_err(),
            "an over-cap point count must stop the probe"
        );
    }
}
