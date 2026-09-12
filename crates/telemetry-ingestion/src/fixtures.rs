//! Semantic OTLP fixtures: wire messages built as the generated types, then
//! encoded with prost and fed through the pipeline — the same bytes an
//! emitter would put on the wire, minus a transport.
//!
//! Every builder defaults to a legal, minimal message; tests opt into the
//! weirdness they exercise. Timestamps are tiny but non-zero where the wire
//! convention "0 = absent" would otherwise change the meaning.

use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use runtime_trail_telemetry_model::{AdmissionTime, EntityId};

use crate::pipeline::ExportOutcome;
use crate::signal::{RecordOutcome, RecordRejection};

use crate::otlp::opentelemetry::{
    collector::{
        logs::v1 as logs_collector, metrics::v1 as metrics_collector, trace::v1 as trace_collector,
    },
    common::v1 as common,
    logs::v1 as logs,
    metrics::v1 as metrics,
    resource::v1 as resource,
    trace::v1 as trace,
};
use crate::pipeline::Pipeline;
use crate::queue::{
    BoundedQueue, PIPELINE_QUEUE_NAME, QUEUE_CEILING_BYTES, QueuedRecord, RecordSink,
};

/// A pipeline over an in-test bounded queue, ready to ingest.
pub(crate) struct Harness {
    pub pipeline: Arc<Pipeline>,
    pub queue: Arc<BoundedQueue>,
}

impl Harness {
    /// The default harness: contract ceilings everywhere.
    pub(crate) fn new() -> Self {
        Self::with_queue_ceiling(QUEUE_CEILING_BYTES)
    }

    /// A harness whose hand-off queue saturates at `ceiling` accounted
    /// bytes — for the overflow fixtures.
    pub(crate) fn with_queue_ceiling(ceiling: usize) -> Self {
        let queue = BoundedQueue::new(PIPELINE_QUEUE_NAME, ceiling);
        let pipeline = Arc::new(Pipeline::new(Arc::clone(&queue) as Arc<dyn RecordSink>));
        Self { pipeline, queue }
    }

    /// Drains every queued record, front to back.
    pub(crate) fn drain(&self) -> Vec<QueuedRecord> {
        let mut drained = Vec::new();
        while let Some(record) = self.queue.pop_timeout(Duration::ZERO) {
            drained.push(record);
        }
        drained
    }
}

/// The admission time every fixture uses.
pub(crate) fn now() -> AdmissionTime {
    AdmissionTime::from_unix_nano(42)
}

// ---------------------------------------------------------------- values

pub(crate) fn str_value(text: &str) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::StringValue(text.to_owned())),
    }
}

pub(crate) fn int_value(value: i64) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::IntValue(value)),
    }
}

pub(crate) fn double_value(value: f64) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::DoubleValue(value)),
    }
}

pub(crate) fn bytes_value(bytes: Vec<u8>) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::BytesValue(bytes)),
    }
}

pub(crate) fn array_value(items: Vec<common::AnyValue>) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::ArrayValue(common::ArrayValue {
            values: items,
        })),
    }
}

pub(crate) fn kvlist_value(entries: Vec<common::KeyValue>) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::KvlistValue(
            common::KeyValueList { values: entries },
        )),
    }
}

/// An `AnyValue` with no kind set — the empty value.
pub(crate) fn empty_value() -> common::AnyValue {
    common::AnyValue { value: None }
}

pub(crate) fn attr(key: &str, value: common::AnyValue) -> common::KeyValue {
    common::KeyValue {
        key: key.to_owned(),
        value: Some(value),
        key_strindex: 0,
    }
}

// ------------------------------------------------------------- envelopes

pub(crate) fn resource(attributes: Vec<common::KeyValue>) -> resource::Resource {
    resource::Resource {
        attributes,
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    }
}

pub(crate) fn scope(name: &str) -> common::InstrumentationScope {
    common::InstrumentationScope {
        name: name.to_owned(),
        version: String::new(),
        attributes: Vec::new(),
        dropped_attributes_count: 0,
    }
}

pub(crate) fn trace_span(name: &str, trace_id: [u8; 16], span_id: [u8; 8]) -> trace::Span {
    trace::Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        trace_state: String::new(),
        parent_span_id: Vec::new(),
        flags: 0,
        name: name.to_owned(),
        kind: trace::span::SpanKind::Server as i32,
        start_time_unix_nano: 1,
        end_time_unix_nano: 2,
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        events: Vec::new(),
        dropped_events_count: 0,
        links: Vec::new(),
        dropped_links_count: 0,
        status: None,
    }
}

pub(crate) fn scope_spans(
    scope: Option<common::InstrumentationScope>,
    spans: Vec<trace::Span>,
) -> trace::ScopeSpans {
    trace::ScopeSpans {
        scope,
        spans,
        schema_url: String::new(),
    }
}

pub(crate) fn resource_spans(
    resource: Option<resource::Resource>,
    scope_spans: Vec<trace::ScopeSpans>,
) -> trace::ResourceSpans {
    trace::ResourceSpans {
        resource,
        scope_spans,
        schema_url: String::new(),
    }
}

pub(crate) fn traces_request(
    resource_spans: Vec<trace::ResourceSpans>,
) -> trace_collector::ExportTraceServiceRequest {
    trace_collector::ExportTraceServiceRequest { resource_spans }
}

pub(crate) fn log_record() -> logs::LogRecord {
    logs::LogRecord {
        time_unix_nano: 0,
        observed_time_unix_nano: 0,
        severity_number: 0,
        severity_text: String::new(),
        body: None,
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: Vec::new(),
        span_id: Vec::new(),
        event_name: String::new(),
    }
}

pub(crate) fn resource_logs(
    resource: Option<resource::Resource>,
    scope_logs: Vec<logs::ScopeLogs>,
) -> logs::ResourceLogs {
    logs::ResourceLogs {
        resource,
        scope_logs,
        schema_url: String::new(),
    }
}

pub(crate) fn scope_logs(
    scope: Option<common::InstrumentationScope>,
    log_records: Vec<logs::LogRecord>,
) -> logs::ScopeLogs {
    logs::ScopeLogs {
        scope,
        log_records,
        schema_url: String::new(),
    }
}

pub(crate) fn logs_request(
    resource_logs: Vec<logs::ResourceLogs>,
) -> logs_collector::ExportLogsServiceRequest {
    logs_collector::ExportLogsServiceRequest { resource_logs }
}

pub(crate) fn metric(name: &str, data: metrics::metric::Data) -> metrics::Metric {
    metrics::Metric {
        name: name.to_owned(),
        description: String::new(),
        unit: String::new(),
        metadata: Vec::new(),
        data: Some(data),
    }
}

/// A gauge metric carrying its descriptor: the name plus the description,
/// unit and metadata the stream identity carries since the descriptor
/// joined it.
pub(crate) fn described_metric(
    name: &str,
    description: &str,
    unit: &str,
    metadata: Vec<common::KeyValue>,
    points: Vec<metrics::NumberDataPoint>,
) -> metrics::Metric {
    metrics::Metric {
        name: name.to_owned(),
        description: description.to_owned(),
        unit: unit.to_owned(),
        metadata,
        data: Some(metrics::metric::Data::Gauge(metrics::Gauge {
            data_points: points,
        })),
    }
}

pub(crate) fn number_point(value: metrics::number_data_point::Value) -> metrics::NumberDataPoint {
    metrics::NumberDataPoint {
        attributes: Vec::new(),
        start_time_unix_nano: 1,
        time_unix_nano: 10,
        exemplars: Vec::new(),
        flags: 0,
        value: Some(value),
    }
}

pub(crate) fn gauge(points: Vec<metrics::NumberDataPoint>) -> metrics::metric::Data {
    metrics::metric::Data::Gauge(metrics::Gauge {
        data_points: points,
    })
}

/// A delta/cumulative toggle for sum fixtures.
pub(crate) fn sum(
    temporality: i32,
    monotonic: bool,
    points: Vec<metrics::NumberDataPoint>,
) -> metrics::metric::Data {
    metrics::metric::Data::Sum(metrics::Sum {
        data_points: points,
        aggregation_temporality: temporality,
        is_monotonic: monotonic,
    })
}

/// A double measurement.
pub(crate) fn as_double(value: f64) -> metrics::number_data_point::Value {
    metrics::number_data_point::Value::AsDouble(value)
}

/// An integer measurement.
pub(crate) fn as_int(value: i64) -> metrics::number_data_point::Value {
    metrics::number_data_point::Value::AsInt(value)
}

pub(crate) fn scope_metrics(
    scope: Option<common::InstrumentationScope>,
    metrics_list: Vec<metrics::Metric>,
) -> metrics::ScopeMetrics {
    metrics::ScopeMetrics {
        scope,
        metrics: metrics_list,
        schema_url: String::new(),
    }
}

pub(crate) fn resource_metrics(
    resource: Option<resource::Resource>,
    scope_metrics: Vec<metrics::ScopeMetrics>,
) -> metrics::ResourceMetrics {
    metrics::ResourceMetrics {
        resource,
        scope_metrics,
        schema_url: String::new(),
    }
}

pub(crate) fn metrics_request(
    resource_metrics: Vec<metrics::ResourceMetrics>,
) -> metrics_collector::ExportMetricsServiceRequest {
    metrics_collector::ExportMetricsServiceRequest { resource_metrics }
}

// ------------------------------------------------------------------ wire

/// Encodes a fixture exactly as an emitter would put it on the wire.
pub(crate) fn encode<M: Message>(message: &M) -> Vec<u8> {
    message.encode_to_vec()
}

// -------------------------------------------------------------- assertions

/// A valid trace id that is not zero.
pub(crate) const T1: [u8; 16] = [0x01; 16];
/// A second valid trace id.
pub(crate) const T2: [u8; 16] = [0x02; 16];
/// A valid span id that is not zero.
pub(crate) const S1: [u8; 8] = [0x11; 8];
/// A second valid span id.
pub(crate) const S2: [u8; 8] = [0x22; 8];

/// The refusal reason recorded at `position`, panicking with the whole
/// outcome if that record was not refused.
pub(crate) fn rejected_reason(outcome: &ExportOutcome, position: usize) -> &RecordRejection {
    match &outcome.records[position] {
        RecordOutcome::Rejected { reason } => reason,
        other => panic!("record {position} was not rejected: {other:?}"),
    }
}

/// The entity id standing at `position` (admitted or collapsed), panicking
/// with the whole outcome if the record has neither.
pub(crate) fn standing_entity(outcome: &ExportOutcome, position: usize) -> EntityId {
    match &outcome.records[position] {
        RecordOutcome::Admitted { entity }
        | RecordOutcome::Collapsed { entity }
        | RecordOutcome::Conflict { entity } => *entity,
        other @ RecordOutcome::Rejected { .. } => {
            panic!("record {position} has no standing entity: {other:?}")
        }
    }
}

/// Builds a protobuf varint (for the hand-crafted wire fixtures).
pub(crate) fn varint(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(value & 0x7F).expect("a masked varint byte fits u8");
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// One length-delimited field: tag, length, payload.
pub(crate) fn field(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    varint(payload.len(), &mut out);
    out.extend_from_slice(payload);
    out
}
