//! OTLP → model translation: the only place in the core where protobuf
//! becomes telemetry.
//!
//! Every rule here comes from `docs/architecture/telemetry-model.md`:
//! preserve verbatim, refuse what cannot be represented, normalise only at
//! the edge of decode. The translation is by **move**: strings, byte
//! vectors and attribute values decoded by prost are moved into the model
//! record, not copied, so an export's records are built once and then
//! shared (through the ledger's `Arc`s) for the rest of their life.
//!
//! # Wire-presence conventions (decode-time normalisation, never mutation)
//!
//! OTLP is proto3; scalar fields without the `optional` keyword cannot
//! distinguish "absent" from "zero". Where the model has an `Option`, the
//! wire's zero value reads as **absent** — the convention the OTLP
//! specification itself states. Where the wire *can* express presence
//! (`optional` fields, `oneof`s, length-prefixed `bytes`), presence is
//! preserved exactly: a log record whose 16 zero bytes of `trace_id` were
//! explicitly sent keeps the invalid id, verbatim; a histogram `sum` of
//! `-0.0` sent under `optional double` is a value, not an absence.
//!
//! # Profiling-only fields and the refusal rule
//!
//! `AnyValue.string_value_strindex` and `KeyValue.key_strindex` belong to
//! the Profiling signal's string-table encoding. The proto's own receiver
//! contract says non-Profiling receivers treat them as absent. Where that
//! reading leaves a field with nothing to carry — a value-shaped field
//! with no value, or an attribute with no key — the record is **refused**;
//! refusing is the honest alternative to silently admitting a half-empty
//! attribute or dropping what the emitter sent (ADR 0006).

use std::sync::Arc;

use runtime_trail_telemetry_model::metrics::SummaryPoint;
use runtime_trail_telemetry_model::{
    Attributes, EmitterDroppedCounts, Exemplar, ExponentialBuckets, ExponentialHistogramPoint,
    Float, HistogramPoint, InstrumentationScope, LogRecord, MetricNumber, MetricPoint, NumberPoint,
    QuantileValue, Resource, SeverityNumber, Span, SpanEvent, SpanId, SpanKind, SpanLink,
    SpanStatus, SpanStatusCode, StreamIdentity, StreamKind, Temporality, TraceContext, TraceFlags,
    TraceId, TraceState, TraceStateEntry, Value,
};

use crate::otlp::opentelemetry::{
    common::v1 as wire, logs::v1 as wire_logs, metrics::v1 as wire_metrics,
    resource::v1 as wire_resource, trace::v1 as wire_trace,
};
use crate::signal::{RecordRejection, Unrepresentable};

/// A translation result: the model record, or the per-record refusal naming
/// why the bytes cannot cross.
pub(crate) type Translated<T> = Result<T, RecordRejection>;

fn unrepresentable<T>(error: Unrepresentable) -> Translated<T> {
    Err(RecordRejection::Unrepresentable(error))
}

/// The resource and instrumentation scope one envelope carries, shared by
/// every record beneath it — one allocation per envelope level, never one
/// per record (ADR 0008). The pipeline builds it from [`resource`] and
/// [`scope`].
pub(crate) struct Envelope {
    pub(crate) resource: Arc<Resource>,
    pub(crate) scope: Arc<InstrumentationScope>,
}

impl Envelope {
    /// An owned copy of the resource, for the metric path: the model's
    /// [`StreamIdentity`] owns its resource and scope by value, so each
    /// **metric stream** (not each point) clones once; the ledger then
    /// interns the identity and every point of the stream shares that one
    /// `Arc`.
    pub(crate) fn resource_copy(&self) -> Resource {
        (*self.resource).clone()
    }

    /// An owned copy of the scope, for the metric path, at the same cost
    /// shape as [`Envelope::resource_copy`].
    pub(crate) fn scope_copy(&self) -> InstrumentationScope {
        (*self.scope).clone()
    }
}

/// Translates one `Resource` message (with the `schema_url` its envelope
/// carries — the resource message itself carries none). Shared by every
/// record and every scope beneath the envelope. Refuses when the resource
/// carries something the model has no slot for.
pub(crate) fn resource(
    resource: wire_resource::Resource,
    schema_url: &str,
) -> Translated<Arc<Resource>> {
    if !resource.entity_refs.is_empty() {
        return unrepresentable(Unrepresentable::UnsupportedEntityRefs);
    }
    Ok(Arc::new(Resource {
        attributes: attributes(resource.attributes, "resource attribute")?,
        schema_url: present_string(schema_url),
        dropped_attributes_count: resource.dropped_attributes_count,
    }))
}

/// Translates one `InstrumentationScope` message (with its envelope's
/// `schema_url`). Shared by every record beneath the envelope.
pub(crate) fn scope(
    scope: wire::InstrumentationScope,
    schema_url: &str,
) -> Translated<Arc<InstrumentationScope>> {
    Ok(Arc::new(InstrumentationScope {
        name: scope.name,
        version: present_string(&scope.version),
        attributes: attributes(scope.attributes, "scope attribute")?,
        schema_url: present_string(schema_url),
        dropped_attributes_count: scope.dropped_attributes_count,
    }))
}

/// proto3 `string` without the `optional` keyword: `""` is the unset
/// reading, so the model's `None` (absent) is what the wire can produce.
/// The model keeps absent and empty distinct; the wire simply cannot send
/// the empty-present state to this decoder.
fn present_string(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_owned())
}

/// Wire `KeyValue`s → the model's attribute map. Duplicate keys are refused
/// by the model's own construction.
fn attributes(pairs: Vec<wire::KeyValue>, field: &'static str) -> Translated<Attributes> {
    let converted: Vec<(String, Value)> = pairs
        .into_iter()
        .map(|kv| key_value(kv, field))
        .collect::<Translated<_>>()?;
    Attributes::from_pairs(converted).map_err(RecordRejection::DuplicateKey)
}

/// One wire `KeyValue` → one model entry. The key is examined exactly like
/// the value: a key present only as a Profiling string-table reference is
/// read as absent by the proto's own receiver contract, which leaves the
/// attribute keyless — refused, in the same `MissingValue` family as a
/// value-shaped field with no value, naming what is missing.
fn key_value(kv: wire::KeyValue, field: &'static str) -> Translated<(String, Value)> {
    if kv.key_strindex != 0 {
        return unrepresentable(Unrepresentable::MissingValue {
            field: key_field(field),
        });
    }
    Ok((kv.key, wire_value(kv.value, field)?))
}

/// The field name a keyless attribute refuses under: the value path's
/// field name, suffixed with what is missing — the key. The set of field
/// names is closed; a new call site extends this match.
fn key_field(field: &'static str) -> &'static str {
    match field {
        "resource attribute" => "resource attribute key",
        "scope attribute" => "scope attribute key",
        "span attribute" => "span attribute key",
        "span event attribute" => "span event attribute key",
        "span link attribute" => "span link attribute key",
        "log attribute" => "log attribute key",
        "data point attribute" => "data point attribute key",
        "exemplar filtered attribute" => "exemplar filtered attribute key",
        other => other,
    }
}

/// Wire `AnyValue` → the model's value union. Homogeneity and key
/// uniqueness are the model's own laws, refused at construction; an absent
/// or empty `AnyValue` is unrepresentable, not an empty value.
fn wire_value(av: Option<wire::AnyValue>, field: &'static str) -> Translated<Value> {
    use crate::otlp::opentelemetry::common::v1::any_value::Value as WireValue;
    let Some(any) = av else {
        return unrepresentable(Unrepresentable::MissingValue { field });
    };
    match any.value {
        Some(WireValue::StringValue(text)) => Ok(Value::String(text)),
        Some(WireValue::BoolValue(flag)) => Ok(Value::Bool(flag)),
        Some(WireValue::IntValue(integer)) => Ok(Value::Int(integer)),
        Some(WireValue::DoubleValue(double)) => Ok(Value::Double(Float::new(double))),
        Some(WireValue::BytesValue(bytes)) => Ok(Value::Bytes(bytes)),
        Some(WireValue::ArrayValue(array)) => {
            let items: Vec<Value> = array
                .values
                .into_iter()
                .map(|item| wire_value(Some(item), field))
                .collect::<Translated<_>>()?;
            Value::array(items).map_err(RecordRejection::MixedKindArray)
        }
        Some(WireValue::KvlistValue(list)) => {
            let entries: Vec<(String, Value)> = list
                .values
                .into_iter()
                .map(|kv| key_value(kv, field))
                .collect::<Translated<_>>()?;
            Value::kv_list(entries).map_err(RecordRejection::DuplicateKey)
        }
        // Profiling-only string-table reference: absent by the proto's own
        // receiver contract, which leaves this AnyValue empty.
        Some(WireValue::StringValueStrindex(_)) | None => {
            unrepresentable(Unrepresentable::MissingValue { field })
        }
    }
}

/// A trace id the wire sent in full: 16 bytes, verbatim — the all-zero
/// encoding included (invalid, preserved as sent).
fn wire_trace_id(bytes: &[u8], field: &'static str) -> Translated<TraceId> {
    <[u8; TraceId::LENGTH]>::try_from(bytes)
        .map(TraceId::from_bytes)
        .map_err(|_| {
            RecordRejection::Unrepresentable(Unrepresentable::IdLength {
                field,
                expected: TraceId::LENGTH,
                found: bytes.len(),
            })
        })
}

/// A span id the wire sent in full: 8 bytes, verbatim.
fn wire_span_id(bytes: &[u8], field: &'static str) -> Translated<SpanId> {
    <[u8; SpanId::LENGTH]>::try_from(bytes)
        .map(SpanId::from_bytes)
        .map_err(|_| {
            RecordRejection::Unrepresentable(Unrepresentable::IdLength {
                field,
                expected: SpanId::LENGTH,
                found: bytes.len(),
            })
        })
}

/// Span-level ids (a span's own, a link's): proto3 `bytes` default — an
/// absent field decodes empty, and the model reads that as the all-zero
/// encoding, which is exactly the invalid id the model preserves as sent.
/// Explicitly sent zero bytes decode the same way: one invalid id, however
/// the emitter wrote it.
fn span_trace_id(bytes: &[u8]) -> Translated<TraceId> {
    if bytes.is_empty() {
        Ok(TraceId::from_bytes([0; TraceId::LENGTH]))
    } else {
        wire_trace_id(bytes, "span trace_id")
    }
}

fn span_span_id(bytes: &[u8]) -> Translated<SpanId> {
    if bytes.is_empty() {
        Ok(SpanId::from_bytes([0; SpanId::LENGTH]))
    } else {
        wire_span_id(bytes, "span span_id")
    }
}

/// Log-level ids: independently optional, presence carried by the `bytes`
/// length. Absent stays absent; explicitly sent zero bytes stay — the model
/// never fabricates an id to fill an absent half.
fn optional_trace_id(bytes: &[u8]) -> Translated<Option<TraceId>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        wire_trace_id(bytes, "log trace_id").map(Some)
    }
}

fn optional_span_id(bytes: &[u8]) -> Translated<Option<SpanId>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        wire_span_id(bytes, "log span_id").map(Some)
    }
}

/// A parent span id: absent (empty) is a root span; an explicitly sent zero
/// is the zero parent — distinct facts, both preserved.
fn parent_span_id(bytes: &[u8]) -> Translated<Option<SpanId>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        wire_span_id(bytes, "span parent_span_id").map(Some)
    }
}

/// The W3C `trace_state` string → the model's ordered (vendor, value)
/// entries, in the order sent. A member without `=` is not representable as
/// an entry pair; the record is refused rather than silently reshaped.
fn trace_state(raw: &str) -> Translated<TraceState> {
    if raw.is_empty() {
        return Ok(TraceState::default());
    }
    let mut entries = Vec::new();
    for member in raw.split(',') {
        let Some((vendor, value)) = member.split_once('=') else {
            return unrepresentable(Unrepresentable::TraceState {
                raw: raw.to_owned(),
            });
        };
        entries.push(TraceStateEntry {
            vendor: vendor.trim().to_owned(),
            value: value.trim().to_owned(),
        });
    }
    Ok(TraceState::from_entries(entries))
}

/// Span kinds: all six OTLP values, distinct. An unknown enum value is
/// refused — the model has no seventh kind to coerce to.
fn span_kind(value: i32) -> Translated<SpanKind> {
    match value {
        0 => Ok(SpanKind::Unspecified),
        1 => Ok(SpanKind::Internal),
        2 => Ok(SpanKind::Server),
        3 => Ok(SpanKind::Client),
        4 => Ok(SpanKind::Producer),
        5 => Ok(SpanKind::Consumer),
        other => unrepresentable(Unrepresentable::UnknownEnumValue {
            field: "span kind",
            value: other,
        }),
    }
}

fn status_code(value: i32) -> Translated<SpanStatusCode> {
    match value {
        0 => Ok(SpanStatusCode::Unset),
        1 => Ok(SpanStatusCode::Ok),
        2 => Ok(SpanStatusCode::Error),
        other => unrepresentable(Unrepresentable::UnknownEnumValue {
            field: "status code",
            value: other,
        }),
    }
}

/// Aggregation temporality: unspecified reads as absent; the shape law
/// refuses the interval kinds that cannot carry an absent temporality.
fn temporality(value: i32) -> Translated<Option<Temporality>> {
    match value {
        0 => Ok(None),
        1 => Ok(Some(Temporality::Delta)),
        2 => Ok(Some(Temporality::Cumulative)),
        other => unrepresentable(Unrepresentable::UnknownEnumValue {
            field: "aggregation_temporality",
            value: other,
        }),
    }
}

/// A log severity number: absent is absent; out-of-domain values are
/// refused with the value they carried.
fn severity_number(
    value: i32,
) -> Translated<Option<runtime_trail_telemetry_model::SeverityNumber>> {
    if value == 0 {
        return Ok(None);
    }
    let number = u8::try_from(value).map_err(|_| {
        RecordRejection::Unrepresentable(Unrepresentable::UnknownEnumValue {
            field: "severity_number",
            value,
        })
    })?;
    SeverityNumber::try_new(number)
        .map(Some)
        .map_err(RecordRejection::Severity)
}

/// A span, translated verbatim: ids (invalid included), flags at full
/// width, parent presence, name, kind, timestamps, events and links in
/// emitter order, the two-field status, dropped counts, resource and scope.
pub(crate) fn span(proto: wire_trace::Span, envelope: &Envelope) -> Translated<Span> {
    let status = proto.status.unwrap_or_default();
    Ok(Span {
        context: TraceContext {
            trace_id: span_trace_id(&proto.trace_id)?,
            span_id: span_span_id(&proto.span_id)?,
            flags: TraceFlags::new(proto.flags),
            tracestate: trace_state(&proto.trace_state)?,
        },
        parent_span_id: parent_span_id(&proto.parent_span_id)?,
        name: proto.name,
        kind: span_kind(proto.kind)?,
        start_time_unix_nano: proto.start_time_unix_nano,
        end_time_unix_nano: (proto.end_time_unix_nano != 0).then_some(proto.end_time_unix_nano),
        resource: Arc::clone(&envelope.resource),
        scope: Arc::clone(&envelope.scope),
        attributes: attributes(proto.attributes, "span attribute")?,
        emitter_dropped: EmitterDroppedCounts {
            attributes: proto.dropped_attributes_count,
            events: proto.dropped_events_count,
            links: proto.dropped_links_count,
        },
        events: proto
            .events
            .into_iter()
            .map(|event| {
                Ok(SpanEvent {
                    time_unix_nano: (event.time_unix_nano != 0).then_some(event.time_unix_nano),
                    name: event.name,
                    attributes: attributes(event.attributes, "span event attribute")?,
                    dropped_attribute_count: event.dropped_attributes_count,
                })
            })
            .collect::<Translated<Vec<_>>>()?,
        links: proto
            .links
            .into_iter()
            .map(|link| {
                Ok(SpanLink {
                    context: TraceContext {
                        trace_id: span_trace_id(&link.trace_id)?,
                        span_id: span_span_id(&link.span_id)?,
                        flags: TraceFlags::new(link.flags),
                        tracestate: trace_state(&link.trace_state)?,
                    },
                    attributes: attributes(link.attributes, "span link attribute")?,
                    dropped_attribute_count: link.dropped_attributes_count,
                })
            })
            .collect::<Translated<Vec<_>>>()?,
        status: SpanStatus {
            code: status_code(status.code)?,
            message: status.message,
        },
    })
}

/// A log record, translated verbatim: two timestamps, two severities, a
/// full value body, three independent optional trace-context facts, the
/// event name, the dropped count, resource and scope.
pub(crate) fn log_record(
    proto: wire_logs::LogRecord,
    envelope: &Envelope,
) -> Translated<LogRecord> {
    Ok(LogRecord {
        timestamp_unix_nano: (proto.time_unix_nano != 0).then_some(proto.time_unix_nano),
        observed_timestamp_unix_nano: (proto.observed_time_unix_nano != 0)
            .then_some(proto.observed_time_unix_nano),
        severity_number: severity_number(proto.severity_number)?,
        severity_text: present_string(&proto.severity_text),
        body: match proto.body.and_then(|any| any.value) {
            Some(body) => Some(wire_value(
                Some(wire::AnyValue { value: Some(body) }),
                "log body",
            )?),
            None => None,
        },
        resource: Arc::clone(&envelope.resource),
        scope: Arc::clone(&envelope.scope),
        attributes: attributes(proto.attributes, "log attribute")?,
        dropped_attribute_count: proto.dropped_attributes_count,
        trace_id: optional_trace_id(&proto.trace_id)?,
        span_id: optional_span_id(&proto.span_id)?,
        trace_flags: (proto.flags != 0).then_some(TraceFlags::new(proto.flags)),
        event_name: present_string(&proto.event_name),
    })
}

/// One metric stream's identity from a `Metric` message: name, description
/// and unit verbatim (absent is not empty), metadata as an attribute map,
/// resource and scope, and the kind/temporality pair the `data` oneof
/// declares. A metric with no data has no stream kind to be — refused.
pub(crate) fn stream_identity(
    proto: &wire_metrics::Metric,
    envelope: &Envelope,
) -> Translated<StreamIdentity> {
    use crate::otlp::opentelemetry::metrics::v1::metric::Data;
    let (kind, temporality) = match &proto.data {
        Some(Data::Gauge(_)) => (StreamKind::Gauge, None),
        Some(Data::Sum(sum)) => (
            StreamKind::Sum {
                monotonic: sum.is_monotonic,
            },
            temporality(sum.aggregation_temporality)?,
        ),
        Some(Data::Histogram(histogram)) => (
            StreamKind::Histogram,
            temporality(histogram.aggregation_temporality)?,
        ),
        Some(Data::ExponentialHistogram(histogram)) => (
            StreamKind::ExponentialHistogram,
            temporality(histogram.aggregation_temporality)?,
        ),
        Some(Data::Summary(_)) => (StreamKind::Summary, None),
        None => return unrepresentable(Unrepresentable::EmptyMetric),
    };
    Ok(StreamIdentity {
        resource: envelope.resource_copy(),
        scope: envelope.scope_copy(),
        name: proto.name.clone(),
        kind,
        temporality,
    })
}

/// The data points one `Metric`'s `data` oneof carries, translated by
/// shape and **moved** out of the wire message (no per-point copy of wire
/// data). Each element keeps its own result, so one untranslatable point
/// never takes its siblings' positions down with it. The points are
/// wire-coherent with the kind by construction (a `Sum` carries only
/// `NumberDataPoint`s); the shape law the ledger enforces re-checks the
/// pair anyway, so a point cannot bypass it.
pub(crate) fn into_points(proto: wire_metrics::Metric) -> Vec<Translated<MetricPoint>> {
    use crate::otlp::opentelemetry::metrics::v1::metric::Data;
    match proto.data {
        Some(Data::Gauge(gauge)) => gauge.data_points.into_iter().map(number_point).collect(),
        Some(Data::Sum(sum)) => sum.data_points.into_iter().map(number_point).collect(),
        Some(Data::Histogram(histogram)) => histogram
            .data_points
            .into_iter()
            .map(histogram_point)
            .collect(),
        Some(Data::ExponentialHistogram(histogram)) => histogram
            .data_points
            .into_iter()
            .map(exponential_histogram_point)
            .collect(),
        Some(Data::Summary(summary)) => {
            summary.data_points.into_iter().map(summary_point).collect()
        }
        None => Vec::new(),
    }
}

/// The number of data points one `Metric` carries — for the per-export
/// point-count budget, counted before anything is admitted.
pub(crate) fn point_count(proto: &wire_metrics::Metric) -> usize {
    use crate::otlp::opentelemetry::metrics::v1::metric::Data;
    match &proto.data {
        Some(Data::Gauge(gauge)) => gauge.data_points.len(),
        Some(Data::Sum(sum)) => sum.data_points.len(),
        Some(Data::Histogram(histogram)) => histogram.data_points.len(),
        Some(Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
        Some(Data::Summary(summary)) => summary.data_points.len(),
        None => 0,
    }
}

fn number_point(proto: wire_metrics::NumberDataPoint) -> Translated<MetricPoint> {
    use crate::otlp::opentelemetry::metrics::v1::number_data_point::Value;
    let value = match proto.value {
        Some(Value::AsDouble(double)) => MetricNumber::double(double),
        Some(Value::AsInt(integer)) => MetricNumber::int(integer),
        None => {
            return unrepresentable(Unrepresentable::MissingValue {
                field: "data point value",
            });
        }
    };
    Ok(MetricPoint::Number(NumberPoint {
        attributes: attributes(proto.attributes, "data point attribute")?,
        start_time_unix_nano: (proto.start_time_unix_nano != 0)
            .then_some(proto.start_time_unix_nano),
        time_unix_nano: proto.time_unix_nano,
        value,
        flags: proto.flags,
        exemplars: exemplars(proto.exemplars)?,
    }))
}

fn histogram_point(proto: wire_metrics::HistogramDataPoint) -> Translated<MetricPoint> {
    let Ok(start_time_unix_nano) = require_start_time(proto.start_time_unix_nano) else {
        return unrepresentable(Unrepresentable::MissingValue {
            field: "histogram start_time_unix_nano",
        });
    };
    Ok(MetricPoint::Histogram(HistogramPoint {
        attributes: attributes(proto.attributes, "data point attribute")?,
        start_time_unix_nano,
        time_unix_nano: proto.time_unix_nano,
        count: proto.count,
        sum: proto.sum.map(Float::new),
        bucket_counts: proto.bucket_counts,
        explicit_bounds: proto.explicit_bounds.into_iter().map(Float::new).collect(),
        min: proto.min.map(Float::new),
        max: proto.max.map(Float::new),
        flags: proto.flags,
        exemplars: exemplars(proto.exemplars)?,
    }))
}

fn exponential_histogram_point(
    proto: wire_metrics::ExponentialHistogramDataPoint,
) -> Translated<MetricPoint> {
    let Ok(start_time_unix_nano) = require_start_time(proto.start_time_unix_nano) else {
        return unrepresentable(Unrepresentable::MissingValue {
            field: "exponential histogram start_time_unix_nano",
        });
    };
    Ok(MetricPoint::ExponentialHistogram(
        ExponentialHistogramPoint {
            attributes: attributes(proto.attributes, "data point attribute")?,
            start_time_unix_nano,
            time_unix_nano: proto.time_unix_nano,
            count: proto.count,
            sum: proto.sum.map(Float::new),
            scale: proto.scale,
            zero_count: proto.zero_count,
            zero_threshold: Float::new(proto.zero_threshold),
            positive: proto.positive.map_or_else(
                || ExponentialBuckets {
                    offset: 0,
                    bucket_counts: Vec::new(),
                },
                |buckets| ExponentialBuckets {
                    offset: buckets.offset,
                    bucket_counts: buckets.bucket_counts,
                },
            ),
            negative: proto.negative.map_or_else(
                || ExponentialBuckets {
                    offset: 0,
                    bucket_counts: Vec::new(),
                },
                |buckets| ExponentialBuckets {
                    offset: buckets.offset,
                    bucket_counts: buckets.bucket_counts,
                },
            ),
            min: proto.min.map(Float::new),
            max: proto.max.map(Float::new),
            flags: proto.flags,
            exemplars: exemplars(proto.exemplars)?,
        },
    ))
}

fn summary_point(proto: wire_metrics::SummaryDataPoint) -> Translated<MetricPoint> {
    let Ok(start_time_unix_nano) = require_start_time(proto.start_time_unix_nano) else {
        return unrepresentable(Unrepresentable::MissingValue {
            field: "summary start_time_unix_nano",
        });
    };
    Ok(MetricPoint::Summary(SummaryPoint {
        attributes: attributes(proto.attributes, "data point attribute")?,
        start_time_unix_nano,
        time_unix_nano: proto.time_unix_nano,
        count: proto.count,
        // proto3 `double` without `optional`: positive zero is
        // indistinguishable from unset on the wire, so it reads as absent.
        // Negative zero is a distinct encoding — the model's equality law
        // makes -0.0 ≠ 0.0 — and any non-zero or non-finite value is a
        // value: all of them survive with their bits.
        sum: (proto.sum != 0.0 || proto.sum.is_sign_negative()).then_some(Float::new(proto.sum)),
        quantiles: proto
            .quantile_values
            .into_iter()
            .map(|quantile| QuantileValue {
                quantile: Float::new(quantile.quantile),
                value: Float::new(quantile.value),
            })
            .collect(),
        flags: proto.flags,
        // The summary shape on the wire carries no exemplars — the model's
        // slot stays empty, never fabricated.
        exemplars: Vec::new(),
    }))
}

/// Interval kinds require a start time; proto3's `fixed64` reads zero as
/// absent, and an absent start on an interval point is the contract's
/// refused shape — named here, where the wire said it.
fn require_start_time(start_time_unix_nano: u64) -> Result<u64, ()> {
    if start_time_unix_nano == 0 {
        Err(())
    } else {
        Ok(start_time_unix_nano)
    }
}

fn exemplars(
    proto: Vec<wire_metrics::Exemplar>,
) -> Translated<Vec<runtime_trail_telemetry_model::Exemplar>> {
    proto
        .into_iter()
        .map(|exemplar| {
            use crate::otlp::opentelemetry::metrics::v1::exemplar::Value;
            let value = match exemplar.value {
                Some(Value::AsDouble(double)) => MetricNumber::double(double),
                Some(Value::AsInt(integer)) => MetricNumber::int(integer),
                None => {
                    return unrepresentable(Unrepresentable::MissingValue {
                        field: "exemplar value",
                    });
                }
            };
            Ok(Exemplar {
                value,
                time_unix_nano: exemplar.time_unix_nano,
                filtered_attributes: attributes(
                    exemplar.filtered_attributes,
                    "exemplar filtered attribute",
                )?,
                trace_id: optional_trace_id(&exemplar.trace_id)?,
                span_id: optional_span_id(&exemplar.span_id)?,
            })
        })
        .collect()
}
