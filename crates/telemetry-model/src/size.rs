//! Accounted size — the single definition byte ceilings count.
//!
//! `docs/architecture/telemetry-model.md` ("Information budgets") defines a
//! record's accounted size as the bytes of its value payloads, its
//! attribute keys and structure names, plus a fixed per-structure overhead,
//! so that everything variable-length is counted and byte ceilings have one
//! definition no matter which storage mode enforces them. This module is
//! that definition, in one place. It carries no numbers from
//! `runtime-constraints.md` — only the counting rule their ceilings
//! consume.
//!
//! The formula, exactly:
//!
//! - booleans count 1 byte; 64-bit integers and doubles count 8 bytes;
//! - strings and byte strings count their payload byte length;
//! - arrays count [`STRUCTURE_FIXED_BYTES`] plus their items;
//! - key-value lists count [`STRUCTURE_FIXED_BYTES`] plus, per entry,
//!   [`STRUCTURE_FIXED_BYTES`] + key bytes + value size;
//! - attribute maps count, per entry, the same as a key-value list entry;
//! - tracestate entries count [`STRUCTURE_FIXED_BYTES`] + vendor bytes +
//!   value bytes;
//! - each record, event, link, exemplar, quantile, bucket layout and
//!   stream identity part counts [`STRUCTURE_FIXED_BYTES`] plus its own
//!   fixed-width fields and variable-length parts above.

/// The fixed overhead charged once per structure — the constant that makes
/// the count include structure, not only payloads.
pub const STRUCTURE_FIXED_BYTES: usize = 16;

/// Anything whose accounted size the model can state.
pub trait Accounted {
    /// The accounted size in bytes, by the formula in the module docs.
    #[must_use]
    fn accounted_size(&self) -> usize;
}

/// The accounted size of one attribute entry: fixed per-entry overhead +
/// key bytes + value size. This is the spend the attribute-value budget
/// measures (keys and names count).
#[must_use]
pub fn attribute_entry_size(key: &str, value: &crate::values::Value) -> usize {
    STRUCTURE_FIXED_BYTES + key.len() + value.accounted_size()
}

impl Accounted for crate::values::Float {
    fn accounted_size(&self) -> usize {
        8
    }
}

impl Accounted for crate::values::PrimitiveValue {
    fn accounted_size(&self) -> usize {
        match self {
            Self::String(text) => text.len(),
            Self::Bool(_) => 1,
            Self::Int(_) | Self::Double(_) => 8,
            Self::Bytes(bytes) => bytes.len(),
        }
    }
}

impl Accounted for crate::values::HomogeneousArray {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + self.iter().map(Accounted::accounted_size).sum::<usize>()
    }
}

impl Accounted for crate::values::KeyValueList {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self
                .iter()
                .map(|(key, value)| attribute_entry_size(key, value))
                .sum::<usize>()
    }
}

impl Accounted for crate::values::Value {
    fn accounted_size(&self) -> usize {
        match self {
            Self::String(text) => text.len(),
            Self::Bool(_) => 1,
            Self::Int(_) | Self::Double(_) => 8,
            Self::Bytes(bytes) => bytes.len(),
            Self::Array(array) => array.accounted_size(),
            Self::KvList(list) => list.accounted_size(),
        }
    }
}

impl Accounted for crate::values::Attributes {
    fn accounted_size(&self) -> usize {
        self.iter()
            .map(|(key, value)| attribute_entry_size(key, value))
            .sum()
    }
}

impl Accounted for crate::context::TraceId {
    fn accounted_size(&self) -> usize {
        Self::LENGTH
    }
}

impl Accounted for crate::context::SpanId {
    fn accounted_size(&self) -> usize {
        Self::LENGTH
    }
}

impl Accounted for crate::context::TraceStateEntry {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + self.vendor.len() + self.value.len()
    }
}

impl Accounted for crate::context::TraceState {
    fn accounted_size(&self) -> usize {
        self.iter().map(Accounted::accounted_size).sum::<usize>()
    }
}

impl Accounted for crate::context::TraceContext {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.trace_id.accounted_size()
            + self.span_id.accounted_size()
            + 1 // flags: one preserved byte
            + self.tracestate.accounted_size()
    }
}

impl Accounted for crate::resources::Resource {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.attributes.accounted_size()
            + self
                .schema_url
                .as_ref()
                .map_or(0, |url| STRUCTURE_FIXED_BYTES + url.len())
    }
}

impl Accounted for crate::resources::InstrumentationScope {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.name.len()
            + self
                .version
                .as_ref()
                .map_or(0, |version| STRUCTURE_FIXED_BYTES + version.len())
            + self.attributes.accounted_size()
            + self
                .schema_url
                .as_ref()
                .map_or(0, |url| STRUCTURE_FIXED_BYTES + url.len())
    }
}

impl Accounted for crate::spans::SpanStatus {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + 1 + self.message.len() // code + message
    }
}

impl Accounted for crate::spans::SpanEvent {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + 8 // emitter-clock time, or its absence
            + self.name.len()
            + self.attributes.accounted_size()
            + 4 // dropped_attribute_count
    }
}

impl Accounted for crate::spans::SpanLink {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + self.context.accounted_size() + self.attributes.accounted_size() + 4 // dropped_attribute_count
    }
}

impl Accounted for crate::spans::Span {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.context.accounted_size()
            + self
                .parent_span_id
                .map_or(0, |parent| parent.accounted_size())
            + self.name.len()
            + 1 // kind
            + 8 // start_time
            + self.end_time_unix_nano.map_or(0, |_| 8)
            + self.attributes.accounted_size()
            + 12 // three emitter-reported dropped counts, u32 each
            + self
                .events
                .iter()
                .map(Accounted::accounted_size)
                .sum::<usize>()
            + self.links.iter().map(Accounted::accounted_size).sum::<usize>()
            + self.status.accounted_size()
    }
}

impl Accounted for crate::logs::LogRecord {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self
                .timestamp_unix_nano
                .map_or(0, |_| 8)
            + self
                .observed_timestamp_unix_nano
                .map_or(0, |_| 8)
            + self.severity_number.map_or(0, |_| 1)
            + self.severity_text.as_ref().map_or(0, String::len)
            + self.body.as_ref().map_or(0, Accounted::accounted_size)
            + self.attributes.accounted_size()
            + 4 // dropped_attribute_count
            + self
                .trace_context
                .as_ref()
                .map_or(0, Accounted::accounted_size)
    }
}

impl Accounted for crate::metrics::MetricNumber {
    fn accounted_size(&self) -> usize {
        match self {
            Self::Int(_) | Self::Double(_) => 8,
        }
    }
}

impl Accounted for crate::metrics::Exemplar {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.value.accounted_size()
            + 8 // time
            + self.filtered_attributes.accounted_size()
            + self
                .trace_context
                .as_ref()
                .map_or(0, Accounted::accounted_size)
    }
}

impl Accounted for crate::metrics::QuantileValue {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + 16 // quantile + value
    }
}

impl Accounted for crate::metrics::ExponentialBuckets {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + 4 + self.bucket_counts.len() * 8 // offset + counts
    }
}

impl Accounted for crate::metrics::MetricPoint {
    fn accounted_size(&self) -> usize {
        match self {
            Self::Number(point) => {
                STRUCTURE_FIXED_BYTES
                    + point.attributes.accounted_size()
                    + point.start_time_unix_nano.map_or(0, |_| 8)
                    + 8 // time
                    + point.value.accounted_size()
                    + point
                        .exemplars
                        .iter()
                        .map(Accounted::accounted_size)
                        .sum::<usize>()
            }
            Self::Histogram(point) => {
                STRUCTURE_FIXED_BYTES
                    + point.attributes.accounted_size()
                    + 16 // start + end
                    + 8 // count
                    + point.sum.map_or(0, |_| 8)
                    + point.bucket_counts.len() * 8
                    + point.explicit_bounds.len() * 8
                    + point.min.map_or(0, |_| 8)
                    + point.max.map_or(0, |_| 8)
                    + point
                        .exemplars
                        .iter()
                        .map(Accounted::accounted_size)
                        .sum::<usize>()
            }
            Self::ExponentialHistogram(point) => {
                STRUCTURE_FIXED_BYTES
                    + point.attributes.accounted_size()
                    + 16 // start + end
                    + 8 // count
                    + point.sum.map_or(0, |_| 8)
                    + 4 // scale
                    + 8 // zero_count
                    + 8 // zero_threshold
                    + point.positive.accounted_size()
                    + point.negative.accounted_size()
                    + point.min.map_or(0, |_| 8)
                    + point.max.map_or(0, |_| 8)
                    + point
                        .exemplars
                        .iter()
                        .map(Accounted::accounted_size)
                        .sum::<usize>()
            }
            Self::Summary(point) => {
                STRUCTURE_FIXED_BYTES
                    + point.attributes.accounted_size()
                    + 16 // start + end
                    + 8 // count
                    + point.sum.map_or(0, |_| 8)
                    + point
                        .quantiles
                        .iter()
                        .map(Accounted::accounted_size)
                        .sum::<usize>()
                    + point
                        .exemplars
                        .iter()
                        .map(Accounted::accounted_size)
                        .sum::<usize>()
            }
        }
    }
}

impl Accounted for crate::metrics::MetricStream {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.identity.name.len()
            + self.description.as_ref().map_or(0, String::len)
            + self.unit.as_ref().map_or(0, String::len)
            + self.identity.resource.accounted_size()
            + self.identity.scope.accounted_size()
            + 1 // kind, temporality included
            + self
                .points
                .iter()
                .map(Accounted::accounted_size)
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState, TraceStateEntry};
    use crate::logs::LogRecord;
    use crate::metrics::{Exemplar, MetricNumber, MetricPoint, NumberPoint};
    use crate::resources::{InstrumentationScope, Resource};
    use crate::spans::{EmitterDroppedCounts, Span, SpanKind, SpanStatus, SpanStatusCode};
    use crate::values::{Attributes, Float, PrimitiveValue, Value};

    fn attribute(key: &str, text: &str) -> Attributes {
        Attributes::from_pairs(vec![(key.to_owned(), Value::String(text.to_owned()))])
    }

    #[test]
    fn scalar_sizes_follow_the_formula() {
        assert_eq!(Value::Bool(true).accounted_size(), 1);
        assert_eq!(Value::Int(-1).accounted_size(), 8);
        assert_eq!(Value::Double(Float::new(1.5)).accounted_size(), 8);
        assert_eq!(Value::String(String::new()).accounted_size(), 0);
        assert_eq!(Value::String("abcde".to_owned()).accounted_size(), 5);
        assert_eq!(Value::Bytes(vec![1, 2, 3]).accounted_size(), 3);
    }

    #[test]
    fn nested_structures_charge_the_fixed_overhead_once_per_structure() {
        let empty_array = Value::array(Vec::new()).expect("empty is homogeneous");
        assert_eq!(empty_array.accounted_size(), STRUCTURE_FIXED_BYTES);
        let items =
            Value::array(vec![PrimitiveValue::Int(1), PrimitiveValue::Int(2)]).expect("one kind");
        assert_eq!(items.accounted_size(), STRUCTURE_FIXED_BYTES + 8 + 8);
        let list = Value::kv_list(vec![("key".to_owned(), Value::Int(7))]);
        assert_eq!(
            list.accounted_size(),
            STRUCTURE_FIXED_BYTES + STRUCTURE_FIXED_BYTES + "key".len() + 8
        );
    }

    #[test]
    fn an_attribute_entry_counts_key_and_value_and_overhead() {
        let value = Value::String("payload!".to_owned()); // 8 bytes
        assert_eq!(
            attribute_entry_size("service.name", &value),
            STRUCTURE_FIXED_BYTES + "service.name".len() + 8
        );
        let map = attribute("service.name", "payload!");
        assert_eq!(
            map.accounted_size(),
            attribute_entry_size("service.name", &value)
        );
    }

    #[test]
    fn everything_variable_length_is_counted_on_a_span() {
        let context = TraceContext {
            trace_id: TraceId::from_bytes([1; 16]),
            span_id: SpanId::from_bytes([2; 8]),
            flags: TraceFlags::new(1),
            tracestate: TraceState::from_entries(vec![TraceStateEntry {
                vendor: "vendor".to_owned(), // 6
                value: "v".to_owned(),       // 1
            }]),
        };
        let span = Span {
            parent_span_id: Some(SpanId::from_bytes([3; 8])),
            context: context.clone(),
            name: "op".to_owned(),
            kind: SpanKind::Server,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: attribute("k", "vv"),
            emitter_dropped: EmitterDroppedCounts {
                attributes: 1,
                events: 2,
                links: 3,
            },
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: SpanStatusCode::Error,
                message: "boom".to_owned(),
            },
        };
        let expected = STRUCTURE_FIXED_BYTES
            + (STRUCTURE_FIXED_BYTES + 16 + 8 + 1 + (STRUCTURE_FIXED_BYTES + 6 + 1))
            + 8 // parent
            + 2 // name
            + 1 // kind
            + 8 // start
            + 8 // end
            + attribute_entry_size("k", &Value::String("vv".to_owned()))
            + 12 // dropped counts
            + (STRUCTURE_FIXED_BYTES + 1 + 4); // status
        assert_eq!(span.accounted_size(), expected);
        // One more byte of name is one more accounted byte: the count moves
        // with every variable-length part.
        let longer = Span {
            name: "op2".to_owned(),
            ..span
        };
        assert_eq!(longer.accounted_size(), expected + 1);
    }

    #[test]
    fn log_record_accounting_covers_every_variable_field() {
        let bare = LogRecord {
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: None,
            severity_text: None,
            body: None,
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_context: None,
        };
        assert_eq!(bare.accounted_size(), STRUCTURE_FIXED_BYTES + 4);
        let fuller = LogRecord {
            timestamp_unix_nano: Some(1),
            observed_timestamp_unix_nano: Some(2),
            severity_number: crate::logs::SeverityNumber::try_new(9).ok(),
            severity_text: Some("ERROR".to_owned()),
            body: Some(Value::Int(1)),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_context: None,
        };
        assert_eq!(
            fuller.accounted_size(),
            STRUCTURE_FIXED_BYTES + 8 + 8 + 1 + 5 + 8 + 4
        );
    }

    #[test]
    fn point_accounting_moves_with_exemplars() {
        let point = MetricPoint::Number(NumberPoint::measurement(
            5,
            MetricNumber::int(1),
            Attributes::default(),
            Vec::new(),
        ));
        let bare = point.accounted_size();
        let with_exemplar = MetricPoint::Number(NumberPoint::measurement(
            5,
            MetricNumber::int(1),
            Attributes::default(),
            vec![Exemplar {
                value: MetricNumber::int(2),
                time_unix_nano: 6,
                filtered_attributes: Attributes::default(),
                trace_context: None,
            }],
        ));
        assert_eq!(
            with_exemplar.accounted_size(),
            bare + STRUCTURE_FIXED_BYTES + 8 + 8
        );
    }

    #[test]
    fn stream_accounting_includes_identity_parts_and_points() {
        let stream = crate::metrics::MetricStream::new(
            crate::metrics::StreamIdentity {
                resource: Resource {
                    attributes: Attributes::default(),
                    schema_url: None,
                },
                scope: InstrumentationScope {
                    name: "scope".to_owned(),
                    version: None,
                    attributes: Attributes::default(),
                    schema_url: None,
                },
                name: "requests".to_owned(),
                kind: crate::metrics::StreamKind::Gauge,
                temporality: None,
            },
            Some("an in-flight gauge".to_owned()),
            Some("1".to_owned()),
            Vec::new(),
        )
        .expect("a coherent stream");
        let bare = stream.accounted_size();
        assert!(bare > STRUCTURE_FIXED_BYTES + "requests".len());
        let with_point = crate::metrics::MetricStream {
            points: vec![MetricPoint::Number(NumberPoint::measurement(
                1,
                MetricNumber::int(0),
                Attributes::default(),
                Vec::new(),
            ))],
            ..stream
        };
        assert!(with_point.accounted_size() > bare);
    }
}
