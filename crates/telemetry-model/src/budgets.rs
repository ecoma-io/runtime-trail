//! The budget taxonomy, its numbers, and the admission-gate checks.
//!
//! The **taxonomy** is owned by `docs/architecture/telemetry-model.md`
//! ("Information budgets"). The **numbers** are owned by
//! `docs/architecture/runtime-constraints.md` ("Numeric limits"), mirrored
//! here as named constants — one home, no scattered duplicates: the table
//! there is normative, these constants carry its defaults and name it as
//! the owner. A budget exceeded is a **refusal at admission**, never a
//! truncation: a record is admitted complete and immutable, or rejected
//! with the budget named (ADR 0006).
//!
//! Budget refusals are non-retryable by construction: every model budget
//! is a property of the payload, so retrying cannot shrink it. The one
//! retryable admission signal (queue saturation) is not a model budget and
//! is owned by ingestion.

use crate::logs::LogRecord;
use crate::metrics::MetricPoint;
use crate::resources::{InstrumentationScope, Resource};
use crate::spans::Span;
use crate::values::{Attributes, Value};
use std::fmt;

/// The numeric limits, mirrored from their owner.
///
/// **Owner: `docs/architecture/runtime-constraints.md`, "Numeric limits
/// (targets)".** That table is normative; these are its defaults, encoded
/// as the constants the admission gates below check against. Changing a
/// default is an architecture change — it changes there first, and the PR
/// states its effect on the targets table.
pub mod limits {
    /// OTLP payload ceiling: one OTLP export request. Enforced at the
    /// transport edge, before the payload is parsed — ingestion's gate,
    /// not a model check.
    pub const OTLP_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
    /// Attributes per signal: per span, log record, data point, resource.
    pub const ATTRIBUTES_PER_SIGNAL: usize = 256;
    /// Attributes per span event, link or exemplar — each carries its own
    /// attribute set.
    pub const ATTRIBUTES_PER_NESTED_SET: usize = 64;
    /// One attribute's accounted size (model accounting — keys and names
    /// count).
    pub const ATTRIBUTE_VALUE_BYTES: usize = 4 * 1024;
    /// Span events per span.
    pub const SPAN_EVENTS_PER_SPAN: usize = 128;
    /// Links per span.
    pub const SPAN_LINKS_PER_SPAN: usize = 32;
    /// Exemplars per data point.
    pub const EXEMPLARS_PER_DATA_POINT: usize = 4;
    /// Key-value list depth: nested kvlists inside attribute values.
    pub const KEY_VALUE_LIST_DEPTH: usize = 8;
    /// Data points per export; overflow rejects the whole export — a
    /// payload property, non-retryable.
    pub const DATA_POINTS_PER_EXPORT: usize = 10_000;
}

/// A budget refused the record.
///
/// Non-retryable by construction: every model budget is a property of the
/// payload, so retrying cannot fix it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetRejection {
    /// Which budget was exceeded.
    pub budget: BudgetName,
    /// The limit the budget sets.
    pub limit: usize,
    /// The spend that was observed against it.
    pub observed: usize,
}

impl BudgetRejection {
    /// Always `false`: a model-budget refusal is never retryable.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        false
    }
}

impl fmt::Display for BudgetRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "budget {} exceeded: limit {}, observed {} (non-retryable)",
            self.budget.slug(),
            self.limit,
            self.observed
        )
    }
}

impl std::error::Error for BudgetRejection {}

/// The budget taxonomy — the names admission can refuse a record by.
///
/// Owned by `docs/architecture/telemetry-model.md` ("Information
/// budgets"); the numbers behind each name live in
/// [`limits`], mirrored from `docs/architecture/runtime-constraints.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BudgetName {
    /// Attribute count per span, log record, data point and resource.
    AttributesPerSignal,
    /// Attribute count per span event, link or exemplar.
    AttributesPerNestedSet,
    /// One attribute's accounted size — keys and names count.
    AttributeValueSize,
    /// Span events per span.
    EventsPerSpan,
    /// Links per span.
    LinksPerSpan,
    /// Exemplars per data point.
    ExemplarsPerDataPoint,
    /// Key-value list depth nested inside attribute values.
    KeyValueListDepth,
    /// Data points per export; overflow rejects the whole export.
    DataPointsPerExport,
}

impl BudgetName {
    /// A stable slug for the budget, for wire signals that name it
    /// (OTLP `partial_success`) and for logs.
    #[must_use]
    pub const fn slug(self) -> &'static str {
        match self {
            Self::AttributesPerSignal => "attributes_per_signal",
            Self::AttributesPerNestedSet => "attributes_per_nested_set",
            Self::AttributeValueSize => "attribute_value_size",
            Self::EventsPerSpan => "events_per_span",
            Self::LinksPerSpan => "links_per_span",
            Self::ExemplarsPerDataPoint => "exemplars_per_data_point",
            Self::KeyValueListDepth => "key_value_list_depth",
            Self::DataPointsPerExport => "data_points_per_export",
        }
    }
}

fn refuse(budget: BudgetName, limit: usize, observed: usize) -> BudgetRejection {
    BudgetRejection {
        budget,
        limit,
        observed,
    }
}

/// Checks one attribute entry: its accounted size (key and names count)
/// and its key-value-list depth. The conservative reading of the
/// attribute-value ceiling — the whole entry is the spend.
fn check_attribute_entry(key: &str, value: &Value) -> Result<(), BudgetRejection> {
    let entry_size = crate::size::attribute_entry_size(key, value);
    if entry_size > limits::ATTRIBUTE_VALUE_BYTES {
        return Err(refuse(
            BudgetName::AttributeValueSize,
            limits::ATTRIBUTE_VALUE_BYTES,
            entry_size,
        ));
    }
    let depth = value.kv_depth();
    if depth > limits::KEY_VALUE_LIST_DEPTH {
        return Err(refuse(
            BudgetName::KeyValueListDepth,
            limits::KEY_VALUE_LIST_DEPTH,
            depth,
        ));
    }
    Ok(())
}

/// Checks one attribute set against one of the two attribute-count
/// budgets, and every entry against the value-size and depth budgets.
fn check_attribute_set(
    attributes: &Attributes,
    count_budget: BudgetName,
    limit: usize,
) -> Result<(), BudgetRejection> {
    let count = attributes.len();
    if count > limit {
        return Err(refuse(count_budget, limit, count));
    }
    for (key, value) in attributes {
        check_attribute_entry(key, value)?;
    }
    Ok(())
}

/// The admission gate for a span: the span's attribute set, its events and
/// links (counts and attribute sets), the value sizes and list depths
/// throughout.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the span trips, naming the
/// budget, its limit and the observed spend.
pub fn check_span(span: &Span) -> Result<(), BudgetRejection> {
    check_attribute_set(
        &span.attributes,
        BudgetName::AttributesPerSignal,
        limits::ATTRIBUTES_PER_SIGNAL,
    )?;
    let events = span.events.len();
    if events > limits::SPAN_EVENTS_PER_SPAN {
        return Err(refuse(
            BudgetName::EventsPerSpan,
            limits::SPAN_EVENTS_PER_SPAN,
            events,
        ));
    }
    for event in &span.events {
        check_event_or_link_attributes(&event.attributes)?;
    }
    let links = span.links.len();
    if links > limits::SPAN_LINKS_PER_SPAN {
        return Err(refuse(
            BudgetName::LinksPerSpan,
            limits::SPAN_LINKS_PER_SPAN,
            links,
        ));
    }
    for link in &span.links {
        check_event_or_link_attributes(&link.attributes)?;
    }
    Ok(())
}

fn check_event_or_link_attributes(attributes: &Attributes) -> Result<(), BudgetRejection> {
    check_attribute_set(
        attributes,
        BudgetName::AttributesPerNestedSet,
        limits::ATTRIBUTES_PER_NESTED_SET,
    )
}

/// The admission gate for a log record: its attribute set, value sizes and
/// list depths. The body is a full value and is counted by the record's
/// accounted size; the taxonomy caps attribute sets, not bodies.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the record trips.
pub fn check_log_record(record: &LogRecord) -> Result<(), BudgetRejection> {
    check_attribute_set(
        &record.attributes,
        BudgetName::AttributesPerSignal,
        limits::ATTRIBUTES_PER_SIGNAL,
    )
}

/// The admission gate for one metric data point: its attribute set, its
/// exemplar count and their attribute sets.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the point trips.
pub fn check_point(point: &MetricPoint) -> Result<(), BudgetRejection> {
    check_attribute_set(
        point.attributes(),
        BudgetName::AttributesPerSignal,
        limits::ATTRIBUTES_PER_SIGNAL,
    )?;
    let exemplars = point.exemplars().len();
    if exemplars > limits::EXEMPLARS_PER_DATA_POINT {
        return Err(refuse(
            BudgetName::ExemplarsPerDataPoint,
            limits::EXEMPLARS_PER_DATA_POINT,
            exemplars,
        ));
    }
    for exemplar in point.exemplars() {
        check_attribute_set(
            &exemplar.filtered_attributes,
            BudgetName::AttributesPerNestedSet,
            limits::ATTRIBUTES_PER_NESTED_SET,
        )?;
    }
    Ok(())
}

/// The admission gate for a resource: its attribute set.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the resource trips.
pub fn check_resource(resource: &Resource) -> Result<(), BudgetRejection> {
    check_attribute_set(
        &resource.attributes,
        BudgetName::AttributesPerSignal,
        limits::ATTRIBUTES_PER_SIGNAL,
    )
}

/// The admission gate for a scope's own attribute set.
///
/// The taxonomy's applies-to column does not name scopes; this gate applies
/// the general per-signal attribute cap to them rather than leaving scope
/// attributes ungated under a ninth, invented budget name.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the scope trips.
pub fn check_scope(scope: &InstrumentationScope) -> Result<(), BudgetRejection> {
    check_attribute_set(
        &scope.attributes,
        BudgetName::AttributesPerSignal,
        limits::ATTRIBUTES_PER_SIGNAL,
    )
}

/// The admission gate for one export's point count. Overflow rejects the
/// whole export — a payload property, non-retryable; the caller (ingestion)
/// applies the verdict to the entire request.
///
/// # Errors
///
/// Returns [`BudgetRejection`] naming
/// [`BudgetName::DataPointsPerExport`] when `count` exceeds the cap.
pub fn check_export_point_count(count: usize) -> Result<(), BudgetRejection> {
    if count > limits::DATA_POINTS_PER_EXPORT {
        return Err(refuse(
            BudgetName::DataPointsPerExport,
            limits::DATA_POINTS_PER_EXPORT,
            count,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState};
    use crate::metrics::{Exemplar, MetricNumber, NumberPoint};
    use crate::spans::{
        EmitterDroppedCounts, Span, SpanEvent, SpanKind, SpanLink, SpanStatus, SpanStatusCode,
    };
    use crate::values::Value;

    fn attributes_of(count: usize) -> Attributes {
        Attributes::from_pairs(
            (0..i64::try_from(count).expect("a test fits an i64"))
                .map(|index| (format!("k{index}"), Value::Int(index)))
                .collect(),
        )
    }

    fn span_with(attributes: Attributes) -> Span {
        Span {
            context: TraceContext {
                trace_id: TraceId::from_bytes([1; 16]),
                span_id: SpanId::from_bytes([2; 8]),
                flags: TraceFlags::new(1),
                tracestate: TraceState::default(),
            },
            parent_span_id: None,
            name: "op".to_owned(),
            kind: SpanKind::Internal,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes,
            emitter_dropped: EmitterDroppedCounts::default(),
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: SpanStatusCode::Unset,
                message: String::new(),
            },
        }
    }

    fn number_point(attributes: Attributes, exemplars: Vec<Exemplar>) -> MetricPoint {
        MetricPoint::Number(NumberPoint::measurement(
            1,
            MetricNumber::int(1),
            attributes,
            exemplars,
        ))
    }

    fn exemplar_with(attributes: Attributes) -> Exemplar {
        Exemplar {
            value: MetricNumber::int(1),
            time_unix_nano: 1,
            filtered_attributes: attributes,
            trace_context: None,
        }
    }

    fn assert_rejected(
        result: Result<(), BudgetRejection>,
        budget: BudgetName,
        limit: usize,
        observed: usize,
    ) {
        let rejection = result.expect_err("over-budget input is refused at admission");
        assert_eq!(rejection.budget, budget);
        assert_eq!(rejection.limit, limit);
        assert_eq!(rejection.observed, observed);
        assert!(!rejection.is_retryable());
    }

    #[test]
    fn attributes_per_signal_admits_the_cap_and_refuses_one_more() {
        let at_cap = span_with(attributes_of(limits::ATTRIBUTES_PER_SIGNAL));
        assert!(check_span(&at_cap).is_ok());
        let over = span_with(attributes_of(limits::ATTRIBUTES_PER_SIGNAL + 1));
        assert_rejected(
            check_span(&over),
            BudgetName::AttributesPerSignal,
            limits::ATTRIBUTES_PER_SIGNAL,
            limits::ATTRIBUTES_PER_SIGNAL + 1,
        );
        let log = LogRecord {
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: None,
            severity_text: None,
            body: None,
            attributes: attributes_of(limits::ATTRIBUTES_PER_SIGNAL + 1),
            dropped_attribute_count: 0,
            trace_context: None,
        };
        assert_rejected(
            check_log_record(&log),
            BudgetName::AttributesPerSignal,
            limits::ATTRIBUTES_PER_SIGNAL,
            limits::ATTRIBUTES_PER_SIGNAL + 1,
        );
        let resource = crate::resources::Resource {
            attributes: attributes_of(limits::ATTRIBUTES_PER_SIGNAL + 1),
            schema_url: None,
        };
        assert_rejected(
            check_resource(&resource),
            BudgetName::AttributesPerSignal,
            limits::ATTRIBUTES_PER_SIGNAL,
            limits::ATTRIBUTES_PER_SIGNAL + 1,
        );
    }

    #[test]
    fn attributes_per_nested_set_gates_events_links_and_exemplars() {
        let mut span = span_with(Attributes::default());
        span.events.push(SpanEvent {
            time_unix_nano: Some(1),
            name: "event".to_owned(),
            attributes: attributes_of(limits::ATTRIBUTES_PER_NESTED_SET + 1),
            dropped_attribute_count: 0,
        });
        assert_rejected(
            check_span(&span),
            BudgetName::AttributesPerNestedSet,
            limits::ATTRIBUTES_PER_NESTED_SET,
            limits::ATTRIBUTES_PER_NESTED_SET + 1,
        );
        let mut linked = span_with(Attributes::default());
        linked.links.push(SpanLink {
            context: span.context.clone(),
            attributes: attributes_of(limits::ATTRIBUTES_PER_NESTED_SET + 1),
            dropped_attribute_count: 0,
        });
        assert_rejected(
            check_span(&linked),
            BudgetName::AttributesPerNestedSet,
            limits::ATTRIBUTES_PER_NESTED_SET,
            limits::ATTRIBUTES_PER_NESTED_SET + 1,
        );
        let point = number_point(
            Attributes::default(),
            vec![exemplar_with(attributes_of(
                limits::ATTRIBUTES_PER_NESTED_SET + 1,
            ))],
        );
        assert_rejected(
            check_point(&point),
            BudgetName::AttributesPerNestedSet,
            limits::ATTRIBUTES_PER_NESTED_SET,
            limits::ATTRIBUTES_PER_NESTED_SET + 1,
        );
    }

    #[test]
    fn attribute_value_size_counts_keys_and_names() {
        // The budget is the whole entry's accounted size: fixed per-entry
        // overhead + key bytes + value accounted bytes.
        let headroom =
            limits::ATTRIBUTE_VALUE_BYTES - crate::size::STRUCTURE_FIXED_BYTES - "k".len();
        let at_cap = span_with(Attributes::from_pairs(vec![(
            "k".to_owned(),
            Value::String("x".repeat(headroom)),
        )]));
        assert!(check_span(&at_cap).is_ok(), "exactly the cap is admitted");
        let one_over = span_with(Attributes::from_pairs(vec![(
            "k".to_owned(),
            Value::String("x".repeat(headroom + 1)),
        )]));
        assert_rejected(
            check_span(&one_over),
            BudgetName::AttributeValueSize,
            limits::ATTRIBUTE_VALUE_BYTES,
            limits::ATTRIBUTE_VALUE_BYTES + 1,
        );
    }

    #[test]
    fn events_and_links_per_span_are_gated_separately() {
        let event = SpanEvent {
            time_unix_nano: Some(1),
            name: "e".to_owned(),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
        };
        let mut many_events = span_with(Attributes::default());
        many_events
            .events
            .extend(std::iter::repeat_n(event, limits::SPAN_EVENTS_PER_SPAN + 1));
        assert_rejected(
            check_span(&many_events),
            BudgetName::EventsPerSpan,
            limits::SPAN_EVENTS_PER_SPAN,
            limits::SPAN_EVENTS_PER_SPAN + 1,
        );
        let link = SpanLink {
            context: span_with(Attributes::default()).context,
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
        };
        let mut many_links = span_with(Attributes::default());
        many_links
            .links
            .extend(std::iter::repeat_n(link, limits::SPAN_LINKS_PER_SPAN + 1));
        assert_rejected(
            check_span(&many_links),
            BudgetName::LinksPerSpan,
            limits::SPAN_LINKS_PER_SPAN,
            limits::SPAN_LINKS_PER_SPAN + 1,
        );
    }

    #[test]
    fn exemplars_per_data_point_is_gated() {
        let point = number_point(
            Attributes::default(),
            std::iter::repeat_n(
                exemplar_with(Attributes::default()),
                limits::EXEMPLARS_PER_DATA_POINT + 1,
            )
            .collect(),
        );
        assert_rejected(
            check_point(&point),
            BudgetName::ExemplarsPerDataPoint,
            limits::EXEMPLARS_PER_DATA_POINT,
            limits::EXEMPLARS_PER_DATA_POINT + 1,
        );
        let at_cap = number_point(
            Attributes::default(),
            std::iter::repeat_n(
                exemplar_with(Attributes::default()),
                limits::EXEMPLARS_PER_DATA_POINT,
            )
            .collect(),
        );
        assert!(check_point(&at_cap).is_ok());
    }

    #[test]
    fn key_value_list_depth_is_gated_on_attribute_values() {
        let nested = |depth: usize| -> Value {
            let mut value = Value::Int(0);
            for _ in 0..depth {
                value = Value::kv_list(vec![("n".to_owned(), value)]);
            }
            value
        };
        let at_cap = span_with(Attributes::from_pairs(vec![(
            "k".to_owned(),
            nested(limits::KEY_VALUE_LIST_DEPTH),
        )]));
        assert!(check_span(&at_cap).is_ok());
        let one_over = span_with(Attributes::from_pairs(vec![(
            "k".to_owned(),
            nested(limits::KEY_VALUE_LIST_DEPTH + 1),
        )]));
        assert_rejected(
            check_span(&one_over),
            BudgetName::KeyValueListDepth,
            limits::KEY_VALUE_LIST_DEPTH,
            limits::KEY_VALUE_LIST_DEPTH + 1,
        );
    }

    #[test]
    fn data_points_per_export_rejects_the_whole_export() {
        assert!(check_export_point_count(limits::DATA_POINTS_PER_EXPORT).is_ok());
        assert_rejected(
            check_export_point_count(limits::DATA_POINTS_PER_EXPORT + 1),
            BudgetName::DataPointsPerExport,
            limits::DATA_POINTS_PER_EXPORT,
            limits::DATA_POINTS_PER_EXPORT + 1,
        );
    }

    #[test]
    fn a_within_cap_stream_of_points_passes_gate_by_gate() {
        let stream = crate::metrics::MetricStream::new(
            crate::metrics::StreamIdentity {
                resource: crate::resources::Resource {
                    attributes: Attributes::default(),
                    schema_url: None,
                },
                scope: crate::resources::InstrumentationScope {
                    name: String::new(),
                    version: None,
                    attributes: Attributes::default(),
                    schema_url: None,
                },
                name: "requests".to_owned(),
                kind: crate::metrics::StreamKind::Gauge,
                temporality: None,
            },
            None,
            None,
            vec![number_point(Attributes::default(), Vec::new())],
        )
        .expect("a coherent stream");
        assert!(check_resource(&stream.identity.resource).is_ok());
        for point in &stream.points {
            assert!(check_point(point).is_ok());
        }
        assert!(check_export_point_count(stream.points.len()).is_ok());
    }
}
