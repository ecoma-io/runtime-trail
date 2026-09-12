//! The budget taxonomy, its numbers, and the admission-gate checks.
//!
//! The **taxonomy** is owned by `docs/architecture/telemetry-model.md`
//! ("Information budgets"). The **numbers** are owned by
//! `docs/architecture/runtime-constraints.md` ("Numeric limits"),
//! carried here as [`BudgetLimits`]: that table is normative,
//! [`BudgetLimits::default`] is its verbatim mirror, and an operator may
//! tune the values at startup only — never mid-session. Changing a default
//! is an architecture change — it changes there first, and the PR states
//! its effect on the targets table. A budget exceeded is a **refusal at
//! admission**, never a truncation: a record is admitted complete and
//! immutable, or rejected with the budget named (ADR 0006).
//!
//! Budget refusals are non-retryable by construction: every model budget
//! is a property of the payload, so retrying cannot shrink it. The one
//! retryable admission signal (queue saturation) is not a model budget and
//! is owned by ingestion.
//!
//! # The nesting budget is checked iteratively
//!
//! The key-value-list-depth check is a bound on *every recursive structure
//! a record carries*: key-value lists and arrays alike count, and every
//! value a record carries is checked — attribute values, log bodies,
//! events, links, exemplars. The walk (`Value::exceeds_nesting`) is
//! iterative over an explicit stack and stops at the first level past the
//! limit, so a value nested 60,000 levels deep is refused without walking
//! it — and without the per-level recursion the old check used, which
//! aborted the process inside the very gate meant to protect it.
//!
//! **Admitted-data invariant:** every value an *admitted* record carries
//! nests at most [`BudgetLimits::key_value_list_depth`] levels deep (the
//! gate below enforces it on every value path). That invariant is what
//! makes the recursive `PartialEq`/`Debug`/`Clone` glue the ledger and
//! storage run over admitted records bounded; `Drop` is iterative
//! regardless (see `crate::values`), because a refused value must still be
//! droppable.

use crate::logs::LogRecord;
use crate::metrics::{MetricPoint, MetricStream, StreamIdentity};
use crate::resources::{InstrumentationScope, Resource};
use crate::spans::Span;
use crate::values::{Attributes, Value};
use std::fmt;

/// The OTLP payload ceiling: one OTLP export request, enforced at the
/// transport edge before the payload is parsed. Ingestion's gate, not a
/// model check — it stands apart from [`BudgetLimits`], which the model
/// gates consume.
///
/// **Owner: `docs/architecture/runtime-constraints.md`, "Numeric limits
/// (targets)"** (default: 4 MiB).
pub const OTLP_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// The numeric limits the admission gates check against.
///
/// **Owner: `docs/architecture/runtime-constraints.md`, "Numeric limits
/// (targets)".** That table is normative; [`BudgetLimits::default`] carries
/// its numbers verbatim. An operator may tune the values at startup only —
/// never mid-session — and changing a default is an architecture change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetLimits {
    /// Attributes per signal: per span, log record, data point, resource.
    pub attributes_per_signal: usize,
    /// Attributes per span event, link or exemplar — each carries its own
    /// attribute set.
    pub attributes_per_nested_set: usize,
    /// One attribute's accounted size (model accounting — keys and names
    /// count).
    pub attribute_value_bytes: usize,
    /// Span events per span.
    pub span_events_per_span: usize,
    /// Links per span.
    pub span_links_per_span: usize,
    /// Exemplars per data point.
    pub exemplars_per_data_point: usize,
    /// Key-value list depth: the nesting budget every value a record
    /// carries is held to (key-value lists and arrays both count).
    pub key_value_list_depth: usize,
    /// Data points per export; overflow rejects the whole export — a
    /// payload property, non-retryable.
    pub data_points_per_export: usize,
}

impl Default for BudgetLimits {
    /// The numbers in `docs/architecture/runtime-constraints.md`, "Numeric
    /// limits (targets)" — the normative owner of every value here.
    fn default() -> Self {
        Self {
            attributes_per_signal: 256,
            attributes_per_nested_set: 64,
            attribute_value_bytes: 4 * 1024,
            span_events_per_span: 128,
            span_links_per_span: 32,
            exemplars_per_data_point: 4,
            key_value_list_depth: 8,
            data_points_per_export: 10_000,
        }
    }
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
/// budgets"); the numbers behind each name live in [`BudgetLimits`],
/// mirrored from `docs/architecture/runtime-constraints.md`.
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

/// Checks one value's nesting depth against the key-value-list budget.
///
/// The walk is [`Value::exceeds_nesting`]: iterative, and stopped at the
/// first level past the limit. The `observed` figure is therefore a
/// **truthful lower bound** — `limit + 1`, "at least this deep" — not the
/// value's true maximum depth, which a refusal never needs to know.
fn check_value_depth(value: &Value, limits: &BudgetLimits) -> Result<(), BudgetRejection> {
    let limit = limits.key_value_list_depth;
    if value.exceeds_nesting(limit) {
        return Err(refuse(BudgetName::KeyValueListDepth, limit, limit + 1));
    }
    Ok(())
}

/// Checks one attribute entry: its accounted size (key and names count)
/// and its nesting depth (key-value lists *and* arrays count). The
/// conservative reading of the attribute-value ceiling — the whole entry
/// is the spend.
fn check_attribute_entry(
    key: &str,
    value: &Value,
    limits: &BudgetLimits,
) -> Result<(), BudgetRejection> {
    let entry_size = crate::size::attribute_entry_size(key, value);
    if entry_size > limits.attribute_value_bytes {
        return Err(refuse(
            BudgetName::AttributeValueSize,
            limits.attribute_value_bytes,
            entry_size,
        ));
    }
    check_value_depth(value, limits)
}

/// Checks one attribute set against one of the two attribute-count
/// budgets, and every entry against the value-size and depth budgets.
fn check_attribute_set(
    attributes: &Attributes,
    count_budget: BudgetName,
    limit: usize,
    limits: &BudgetLimits,
) -> Result<(), BudgetRejection> {
    let count = attributes.len();
    if count > limit {
        return Err(refuse(count_budget, limit, count));
    }
    for (key, value) in attributes {
        check_attribute_entry(key, value, limits)?;
    }
    Ok(())
}

fn check_event_or_link_attributes(
    attributes: &Attributes,
    limits: &BudgetLimits,
) -> Result<(), BudgetRejection> {
    check_attribute_set(
        attributes,
        BudgetName::AttributesPerNestedSet,
        limits.attributes_per_nested_set,
        limits,
    )
}

/// The admission gate for a span: the span's attribute set, its events and
/// links (counts and attribute sets), its resource's and scope's attribute
/// sets, and the value sizes and nesting depths throughout — every value
/// the record carries, since the record carries them all.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the span trips, naming the
/// budget, its limit and the observed spend.
pub fn check_span(span: &Span, limits: &BudgetLimits) -> Result<(), BudgetRejection> {
    check_resource(&span.resource, limits)?;
    check_scope(&span.scope, limits)?;
    check_attribute_set(
        &span.attributes,
        BudgetName::AttributesPerSignal,
        limits.attributes_per_signal,
        limits,
    )?;
    let events = span.events.len();
    if events > limits.span_events_per_span {
        return Err(refuse(
            BudgetName::EventsPerSpan,
            limits.span_events_per_span,
            events,
        ));
    }
    for event in &span.events {
        check_event_or_link_attributes(&event.attributes, limits)?;
    }
    let links = span.links.len();
    if links > limits.span_links_per_span {
        return Err(refuse(
            BudgetName::LinksPerSpan,
            limits.span_links_per_span,
            links,
        ));
    }
    for link in &span.links {
        check_event_or_link_attributes(&link.attributes, limits)?;
    }
    Ok(())
}

/// The admission gate for a log record: its attribute set, its body's
/// nesting depth, and its resource's and scope's attribute sets.
///
/// The body is a full value and is counted by the record's accounted
/// size; the taxonomy caps attribute-set sizes, not bodies — but the
/// nesting budget is about *every recursive structure a record carries*,
/// so a 60,000-deep body is refused exactly like a 60,000-deep attribute.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the record trips.
pub fn check_log_record(record: &LogRecord, limits: &BudgetLimits) -> Result<(), BudgetRejection> {
    check_resource(&record.resource, limits)?;
    check_scope(&record.scope, limits)?;
    check_attribute_set(
        &record.attributes,
        BudgetName::AttributesPerSignal,
        limits.attributes_per_signal,
        limits,
    )?;
    if let Some(body) = &record.body {
        check_value_depth(body, limits)?;
    }
    Ok(())
}

/// The admission gate for one metric data point: its attribute set, its
/// exemplar count and their attribute sets, and the nesting depth of every
/// value the point carries — point attributes and exemplar
/// filtered-attributes alike.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the point trips.
pub fn check_point(point: &MetricPoint, limits: &BudgetLimits) -> Result<(), BudgetRejection> {
    check_attribute_set(
        point.attributes(),
        BudgetName::AttributesPerSignal,
        limits.attributes_per_signal,
        limits,
    )?;
    let exemplars = point.exemplars().len();
    if exemplars > limits.exemplars_per_data_point {
        return Err(refuse(
            BudgetName::ExemplarsPerDataPoint,
            limits.exemplars_per_data_point,
            exemplars,
        ));
    }
    for exemplar in point.exemplars() {
        check_attribute_set(
            &exemplar.filtered_attributes,
            BudgetName::AttributesPerNestedSet,
            limits.attributes_per_nested_set,
            limits,
        )?;
    }
    Ok(())
}

/// The admission gate for a resource: its attribute set.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the resource trips.
pub fn check_resource(resource: &Resource, limits: &BudgetLimits) -> Result<(), BudgetRejection> {
    check_attribute_set(
        &resource.attributes,
        BudgetName::AttributesPerSignal,
        limits.attributes_per_signal,
        limits,
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
pub fn check_scope(
    scope: &InstrumentationScope,
    limits: &BudgetLimits,
) -> Result<(), BudgetRejection> {
    check_attribute_set(
        &scope.attributes,
        BudgetName::AttributesPerSignal,
        limits.attributes_per_signal,
        limits,
    )
}

/// The stream-level share of the admission gates: the identity's resource
/// and scope attribute sets and the stream's metadata attribute set — the
/// parts of a metric stream that belong to no single point.
///
/// A stream's metadata belongs to no single point, so the per-point gate
/// ([`check_point`]) cannot see it. The admission ledger's
/// `admit_metric_point` applies this gate on the path that actually admits
/// streams, and [`check_metric_stream`] reuses it for the whole-stream
/// view — one statement of the stream-level law, two applications.
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] the stream-level parts trip.
pub fn check_stream_identity(
    stream: &StreamIdentity,
    limits: &BudgetLimits,
) -> Result<(), BudgetRejection> {
    check_resource(&stream.resource, limits)?;
    check_scope(&stream.scope, limits)?;
    check_attribute_set(
        &stream.metadata,
        BudgetName::AttributesPerSignal,
        limits.attributes_per_signal,
        limits,
    )
}

/// The admission gate for a whole metric stream: the stream-level gates
/// through [`check_stream_identity`], then every point through
/// [`check_point`].
///
/// # Errors
///
/// Returns the first [`BudgetRejection`] any part of the stream trips.
pub fn check_metric_stream(
    stream: &MetricStream,
    limits: &BudgetLimits,
) -> Result<(), BudgetRejection> {
    check_stream_identity(&stream.identity, limits)?;
    for point in &stream.points {
        check_point(point, limits)?;
    }
    Ok(())
}

/// The admission gate for one export's point count. Overflow rejects the
/// whole export — a payload property, non-retryable; the caller (ingestion)
/// applies the verdict to the entire request.
///
/// # Errors
///
/// Returns [`BudgetRejection`] naming
/// [`BudgetName::DataPointsPerExport`] when `count` exceeds the cap.
pub fn check_export_point_count(
    count: usize,
    limits: &BudgetLimits,
) -> Result<(), BudgetRejection> {
    if count > limits.data_points_per_export {
        return Err(refuse(
            BudgetName::DataPointsPerExport,
            limits.data_points_per_export,
            count,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState};
    use crate::metrics::{Exemplar, MetricNumber, NumberPoint, StreamIdentity, StreamKind};
    use crate::resources::{InstrumentationScope, Resource};
    use crate::spans::{
        EmitterDroppedCounts, Span, SpanEvent, SpanKind, SpanLink, SpanStatus, SpanStatusCode,
    };
    use crate::values::Value;

    fn limits() -> BudgetLimits {
        BudgetLimits::default()
    }

    fn attributes_of(count: usize) -> Attributes {
        Attributes::from_pairs(
            (0..i64::try_from(count).expect("a test fits an i64"))
                .map(|index| (format!("k{index}"), Value::Int(index)))
                .collect(),
        )
        .expect("unique keys")
    }

    fn resource() -> Resource {
        Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    fn scope() -> InstrumentationScope {
        InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
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
            resource: resource().into(),
            scope: scope().into(),
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
            trace_id: None,
            span_id: None,
        }
    }

    fn bare_log() -> LogRecord {
        LogRecord {
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: None,
            severity_text: None,
            body: None,
            resource: resource().into(),
            scope: scope().into(),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_id: None,
            span_id: None,
            trace_flags: None,
            event_name: None,
        }
    }

    fn nested_kv(depth: usize) -> Value {
        let mut value = Value::Int(0);
        for _ in 0..depth {
            value = Value::kv_list(vec![("n".to_owned(), value)]).expect("unique keys");
        }
        value
    }

    fn nested_array(depth: usize) -> Value {
        let mut value = Value::Int(0);
        for _ in 0..depth {
            value = Value::array(vec![value]).expect("one kind");
        }
        value
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
    fn the_default_limits_mirror_the_normative_table() {
        // The numbers are owned by runtime-constraints.md, "Numeric limits
        // (targets)"; this is the mirror check — change the table, then
        // change this, never silently.
        let default = BudgetLimits::default();
        assert_eq!(default.attributes_per_signal, 256);
        assert_eq!(default.attributes_per_nested_set, 64);
        assert_eq!(default.attribute_value_bytes, 4 * 1024);
        assert_eq!(default.span_events_per_span, 128);
        assert_eq!(default.span_links_per_span, 32);
        assert_eq!(default.exemplars_per_data_point, 4);
        assert_eq!(default.key_value_list_depth, 8);
        assert_eq!(default.data_points_per_export, 10_000);
        assert_eq!(OTLP_PAYLOAD_BYTES, 4 * 1024 * 1024);
    }

    #[test]
    fn attributes_per_signal_admits_the_cap_and_refuses_one_more() {
        let at_cap = span_with(attributes_of(limits().attributes_per_signal));
        assert!(check_span(&at_cap, &limits()).is_ok());
        let over = span_with(attributes_of(limits().attributes_per_signal + 1));
        assert_rejected(
            check_span(&over, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
        let log = LogRecord {
            attributes: attributes_of(limits().attributes_per_signal + 1),
            ..bare_log()
        };
        assert_rejected(
            check_log_record(&log, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
        let resource = Resource {
            attributes: attributes_of(limits().attributes_per_signal + 1),
            schema_url: None,
            dropped_attributes_count: 0,
        };
        assert_rejected(
            check_resource(&resource, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
    }

    #[test]
    fn a_span_gate_covers_its_resource_and_scope_attribute_sets() {
        // The span carries its resource and scope now; a span gate that
        // skipped them would be a door around the budgets.
        let resourced = Span {
            resource: Resource {
                attributes: attributes_of(limits().attributes_per_signal + 1),
                schema_url: None,
                dropped_attributes_count: 0,
            }
            .into(),
            ..span_with(Attributes::default())
        };
        assert_rejected(
            check_span(&resourced, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
        let scoped = Span {
            scope: InstrumentationScope {
                name: "scope".to_owned(),
                version: None,
                attributes: attributes_of(limits().attributes_per_signal + 1),
                schema_url: None,
                dropped_attributes_count: 0,
            }
            .into(),
            ..span_with(Attributes::default())
        };
        assert_rejected(
            check_span(&scoped, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
    }

    #[test]
    fn the_scope_gate_admits_the_cap_and_refuses_one_more() {
        let at_cap = InstrumentationScope {
            name: "scope".to_owned(),
            version: None,
            attributes: attributes_of(limits().attributes_per_signal),
            schema_url: None,
            dropped_attributes_count: 0,
        };
        assert!(check_scope(&at_cap, &limits()).is_ok());
        let over = InstrumentationScope {
            attributes: attributes_of(limits().attributes_per_signal + 1),
            ..at_cap
        };
        assert_rejected(
            check_scope(&over, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
    }

    #[test]
    fn attributes_per_nested_set_gates_events_links_and_exemplars() {
        let mut span = span_with(Attributes::default());
        span.events.push(SpanEvent {
            time_unix_nano: Some(1),
            name: "event".to_owned(),
            attributes: attributes_of(limits().attributes_per_nested_set + 1),
            dropped_attribute_count: 0,
        });
        assert_rejected(
            check_span(&span, &limits()),
            BudgetName::AttributesPerNestedSet,
            limits().attributes_per_nested_set,
            limits().attributes_per_nested_set + 1,
        );
        let mut linked = span_with(Attributes::default());
        linked.links.push(SpanLink {
            context: span.context.clone(),
            attributes: attributes_of(limits().attributes_per_nested_set + 1),
            dropped_attribute_count: 0,
        });
        assert_rejected(
            check_span(&linked, &limits()),
            BudgetName::AttributesPerNestedSet,
            limits().attributes_per_nested_set,
            limits().attributes_per_nested_set + 1,
        );
        let point = number_point(
            Attributes::default(),
            vec![exemplar_with(attributes_of(
                limits().attributes_per_nested_set + 1,
            ))],
        );
        assert_rejected(
            check_point(&point, &limits()),
            BudgetName::AttributesPerNestedSet,
            limits().attributes_per_nested_set,
            limits().attributes_per_nested_set + 1,
        );
    }

    #[test]
    fn attribute_value_size_counts_keys_and_names() {
        // The budget is the whole entry's accounted size: per-entry
        // overhead + key bytes + value accounted bytes.
        let headroom = limits().attribute_value_bytes - crate::size::KEYED_ENTRY_BYTES - "k".len();
        let at_cap = span_with(
            Attributes::from_pairs(vec![("k".to_owned(), Value::String("x".repeat(headroom)))])
                .expect("unique keys"),
        );
        assert!(
            check_span(&at_cap, &limits()).is_ok(),
            "exactly the cap is admitted"
        );
        let one_over = span_with(
            Attributes::from_pairs(vec![(
                "k".to_owned(),
                Value::String("x".repeat(headroom + 1)),
            )])
            .expect("unique keys"),
        );
        assert_rejected(
            check_span(&one_over, &limits()),
            BudgetName::AttributeValueSize,
            limits().attribute_value_bytes,
            limits().attribute_value_bytes + 1,
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
        many_events.events.extend(std::iter::repeat_n(
            event,
            limits().span_events_per_span + 1,
        ));
        assert_rejected(
            check_span(&many_events, &limits()),
            BudgetName::EventsPerSpan,
            limits().span_events_per_span,
            limits().span_events_per_span + 1,
        );
        let link = SpanLink {
            context: span_with(Attributes::default()).context,
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
        };
        let mut many_links = span_with(Attributes::default());
        many_links
            .links
            .extend(std::iter::repeat_n(link, limits().span_links_per_span + 1));
        assert_rejected(
            check_span(&many_links, &limits()),
            BudgetName::LinksPerSpan,
            limits().span_links_per_span,
            limits().span_links_per_span + 1,
        );
    }

    #[test]
    fn exemplars_per_data_point_is_gated() {
        let point = number_point(
            Attributes::default(),
            std::iter::repeat_n(
                exemplar_with(Attributes::default()),
                limits().exemplars_per_data_point + 1,
            )
            .collect(),
        );
        assert_rejected(
            check_point(&point, &limits()),
            BudgetName::ExemplarsPerDataPoint,
            limits().exemplars_per_data_point,
            limits().exemplars_per_data_point + 1,
        );
        let at_cap = number_point(
            Attributes::default(),
            std::iter::repeat_n(
                exemplar_with(Attributes::default()),
                limits().exemplars_per_data_point,
            )
            .collect(),
        );
        assert!(check_point(&at_cap, &limits()).is_ok());
    }

    #[test]
    fn key_value_list_depth_is_gated_on_attribute_values() {
        let at_cap = span_with(
            Attributes::from_pairs(vec![(
                "k".to_owned(),
                nested_kv(limits().key_value_list_depth),
            )])
            .expect("unique keys"),
        );
        assert!(check_span(&at_cap, &limits()).is_ok());
        let one_over = span_with(
            Attributes::from_pairs(vec![(
                "k".to_owned(),
                nested_kv(limits().key_value_list_depth + 1),
            )])
            .expect("unique keys"),
        );
        assert_rejected(
            check_span(&one_over, &limits()),
            BudgetName::KeyValueListDepth,
            limits().key_value_list_depth,
            limits().key_value_list_depth + 1,
        );
    }

    #[test]
    fn array_nesting_counts_toward_the_depth_budget_too() {
        // The depth budget is about every recursive structure a record
        // carries; an array chain is as recursive as a kvlist chain.
        let arrays_at_cap = span_with(
            Attributes::from_pairs(vec![(
                "k".to_owned(),
                nested_array(limits().key_value_list_depth),
            )])
            .expect("unique keys"),
        );
        assert!(check_span(&arrays_at_cap, &limits()).is_ok());
        let arrays_over = span_with(
            Attributes::from_pairs(vec![(
                "k".to_owned(),
                nested_array(limits().key_value_list_depth + 1),
            )])
            .expect("unique keys"),
        );
        assert_rejected(
            check_span(&arrays_over, &limits()),
            BudgetName::KeyValueListDepth,
            limits().key_value_list_depth,
            limits().key_value_list_depth + 1,
        );
    }

    #[test]
    fn a_sixty_thousand_deep_attribute_is_refused_without_recursing() {
        // BLOCKER 1 regression: the old recursive kv_depth aborted the
        // process inside the gate. The gate must refuse on the default
        // test-thread stack and exit early. Such a chain trips two budgets
        // at once (its accounted size is far over the value ceiling too);
        // with the value budget lifted the depth budget is what fires, and
        // its observed figure is the truthful lower bound.
        let deep = nested_kv(60_000);
        let span =
            span_with(Attributes::from_pairs(vec![("k".to_owned(), deep)]).expect("unique keys"));
        let depth_limits = BudgetLimits {
            attribute_value_bytes: usize::MAX,
            ..BudgetLimits::default()
        };
        let rejection = check_span(&span, &depth_limits)
            .expect_err("a 60,000-deep attribute is over the nesting budget");
        assert_eq!(rejection.budget, BudgetName::KeyValueListDepth);
        assert_eq!(rejection.observed, depth_limits.key_value_list_depth + 1);
        // Under the default limits the same delivery refuses earlier, on
        // the size budget — a truthful refusal either way.
        assert_eq!(
            check_span(&span, &limits())
                .expect_err("over budget")
                .budget,
            BudgetName::AttributeValueSize
        );
    }

    #[test]
    fn a_sixty_thousand_deep_log_body_is_refused() {
        let log = LogRecord {
            body: Some(nested_kv(60_000)),
            ..bare_log()
        };
        let rejection = check_log_record(&log, &limits())
            .expect_err("a 60,000-deep body is over the nesting budget");
        assert_eq!(rejection.budget, BudgetName::KeyValueListDepth);
        // A within-budget body passes.
        let ok = LogRecord {
            body: Some(nested_kv(limits().key_value_list_depth)),
            ..bare_log()
        };
        assert!(check_log_record(&ok, &limits()).is_ok());
    }

    #[test]
    fn data_points_per_export_rejects_the_whole_export() {
        assert!(check_export_point_count(limits().data_points_per_export, &limits()).is_ok());
        assert_rejected(
            check_export_point_count(limits().data_points_per_export + 1, &limits()),
            BudgetName::DataPointsPerExport,
            limits().data_points_per_export,
            limits().data_points_per_export + 1,
        );
    }

    #[test]
    fn the_stream_gate_covers_metadata_resource_scope_and_points() {
        let stream = crate::metrics::MetricStream::new(
            StreamIdentity {
                resource: resource(),
                scope: scope(),
                name: "requests".to_owned(),
                description: None,
                unit: None,
                metadata: attributes_of(limits().attributes_per_signal + 1),
                kind: StreamKind::Gauge,
                temporality: None,
            },
            vec![number_point(Attributes::default(), Vec::new())],
        )
        .expect("a coherent stream");
        assert_rejected(
            check_metric_stream(&stream, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
    }

    #[test]
    fn the_stream_identity_gate_gates_metadata_for_the_point_path() {
        // The ledger admits points, not streams; `check_stream_identity` is the
        // gate its point path applies to the stream-level parts. Within-cap
        // metadata passes; one entry over the per-signal cap refuses with the
        // budget named and the observed count carried.
        let identity = StreamIdentity {
            resource: resource(),
            scope: scope(),
            name: "requests".to_owned(),
            description: None,
            unit: None,
            metadata: attributes_of(limits().attributes_per_signal),
            kind: StreamKind::Gauge,
            temporality: None,
        };
        assert!(check_stream_identity(&identity, &limits()).is_ok());
        let over = StreamIdentity {
            metadata: attributes_of(limits().attributes_per_signal + 1),
            ..identity.clone()
        };
        assert_rejected(
            check_stream_identity(&over, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
    }

    #[test]
    fn the_whole_stream_gate_refuses_an_over_cap_point() {
        let over_point = crate::metrics::MetricStream::new(
            StreamIdentity {
                resource: resource(),
                scope: scope(),
                name: "requests".to_owned(),
                description: None,
                unit: None,
                metadata: Attributes::default(),
                kind: StreamKind::Gauge,
                temporality: None,
            },
            vec![number_point(
                attributes_of(limits().attributes_per_signal + 1),
                Vec::new(),
            )],
        )
        .expect("a coherent stream");
        assert_rejected(
            check_metric_stream(&over_point, &limits()),
            BudgetName::AttributesPerSignal,
            limits().attributes_per_signal,
            limits().attributes_per_signal + 1,
        );
        let clean = crate::metrics::MetricStream::new(
            StreamIdentity {
                resource: resource(),
                scope: scope(),
                name: "requests".to_owned(),
                description: None,
                unit: None,
                metadata: Attributes::default(),
                kind: StreamKind::Gauge,
                temporality: None,
            },
            vec![number_point(Attributes::default(), Vec::new())],
        )
        .expect("a coherent stream");
        assert!(check_metric_stream(&clean, &limits()).is_ok());
        assert!(check_export_point_count(clean.points.len(), &limits()).is_ok());
    }
}
