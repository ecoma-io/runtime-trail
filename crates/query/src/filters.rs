//! Content filters on the records flow: service, time range, severity and
//! scope identity, as predicates over the record view.
//!
//! The vocabulary is the [model's](../../docs/architecture/telemetry-model.md)
//! and the [query contract's](../../docs/architecture/query-model.md) —
//! resource identity, model timestamps, severity, scope identity — never a
//! storage concept. Each kind carries its own filter struct, so an invalid
//! state is *unrepresentable*: a span or metric query cannot express a
//! severity filter because [`SpanFilters`] and [`MetricFilters`] carry no
//! severity field, and there is no invalid-query error to invent. An
//! all-`None` filter struct matches every record of its kind, which is why
//! a kind-only query and a kind-plus-empty-filters query are one query:
//! identical canonical bytes, identical fingerprint, identical pages.
//!
//! Every filter is a *predicate*: the walk examines records in residency
//! order and skips the non-matching ones. A filtered-out record is still
//! examined — it charges `max_scan` — and it never charges
//! `max_results`/`max_bytes` and never enters an `omitted` count. Filters
//! never reorder anything and never enter a driver.
//!
//! # Match semantics
//!
//! - **Service** — exact string equality against the resource identity's
//!   `service.name` attribute. "Service" is not a typed model field
//!   (telemetry-model.md, "Resource and scope identity") — it is read here
//!   like any attribute, and only a string value names a service: a record
//!   whose resource carries no `service.name` (or a non-string one) matches
//!   nothing when the filter is set. For a metric point the resource is the
//!   joined `StreamIdentity`'s.
//! - **Time range** — half-open `[from, to)` in unix nanos against the
//!   kind's model timestamp: a span's `start_time_unix_nano`; a log
//!   record's `timestamp_unix_nano`, falling back to
//!   `observed_timestamp_unix_nano` when the timestamp is absent; a metric
//!   point's `time_unix_nano` — the interval end or measurement time,
//!   uniform across all four point variants. A record with no time key for
//!   its kind is outside any bounded range: neither side matches.
//! - **Severity** — logs only: a minimum [`SeverityNumber`], matching
//!   `>=` the threshold. `severity_text` is not filtered in M2, and a log
//!   with no severity number is outside a severity-filtered answer.
//! - **Scope** — exact name plus exact version. The filter names the
//!   version's *presence*: a filter without a version matches only scopes
//!   sent without one — absent is never equal to the empty string, and the
//!   same name under a different version is a different scope.

use runtime_trail_telemetry_model::{
    InstrumentationScope, LogRecord, Resource, SeverityNumber, Value,
};

use crate::engine::RecordView;

/// The resource attribute whose value names the service.
const SERVICE_NAME_KEY: &str = "service.name";

/// The canonical-encoding presence byte marking a set filter.
const PRESENT: u8 = 1;

/// The canonical-encoding presence byte marking an unset filter.
const ABSENT: u8 = 0;

/// The service filter: exact string equality against the resource
/// identity's `service.name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceFilter {
    name: String,
}

/// The time-range filter: a half-open `[from, to)` window in unix nanos on
/// the emitter's clock.
///
/// `from` is inclusive, `to` exclusive: a record whose time key is exactly
/// `to` is outside the window. Both bounds are required — a one-sided or
/// unbounded window is not a filter this flow expresses. A window whose
/// `from` is not below its `to` is simply empty and matches nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRangeFilter {
    from: u64,
    to: u64,
}

/// The severity filter: a minimum [`SeverityNumber`], matching `>=` the
/// threshold. Logs only — spans and metric points carry no severity field,
/// so their filter structs cannot express this.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeverityFilter {
    minimum: SeverityNumber,
}

/// The scope filter: exact name plus exact version, presence included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeFilter {
    name: String,
    version: Option<String>,
}

impl ServiceFilter {
    /// A filter naming one service.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// The service name the filter selects.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether the record's resource identity names exactly this service:
    /// a present string value, byte-equal to the filter's name.
    #[must_use]
    fn matches(&self, record: &RecordView) -> bool {
        match resource_of(record).identity().get(SERVICE_NAME_KEY) {
            Some(Value::String(name)) => *name == self.name,
            _ => false,
        }
    }

    /// Appends the filter's canonical bytes: the presence byte, the name's
    /// length, the name's UTF-8 bytes.
    fn push_canonical(&self, bytes: &mut Vec<u8>) {
        bytes.push(PRESENT);
        push_str(bytes, &self.name);
    }
}

impl TimeRangeFilter {
    /// A filter naming the half-open window `[from, to)`.
    #[must_use]
    pub const fn new(from: u64, to: u64) -> Self {
        Self { from, to }
    }

    /// The window's inclusive lower bound.
    #[must_use]
    pub const fn from(&self) -> u64 {
        self.from
    }

    /// The window's exclusive upper bound.
    #[must_use]
    pub const fn to(&self) -> u64 {
        self.to
    }

    /// Whether the record's kind-specific time key lies in the half-open
    /// window. A record with no time key is outside: neither side of a
    /// bounded window matches it.
    #[must_use]
    fn matches(&self, record: &RecordView) -> bool {
        match time_key_of(record) {
            Some(time) => time >= self.from && time < self.to,
            None => false,
        }
    }

    /// Appends the filter's canonical bytes: the presence byte, then both
    /// bounds.
    fn push_canonical(&self, bytes: &mut Vec<u8>) {
        bytes.push(PRESENT);
        bytes.extend_from_slice(&self.from.to_le_bytes());
        bytes.extend_from_slice(&self.to.to_le_bytes());
    }
}

impl SeverityFilter {
    /// A filter selecting records at or above `minimum`.
    #[must_use]
    pub const fn new(minimum: SeverityNumber) -> Self {
        Self { minimum }
    }

    /// The threshold: records at or above it match.
    #[must_use]
    pub const fn minimum(&self) -> SeverityNumber {
        self.minimum
    }

    /// Whether the record's severity number is present and `>=` the
    /// threshold. A record with no severity is outside.
    #[must_use]
    fn matches(self, record: &RecordView) -> bool {
        let RecordView::LogRecord(record) = record else {
            return false;
        };
        match record.severity_number {
            Some(severity) => severity.get() >= self.minimum.get(),
            None => false,
        }
    }

    /// Appends the filter's canonical bytes: the presence byte, then the
    /// threshold's value.
    fn push_canonical(self, bytes: &mut Vec<u8>) {
        bytes.push(PRESENT);
        bytes.push(self.minimum.get());
    }
}

impl ScopeFilter {
    /// A filter naming one scope: `version` of `None` matches only scopes
    /// sent without a version.
    #[must_use]
    pub fn new(name: impl Into<String>, version: Option<String>) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }

    /// The scope name the filter selects.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The scope version the filter selects; `None` selects scopes sent
    /// without one.
    #[must_use]
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Whether the record's scope carries exactly this name and version —
    /// absent never equals the empty string.
    #[must_use]
    fn matches(&self, record: &RecordView) -> bool {
        let scope = scope_of(record);
        scope.name == self.name && scope.version == self.version
    }

    /// Appends the filter's canonical bytes: the presence byte, the name,
    /// the version's presence byte, then the version when present.
    fn push_canonical(&self, bytes: &mut Vec<u8>) {
        bytes.push(PRESENT);
        push_str(bytes, &self.name);
        match &self.version {
            Some(version) => {
                bytes.push(PRESENT);
                push_str(bytes, version);
            }
            None => bytes.push(ABSENT),
        }
    }
}

/// Span filters: service, time range and scope identity. No severity field
/// exists to set: the invalid state is unrepresentable (F2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanFilters {
    /// The service filter, when set.
    pub service: Option<ServiceFilter>,
    /// The half-open window over `Span::start_time_unix_nano`, when set.
    pub time_range: Option<TimeRangeFilter>,
    /// The scope-identity filter, when set.
    pub scope: Option<ScopeFilter>,
}

/// Log filters: service, time range, severity and scope identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogFilters {
    /// The service filter, when set.
    pub service: Option<ServiceFilter>,
    /// The half-open window over `LogRecord::timestamp_unix_nano` —
    /// falling back to `observed_timestamp_unix_nano` — when set.
    pub time_range: Option<TimeRangeFilter>,
    /// The minimum-severity filter, when set.
    pub severity: Option<SeverityFilter>,
    /// The scope-identity filter, when set.
    pub scope: Option<ScopeFilter>,
}

/// Metric filters: service, time range and scope identity. The service and
/// scope filters read the joined stream identity (`StreamIdentity`) — the
/// stream's resource and scope are the point's. No severity field exists to
/// set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricFilters {
    /// The service filter, when set.
    pub service: Option<ServiceFilter>,
    /// The half-open window over the point's `time_unix_nano`, when set.
    pub time_range: Option<TimeRangeFilter>,
    /// The scope-identity filter, when set.
    pub scope: Option<ScopeFilter>,
}

impl SpanFilters {
    /// The all-`None` filter set: every span matches — byte-identical to a
    /// kind-only query (F2).
    #[must_use]
    pub const fn all_none() -> Self {
        Self {
            service: None,
            time_range: None,
            scope: None,
        }
    }

    /// Whether the span view matches every set filter: a predicate, never
    /// an order (F4).
    #[must_use]
    pub fn matches(&self, view: &RecordView) -> bool {
        self.service
            .as_ref()
            .is_none_or(|filter| filter.matches(view))
            && self
                .time_range
                .as_ref()
                .is_none_or(|filter| filter.matches(view))
            && self
                .scope
                .as_ref()
                .is_none_or(|filter| filter.matches(view))
    }

    /// Appends the filter set's canonical bytes: one field per fixed slot,
    /// a presence byte when unset (F5).
    fn push_canonical(&self, bytes: &mut Vec<u8>) {
        match &self.service {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
        match &self.time_range {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
        match &self.scope {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
    }
}

impl LogFilters {
    /// The all-`None` filter set: every log matches (F2).
    #[must_use]
    pub const fn all_none() -> Self {
        Self {
            service: None,
            time_range: None,
            severity: None,
            scope: None,
        }
    }

    /// Whether the log view matches every set filter (F4).
    #[must_use]
    pub fn matches(&self, view: &RecordView) -> bool {
        self.service
            .as_ref()
            .is_none_or(|filter| filter.matches(view))
            && self
                .time_range
                .as_ref()
                .is_none_or(|filter| filter.matches(view))
            && self
                .severity
                .as_ref()
                .is_none_or(|filter| filter.matches(view))
            && self
                .scope
                .as_ref()
                .is_none_or(|filter| filter.matches(view))
    }

    /// Appends the filter set's canonical bytes (F5).
    fn push_canonical(&self, bytes: &mut Vec<u8>) {
        match &self.service {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
        match &self.time_range {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
        match &self.severity {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
        match &self.scope {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
    }
}

impl MetricFilters {
    /// The all-`None` filter set: every point matches (F2).
    #[must_use]
    pub const fn all_none() -> Self {
        Self {
            service: None,
            time_range: None,
            scope: None,
        }
    }

    /// Whether the metric view matches every set filter — the service and
    /// scope read the joined stream identity (F4).
    #[must_use]
    pub fn matches(&self, view: &RecordView) -> bool {
        self.service
            .as_ref()
            .is_none_or(|filter| filter.matches(view))
            && self
                .time_range
                .as_ref()
                .is_none_or(|filter| filter.matches(view))
            && self
                .scope
                .as_ref()
                .is_none_or(|filter| filter.matches(view))
    }

    /// Appends the filter set's canonical bytes (F5).
    fn push_canonical(&self, bytes: &mut Vec<u8>) {
        match &self.service {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
        match &self.time_range {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
        match &self.scope {
            Some(filter) => filter.push_canonical(bytes),
            None => bytes.push(ABSENT),
        }
    }
}

/// The filters a records query carries: kind-tagged, so the filter struct
/// always answers the query's [`SignalKind`](crate::engine::SignalKind) —
/// a log query cannot arrive carrying span filters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordsFilters {
    /// Span filters.
    Spans(SpanFilters),
    /// Log filters.
    LogRecords(LogFilters),
    /// Metric filters.
    MetricPoints(MetricFilters),
}

impl RecordsFilters {
    /// Whether the record view matches the filter set. The walk pairs a
    /// filter set only with its own kind's views; anything else is a
    /// construction bug, so debug builds assert it and release builds fall
    /// back to the kind-only behavior — the record is not filtered out by
    /// a filter set that cannot name it.
    #[must_use]
    pub fn matches(&self, view: &RecordView) -> bool {
        match (self, view) {
            (Self::Spans(filters), RecordView::Span(_)) => filters.matches(view),
            (Self::LogRecords(filters), RecordView::LogRecord(_)) => filters.matches(view),
            (Self::MetricPoints(filters), RecordView::MetricPoint { .. }) => filters.matches(view),
            _ => {
                debug_assert!(false, "a filter set meets only its own kind's record views");
                true
            }
        }
    }

    /// Appends the filter set's canonical bytes to the query description:
    /// every filter field, in a fixed order per kind, each with a presence
    /// byte when unset (F5). Strings are length-prefixed, so two distinct
    /// filter sets never encode to the same bytes — the fingerprint the
    /// bytes hash is a faithful filter-set identity.
    pub(crate) fn push_canonical(&self, bytes: &mut Vec<u8>) {
        match self {
            Self::Spans(filters) => filters.push_canonical(bytes),
            Self::LogRecords(filters) => filters.push_canonical(bytes),
            Self::MetricPoints(filters) => filters.push_canonical(bytes),
        }
    }
}

/// The record's resource: a span's or a log record's own resource, a
/// metric point's joined stream identity's resource.
fn resource_of(view: &RecordView) -> &Resource {
    match view {
        RecordView::Span(span) => &span.resource,
        RecordView::LogRecord(record) => &record.resource,
        RecordView::MetricPoint { stream, .. } => &stream.resource,
    }
}

/// The record's scope: a span's or a log record's own scope, a metric
/// point's joined stream identity's scope.
fn scope_of(view: &RecordView) -> &InstrumentationScope {
    match view {
        RecordView::Span(span) => &span.scope,
        RecordView::LogRecord(record) => &record.scope,
        RecordView::MetricPoint { stream, .. } => &stream.scope,
    }
}

/// The kind's time key for a record view: the span's start time; the log
/// record's event timestamp falling back to its observation timestamp; the
/// metric point's `time_unix_nano`, which every point variant carries.
fn time_key_of(view: &RecordView) -> Option<u64> {
    match view {
        RecordView::Span(span) => Some(span.start_time_unix_nano),
        RecordView::LogRecord(record) => log_time_key(record),
        RecordView::MetricPoint { point, .. } => Some(point.time_unix_nano()),
    }
}

/// A log record's time key: `timestamp_unix_nano` when the emitter sent
/// one, else `observed_timestamp_unix_nano`; a record with neither has no
/// time key (F3).
fn log_time_key(record: &LogRecord) -> Option<u64> {
    record
        .timestamp_unix_nano
        .or(record.observed_timestamp_unix_nano)
}

/// Appends one length-prefixed UTF-8 string: its byte length, then the
/// bytes. The prefix keeps the query description injective — no two
/// distinct filter sets encode to the same bytes (F5).
fn push_str(bytes: &mut Vec<u8>, value: &str) {
    let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runtime_trail_telemetry_model::{
        Attributes, EmitterDroppedCounts, MetricNumber, MetricPoint, NumberPoint, Span, SpanId,
        SpanKind, SpanStatus, SpanStatusCode, StreamIdentity, StreamKind, TraceContext, TraceFlags,
        TraceId, TraceState,
    };

    use super::*;

    /// A resource whose attribute map names `service` or carries nothing.
    fn resource(service: Option<&str>) -> Resource {
        let attributes = match service {
            Some(name) => Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String(name.to_owned()),
            )])
            .expect("one key"),
            None => Attributes::default(),
        };
        Resource {
            attributes,
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    /// A scope with the given name and version.
    fn scope(name: &str, version: Option<&str>) -> InstrumentationScope {
        InstrumentationScope {
            name: name.to_owned(),
            version: version.map(str::to_owned),
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    /// A span view over a service, scope and start time.
    fn span_view(service: Option<&str>, scope: &InstrumentationScope, start: u64) -> RecordView {
        RecordView::Span(Arc::new(Span {
            context: TraceContext {
                trace_id: TraceId::from_bytes([1; 16]),
                span_id: SpanId::from_bytes([2; 8]),
                flags: TraceFlags::new(1),
                tracestate: TraceState::default(),
            },
            parent_span_id: None,
            name: "span".to_owned(),
            kind: SpanKind::Server,
            start_time_unix_nano: start,
            end_time_unix_nano: None,
            resource: Arc::new(resource(service)),
            scope: Arc::new(scope.clone()),
            attributes: Attributes::default(),
            emitter_dropped: EmitterDroppedCounts::default(),
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: SpanStatusCode::Unset,
                message: String::new(),
            },
        }))
    }

    /// A log view over every filter-relevant field.
    #[allow(clippy::too_many_arguments)]
    fn log_view(
        service: Option<&str>,
        scope: &InstrumentationScope,
        timestamp: Option<u64>,
        observed: Option<u64>,
        severity: Option<SeverityNumber>,
    ) -> RecordView {
        RecordView::LogRecord(Arc::new(LogRecord {
            timestamp_unix_nano: timestamp,
            observed_timestamp_unix_nano: observed,
            severity_number: severity,
            severity_text: None,
            body: None,
            resource: Arc::new(resource(service)),
            scope: Arc::new(scope.clone()),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_id: None,
            span_id: None,
            trace_flags: None,
            event_name: None,
        }))
    }

    /// A metric point view whose joined stream identity carries the given
    /// service and scope, and whose point sits at `time`.
    fn point_view(service: Option<&str>, scope: &InstrumentationScope, time: u64) -> RecordView {
        RecordView::MetricPoint {
            point: Arc::new(MetricPoint::Number(NumberPoint::measurement(
                time,
                MetricNumber::int(1),
                Attributes::default(),
                Vec::new(),
            ))),
            stream: Arc::new(StreamIdentity {
                resource: resource(service),
                scope: scope.clone(),
                name: "requests".to_owned(),
                description: None,
                unit: None,
                metadata: Attributes::default(),
                kind: StreamKind::Gauge,
                temporality: None,
            }),
        }
    }

    /// The service predicate is exact string equality against
    /// `service.name`, and a resource without one matches nothing (F3).
    #[test]
    fn service_matches_exactly_and_a_missing_name_matches_nothing() {
        let scope = scope("s", None);
        let filter = ServiceFilter::new("checkout");
        assert!(filter.matches(&span_view(Some("checkout"), &scope, 10)));
        assert!(!filter.matches(&span_view(Some("payments"), &scope, 10)));
        assert!(
            !filter.matches(&span_view(None, &scope, 10)),
            "no service.name matches nothing under a set filter"
        );
        assert!(
            !filter.matches(&log_view(None, &scope, None, None, None)),
            "the same rule for logs"
        );
        assert!(
            !filter.matches(&point_view(None, &scope, 10)),
            "and for points, whose service rides the stream identity"
        );
        // An empty value under a present key is a value, distinct from the
        // key's absence.
        assert!(!filter.matches(&span_view(Some(""), &scope, 10)));
        assert!(ServiceFilter::new("").matches(&span_view(Some(""), &scope, 10)));
    }

    /// The window is half-open on both ends, and a record with no time key
    /// for its kind is outside any bounded range (F3).
    #[test]
    fn time_range_is_half_open_and_excludes_records_without_a_time_key() {
        let scope = scope("s", None);
        let window = TimeRangeFilter::new(100, 200);
        assert!(
            window.matches(&span_view(Some("a"), &scope, 100)),
            "from in"
        );
        assert!(window.matches(&span_view(Some("a"), &scope, 199)));
        assert!(
            !window.matches(&span_view(Some("a"), &scope, 200)),
            "to out"
        );
        assert!(!window.matches(&span_view(Some("a"), &scope, 99)));

        let no_time = log_view(Some("a"), &scope, None, None, None);
        assert!(
            !window.matches(&no_time),
            "no timestamp and no observation time: outside any bounded range"
        );
        assert!(
            !TimeRangeFilter::new(0, u64::MAX).matches(&no_time),
            "even the widest bounded range excludes it"
        );
    }

    /// The time key is the kind's own model timestamp: a log's event time
    /// falling back to its observation time, and the point's
    /// `time_unix_nano` (F3).
    #[test]
    fn each_kind_filters_on_its_own_model_timestamp() {
        let scope = scope("s", None);
        let window = TimeRangeFilter::new(100, 200);
        // The observation timestamp is the fallback, never the primary.
        assert!(window.matches(&log_view(Some("a"), &scope, None, Some(150), None)));
        assert!(!window.matches(&log_view(Some("a"), &scope, None, Some(250), None)));
        assert!(
            !window.matches(&log_view(Some("a"), &scope, Some(50), Some(150), None)),
            "the event timestamp wins when both were sent"
        );
        assert!(window.matches(&point_view(Some("a"), &scope, 150)));
        assert!(!window.matches(&point_view(Some("a"), &scope, 200)));
        assert!(!window.matches(&point_view(Some("a"), &scope, 99)));
    }

    /// Severity is a floor: at-threshold matches, below fails, and a log
    /// with no severity is outside a severity-filtered answer (F3).
    #[test]
    fn severity_is_a_minimum_and_absent_severity_is_outside() {
        let scope = scope("s", None);
        let filter = SeverityFilter::new(SeverityNumber::try_new(9).expect("in domain"));
        let at = SeverityNumber::try_new(9).expect("in domain");
        let above = SeverityNumber::try_new(10).expect("in domain");
        let below = SeverityNumber::try_new(8).expect("in domain");
        assert!(filter.matches(&log_view(Some("a"), &scope, None, None, Some(at))));
        assert!(filter.matches(&log_view(Some("a"), &scope, None, None, Some(above))));
        assert!(!filter.matches(&log_view(Some("a"), &scope, None, None, Some(below))));
        assert!(
            !filter.matches(&log_view(Some("a"), &scope, None, None, None)),
            "no severity number: outside"
        );
    }

    /// The scope predicate is exact name plus exact version, presence
    /// included: absent never equals the empty string (F3).
    #[test]
    fn scope_matches_exact_name_and_version_including_absence() {
        let checkout = ScopeFilter::new("scope", Some("1.2.3".to_owned()));
        let versionless = ScopeFilter::new("scope", None);
        assert!(checkout.matches(&span_view(None, &scope("scope", Some("1.2.3")), 10)));
        assert!(
            !checkout.matches(&span_view(None, &scope("scope", Some("1.2.4")), 10)),
            "a different version is a different scope"
        );
        assert!(
            !checkout.matches(&span_view(None, &scope("scope", None), 10)),
            "a present filter version does not match an absent one"
        );
        assert!(versionless.matches(&span_view(None, &scope("scope", None), 10)));
        assert!(
            !versionless.matches(&span_view(None, &scope("scope", Some("")), 10)),
            "absent is not the empty string"
        );
        assert!(
            !versionless.matches(&span_view(None, &scope("other", None), 10)),
            "the name still has to match"
        );
    }

    /// An all-`None` filter set matches every record of its kind (F2).
    #[test]
    fn all_none_filters_match_everything() {
        let scope = scope("s", None);
        assert!(SpanFilters::all_none().matches(&span_view(Some("a"), &scope, 10)));
        assert!(LogFilters::all_none().matches(&log_view(None, &scope, None, None, None)));
        assert!(MetricFilters::all_none().matches(&point_view(None, &scope, 10)));
    }

    /// Severity is unexpressible for spans and metric points *by
    /// construction* (F2): this test builds each filter struct with an
    /// exhaustive field list, so a struct that grows a severity field
    /// breaks the compile, not a runtime assertion.
    #[test]
    fn span_and_metric_filters_carry_no_severity_field() {
        let span = SpanFilters {
            service: None,
            time_range: None,
            scope: None,
        };
        let metric = MetricFilters {
            service: None,
            time_range: None,
            scope: None,
        };
        assert_eq!(span, SpanFilters::all_none());
        assert_eq!(metric, MetricFilters::all_none());
    }

    /// The canonical encoding separates filter sets: an all-`None` set
    /// differs from one with a filter set, and the encoding is injective
    /// even across string content that would otherwise collide — a service
    /// name carrying the presence and length bytes of later fields cannot
    /// impersonate another set (F5).
    #[test]
    fn canonical_encodings_distinguish_every_filter_set() {
        fn span_bytes(filters: &SpanFilters) -> Vec<u8> {
            let mut bytes = Vec::new();
            filters.push_canonical(&mut bytes);
            bytes
        }

        let empty = span_bytes(&SpanFilters::all_none());
        let replay = span_bytes(&SpanFilters::all_none());
        assert_eq!(empty, replay, "the encoding is deterministic");
        assert_eq!(empty, vec![ABSENT, ABSENT, ABSENT]);

        let service = span_bytes(&SpanFilters {
            service: Some(ServiceFilter::new("a")),
            time_range: None,
            scope: None,
        });
        assert_ne!(empty, service);

        // Two sets whose only difference is inside string content: the
        // length prefixes keep them distinct byte strings.
        let hostile = span_bytes(&SpanFilters {
            service: Some(ServiceFilter::new("\u{1}b")),
            time_range: None,
            scope: None,
        });
        let innocent = span_bytes(&SpanFilters {
            service: None,
            time_range: None,
            scope: Some(ScopeFilter::new("b", None)),
        });
        assert_ne!(hostile, innocent, "no structural collision");
    }
}
