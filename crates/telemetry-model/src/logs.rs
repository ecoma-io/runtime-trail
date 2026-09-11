//! Log records.
//!
//! `docs/architecture/telemetry-model.md`, "Log records": event time and
//! observation time are two distinct fields; severity number and severity
//! text are independent preserved fields with no coercion in either
//! direction; the body is a full value; trace context present or absent is
//! a model fact; the dropped-attribute count is preserved.
//!
//! The trace context is three *independent* optional fields — OTLP's log
//! `trace_id`, `span_id` and `flags` are independently optional on the
//! wire, and the model preserves each one's presence verbatim: a record
//! that sent a span id without a trace id keeps exactly that, and nothing
//! fabricates a zero id or drops a sent field. [`LogRecord::correlation_pair`]
//! is the convenience read for the pair the Correlation Engine needs.

use crate::context::{SpanId, TraceFlags, TraceId};
use crate::resources::{InstrumentationScope, Resource};
use crate::values::{Attributes, Value};
use std::fmt;
use std::sync::Arc;

/// A severity number: the integer 1–24 the emitter sent.
///
/// Values outside 1–24 are outside the model's domain and are rejected at
/// construction, never coerced. The mapped display name of a severity is a
/// view concern; nothing here maps or derives one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SeverityNumber(u8);

impl SeverityNumber {
    /// The smallest in-domain severity number.
    pub const MIN: u8 = 1;
    /// The largest in-domain severity number.
    pub const MAX: u8 = 24;

    /// Wraps a severity number the emitter sent.
    ///
    /// # Errors
    ///
    /// Returns [`SeverityOutOfRange`] for any value outside 1–24. The input
    /// is rejected, not clamped or wrapped.
    pub fn try_new(number: u8) -> Result<Self, SeverityOutOfRange> {
        if (Self::MIN..=Self::MAX).contains(&number) {
            Ok(Self(number))
        } else {
            Err(SeverityOutOfRange { attempted: number })
        }
    }

    /// The integer the emitter sent, in 1–24.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// A severity number outside the model's domain (1–24) was offered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeverityOutOfRange {
    /// The rejected value.
    pub attempted: u8,
}

impl fmt::Display for SeverityOutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "severity number {} is outside the domain 1–24",
            self.attempted
        )
    }
}

impl std::error::Error for SeverityOutOfRange {}

/// One log record, exactly as the emitter sent it.
///
/// Two byte-identical log records are two records: OTLP defines no
/// log-record identity, so admission never collapses one onto another (see
/// `docs/architecture/telemetry-model.md`, "Record identity and duplicate
/// delivery").
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogRecord {
    /// Event time, in nanoseconds on the emitter's clock; `None` when the
    /// emitter sent none. Never synthesised from
    /// [`LogRecord::observed_timestamp_unix_nano`].
    pub timestamp_unix_nano: Option<u64>,
    /// Observation time, in nanoseconds on the emitter's clock; `None`
    /// when the emitter sent none. Never synthesised from
    /// [`LogRecord::timestamp_unix_nano`].
    pub observed_timestamp_unix_nano: Option<u64>,
    /// The severity *number* (1–24), independent of
    /// [`LogRecord::severity_text`].
    pub severity_number: Option<SeverityNumber>,
    /// The severity *text*, free-form emitter text, independent of
    /// [`LogRecord::severity_number`]. No coercion happens in either
    /// direction.
    pub severity_text: Option<String>,
    /// The body — a full value, not necessarily a string.
    pub body: Option<Value>,
    /// The resource the record was emitted under, shared with the batch.
    pub resource: Arc<Resource>,
    /// The instrumentation scope the record was emitted under, shared with
    /// the batch.
    pub scope: Arc<InstrumentationScope>,
    /// The record's attributes.
    pub attributes: Attributes,
    /// The emitter's `dropped_attributes_count` for this record.
    pub dropped_attribute_count: u32,
    /// The trace id, when the emitter sent one — independently optional,
    /// preserved verbatim, never fabricated.
    pub trace_id: Option<TraceId>,
    /// The span id, when the emitter sent one — independently optional,
    /// preserved verbatim, never fabricated.
    pub span_id: Option<SpanId>,
    /// The wire flags field, when the emitter sent one — independently
    /// optional, all 32 bits preserved.
    pub trace_flags: Option<TraceFlags>,
    /// The event name, when the emitter sent one (OTLP
    /// `LogRecord.event_name`).
    pub event_name: Option<String>,
}

impl LogRecord {
    /// The correlation pair — `(trace_id, span_id)` — but only when *both*
    /// are present. A record carrying one without the other has no pair:
    /// nothing here fabricates the missing half.
    #[must_use]
    pub fn correlation_pair(&self) -> Option<(TraceId, SpanId)> {
        match (self.trace_id, self.span_id) {
            (Some(trace_id), Some(span_id)) => Some((trace_id, span_id)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(trace: [u8; 16], span: [u8; 8]) -> (TraceId, SpanId) {
        (TraceId::from_bytes(trace), SpanId::from_bytes(span))
    }

    fn empty_resource() -> Arc<Resource> {
        Arc::new(Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        })
    }

    fn empty_scope() -> Arc<InstrumentationScope> {
        Arc::new(InstrumentationScope {
            name: "scope".to_owned(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        })
    }

    fn record() -> LogRecord {
        LogRecord {
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: None,
            severity_text: None,
            body: None,
            resource: empty_resource(),
            scope: empty_scope(),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_id: None,
            span_id: None,
            trace_flags: None,
            event_name: None,
        }
    }

    #[test]
    fn severity_numbers_accept_the_whole_domain_and_nothing_outside_it() {
        assert_eq!(SeverityNumber::try_new(1).map(SeverityNumber::get), Ok(1));
        assert_eq!(SeverityNumber::try_new(24).map(SeverityNumber::get), Ok(24));
        let rejected = SeverityNumber::try_new(25).expect_err("outside 1–24");
        assert_eq!(rejected.attempted, 25);
        assert_eq!(
            SeverityNumber::try_new(0)
                .expect_err("outside 1–24")
                .attempted,
            0
        );
    }

    #[test]
    fn event_time_and_observation_time_are_two_distinct_fields() {
        let record = LogRecord {
            timestamp_unix_nano: Some(1_000),
            observed_timestamp_unix_nano: Some(2_000),
            ..record()
        };
        assert_ne!(
            record.timestamp_unix_nano, record.observed_timestamp_unix_nano,
            "neither field substitutes for the other"
        );
        let observation_only = LogRecord {
            timestamp_unix_nano: None,
            ..record.clone()
        };
        assert_ne!(observation_only, record);
        assert!(observation_only.timestamp_unix_nano.is_none());
    }

    #[test]
    fn severity_number_and_severity_text_are_independent() {
        let number_only = LogRecord {
            severity_number: SeverityNumber::try_new(9).ok(),
            ..record()
        };
        let text_only = LogRecord {
            severity_number: None,
            severity_text: Some("NOTICE-ish".to_owned()),
            ..number_only.clone()
        };
        assert_ne!(number_only, text_only);
        assert!(number_only.severity_text.is_none());
        assert!(text_only.severity_number.is_none());
        let both = LogRecord {
            severity_number: SeverityNumber::try_new(17).ok(),
            severity_text: Some("custom label".to_owned()),
            ..number_only
        };
        assert_eq!(both.severity_number.map(SeverityNumber::get), Some(17));
        assert_eq!(both.severity_text.as_deref(), Some("custom label"));
    }

    #[test]
    fn the_body_is_a_full_value_not_necessarily_a_string() {
        let body = Value::kv_list(vec![
            ("error".to_owned(), Value::Bool(true)),
            ("code".to_owned(), Value::Int(503)),
        ])
        .expect("ok");
        let record = LogRecord {
            body: Some(body.clone()),
            ..record()
        };
        assert_eq!(record.body, Some(body));
    }

    #[test]
    fn trace_context_fields_are_three_independent_optional_facts() {
        let (trace, span) = ids([9; 16], [7; 8]);
        let full = LogRecord {
            trace_id: Some(trace),
            span_id: Some(span),
            trace_flags: Some(TraceFlags::new(1)),
            ..record()
        };
        let trace_only = LogRecord {
            trace_id: Some(trace),
            ..record()
        };
        assert_ne!(full, trace_only, "each field's presence is its own fact");
        assert!(trace_only.span_id.is_none());
        assert!(trace_only.trace_flags.is_none());
        let span_only = LogRecord {
            span_id: Some(span),
            ..record()
        };
        assert_ne!(
            trace_only, span_only,
            "a span id without a trace id is preserved exactly as sent"
        );
        assert!(span_only.trace_id.is_none());
    }

    #[test]
    fn the_correlation_pair_exists_only_when_both_ids_are_present() {
        let (trace, span) = ids([9; 16], [7; 8]);
        assert_eq!(record().correlation_pair(), None, "nothing sent");
        let both = LogRecord {
            trace_id: Some(trace),
            span_id: Some(span),
            ..record()
        };
        assert_eq!(both.correlation_pair(), Some((trace, span)));
        let half = LogRecord {
            trace_id: Some(trace),
            ..record()
        };
        assert_eq!(
            half.correlation_pair(),
            None,
            "one id alone is not a pair; nothing is fabricated"
        );
    }

    #[test]
    fn flags_are_wide_and_preserved_when_sent() {
        let (trace, span) = ids([9; 16], [7; 8]);
        let remote = TraceFlags::new((1 << 8) | 1);
        let record = LogRecord {
            trace_id: Some(trace),
            span_id: Some(span),
            trace_flags: Some(remote),
            ..record()
        };
        assert_eq!(record.trace_flags.map(TraceFlags::bits), Some(1 << 8 | 1));
    }

    #[test]
    fn the_event_name_is_preserved_when_sent() {
        let named = LogRecord {
            event_name: Some("user.login".to_owned()),
            ..record()
        };
        assert_eq!(named.event_name.as_deref(), Some("user.login"));
        assert!(
            record().event_name.is_none(),
            "absent is distinct from empty"
        );
        let empty_named = LogRecord {
            event_name: Some(String::new()),
            ..record()
        };
        assert_ne!(named, empty_named);
    }

    #[test]
    fn resource_and_scope_ride_on_the_record() {
        let record = record();
        assert_eq!(Arc::strong_count(&record.resource), 1);
        let cloned = record.clone();
        assert!(Arc::ptr_eq(&record.resource, &cloned.resource));
        assert!(Arc::ptr_eq(&record.scope, &cloned.scope));
    }

    #[test]
    fn the_dropped_attribute_count_is_preserved() {
        let record = LogRecord {
            dropped_attribute_count: 41,
            ..record()
        };
        assert_eq!(record.dropped_attribute_count, 41);
    }
}
