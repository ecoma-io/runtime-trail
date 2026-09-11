//! Log records.
//!
//! `docs/architecture/telemetry-model.md`, "Log records": event time and
//! observation time are two distinct fields; severity number and severity
//! text are independent preserved fields with no coercion in either
//! direction; the body is a full value; trace context present or absent is
//! a model fact; the dropped-attribute count is preserved.

use crate::context::TraceContext;
use crate::values::{Attributes, Value};
use std::fmt;

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
    /// The record's attributes.
    pub attributes: Attributes,
    /// The emitter's `dropped_attributes_count` for this record.
    pub dropped_attribute_count: u32,
    /// The trace context, when the emitter sent one. Absent means "the
    /// emitter sent none", not "unknown".
    pub trace_context: Option<TraceContext>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{SpanId, TraceFlags, TraceId, TraceState};

    fn trace_context() -> TraceContext {
        TraceContext {
            trace_id: TraceId::from_bytes([9; 16]),
            span_id: SpanId::from_bytes([7; 8]),
            flags: TraceFlags::new(1),
            tracestate: TraceState::default(),
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
            severity_number: None,
            severity_text: None,
            body: None,
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_context: None,
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
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: SeverityNumber::try_new(9).ok(),
            severity_text: None,
            body: None,
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_context: None,
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
        ]);
        let record = LogRecord {
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: None,
            severity_text: None,
            body: Some(body.clone()),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_context: None,
        };
        assert_eq!(record.body, Some(body));
    }

    #[test]
    fn trace_context_presence_is_a_fact_not_a_unknown() {
        let with_context = LogRecord {
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: None,
            severity_text: None,
            body: None,
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_context: Some(trace_context()),
        };
        let without_context = LogRecord {
            trace_context: None,
            ..with_context.clone()
        };
        assert_ne!(with_context, without_context);
        assert_eq!(with_context.trace_context, Some(trace_context()));
        assert!(without_context.trace_context.is_none());
    }

    #[test]
    fn the_dropped_attribute_count_is_preserved() {
        let record = LogRecord {
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: None,
            severity_text: None,
            body: None,
            attributes: Attributes::default(),
            dropped_attribute_count: 41,
            trace_context: None,
        };
        assert_eq!(record.dropped_attribute_count, 41);
    }
}
