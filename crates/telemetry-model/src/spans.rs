//! Spans.
//!
//! `docs/architecture/telemetry-model.md`, "Spans": the trace context, an
//! optional parent, the name verbatim, all six OTLP kinds distinct,
//! nanosecond timestamps on the emitter's clock, an unset end distinct from
//! any ended value, events and links in emitter order, a two-field status
//! preserved even when unset, and the emitter-reported dropped counts kept
//! as data.
//!
//! OTLP attaches resource and scope at the envelope level; the model
//! flattens them onto every span because contract rule 2 makes resource and
//! scope identity first-class for correlation (the metric side already
//! carries them via its stream identity). Both ride in an `Arc` so the many
//! records of one batch share one allocation.

use crate::context::{SpanId, TraceContext, TraceId};
use crate::resources::{InstrumentationScope, Resource};
use crate::values::Attributes;
use std::sync::Arc;

/// The span kind — all six OTLP values, distinct, never collapsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SpanKind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

/// The status code: unset, ok, or error. Never derived from the kind or
/// the events.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum SpanStatusCode {
    #[default]
    Unset,
    Ok,
    Error,
}

/// The two status fields, both preserved — even when the code is unset.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SpanStatus {
    /// The code as sent.
    pub code: SpanStatusCode,
    /// The message as sent, preserved even under [`SpanStatusCode::Unset`].
    pub message: String,
}

/// The loss the emitter itself reported, preserved as data.
///
/// These counts are part of faithfulness and part of eviction
/// observability: emitter-side loss is visible through them, runtime-side
/// refusal through budget rejections.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct EmitterDroppedCounts {
    /// `dropped_attributes_count` as the emitter sent it.
    pub attributes: u32,
    /// `dropped_events_count` as the emitter sent it.
    pub events: u32,
    /// `dropped_links_count` as the emitter sent it.
    pub links: u32,
}

/// One span event, at the position in the emitter's order where it was
/// sent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SpanEvent {
    /// The event's own timestamp in nanoseconds on the emitter's clock;
    /// `None` when the emitter sent none.
    pub time_unix_nano: Option<u64>,
    /// The event name, verbatim.
    pub name: String,
    /// The event's attributes.
    pub attributes: Attributes,
    /// The emitter's `dropped_attributes_count` for this event.
    pub dropped_attribute_count: u32,
}

/// One span link: the linked trace context and its attributes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SpanLink {
    /// The linked trace context, including flags and, where sent,
    /// tracestate.
    pub context: TraceContext,
    /// The link's attributes.
    pub attributes: Attributes,
    /// The emitter's `dropped_attributes_count` for this link.
    pub dropped_attribute_count: u32,
}

/// A span: trace context, optional parent, name, kind, emitter-clock
/// nanosecond timestamps, resource and scope identity, attributes, events,
/// links, status and the emitter-reported dropped counts.
///
/// `end_time_unix_nano == None` means the emitter never ended the span; it
/// is distinct from any ended value, including `Some(t)` where
/// `t == start_time_unix_nano` (a valid zero-length span). The runtime's
/// own admission time is never stored here — it is separate metadata on the
/// admitted record, and never substitutes for an emitter timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Span {
    /// The span's own trace context: trace id, span id, flags, tracestate.
    pub context: TraceContext,
    /// The parent span id as sent: `None` is an absent parent (a root
    /// span), `Some(zero)` is the zero parent the emitter sent — distinct
    /// facts, both preserved.
    pub parent_span_id: Option<SpanId>,
    /// The span name, verbatim.
    pub name: String,
    /// The kind, one of six, never collapsed.
    pub kind: SpanKind,
    /// Start time in nanoseconds on the emitter's clock.
    pub start_time_unix_nano: u64,
    /// End time in nanoseconds on the emitter's clock; `None` while the
    /// span is unfinished.
    pub end_time_unix_nano: Option<u64>,
    /// The resource the span was emitted under, shared with the batch.
    pub resource: Arc<Resource>,
    /// The instrumentation scope the span was emitted under, shared with
    /// the batch.
    pub scope: Arc<InstrumentationScope>,
    /// The span's attributes.
    pub attributes: Attributes,
    /// The dropped counts the emitter reported for this span.
    pub emitter_dropped: EmitterDroppedCounts,
    /// The span's events, in the order the emitter sent them.
    pub events: Vec<SpanEvent>,
    /// The span's links, in the order the emitter sent them.
    pub links: Vec<SpanLink>,
    /// The two-field status, preserved.
    pub status: SpanStatus,
}

impl Span {
    /// The span's natural identity: `(trace_id, span_id)` when both ids
    /// are valid, `None` when either is the all-zero encoding — an invalid
    /// id gives nothing in the wire data to be identical to.
    #[must_use]
    pub fn natural_identity(&self) -> Option<(TraceId, SpanId)> {
        if self.context.trace_id.is_valid() && self.context.span_id.is_valid() {
            Some((self.context.trace_id, self.context.span_id))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{SpanId as SpanIdT, TraceFlags, TraceId, TraceState};

    fn span_context() -> TraceContext {
        TraceContext {
            trace_id: TraceId::from_bytes([1; 16]),
            span_id: SpanIdT::from_bytes([2; 8]),
            flags: TraceFlags::new(1),
            tracestate: TraceState::default(),
        }
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

    fn span(kind: SpanKind) -> Span {
        Span {
            context: span_context(),
            parent_span_id: None,
            name: "op".to_owned(),
            kind,
            start_time_unix_nano: 100,
            end_time_unix_nano: Some(200),
            resource: empty_resource(),
            scope: empty_scope(),
            attributes: Attributes::default(),
            emitter_dropped: EmitterDroppedCounts::default(),
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: SpanStatusCode::Unset,
                message: String::new(),
            },
        }
    }

    #[test]
    fn all_six_kinds_are_distinct() {
        let kinds = [
            SpanKind::Unspecified,
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ];
        for (index, first) in kinds.iter().enumerate() {
            for second in kinds.iter().skip(index + 1) {
                assert_ne!(first, second, "kinds are never collapsed");
            }
        }
        assert_eq!(kinds.len(), 6);
    }

    #[test]
    fn an_unset_end_is_distinct_from_a_zero_length_end() {
        let started = span(SpanKind::Client);
        let zero_length = Span {
            end_time_unix_nano: Some(started.start_time_unix_nano),
            ..started.clone()
        };
        assert_eq!(
            zero_length.start_time_unix_nano,
            zero_length.end_time_unix_nano.expect("ended"),
            "end == start is a valid zero-length span"
        );
        let unfinished = Span {
            end_time_unix_nano: None,
            ..zero_length.clone()
        };
        assert_ne!(unfinished, zero_length, "unfinished is a distinct fact");
        assert!(unfinished.end_time_unix_nano.is_none());
    }

    #[test]
    fn an_absent_parent_is_distinct_from_the_zero_parent() {
        let root = span(SpanKind::Server);
        assert!(root.parent_span_id.is_none(), "absent parent");
        let zero_parent = Span {
            parent_span_id: Some(SpanIdT::from_bytes([0; 8])),
            ..root.clone()
        };
        assert_ne!(root, zero_parent);
        assert_eq!(
            zero_parent.parent_span_id.map(SpanIdT::as_bytes),
            Some([0; 8]),
            "the zero parent is preserved as sent"
        );
    }

    #[test]
    fn resource_and_scope_are_first_class_on_the_record() {
        let recorded = span(SpanKind::Server);
        assert!(
            Arc::ptr_eq(&recorded.resource, &recorded.resource),
            "the resource rides in an Arc shared with the batch"
        );
        let cloned = recorded.clone();
        assert!(
            Arc::ptr_eq(&recorded.resource, &cloned.resource)
                && Arc::ptr_eq(&recorded.scope, &cloned.scope),
            "cloning a span shares the resource and scope allocations"
        );
    }

    #[test]
    fn status_is_two_fields_preserved_even_when_unset() {
        let status = SpanStatus {
            code: SpanStatusCode::Unset,
            message: "nothing went wrong".to_owned(),
        };
        assert_eq!(status.code, SpanStatusCode::Unset);
        assert_eq!(status.message, "nothing went wrong");
        assert_ne!(
            status,
            SpanStatus {
                code: SpanStatusCode::Error,
                message: "nothing went wrong".to_owned(),
            },
            "the code is never derived from anything else"
        );
    }

    #[test]
    fn emitter_reported_dropped_counts_are_preserved_as_data() {
        let dropped = EmitterDroppedCounts {
            attributes: 3,
            events: 0,
            links: 1,
        };
        let recorded = Span {
            emitter_dropped: dropped,
            ..span(SpanKind::Internal)
        };
        assert_eq!(recorded.emitter_dropped, dropped);
    }

    #[test]
    fn event_order_is_the_emitter_order() {
        let event = |name: &str, at: u64| SpanEvent {
            time_unix_nano: Some(at),
            name: name.to_owned(),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
        };
        let events = vec![event("first", 1), event("second", 2), event("third", 3)];
        let recorded = Span {
            events: events.clone(),
            ..span(SpanKind::Producer)
        };
        let names: Vec<&str> = recorded.events.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["first", "second", "third"]);
        assert_eq!(recorded.events, events);
    }

    #[test]
    fn natural_identity_requires_both_ids_valid() {
        let valid = span(SpanKind::Consumer);
        assert_eq!(
            valid.natural_identity(),
            Some((TraceId::from_bytes([1; 16]), SpanIdT::from_bytes([2; 8])))
        );
        let zero_span_id = Span {
            context: TraceContext {
                span_id: SpanIdT::from_bytes([0; 8]),
                ..span_context()
            },
            ..valid.clone()
        };
        assert_eq!(zero_span_id.natural_identity(), None);
        let zero_trace_id = Span {
            context: TraceContext {
                trace_id: TraceId::from_bytes([0; 16]),
                ..span_context()
            },
            ..valid
        };
        assert_eq!(zero_trace_id.natural_identity(), None);
    }
}
