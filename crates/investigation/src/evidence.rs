//! The evidence part of the [`Investigation`](crate::Investigation) envelope.
//!
//! Evidence is record views: the resident records the investigation rests
//! on, each named by the entity id the runtime could give it. Unlike the
//! engine's identity-free views, the envelope carries each view's entity id
//! (invariant 4: "record views + `SignalRef` no dangling"). A view whose
//! record left residency between selection and naming is excluded and
//! named as a `ResidencyHole` in flow coverage — it is never emitted
//! unkeyed.

use std::sync::Arc;

use runtime_trail_telemetry_model::{EntityId, LogRecord, MetricPoint, Span, StreamIdentity};

/// The signal kinds the envelope can carry evidence about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignalKind {
    /// Trace spans.
    Spans,
    /// Log records.
    LogRecords,
    /// Metric points.
    MetricPoints,
}

/// A reference to one resident signal, by kind and entity id. References
/// are how the correlated part names evidence; a reference that does not
/// resolve to an evidence view is a contract violation (invariant 3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignalRef {
    /// The referenced record's kind.
    pub kind: SignalKind,
    /// The referenced record's entity id.
    pub entity: EntityId,
}

/// A span view: a resident span and the entity id it was named by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanEvidence {
    /// The span's entity id, when the span carries a natural identity.
    pub entity: Option<EntityId>,
    /// The resident span.
    pub span: Arc<Span>,
}

/// A log record view: a resident log and the entity id the runtime named
/// it by. Log records are admission-assigned identities, so the id comes
/// from residency, never from the record's own fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEvidence {
    /// The log record's entity id.
    pub entity: Option<EntityId>,
    /// The resident log record.
    pub log: Arc<LogRecord>,
}

/// A metric point view: a resident point, the stream it belongs to, and
/// the entity id the runtime named the point by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointEvidence {
    /// The point's entity id.
    pub entity: Option<EntityId>,
    /// The resident point record.
    pub point: Arc<MetricPoint>,
    /// The point's stream identity, verbatim.
    pub stream: Arc<StreamIdentity>,
}

/// The envelope's evidence part: the resident records the investigation
/// rests on, each carrying the identity the runtime could name it by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Evidence {
    /// The trace's spans, in waterfall order.
    pub spans: Vec<SpanEvidence>,
    /// The trace's related logs, in engine (residency) order.
    pub logs: Vec<LogEvidence>,
    /// The surrounding metric points, in engine (residency) order.
    pub points: Vec<PointEvidence>,
}

impl Evidence {
    /// Builds the evidence part from its three record families.
    #[must_use]
    pub const fn new(
        spans: Vec<SpanEvidence>,
        logs: Vec<LogEvidence>,
        points: Vec<PointEvidence>,
    ) -> Self {
        Self {
            spans,
            logs,
            points,
        }
    }

    /// The entity id behind a signal ref, when the evidence contains it.
    #[must_use]
    pub fn entity_of(&self, kind: &SignalKind, entity: &EntityId) -> bool {
        match kind {
            SignalKind::Spans => self
                .spans
                .iter()
                .any(|view| view.entity.as_ref() == Some(entity)),
            SignalKind::LogRecords => self
                .logs
                .iter()
                .any(|view| view.entity.as_ref() == Some(entity)),
            SignalKind::MetricPoints => self
                .points
                .iter()
                .any(|view| view.entity.as_ref() == Some(entity)),
        }
    }
}

impl SpanEvidence {
    /// A span view.
    #[must_use]
    pub const fn new(entity: Option<EntityId>, span: Arc<Span>) -> Self {
        Self { entity, span }
    }
}

impl LogEvidence {
    /// A log record view.
    #[must_use]
    pub const fn new(entity: Option<EntityId>, log: Arc<LogRecord>) -> Self {
        Self { entity, log }
    }
}

impl PointEvidence {
    /// A point view.
    #[must_use]
    pub const fn new(
        entity: Option<EntityId>,
        point: Arc<MetricPoint>,
        stream: Arc<StreamIdentity>,
    ) -> Self {
        Self {
            entity,
            point,
            stream,
        }
    }
}

impl SignalRef {
    /// A reference to a resident signal.
    #[must_use]
    pub const fn new(kind: SignalKind, entity: EntityId) -> Self {
        Self { kind, entity }
    }
}
