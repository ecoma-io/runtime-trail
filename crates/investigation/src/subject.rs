//! The subject part of the [`Investigation`](crate::Investigation) envelope.
//!
//! Envelope invariant 1: the **requested** subject — what the caller sent —
//! and the **effective** subject — what the runtime actually investigated —
//! are stated separately, never merged. If they differ, the difference is
//! inspectable: the effective root names the span the flow investigated,
//! and the resolution notes say why and how it got there.
//!
//! The contract is
//! [`docs/architecture/investigation-model.md`](../../docs/architecture/investigation-model.md),
//! "subject".

use runtime_trail_telemetry_model::{EntityId, SpanId, TraceId};

/// What the caller asked the runtime to investigate, exactly as sent.
///
/// The flow never rewrites this field: the requested subject is the
/// caller's own words (envelope invariant 1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestedSubject {
    /// The root span the caller named, by its entity id.
    pub root_span: EntityId,
}

/// The span the runtime actually investigated: the effective root of the
/// trace, the trace identity it opened, and the normalisations applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveSubject {
    /// The effective root span.
    pub root: EffectiveRoot,
    /// The resolutions and normalisations the runtime applied while turning
    /// the requested subject into the effective one. Every difference
    /// between requested and effective is named here (invariant 1).
    pub notes: Vec<ResolutionNote>,
}

/// The effective root span: who it is and under what identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveRoot {
    /// The root's entity id — the identity the whole envelope names it by.
    pub entity: EntityId,
    /// The root's span name, verbatim.
    pub name: String,
    /// The root's trace id: the trace the flow investigated.
    pub trace_id: TraceId,
    /// The root's span id.
    pub span_id: SpanId,
}

/// A resolution the runtime applied between the requested and the effective
/// subject. Every note names a fact; none is a judgement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolutionNote {
    /// The requested span is the trace's parentless root: no resolution was
    /// needed beyond confirming it.
    RequestedSpanIsRoot,
    /// The requested span is not the trace's root; the runtime investigated
    /// the trace rooted at the named span.
    TraceRootedAt {
        /// The trace's parentless root span.
        entity: EntityId,
        /// The root's name, verbatim.
        name: String,
        /// The root's span id.
        span_id: SpanId,
    },
    /// No parentless span is resident for the trace (its root was evicted
    /// or sampled out); the runtime investigated from the requested span as
    /// the effective root. The waterfall is the requested span's subtree.
    NoParentlessSpanResident,
    /// The requested root carries no valid trace identity (an all-zero
    /// trace id as sent); the runtime investigated the span alone.
    RootHasNoValidTraceIdentity,
}

/// The envelope's subject part: requested and effective, separately.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subject {
    /// What the caller sent.
    pub requested: RequestedSubject,
    /// What the runtime investigated and how it resolved the request.
    pub effective: EffectiveSubject,
}

impl RequestedSubject {
    /// The requested root span.
    #[must_use]
    pub const fn new(root_span: EntityId) -> Self {
        Self { root_span }
    }
}

impl Subject {
    /// Builds the subject part from the sent request and the resolved root.
    #[must_use]
    pub fn new(requested: RequestedSubject, effective: EffectiveSubject) -> Self {
        Self {
            requested,
            effective,
        }
    }
}

impl EffectiveSubject {
    /// Builds the effective subject from the resolved root and its notes.
    #[must_use]
    pub const fn new(root: EffectiveRoot, notes: Vec<ResolutionNote>) -> Self {
        Self { root, notes }
    }
}

impl EffectiveRoot {
    /// The effective root span's facts.
    #[must_use]
    pub const fn new(entity: EntityId, name: String, trace_id: TraceId, span_id: SpanId) -> Self {
        Self {
            entity,
            name,
            trace_id,
            span_id,
        }
    }
}
