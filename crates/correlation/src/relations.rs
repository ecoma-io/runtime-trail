//! The correlation taxonomy: the relation types, their tiers, the versioned
//! strategies that produce them, and the evidence facts they stand on.
//!
//! The taxonomy is a single definition. The investigation layer re-exports
//! it (its `correlated` module is the seam), so a relation type, a tier or
//! a strategy version can never diverge between the engine and the
//! envelope.

use runtime_trail_telemetry_model::{EntityId, Value};

/// A versioned strategy statement, bound into every relation and into the
/// limits of every envelope the flow composes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrategyVersion {
    /// The strategy's name.
    pub name: String,
    /// The strategy's version.
    pub version: String,
}

impl StrategyVersion {
    /// A named, versioned strategy.
    #[must_use]
    pub const fn new(name: String, version: String) -> Self {
        Self { name, version }
    }
}

/// The tier a relation type stands on: what kind of ground its relations
/// hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// Identity: sampled traces and their members.
    Identity,
    /// Structural: parent/child and resource-sharing relations.
    Structural,
    /// Attachment: exemplars citing the trace context they evidence.
    Attachment,
    /// Context: temporal co-activity within a caller-supplied window.
    Context,
    /// Temporal: relations that hold over time.
    Temporal,
}

/// The relation types the taxonomy pins. Every type is declared here;
/// committed strategies produce some of them and the rest are pinned
/// contract — the types that exist are exactly these, so a relation type is
/// never duplicated or invented between crates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RelationType {
    /// Identity: a log record and the exact span its trace context names.
    SpanIdentity,
    /// Identity: a log record and the resident spans of its trace.
    TraceIdentity,
    /// Structural: a parent span and its child span.
    ParentChild,
    /// Structural: records sharing one resource.
    ResourceContext,
    /// Context: temporal co-activity within a caller-supplied window.
    TemporalCoActivity,
    /// Attachment: an exemplar citing the trace context it evidences.
    ExemplarAttachment,
    /// Identity: a relation inferred from evidence, never produced by a
    /// strategy.
    Inferred,
}

/// The tier a relation type stands on.
#[must_use]
pub const fn tier_of(relation_type: &RelationType) -> Tier {
    match relation_type {
        RelationType::SpanIdentity | RelationType::TraceIdentity | RelationType::Inferred => {
            Tier::Identity
        }
        RelationType::ParentChild | RelationType::ResourceContext => Tier::Structural,
        RelationType::ExemplarAttachment => Tier::Attachment,
        RelationType::TemporalCoActivity => Tier::Temporal,
    }
}

/// One fact a relation stands on: a cited model field and its verbatim
/// value. The cited times of two endpoints make overlap vs. proximity
/// derivable, so the facts are the evidence, never a summary of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceFact {
    /// The cited model field.
    pub field: String,
    /// The field's value, exactly as recorded.
    pub value: Value,
}

impl EvidenceFact {
    /// A field/value pair citing the model.
    #[must_use]
    pub fn new(field: String, value: Value) -> Self {
        Self { field, value }
    }
}

/// A half-open interval `[from, to)` over the model clock (unix
/// nanoseconds). The interval a temporal strategy grounds its pairs on, and
/// the interval a relation states it holds over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Window {
    /// The interval's start, inclusive.
    pub from: u64,
    /// The interval's end, exclusive.
    pub to: u64,
}

impl Window {
    /// A half-open interval over the model clock.
    #[must_use]
    pub const fn new(from: u64, to: u64) -> Self {
        Self { from, to }
    }

    /// The interval's length in clock units.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.to.saturating_sub(self.from)
    }

    /// Whether the interval is empty (nothing can fall inside it).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.from >= self.to
    }
}

/// The signal a correlation endpoint names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SignalKind {
    /// Span records.
    Spans,
    /// Log records.
    LogRecords,
    /// Metric points.
    MetricPoints,
}

/// A resident signal named by entity, with the kind its interpretation
/// depends on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SignalRef {
    /// The endpoint's kind.
    pub kind: SignalKind,
    /// The endpoint's resident entity.
    pub entity: EntityId,
}

impl SignalRef {
    /// A signal ref over a kind and a resident entity.
    #[must_use]
    pub const fn new(kind: SignalKind, entity: EntityId) -> Self {
        Self { kind, entity }
    }
}

/// A relation between two resident signals, produced by a versioned
/// strategy under a taxonomy type. The endpoint ref type `R` is the seam:
/// the engine cites its own [`SignalRef`], and the investigation envelope
/// binds `R` to its evidence refs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation<R> {
    /// The relation's type.
    pub relation_type: RelationType,
    /// The "from" endpoint: a resident signal ref.
    pub from: R,
    /// The "to" endpoint: a resident signal ref.
    pub to: R,
    /// The facts the relation stands on, each citing a model field verbatim.
    pub facts: Vec<EvidenceFact>,
    /// The strategy that produced the relation.
    pub strategy: StrategyVersion,
    /// The window the relation holds over, when the type grounds one.
    pub window: Option<Window>,
}

impl<R> Relation<R> {
    /// Builds a relation.
    #[must_use]
    pub fn new(
        relation_type: RelationType,
        from: R,
        to: R,
        facts: Vec<EvidenceFact>,
        strategy: StrategyVersion,
        window: Option<Window>,
    ) -> Self {
        Self {
            relation_type,
            from,
            to,
            facts,
            strategy,
            window,
        }
    }

    /// The tier the relation's type stands on.
    #[must_use]
    pub fn tier(&self) -> Tier {
        tier_of(&self.relation_type)
    }
}
