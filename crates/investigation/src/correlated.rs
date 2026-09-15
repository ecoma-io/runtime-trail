//! The correlated part of the [`Investigation`](crate::Investigation) envelope.
//!
//! M3 (issue #12) commits the trace investigation flow, whose correlated
//! part is **resident-only and typed-but-empty**: no correlation strategy
//! is implemented yet, so every envelope today carries an empty [`Correlated`]
//! list and an empty strategy-version statement in
//! [`Limits`](crate::limits::Limits). The contract is `docs/architecture/
//! correlation-model.md`; the part is the machine-checkable statement of
//! that contract for the strategies that will fill it.
//!
//! A relation is never a judgement about importance: it names a type, an
//! ordered pair of resident `SignalRef`s, the facts that evidence the
//! relation, and the strategy that produced it. Tiers are a property of the
//! *type*, never a score attached to an instance — [`Relation::tier`]
//! derives the tier from the type.

use runtime_trail_telemetry_model::Value;

use crate::evidence::SignalRef;
use crate::execution::TimeWindow;

/// A correlation strategy: a named, versioned implementation of the
/// correlation contract. The version statement is machine-checkable; an
/// empty version list means "no strategy implemented yet", which the
/// envelope reports rather than inventing strategies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrategyVersion {
    /// The strategy's name.
    pub name: String,
    /// The strategy's version.
    pub version: String,
}

/// The strength tier of a relation type: how strongly the type grounds
/// identity claims. Tiers are a property of the type — [`Relation::tier`]
/// derives them; a relation never carries a per-instance score.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tier {
    /// Identity-grounded: the relation holds only within one trace identity.
    Identity,
    /// Structural: the relation is grounded in the record graph (parent —
    /// child, streams, resource trees).
    Structural,
    /// Attachment: the relation is grounded in exemplars or inline payloads.
    Attachment,
    /// Contextual: the relation is grounded in shared context (service,
    /// resource, time).
    Context,
    /// Temporal: the relation is grounded in time adjacency.
    Temporal,
}

/// The relation taxonomy. `Inferred` is reserved: no strategy may construct
/// an `Inferred` relation (a relation whose evidence cannot name its
/// grounding facts), and the invariant checks reject it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelationType {
    /// Identity: two signal refs of one trace.
    SpanIdentity,
    /// Identity: a span and its trace.
    TraceIdentity,
    /// Structural: a parent span and its child span.
    ParentChild,
    /// Structural: records sharing one resource.
    ResourceContext,
    /// Temporal: records adjacent on the time surface (co-activity).
    TemporalCoActivity,
    /// Attachment: a point's exemplar and the span it sampled.
    ExemplarAttachment,
    /// Trace identity: an inferred relation.
    Inferred,
}

/// The tier of a relation type. A property of the type, never carried per
/// instance ([`Relation::tier`] derives it).
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

/// One grounding fact of a relation: a named field and the value that
/// evidences the relation. Facts are the relation's proof; a relation with
/// no facts is `Inferred` and rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceFact {
    /// The field name the fact reads (model vocabulary, e.g. `trace_id`).
    pub field: String,
    /// The field's value, verbatim.
    pub value: Value,
}

/// One correlated pair: a typed relation between two resident signal refs,
/// its grounding facts, the producing strategy, and the time window it
/// holds over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation {
    /// The relation's type.
    pub relation_type: RelationType,
    /// The "from" endpoint: a resident signal ref.
    pub from: SignalRef,
    /// The "to" endpoint: a resident signal ref.
    pub to: SignalRef,
    /// The grounding facts that evidence the relation. Empty only for
    /// `Inferred` (which the invariant checks reject).
    pub facts: Vec<EvidenceFact>,
    /// The strategy that produced the relation.
    pub strategy: StrategyVersion,
    /// The window the relation holds over, when the type grounds one.
    pub window: Option<TimeWindow>,
}

/// The envelope's correlated part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Correlated {
    /// The relations, in production order. Empty in M3: the correlation
    /// strategies are scaffolding, reported as such.
    pub relations: Vec<Relation>,
}

impl Correlated {
    /// Builds the correlated part from its relations.
    #[must_use]
    pub const fn new(relations: Vec<Relation>) -> Self {
        Self { relations }
    }
}

impl Relation {
    /// The relation's type and endpoints.
    #[must_use]
    pub fn new(
        relation_type: RelationType,
        from: SignalRef,
        to: SignalRef,
        facts: Vec<EvidenceFact>,
        strategy: StrategyVersion,
        window: Option<TimeWindow>,
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

    /// The tier of this relation's type. A property of the type, derived —
    /// never a score attached to the instance.
    #[must_use]
    pub fn tier(&self) -> Tier {
        tier_of(&self.relation_type)
    }
}

impl StrategyVersion {
    /// A named, versioned strategy.
    #[must_use]
    pub const fn new(name: String, version: String) -> Self {
        Self { name, version }
    }
}

impl EvidenceFact {
    /// A grounding fact: a named field and its value.
    #[must_use]
    pub const fn new(field: String, value: Value) -> Self {
        Self { field, value }
    }
}
