//! The correlated part of the [`Investigation`](crate::Investigation) envelope.
//!
//! This module is the seam between the correlation engine and the envelope.
//! The taxonomy — relation types, tiers, strategy versions, evidence facts —
//! is a single definition owned by `runtime-trail-correlation` and
//! re-exported here, so the envelope can never diverge from the engine. The
//! engine's generic [`Relation`] is bound to the envelope's evidence
//! [`SignalRef`]s through the `Relation` type alias, and the two
//! conversions below move the engine's refs and windows into the
//! investigation vocabulary.
//!
//! A relation is never a judgement about importance: it names a type, an
//! ordered pair of resident `SignalRef`s, the facts that evidence the
//! relation, and the strategy that produced it. Tiers are a property of the
//! *type*, never a score attached to an instance — [`Relation::tier`]
//! derives the tier from the type.

use runtime_trail_correlation::relations::SignalRef as EngineSignalRef;
use runtime_trail_correlation::relations::Window as EngineWindow;

use crate::evidence::{SignalKind as EvidenceSignalKind, SignalRef};
use crate::execution::TimeWindow;

/// The relation taxonomy: types, tiers, tier derivation, strategy versions
/// and evidence facts. One definition, owned by the engine; the envelope
/// re-exports it verbatim.
pub use runtime_trail_correlation::relations::{
    EvidenceFact, RelationType, StrategyVersion, Tier, tier_of,
};

/// A relation between two resident signal refs, produced by a versioned
/// strategy under a taxonomy type. The endpoint type is the envelope's
/// [`SignalRef`]; engine-emitted relations convert into these at the seam.
pub type Relation = runtime_trail_correlation::relations::Relation<SignalRef>;

/// The envelope's correlated part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Correlated {
    /// The relations, in production order.
    pub relations: Vec<Relation>,
}

impl Correlated {
    /// Builds the correlated part from its relations.
    #[must_use]
    pub const fn new(relations: Vec<Relation>) -> Self {
        Self { relations }
    }
}

impl From<EngineSignalRef> for SignalRef {
    /// Converts an engine-cited signal ref into the envelope's evidence ref.
    /// Exhaustive over the engine's kinds: the evidence taxonomy is the
    /// engine's taxonomy, and a new kind must land in both or neither.
    fn from(reference: EngineSignalRef) -> Self {
        let kind = match reference.kind {
            runtime_trail_correlation::relations::SignalKind::Spans => EvidenceSignalKind::Spans,
            runtime_trail_correlation::relations::SignalKind::LogRecords => {
                EvidenceSignalKind::LogRecords
            }
            runtime_trail_correlation::relations::SignalKind::MetricPoints => {
                EvidenceSignalKind::MetricPoints
            }
        };
        Self::new(kind, reference.entity)
    }
}

impl From<EngineWindow> for TimeWindow {
    /// Converts an engine window into the envelope's window. Both are
    /// half-open intervals over the model clock, so the conversion is a
    /// type change, never a semantic one.
    fn from(window: EngineWindow) -> Self {
        Self::new(window.from, window.to)
    }
}

impl From<TimeWindow> for EngineWindow {
    /// Converts the envelope's window into the engine's window, for the
    /// boundary the flow hands the engine.
    fn from(window: TimeWindow) -> Self {
        Self::new(window.from, window.to)
    }
}
