//! Admission-assigned identity: entity ids, admission metadata and the
//! admission outcome.
//!
//! `docs/architecture/telemetry-model.md`, "Record identity and duplicate
//! delivery": every record gets an entity id at admission — an opaque,
//! typed identifier unique within the runtime's session, never persisted.
//! A span with a valid trace id and span id keeps its natural identity as
//! its entity id; every other record receives an admission-assigned id.
//! Entity ids are added metadata: they never replace or rewrite emitter
//! data.

pub use crate::budgets::BudgetRejection;
use crate::context::{SpanId, TraceId};
use crate::metrics::StreamShapeError;
use std::num::NonZeroU64;

/// An admission-assigned, opaque entity id.
///
/// Unique within the runtime's session (one process lifetime). Ids are not
/// persisted: a reopened file-backed session reassigns them, and no handle
/// built on them survives a restart. Cursors, relation endpoints and
/// investigation references name entity ids — nothing else does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EntityId {
    /// The natural wire identity of a span whose trace id and span id are
    /// both valid: the same `(trace_id, span_id)` the emitter sent. This
    /// is not an invented id — it is the identity `OTel` itself defines.
    Span {
        /// The span's valid trace id.
        trace_id: TraceId,
        /// The span's valid span id.
        span_id: SpanId,
    },
    /// An id admission assigned, because nothing in the record's wire data
    /// distinguished it from any other record.
    Assigned(AssignedId),
}

impl EntityId {
    /// The assigned serial, when this id is admission-assigned.
    #[must_use]
    pub const fn assigned_serial(self) -> Option<NonZeroU64> {
        match self {
            Self::Span { .. } => None,
            Self::Assigned(assigned) => Some(assigned.serial),
        }
    }
}

/// One admission-assigned serial. Opaque: the value carries no meaning
/// beyond session-scoped uniqueness.
///
/// The derived [`Ord`] is serial order — admission order within the
/// session — the total order the ledger's assigned-id index needs for its
/// ordered map. Opacity is about *meaning*, not orderability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssignedId {
    pub(crate) serial: NonZeroU64,
}

impl AssignedId {
    /// Builds an assigned id from a session serial. Serials start at 1 and
    /// never repeat within a session.
    #[must_use]
    pub const fn from_serial(serial: NonZeroU64) -> Self {
        Self { serial }
    }

    /// The session serial.
    #[must_use]
    pub const fn serial(self) -> NonZeroU64 {
        self.serial
    }
}

/// The runtime's own admission time, kept as separate metadata.
///
/// It never overwrites or substitutes for an emitter timestamp. The value
/// is nanoseconds on the runtime's clock, supplied by the caller — the
/// model owns no clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AdmissionTime(u64);

impl AdmissionTime {
    /// Wraps a nanosecond reading from the runtime's clock.
    #[must_use]
    pub const fn from_unix_nano(nano: u64) -> Self {
        Self(nano)
    }

    /// The nanosecond reading.
    #[must_use]
    pub const fn as_unix_nano(self) -> u64 {
        self.0
    }
}

/// A record as admitted: its entity id, its admission time, and the record
/// itself — untouched. Entity ids and admission time are added metadata.
#[derive(Clone, Debug)]
pub struct Admitted<R> {
    /// The entity id the record is known by for this session.
    pub entity: EntityId,
    /// When the runtime admitted it — separate from every emitter
    /// timestamp.
    pub admitted_at: AdmissionTime,
    /// The record, exactly as admitted: complete and immutable.
    pub record: R,
}

/// What admission did with one delivery.
///
/// There is no truncation outcome anywhere. A delivery has exactly five
/// fates: admitted complete, collapsed onto its already-admitted self,
/// recorded as a conflict, refused by a named budget, or refused as
/// unrepresentable — a shape the contract cannot carry (ADR 0006). Budget
/// rejections carry non-retryable semantics — retrying an over-cap payload
/// cannot shrink it — and so does a shape rejection: retrying the same
/// bytes reproduces the same unrepresentable shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionOutcome {
    /// A new record entered the runtime; the id names it for this session.
    Admitted {
        /// The record's entity id.
        entity: EntityId,
    },
    /// A re-delivery of a record already admitted under a natural
    /// identity (a span or metric point): it collapsed onto the record
    /// already admitted. The id names the surviving record.
    Collapsed {
        /// The surviving record's entity id.
        entity: EntityId,
    },
    /// A delivery conflicted with an already-admitted record under the
    /// same natural identity: the first admitted record stands, and the
    /// conflict is recorded as an admission anomaly. The id names the
    /// record that stands.
    Conflict {
        /// The standing record's entity id.
        entity: EntityId,
    },
    /// Refused at admission by a named budget. Non-retryable: the payload
    /// is the problem, and retrying cannot shrink it.
    Rejected {
        /// The budget, its limit and the observed spend.
        rejection: BudgetRejection,
    },
    /// Refused because the record's shape is one the contract cannot
    /// represent — for a metric point, an identity whose kind disagrees
    /// with the point's shape. Non-retryable: the payload is the problem.
    Invalid {
        /// The shape law the record broke.
        error: StreamShapeError,
    },
}

impl AdmissionOutcome {
    /// The entity id the outcome names, when one exists.
    #[must_use]
    pub const fn entity(&self) -> Option<EntityId> {
        match self {
            Self::Admitted { entity } | Self::Collapsed { entity } | Self::Conflict { entity } => {
                Some(*entity)
            }
            Self::Rejected { .. } | Self::Invalid { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budgets::{BudgetLimits, BudgetName};
    use crate::metrics::PointShape;

    #[test]
    fn assigned_ids_are_opaque_nonzero_session_serials() {
        let first = AssignedId::from_serial(NonZeroU64::new(1).expect("1 is nonzero"));
        let second = AssignedId::from_serial(NonZeroU64::new(2).expect("2 is nonzero"));
        assert_ne!(first, second);
        assert_eq!(first.serial().get(), 1);
    }

    #[test]
    fn span_entity_ids_carry_the_natural_wire_identity() {
        let id = EntityId::Span {
            trace_id: TraceId::from_bytes([1; 16]),
            span_id: SpanId::from_bytes([2; 8]),
        };
        assert_eq!(id.assigned_serial(), None);
        assert_ne!(
            id,
            EntityId::Span {
                trace_id: TraceId::from_bytes([1; 16]),
                span_id: SpanId::from_bytes([3; 8]),
            },
            "different spans are different entities"
        );
        let assigned = EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(7).expect("7 is nonzero"),
        ));
        assert_eq!(assigned.assigned_serial().map(NonZeroU64::get), Some(7));
        assert_ne!(id, assigned);
    }

    #[test]
    fn admission_time_is_metadata_distinct_from_emitter_clocks() {
        let time = AdmissionTime::from_unix_nano(5_000);
        assert_eq!(time.as_unix_nano(), 5_000);
        assert_ne!(
            time,
            AdmissionTime::from_unix_nano(5_001),
            "two admissions stay distinguishable"
        );
    }

    #[test]
    fn refusals_name_no_entity_and_name_a_reason() {
        let limits = BudgetLimits::default();
        let rejected = AdmissionOutcome::Rejected {
            rejection: BudgetRejection {
                budget: BudgetName::EventsPerSpan,
                limit: limits.span_events_per_span,
                observed: limits.span_events_per_span + 1,
            },
        };
        let invalid = AdmissionOutcome::Invalid {
            error: StreamShapeError::KindShapeMismatch {
                index: 0,
                expected: PointShape::Number,
                found: PointShape::Histogram,
            },
        };
        assert_eq!(rejected.entity(), None);
        assert_eq!(invalid.entity(), None);
        assert_ne!(rejected, invalid);
    }

    #[test]
    fn entity_bearing_outcomes_name_exactly_one_entity() {
        let entity = EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(3).expect("3 is nonzero"),
        ));
        assert_eq!(AdmissionOutcome::Admitted { entity }.entity(), Some(entity));
        assert_eq!(
            AdmissionOutcome::Collapsed { entity }.entity(),
            Some(entity)
        );
        assert_eq!(AdmissionOutcome::Conflict { entity }.entity(), Some(entity));
    }
}
