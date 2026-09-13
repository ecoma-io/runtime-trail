//! The residency order: how the resident set is sequenced, scanned and
//! evicted.
//!
//! `docs/architecture/storage-model.md` owns the eviction law: "oldest" is
//! by **admission time** — the model's added metadata — never by emitter
//! event time, which out-of-order emitters would make unpredictable. The
//! same order is the scan order, so what a cursor walks and what retention
//! removes are one deterministic sequence: admission time first, entity id
//! as the tie-break.

use runtime_trail_telemetry_model::{AdmissionTime, EntityId};

use std::cmp::Ordering;

/// A record's position in the residency order: its admission time, with its
/// entity id as the tie-break.
///
/// The total order this key defines is the contract every driver must scan
/// and evict by:
///
/// 1. by [`AdmissionTime`] — earlier admissions are older;
/// 2. on equal admission times, by entity id — a span's natural identity
///    orders before an assigned serial; among spans, by trace-id bytes then
///    span-id bytes; among assigned ids, by session serial.
///
/// Ties therefore never fall back to insertion order, hashing, or anything
/// else a mode might happen to do: two drivers given the same records admit
/// the same order, evict the same "oldest" record, and hand a cursor the
/// same continuation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionKey {
    admitted_at: AdmissionTime,
    entity: EntityId,
}

impl AdmissionKey {
    /// Positions one record: when the runtime admitted it, and by which id
    /// it is known.
    #[must_use]
    pub const fn new(admitted_at: AdmissionTime, entity: EntityId) -> Self {
        Self {
            admitted_at,
            entity,
        }
    }

    /// The record's admission time.
    #[must_use]
    pub const fn admitted_at(&self) -> AdmissionTime {
        self.admitted_at
    }

    /// The record's entity id.
    #[must_use]
    pub const fn entity(&self) -> EntityId {
        self.entity
    }
}

/// The entity-id tie-break: a total order over entity ids, documented on
/// [`AdmissionKey`].
fn entity_order(left: EntityId, right: EntityId) -> Ordering {
    match (left, right) {
        (
            EntityId::Span {
                trace_id: left_trace,
                span_id: left_span,
            },
            EntityId::Span {
                trace_id: right_trace,
                span_id: right_span,
            },
        ) => left_trace
            .as_bytes()
            .cmp(&right_trace.as_bytes())
            .then_with(|| left_span.as_bytes().cmp(&right_span.as_bytes())),
        (EntityId::Span { .. }, EntityId::Assigned(_)) => Ordering::Less,
        (EntityId::Assigned(_), EntityId::Span { .. }) => Ordering::Greater,
        (EntityId::Assigned(left), EntityId::Assigned(right)) => left.serial().cmp(&right.serial()),
    }
}

impl Ord for AdmissionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.admitted_at
            .cmp(&other.admitted_at)
            .then_with(|| entity_order(self.entity, other.entity))
    }
}

impl PartialOrd for AdmissionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// One resident record as a scan yields it: the record together with the
/// residency-order position it was yielded at.
///
/// The key is not extra work the driver did for the scan — it is the same
/// [`AdmissionKey`] the walk orders and retention evicts by, carried across
/// the seam instead of being dropped there
/// ([ADR 0009](../../docs/decisions/0009-ordered-scans-yield-residency-keys.md)).
/// A consumer that needs a record's admission time or entity id reads them
/// from the key; it never re-derives identity from record content — for
/// log records and metric points the admission-assigned id exists nowhere
/// else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanItem<T> {
    /// The record's residency-order position: its admission key.
    pub key: AdmissionKey,
    /// The record, shared as stored.
    pub record: T,
}

/// One page of an ordered scan over the resident set.
///
/// A scan is a location primitive, not a query: it yields keyed records in
/// the residency order ([`AdmissionKey`]) from a position, never filtering
/// by content. `cursor` is the key of the *last item in the page*, set only
/// when a record follows it — pass it as the next call's `after` (which
/// resumes strictly after that key) and no record is ever skipped or
/// repeated. Whenever `cursor` is `Some` it is exactly the last item's
/// `key`: two channels naming one position. `cursor` is `None` at the end
/// of the resident set, so a full walk is: scan from `None`, then from each
/// page's `cursor`, until a page comes back with `cursor: None`. Within one
/// call the page is a stable snapshot; across calls the resident set may
/// have changed, and a cursor simply continues from its key in whatever the
/// set now holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPage<T> {
    /// The page's keyed records, in residency order. Empty only when the
    /// scan started past the last resident record (or the set is empty).
    pub items: Vec<ScanItem<T>>,
    /// The key to resume from; `None` at the end of the resident set.
    pub cursor: Option<AdmissionKey>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_trail_telemetry_model::{AssignedId, SpanId, TraceId};
    use std::num::NonZeroU64;

    fn assigned(serial: u64) -> EntityId {
        EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(serial).expect("serials are nonzero"),
        ))
    }

    fn span_entity(trace: [u8; 16], span: [u8; 8]) -> EntityId {
        EntityId::Span {
            trace_id: TraceId::from_bytes(trace),
            span_id: SpanId::from_bytes(span),
        }
    }

    fn key(nano: u64, entity: EntityId) -> AdmissionKey {
        AdmissionKey::new(AdmissionTime::from_unix_nano(nano), entity)
    }

    #[test]
    fn admission_time_orders_before_any_entity_tie_break() {
        let early = key(1, assigned(u64::MAX));
        let late = key(2, assigned(1));
        assert!(
            early < late,
            "an earlier admission is older, whatever the ids"
        );
    }

    #[test]
    fn equal_admission_times_break_ties_by_entity_id() {
        // Spans order before assigned ids; spans by trace then span bytes;
        // assigned ids by serial.
        assert!(key(5, span_entity([1; 16], [2; 8])) < key(5, assigned(1)));
        assert!(key(5, span_entity([1; 16], [2; 8])) < key(5, span_entity([1; 16], [3; 8])));
        assert!(key(5, span_entity([1; 16], [9; 8])) < key(5, span_entity([2; 16], [1; 8])));
        assert!(key(5, assigned(7)) < key(5, assigned(8)));
        // Equal keys are equal, and the order is a total one: antisymmetric
        // and transitive across every pair shape.
        assert_eq!(key(5, assigned(7)), key(5, assigned(7)));
    }

    #[test]
    fn sorted_keys_are_the_residency_order_the_scans_walk() {
        // The order the store evicts by ("oldest first") is exactly the
        // order a scan yields: one sequence, two contracts.
        let mut keys = vec![
            key(30, assigned(2)),
            key(10, span_entity([9; 16], [1; 8])),
            key(10, assigned(1)),
            key(10, span_entity([1; 16], [1; 8])),
            key(20, assigned(1)),
        ];
        keys.sort();
        let expected = vec![
            key(10, span_entity([1; 16], [1; 8])),
            key(10, span_entity([9; 16], [1; 8])),
            key(10, assigned(1)),
            key(20, assigned(1)),
            key(30, assigned(2)),
        ];
        assert_eq!(keys, expected);
    }

    #[test]
    fn a_key_names_the_position_a_cursor_resumes_from() {
        let key = key(12, assigned(3));
        assert_eq!(key.admitted_at(), AdmissionTime::from_unix_nano(12));
        assert_eq!(key.entity(), assigned(3));
    }
}
