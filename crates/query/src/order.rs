//! Total deterministic ordering over results.
//!
//! Every result is totally ordered, deterministically across identical
//! queries; every tie in the order keys is broken by the record's entity
//! id ([query-model.md](../../docs/architecture/query-model.md),
//! "Ordering, cursors and pagination", invariant 2). Without a total order
//! there is no stable pagination, and an investigator paging through
//! results would see records appear twice or vanish between pages.

use runtime_trail_telemetry_model::EntityId;
use std::cmp::Ordering;

/// The engine's canonical total order over entity ids — the tie-break
/// behind every order key (invariant 2).
///
/// The model deliberately gives [`EntityId`] no `Ord`: identity is
/// session-scoped and the model does not promise a cross-variant order, so
/// the query engine defines its own and owns it here. The canonical rank:
///
/// 1. **Variant first** — a span's natural wire identity orders before an
///    admission-assigned id. The natural identity is the one `OTel` itself
///    defined; an assigned serial exists only where the wire could not
///    distinguish the record.
/// 2. **Spans byte-wise** — by `trace_id` bytes, then `span_id` bytes, in
///    wire order.
/// 3. **Assigned ids by serial** — admission order within the session.
///
/// The order is total (every pair of ids compares) and antisymmetric
/// (`cmp(a, b)` is `cmp(b, a)` reversed). The property pagination rests on
/// is sharper: two *distinct* ids never compare [`Ordering::Equal`] — ids
/// compare equal here exactly when they are equal as `EntityId`s, because
/// within each variant the comparison is over the variant's whole
/// content.
///
/// Deterministic **within a session**: entity ids are session-scoped and
/// never persisted (telemetry-model.md, "Record identity and duplicate
/// delivery" — a reopened session reassigns them), so ordering across
/// restarts is not promised by anyone, and this function promises nothing
/// beyond the session either.
///
/// This is the **engine's** canonical tie-break, chosen for pagination
/// stability. It is not the storage side's residency order (admission time
/// first — storage-model.md): the two agree in tie-break *shape* by
/// design, but that agreement is coherence, not coupling — query code uses
/// the storage contract's types and never its internals (ADR 0003).
#[must_use]
pub fn entity_id_order(left: EntityId, right: EntityId) -> Ordering {
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

/// Sorts `items` in place into the engine's total order: by `key` first,
/// every tie broken by [`entity_id_order`] over `id`
/// ([query-model.md](../../docs/architecture/query-model.md), invariant 2).
///
/// The sort is stable, but stability is never what a caller relies on:
/// entity ids are unique per record in a result set (a result is a set of
/// entities), so `(key, id)` is a total order over the items and the
/// output sequence is independent of the input permutation — which is what
/// makes invariant 8's "identical query + identical resident set ⇒
/// identical first page" hold. `K` is only ever borrowed for comparison;
/// it needs no `Clone`.
pub fn sort_deterministically<T, K>(
    items: &mut [T],
    key: impl Fn(&T) -> K,
    id: impl Fn(&T) -> EntityId,
) where
    K: Ord,
{
    items.sort_by(|a, b| {
        key(a)
            .cmp(&key(b))
            .then_with(|| entity_id_order(id(a), id(b)))
    });
}

#[cfg(test)]
mod tests {
    use runtime_trail_telemetry_model::{AssignedId, SpanId, TraceId};
    use std::num::NonZeroU64;

    use super::*;

    /// An admission-assigned id for the tests: opaque, session-unique by
    /// serial, exactly the shape the model assigns at admission.
    fn assigned(serial: u64) -> EntityId {
        EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(serial).expect("test serials are nonzero"),
        ))
    }

    /// A span entity id from raw wire bytes, as `OTel` sends them.
    fn span_entity(trace: [u8; 16], span: [u8; 8]) -> EntityId {
        EntityId::Span {
            trace_id: TraceId::from_bytes(trace),
            span_id: SpanId::from_bytes(span),
        }
    }

    /// Sorts records of (order key, entity id) into the engine's total
    /// order and returns just the id sequence.
    fn sorted_ids(mut records: Vec<(u64, EntityId)>) -> Vec<EntityId> {
        sort_deterministically(&mut records, |record| record.0, |record| record.1);
        records.into_iter().map(|(_, entity)| entity).collect()
    }

    /// Kills the no-op that drops the entity-id tie-break from the sort:
    /// with the tie-break gone, the stable sort returns equal-key items in
    /// input order and the fixed permutations below would disagree with
    /// each other.
    #[test]
    fn identical_multisets_sort_to_one_sequence_under_any_permutation() {
        // Every record shares one order key, so the entity-id tie-break
        // alone decides the whole output.
        let items: Vec<(u64, EntityId)> = vec![
            (1, span_entity([1; 16], [1; 8])),
            (1, span_entity([2; 16], [9; 8])),
            (1, assigned(4)),
            (1, assigned(2)),
            (1, span_entity([1; 16], [7; 8])),
        ];
        let expected = sorted_ids(items.clone());
        let reversed = sorted_ids(items.iter().rev().copied().collect());
        let rotated = sorted_ids(
            items[1..]
                .iter()
                .chain(items[..1].iter())
                .copied()
                .collect(),
        );
        assert_eq!(
            expected, reversed,
            "reversing the input must not move a tie-broken record"
        );
        assert_eq!(
            expected, rotated,
            "rotating the input must not move a tie-broken record"
        );
    }

    /// Kills the no-op comparator that returns `Equal` for every pair
    /// (ties would fall back to input order) and any mutation that breaks
    /// the id order's totality or antisymmetry.
    #[test]
    fn ties_break_by_entity_id_and_the_id_order_is_total() {
        let ids = vec![
            span_entity([1; 16], [1; 8]),
            span_entity([2; 16], [9; 8]),
            assigned(1),
            assigned(8),
        ];
        for left in &ids {
            for right in &ids {
                let ordering = entity_id_order(*left, *right);
                if left == right {
                    assert_eq!(ordering, Ordering::Equal, "an id equals itself");
                } else {
                    assert_ne!(
                        ordering,
                        Ordering::Equal,
                        "two distinct ids must never compare equal"
                    );
                }
                assert_eq!(
                    ordering,
                    entity_id_order(*right, *left).reverse(),
                    "the id order must be antisymmetric"
                );
            }
        }
        // With every key equal, the sort's output is exactly the ids under
        // the canonical comparator — fed from a reversed input.
        let mut records: Vec<(u64, EntityId)> = ids.iter().map(|id| (3, *id)).rev().collect();
        sort_deterministically(&mut records, |record| record.0, |record| record.1);
        let mut expected_ids = ids.clone();
        expected_ids.sort_by(|left, right| entity_id_order(*left, *right));
        let output: Vec<EntityId> = records.into_iter().map(|(_, entity)| entity).collect();
        assert_eq!(output, expected_ids);
    }

    /// Kills the mutations that flip the variant rank (assigned before
    /// span), reverse the byte-wise comparisons, swap trace-before-span
    /// precedence, or order serials descending — each would reorder the
    /// chain below.
    #[test]
    fn the_canonical_rank_is_span_before_assigned_bytes_within_variants() {
        // Spans before assigned ids; among spans, trace bytes before span
        // bytes; among assigned ids, smaller serial first.
        let chain = [
            span_entity([1; 16], [1; 8]),
            span_entity([1; 16], [2; 8]),
            span_entity([2; 16], [1; 8]),
            assigned(1),
            assigned(2),
        ];
        for (lower, higher) in chain.iter().zip(chain.iter().skip(1)) {
            assert_eq!(
                entity_id_order(*lower, *higher),
                Ordering::Less,
                "the canonical chain must ascend: {lower:?} < {higher:?}"
            );
        }
        // Trace bytes outrank span bytes: a bigger span id cannot pull a
        // smaller trace id ahead.
        assert_eq!(
            entity_id_order(span_entity([1; 16], [9; 8]), span_entity([2; 16], [1; 8])),
            Ordering::Less
        );
    }

    /// Kills the mutation that swaps the precedence — ordering by entity
    /// id first and applying order keys only as a tie-break.
    #[test]
    fn order_keys_rank_before_the_entity_id_tie_break() {
        let mut records = vec![(2, assigned(1)), (1, assigned(9))];
        sort_deterministically(&mut records, |record| record.0, |record| record.1);
        assert_eq!(records[0].0, 1, "the smaller key comes first");
        assert_eq!(
            records[0].1,
            assigned(9),
            "the key winner leads even when its entity id is the larger one"
        );
    }
}
