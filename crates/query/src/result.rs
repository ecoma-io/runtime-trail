//! The answer vocabulary the engine's results speak.
//!
//! These are the shared shapes every query result is built from — the
//! per-part fates of [refuse or degrade](../../docs/architecture/query-model.md),
//! the coverage entries that make truncation truthful, and the deterministic
//! page a caller pages through. They are the seam the engine's machinery is
//! written against: budget machinery produces the refusal and truncation
//! values, ordering and cursor machinery produce the cursor bytes and
//! coverage entries, and every result assembles them here.
//!
//! Vocabulary is the [model's](../../docs/architecture/telemetry-model.md)
//! and the [query contract's](../../docs/architecture/query-model.md) — no
//! storage or transport concept appears in a public shape.

use std::time::Duration;

use runtime_trail_storage::AdmissionKey;
use runtime_trail_telemetry_model::EntityId;

/// The five budget dimensions every query admits with.
///
/// A query without a budget is invalid — the engine takes the budget as a
/// required value, never a default ([query-model.md](../../docs/architecture/query-model.md),
/// invariant 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dimension {
    /// Monotonic duration captured at admission; the engine works against
    /// the remaining time.
    Deadline,
    /// Cap on entities returned.
    Results,
    /// Cap on the canonical encoding of the answer's evidence.
    Bytes,
    /// Cap on scan work — one entity examined per unit.
    Scan,
    /// Cap on memory an aggregation may hold. Expiry refuses; it never
    /// spills to disk.
    AggregationMemory,
}

/// The magnitude a dimension's limit and observed spend are stated in.
///
/// The deadline speaks durations; the byte ceilings speak bytes; the count
/// ceilings speak units. Naming the magnitude keeps a refusal's numbers
/// comparable to the budget the caller set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Magnitude {
    /// A duration — the [`Dimension::Deadline`] dimension.
    Duration(Duration),
    /// A count of units — results or examined entities.
    Units(u64),
    /// A count of bytes — evidence or aggregation memory.
    Bytes(u64),
}

/// A refused part of an answer, naming the dimension, the limit and the
/// observed spend ([query-model.md](../../docs/architecture/query-model.md),
/// invariant 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetRefusal {
    /// The dimension whose expiry caused the refusal.
    pub dimension: Dimension,
    /// The ceiling the caller set.
    pub limit: Magnitude,
    /// What the work in flight had spent when the dimension expired.
    pub observed: Magnitude,
}

/// Where a traversal-shaped part stopped, so the truncation is a named
/// position, never a silent skip.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TruncationPoint {
    /// The opaque cursor that continues the traversal after the last
    /// returned entity. Its bytes are the cursor machinery's encoding;
    /// callers treat them as opaque.
    Cursor(Vec<u8>),
    /// The last entity examined before the budget expired, when the work
    /// has no continuation to offer.
    LastExamined(EntityId),
}

/// A degraded part's named truncation: which dimension expired and where
/// the answer stopped ([query-model.md](../../docs/architecture/query-model.md),
/// invariant 4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Truncation {
    /// The dimension whose expiry truncated the part.
    pub dimension: Dimension,
    /// Where the part stopped.
    pub position: TruncationPoint,
}

/// The fate of one part of an answer.
///
/// Traversal-shaped work degrades (a subset of a set answer is still a true
/// set answer); aggregation-shaped work refuses (a partial aggregate is a
/// false number, and a false number is worse than no number). A flow fails
/// outright only when its subject itself cannot be resolved — never merely
/// because one aggregating part expired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartOutcome {
    /// The part was answered in full within the budget.
    Complete,
    /// The part answered a true subset, with the truncation named.
    Degraded { truncation: Truncation },
    /// The part refused, naming the dimension, limit and observed spend.
    Refused(BudgetRefusal),
}

/// What was and was not covered — the entries that make truncation and
/// residency truthful ([investigation-model.md](../../docs/architecture/investigation-model.md):
/// coverage is what makes truncation truthful).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coverage {
    /// The named gaps and boundaries of this answer's coverage.
    pub entries: Vec<CoverageEntry>,
}

/// One named fact about what an answer did not cover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoverageEntry {
    /// Records the cursor's snapshot knew of are evicted: a hole between
    /// the two nearest resident neighbors, named by both, never a silent
    /// skip.
    EvictionGap { after: EntityId, before: EntityId },
    /// The continuation's snapshot boundary: the result set was fixed at
    /// the first page's admission — the records admitted after this
    /// residency position are outside every later page of the same
    /// continuation. A declared boundary, not a silent skip; a caller
    /// wanting newer data issues a new query.
    SnapshotBoundary { admission: AdmissionKey },
}

/// The run-fact block: what the work cost and what it covered.
///
/// Per-part outcomes and coverage sit **outside** `max_bytes` — they are
/// the answer's truth-telling, bounded by a small fixed allowance so
/// honesty never crowds out data and data never crowds out honesty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Execution {
    /// One outcome per part of the answer, in the answer's own order.
    pub parts: Vec<PartOutcome>,
    /// The coverage statement over the whole answer.
    pub coverage: Coverage,
}

/// One deterministic page of a query's answer.
///
/// Items arrive in the engine's total order (ties broken by entity id);
/// `next_cursor` carries the opaque continuation when more of the same
/// snapshot remains; `execution` is the run-fact block. Identical query +
/// identical resident set + identical budget evaluated by the same driver
/// yield an identical page ([query-model.md](../../docs/architecture/query-model.md),
/// invariant 8) — that is what makes pagination stable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    /// The answer's records, in the total order.
    pub items: Vec<T>,
    /// The opaque continuation into the same snapshot, when the answer has
    /// more to give.
    pub next_cursor: Option<Vec<u8>>,
    /// The run-fact block for this page.
    pub execution: Execution,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use runtime_trail_telemetry_model::AssignedId;

    use super::*;

    /// An assigned entity id for the tests: opaque, session-unique by
    /// serial, exactly the shape the model assigns at admission.
    fn examined() -> EntityId {
        use std::num::NonZeroU64;
        EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(7).expect("7 is not zero"),
        ))
    }

    /// A mixed-parts execution reports each part's fate as it happened:
    /// completed parts stay complete, a degraded part names its truncation,
    /// and a refused part carries dimension, limit and observed spend —
    /// the envelope's honesty is per part, never collapsed into one flag.
    #[test]
    fn execution_reports_each_part_and_names_refusals() {
        let refusal = BudgetRefusal {
            dimension: Dimension::AggregationMemory,
            limit: Magnitude::Bytes(1_048_576),
            observed: Magnitude::Bytes(1_049_600),
        };
        let execution = Execution {
            parts: vec![
                PartOutcome::Complete,
                PartOutcome::Degraded {
                    truncation: Truncation {
                        dimension: Dimension::Scan,
                        position: TruncationPoint::LastExamined(examined()),
                    },
                },
                PartOutcome::Refused(refusal),
            ],
            coverage: Coverage {
                entries: Vec::new(),
            },
        };
        assert_eq!(execution.parts.len(), 3);
        assert!(matches!(execution.parts[0], PartOutcome::Complete));
        assert_eq!(
            execution.parts[1],
            PartOutcome::Degraded {
                truncation: Truncation {
                    dimension: Dimension::Scan,
                    position: TruncationPoint::LastExamined(examined()),
                }
            }
        );
        assert_eq!(execution.parts[2], PartOutcome::Refused(refusal));
    }

    /// An evicted record is a hole with a name: the coverage entry holds
    /// both resident neighbors of the gap, so an investigator sees exactly
    /// what the window lost instead of reading a silent skip as truth.
    #[test]
    fn coverage_names_an_eviction_gap_between_resident_neighbors() {
        let (after, before) = (
            examined(),
            EntityId::Assigned(AssignedId::from_serial(
                NonZeroU64::new(9).expect("9 is not zero"),
            )),
        );
        let entry = CoverageEntry::EvictionGap { after, before };
        let coverage = Coverage {
            entries: vec![entry],
        };
        assert_eq!(
            coverage.entries[0],
            CoverageEntry::EvictionGap { after, before }
        );
    }
}
