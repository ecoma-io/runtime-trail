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

/// A degraded part's named truncation: which dimension expired, where the
/// answer stopped, and how much that position leaves out
/// ([query-model.md](../../docs/architecture/query-model.md), invariant 4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Truncation {
    /// The dimension whose expiry truncated the part.
    pub dimension: Dimension,
    /// Where the part stopped.
    pub position: TruncationPoint,
    /// How many entities `position` leaves out — for `max_bytes`, how
    /// many records' evidence did not fit — when the engine can know it:
    /// a byte-ceiling omission is named by count and position, never
    /// enumerated (budget table, row `max_bytes`). Zero means
    /// *uncounted*, not nothing: a deadline cut mid-traversal and a
    /// scan-ceiling cut into an unexamined tail have not counted what
    /// they did not visit, and coverage then names the rest.
    pub omitted: u64,
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
    /// The record a continuation's cursor points into was evicted: a hole
    /// named by the cursor's last entity — resident or the evicted record
    /// the cursor points into — and the first resident successor the
    /// resumed walk yields, or the snapshot boundary's entity when the
    /// walk yields nothing before it. A forward scan cannot see a record
    /// that is missing: mid-window evictions with no cursor expectation
    /// are invisible to it, and the snapshot boundary entry bounds what
    /// such a window could hide.
    EvictionGap { after: EntityId, before: EntityId },
    /// The continuation's snapshot boundary: the result set was fixed at
    /// the first page's admission — the records admitted after this
    /// residency position are outside every later page of the same
    /// continuation. A declared boundary, not a silent skip; a caller
    /// wanting newer data issues a new query.
    SnapshotBoundary { admission: AdmissionKey },
    /// The walk that counts a byte-ceiling omission stopped here — its
    /// scan or deadline expired mid-count — so every snapshot record
    /// after it is uncounted: the truncation's `omitted` reports the
    /// count the walk had established before the stop (the confirmed
    /// byte omissions), and this entry names the rest — the records the
    /// walk never reached — instead of dressing the uncounted fragment
    /// up as a complete count.
    UncountedTail {
        /// The last record the counting walk examined; everything the
        /// snapshot holds after it is uncounted.
        after: EntityId,
        /// The dimension whose expiry cut the counting walk.
        dimension: Dimension,
    },
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
