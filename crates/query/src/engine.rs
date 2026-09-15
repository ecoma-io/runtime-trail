//! The engine entry: budgeted record queries over the storage contract.
//!
//! [`records`] is the flow's one door: a signal kind, a budget admitted
//! here, and an optional continuation cursor. The walk reads the store's
//! ordered scans — every record arrives with the residency key it was
//! yielded at
//! ([ADR 0009](../../docs/decisions/0009-ordered-scans-yield-residency-keys.md))
//! — and answers in the result vocabulary: a deterministic page whose
//! truncations are named positions and whose coverage names what the
//! answer did not cover
//! ([query-model.md](../../docs/architecture/query-model.md)).
//!
//! The engine sees the *contract*, never a concrete driver (ADR 0003):
//! the store arrives as `&dyn TelemetryStore`. The scan order is the
//! residency order — admission time first, entity id as the tie-break —
//! which for the records flow is exactly the engine's total order
//! ([`crate::order`]), so items are pushed in yield order and there is no
//! re-sort to drift. A cursor's position is the order key value — the
//! anchor record's admission-time nanoseconds — which with the last
//! entity id reconstructs the anchor's [`AdmissionKey`] exactly: a resume
//! never re-walks, and it never needs the anchor record to still be
//! resident (an evicted anchor is named in coverage, never silently
//! skipped).
//!
//! The snapshot bound deserves its own paragraph. A first page walks its
//! kind's whole resident set (or until a budget stops it) and mints its
//! frontier from the *pulled batch tail* — the store's scan cursor of the
//! last batch it pulled, or that batch's last item's key at the kind's
//! end. The bound is inclusive: records at the frontier sit inside the
//! snapshot whether or not the minting page examined them — a budget may
//! stop the page mid-batch, and the batch tail it pulled but never
//! examined settles against `max_scan` at the stop. Every later page of
//! the continuation stops before keys strictly past the boundary and
//! names it in coverage. A continuation passes the boundary through
//! unchanged — the chain's snapshot is fixed by its first page, and a
//! pulled tail past it never widens the view.
//!
//! Filters are the walk's predicate layer
//! ([`crate::filters`]): the query carries its kind's filter struct, and
//! each examined record is matched against it after the scan charge and
//! before every other gate. A filtered-out record was still examined —
//! the scan charge is its cost, and the deadline ran on it — but it is
//! never returned, never byte-charged, and never counted into an
//! omission. Filters never reorder anything: the page's order stays the
//! residency order, and no filter reaches into a driver.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use runtime_trail_storage::{AdmissionKey, TelemetryStore};
use runtime_trail_telemetry_model::{
    Accounted, AdmissionTime, EntityId, LogRecord, MetricPoint, Span, StreamIdentity,
};

use crate::budget::{BudgetSession, QueryBudget};
use crate::cursor::{self, CursorError, CursorPayload};
use crate::filters::{LogFilters, MetricFilters, RecordsFilters, SpanFilters};
use crate::result::{
    BudgetRefusal, Coverage, CoverageEntry, Dimension, Execution, Page, PartOutcome, Truncation,
    TruncationPoint,
};
use crate::spend::TraversalAllowance;

/// The query description's tag byte: the records flow. A future flow's
/// description starts from a different tag byte, so no parameter
/// extension can collide with an older encoding.
const QUERY_TAG: u8 = b'Q';

/// The query description's version byte, bumped on any parameter change.
/// Version 2 grew the filter fields; a version 1 cursor therefore fails
/// [`CursorPayload::verify`] under this engine — the honest rejection, not
/// a silent reinterpretation of bytes minted under the narrower encoding.
const QUERY_VERSION: u8 = 2;

/// Records pulled per driver scan. Batches bound the per-call page the
/// driver materializes without bounding the walk: a budget stops the walk
/// mid-batch, and the batch tail — pulled whole — is the frontier a
/// minted cursor carries.
const SCAN_BATCH: usize = 64;

/// Consecutive empty driver pages with a successor cursor that end the
/// walk. A contract-violating driver that yields an empty page while
/// claiming a successor cursor would spin the engine forever — the scan
/// budget charges only examined items, so no dimension ceiling stops
/// it. One empty continuation is a legal boundary blip a conforming
/// driver may produce at a batch edge; after this many in a row the
/// engine stops paging, mints no further cursor, and names the stall
/// in coverage ([`CoverageEntry::DriverStall`],
/// [`PartOutcome::Stalled`]).
const EMPTY_PAGE_STALL_LIMIT: usize = 3;

/// The signal kind a records query asks for: the model's vocabulary, never
/// the storage layer's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalKind {
    /// Spans.
    Spans,
    /// Log records.
    LogRecords,
    /// Metric points.
    MetricPoints,
}

impl SignalKind {
    /// The kind's byte in the canonical query description.
    const fn tag(self) -> u8 {
        match self {
            Self::Spans => 0,
            Self::LogRecords => 1,
            Self::MetricPoints => 2,
        }
    }
}

/// A records query: the signal kind plus that kind's filter struct
/// ([`crate::filters`]). The pairing is kind-tagged — a query is built
/// with the constructor for its kind, so a log query cannot arrive
/// carrying span filters, and a severity filter cannot be expressed for
/// spans or metrics at all. The all-`None` filter set a [`Self::new`]
/// query carries is byte-identical to a kind-only query: same canonical
/// bytes, same fingerprint, same pages.
///
/// Every parameter joins the canonical query description the fingerprint
/// hashes — a cursor minted under one parameter set never continues under
/// another (invariant 3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordsQuery {
    kind: SignalKind,
    filters: RecordsFilters,
}

impl RecordsQuery {
    /// A query over one signal kind, filtering nothing: every resident
    /// record of the kind is in the answer set.
    #[must_use]
    pub const fn new(kind: SignalKind) -> Self {
        let filters = match kind {
            SignalKind::Spans => RecordsFilters::Spans(SpanFilters::all_none()),
            SignalKind::LogRecords => RecordsFilters::LogRecords(LogFilters::all_none()),
            SignalKind::MetricPoints => RecordsFilters::MetricPoints(MetricFilters::all_none()),
        };
        Self { kind, filters }
    }

    /// A spans query carrying span filters.
    #[must_use]
    pub const fn spans(filters: SpanFilters) -> Self {
        Self {
            kind: SignalKind::Spans,
            filters: RecordsFilters::Spans(filters),
        }
    }

    /// A log-records query carrying log filters.
    #[must_use]
    pub const fn logs(filters: LogFilters) -> Self {
        Self {
            kind: SignalKind::LogRecords,
            filters: RecordsFilters::LogRecords(filters),
        }
    }

    /// A metric-points query carrying metric filters.
    #[must_use]
    pub const fn metric_points(filters: MetricFilters) -> Self {
        Self {
            kind: SignalKind::MetricPoints,
            filters: RecordsFilters::MetricPoints(filters),
        }
    }

    /// The signal kind the query asks for.
    #[must_use]
    pub const fn kind(&self) -> SignalKind {
        self.kind
    }

    /// The query's filter set, kind-tagged to [`Self::kind`].
    #[must_use]
    pub const fn filters(&self) -> &RecordsFilters {
        &self.filters
    }

    /// The canonical query description: a tag byte and a version byte
    /// prefix, then the kind tag, then every filter field in a fixed
    /// order with presence bytes. The fingerprint is taken over these
    /// bytes, so an older encoding can never be mistaken for a newer
    /// parameter set, and two distinct filter sets never share one.
    fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(QUERY_TAG);
        bytes.push(QUERY_VERSION);
        bytes.push(self.kind.tag());
        self.filters.push_canonical(&mut bytes);
        bytes
    }
}

/// One record the engine returns: the record, shared as stored, with the
/// metric variant carrying the interned stream identity it was admitted
/// under. The view carries no identity of its own — a record's residency
/// position lives in its [`AdmissionKey`], which the engine reads from
/// the scan item and never re-derives from record content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordView {
    /// A span, shared as stored.
    Span(Arc<Span>),
    /// A log record, shared as stored.
    LogRecord(Arc<LogRecord>),
    /// A metric point with the interned stream identity it belongs to.
    /// The identity is residency bookkeeping — one shared allocation per
    /// stream (ADR 0008) — carried because the contract hands it back
    /// with every point, never because it is per-record evidence.
    MetricPoint {
        /// The point, exactly as admitted.
        point: Arc<MetricPoint>,
        /// The interned stream identity the point was admitted under.
        stream: Arc<StreamIdentity>,
    },
}

/// Why a records query failed outright.
///
/// In M2 the records flow fails only on its continuation cursor: a
/// malformed byte string, or a cursor presented under a different query,
/// is an error — never a best-effort continuation (invariant 3). Every
/// budget expiry inside the walk is an answer shape (refuse or degrade,
/// [`PartOutcome`]), not an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryError {
    /// The continuation cursor was rejected: its bytes are not a
    /// canonical cursor encoding, or its fingerprint does not match this
    /// query.
    Cursor(CursorError),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self::Cursor(error) = self;
        write!(f, "records query failed: {error}")
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let Self::Cursor(error) = self;
        Some(error)
    }
}

impl From<CursorError> for QueryError {
    fn from(error: CursorError) -> Self {
        Self::Cursor(error)
    }
}

/// One record view's evidence bytes: the model's accounted size for the
/// record ([`Accounted`]) — the one definition byte ceilings count. A
/// metric point's evidence is the point's accounted size only; the stream
/// identity it arrived with is residency bookkeeping shared by the whole
/// stream (ADR 0008), charged once at admission, never per record.
fn evidence_of(record: &RecordView) -> u64 {
    let bytes = match record {
        RecordView::Span(span) => span.accounted_size(),
        RecordView::LogRecord(record) => record.accounted_size(),
        RecordView::MetricPoint { point, .. } => point.accounted_size(),
    };
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// The state of the counting walk a byte-ceiling wall starts: how many
/// records so far would not fit within what the ceiling has left, and the
/// last record the walk counted.
struct Counting {
    omitted: u64,
    last: EntityId,
}

/// One record the driver yielded, normalized across the three kinds: its
/// residency key (never re-derived) and the record view.
struct Yield {
    key: AdmissionKey,
    record: RecordView,
}

/// One page's walk: the state a scan accumulates from batch to batch and
/// record to record. Built fresh per [`records`] call; `run` consumes it
/// into the page.
struct Walk<'a> {
    store: &'a dyn TelemetryStore,
    kind: SignalKind,
    fingerprint: u64,
    /// The filter set every examined record is matched against: the
    /// predicate layer over the record view (F4). Records that fail it are
    /// still scan-charged and deadline-checked; they are never returned,
    /// never byte-charged, and never counted into an omission.
    filters: &'a RecordsFilters,
    /// The admitted budget the walk spends against, held mutably so the
    /// caller (a test included) can read the ledger after the page.
    session: &'a mut BudgetSession,
    /// The continuation's inclusive upper bound: keys strictly past it
    /// are outside the snapshot. `None` on a first page.
    snapshot_bound: Option<AdmissionKey>,
    /// The presented continuation, when the page resumes one.
    presented: Option<CursorPayload>,
    items: Vec<RecordView>,
    coverage: Vec<CoverageEntry>,
    /// Where the next batch resumes: `None` from the kind's start, or
    /// strictly after the presented cursor's anchor mid-continuation.
    after: Option<AdmissionKey>,
    /// The pulled-batch tail key of the last batch pulled: the store's
    /// scan cursor of that batch, or its last item's key at the kind's
    /// end. A first page's minted cursors carry it as their snapshot.
    frontier: Option<AdmissionKey>,
    /// The key of the last record included in the page.
    last_included: Option<AdmissionKey>,
    /// The key of the last record examined — scan-charged — whether
    /// included or not.
    last_examined: Option<AdmissionKey>,
    /// The counting walk a byte-ceiling wall started, when one is open.
    counting: Option<Counting>,
    /// The continuation anchor whose record is no longer resident, until
    /// the walk yields its first resident successor.
    pending_gap: Option<EntityId>,
    /// Set the moment the walk knows its part outcome; `run` stops on it.
    stopped: Option<PartOutcome>,
}

impl Walk<'_> {
    /// Pulls the next batch and normalizes it into yields.
    fn pull(&mut self) -> (Vec<Yield>, Option<AdmissionKey>) {
        match self.kind {
            SignalKind::Spans => {
                let page = self.store.scan_spans(self.after, SCAN_BATCH);
                (
                    page.items
                        .into_iter()
                        .map(|item| Yield {
                            key: item.key,
                            record: RecordView::Span(item.record),
                        })
                        .collect(),
                    page.cursor,
                )
            }
            SignalKind::LogRecords => {
                let page = self.store.scan_log_records(self.after, SCAN_BATCH);
                (
                    page.items
                        .into_iter()
                        .map(|item| Yield {
                            key: item.key,
                            record: RecordView::LogRecord(item.record),
                        })
                        .collect(),
                    page.cursor,
                )
            }
            SignalKind::MetricPoints => {
                let page = self.store.scan_metric_points(self.after, SCAN_BATCH);
                (
                    page.items
                        .into_iter()
                        .map(|item| Yield {
                            key: item.key,
                            record: RecordView::MetricPoint {
                                point: item.record.point,
                                stream: item.record.stream,
                            },
                        })
                        .collect(),
                    page.cursor,
                )
            }
        }
    }

    /// The snapshot a minted cursor carries: a continuation passes its
    /// presented snapshot through unchanged — the chain's boundary is
    /// fixed by its first page — and a first page carries its own pulled
    /// frontier. The frontier is always present by the time a cursor is
    /// minted (a cursor exists only after a record was examined, and an
    /// examination follows a pulled batch); the fallback keys the
    /// snapshot at the anchor itself, which only ever narrows a
    /// continuation.
    fn cursor_bytes(&self, anchor: AdmissionKey) -> Vec<u8> {
        debug_assert!(
            self.snapshot_bound.is_some() || self.frontier.is_some(),
            "a cursor is minted only after a batch was pulled"
        );
        let snapshot = self.snapshot_bound.or(self.frontier).unwrap_or(anchor);
        CursorPayload::new(
            anchor.admitted_at().as_unix_nano(),
            anchor.entity(),
            self.fingerprint,
            snapshot,
        )
        .encode()
    }

    /// The presented cursor, echoed byte for byte — the anchor of last
    /// resort when the walk has nothing included or examined to name:
    /// both degraded shapes that reach for it continue exactly where the
    /// continuation was handed off, so one helper keeps the echoes
    /// identical.
    fn presented_echo(&self) -> Option<TruncationPoint> {
        self.presented
            .as_ref()
            .map(|payload| TruncationPoint::Cursor(payload.encode()))
    }

    /// The degrade a stop names when the walk stopped at or past an
    /// examined record: a cursor anchored at the last *included* record
    /// when the page returned something — examined-but-unreturned records
    /// stay ahead of the cursor, so the continuation is lossless — the
    /// presented cursor echoed when the page returned nothing but
    /// continues a caller's position, and the last examined record when
    /// there is no continuation to offer. Each step names a strictly
    /// weaker truth; a page that examined nothing and continues nothing
    /// is refused upstream, never shaped here.
    fn include_anchored_degrade(
        &self,
        dimension: Dimension,
        omitted: u64,
        last_examined: EntityId,
    ) -> PartOutcome {
        let position = if let Some(anchor) = self.last_included {
            TruncationPoint::Cursor(self.cursor_bytes(anchor))
        } else if let Some(echo) = self.presented_echo() {
            echo
        } else {
            TruncationPoint::LastExamined(last_examined)
        };
        PartOutcome::Degraded {
            truncation: Truncation {
                dimension,
                position,
                omitted,
            },
        }
    }

    /// The walk stops at a record whose scan charge was refused: the
    /// record was never examined. Inside a counting walk the cut leaves
    /// the omission uncounted and coverage names the uncounted rest;
    /// otherwise the degrade anchors at the last examined record. A
    /// continuation whose deadline was already spent before this page
    /// examined anything has no truthful degrade — nothing was examined,
    /// so no cursor of its own was arrived at and an echo of the presented
    /// cursor is a fabricated position, not a named truncation point — and
    /// refuses (invariant 6), exactly as a deadline-dead first page does.
    fn stop_before_examining(&mut self, refusal: BudgetRefusal) {
        if let Some(counting) = self.counting.take() {
            self.coverage.push(CoverageEntry::UncountedTail {
                after: counting.last,
                dimension: refusal.dimension,
            });
            // The omission stays byte-dimensional — the byte ceiling is what
            // these records would not fit — and it reports the count the
            // walk had established before the cut: the examined records that
            // were confirmed byte-omissions. The `UncountedTail` entry names
            // the rest the walk never reached, so the reported count is a
            // confirmed fragment, never a partial number dressed up as
            // complete.
            self.stopped = Some(self.include_anchored_degrade(
                Dimension::Bytes,
                counting.omitted,
                counting.last,
            ));
            return;
        }
        let position = if let Some(anchor) = self.last_examined {
            TruncationPoint::Cursor(self.cursor_bytes(anchor))
        } else {
            self.stopped = Some(PartOutcome::Refused(refusal));
            return;
        };
        self.stopped = Some(PartOutcome::Degraded {
            truncation: Truncation {
                dimension: refusal.dimension,
                position,
                omitted: 0,
            },
        });
    }

    /// The walk reached the snapshot bound or the kind's end: inside a
    /// counting walk the count is the truth; otherwise the part
    /// completed.
    fn finish_counting_or_complete(&mut self) {
        match self.counting.take() {
            Some(counting) => {
                self.stopped = Some(self.include_anchored_degrade(
                    Dimension::Bytes,
                    counting.omitted,
                    counting.last,
                ));
            }
            None => self.stopped = Some(PartOutcome::Complete),
        }
    }

    /// Settles the batch tail a stop abandons: records the driver pulled
    /// whole but the walk will never examine are still work done for this
    /// query, so they consume scan allowance — up to the remaining
    /// ceiling, never past it (a tail beyond the grant is the engine's own
    /// pull-ahead overhead, not chargeable units). The settle runs after
    /// the stop is decided and changes no outcome's shape.
    fn settle(&mut self, unexamined: u64) {
        self.session.settle_abandoned_scan(unexamined);
    }

    /// Processes one yielded record: the snapshot bound, the scan charge
    /// (after which the record is examined), the eviction gap's first
    /// resident successor, then either the counting walk's fit check or
    /// the results and bytes gates and the include itself. Sets `stopped`
    /// when the walk must stop. `unexamined_after` is the number of
    /// records the current batch still holds behind this one — at a stop
    /// they were pulled but will never be examined, and they settle
    /// against `max_scan`.
    fn examine(&mut self, yielded: Yield, unexamined_after: u64) {
        if let Some(bound) = self.snapshot_bound {
            if yielded.key > bound {
                // This record and everything behind it in the batch were
                // pulled past the snapshot: never examined, but pulled.
                self.settle(unexamined_after + 1);
                self.finish_counting_or_complete();
                return;
            }
        }
        // The scan charge: one unit per record the driver yields — a
        // record pulled to be skipped was examined (budget row
        // `max_scan`). The allowance carries the deadline check.
        if let TraversalAllowance::Partial { refusal, .. } =
            self.session.allow_scan(Instant::now(), 1)
        {
            // This record was never examined either — the charge was
            // refused — so it settles with the tail behind it.
            self.settle(unexamined_after + 1);
            self.stop_before_examining(refusal);
            return;
        }
        self.last_examined = Some(yielded.key);
        if let Some(anchor) = self.pending_gap.take() {
            self.coverage.push(CoverageEntry::EvictionGap {
                after: anchor,
                before: yielded.key.entity(),
            });
        }
        // The predicate layer (F4): the record was examined — the scan
        // charge stood and the deadline ran on it — but a non-matching
        // record is not part of the answer set. It returns here without
        // touching the results/bytes gates, the omission count, or the
        // include.
        if !self.filters.matches(&yielded.record) {
            return;
        }
        if let Some(counting) = &mut self.counting {
            // The include phase is over: the walk counts the truth — how
            // many records' evidence would not fit. Counted records are
            // examined, never returned and never byte-charged.
            if self.session.ledger().remaining_bytes() < evidence_of(&yielded.record) {
                counting.omitted += 1;
                counting.last = yielded.key.entity();
            }
            return;
        }
        // Results: charged only for records the page returns — read what
        // remains, and charge exactly what an include takes.
        if self.session.ledger().remaining_results() == 0 {
            // This record was examined, so only the tail behind it
            // settles.
            self.settle(unexamined_after);
            self.stopped =
                Some(self.include_anchored_degrade(Dimension::Results, 0, yielded.key.entity()));
            return;
        }
        if let TraversalAllowance::Partial { refusal, .. } =
            self.session.allow_results(Instant::now(), 1)
        {
            // The deadline died between the scan and the results charges:
            // the record was examined but is not returned, so the cursor
            // anchors before it and the continuation stays lossless.
            self.settle(unexamined_after);
            self.stopped =
                Some(self.include_anchored_degrade(refusal.dimension, 0, yielded.key.entity()));
            return;
        }
        let evidence = evidence_of(&yielded.record);
        if self.session.ledger().remaining_bytes() < evidence {
            // The byte ceiling expires here: stop including, then count
            // the remainder truthfully — this record is the first that
            // does not fit.
            self.counting = Some(Counting {
                omitted: 1,
                last: yielded.key.entity(),
            });
            return;
        }
        if let TraversalAllowance::Partial { refusal, .. } =
            self.session.allow_bytes(Instant::now(), evidence)
        {
            self.settle(unexamined_after);
            self.stopped =
                Some(self.include_anchored_degrade(refusal.dimension, 0, yielded.key.entity()));
            return;
        }
        self.items.push(yielded.record);
        self.last_included = Some(yielded.key);
    }

    /// Assembles the page: an eviction gap whose walk never yielded a
    /// resident successor names the snapshot boundary's entity — the
    /// boundary is where the walk stopped, and it names the rest.
    fn assemble(mut self) -> Page<RecordView> {
        if let Some(after) = self.pending_gap.take() {
            let before = self.snapshot_bound.map_or(after, |bound| bound.entity());
            self.coverage
                .push(CoverageEntry::EvictionGap { after, before });
        }
        let part = self.stopped.unwrap_or(PartOutcome::Complete);
        let next_cursor = match &part {
            PartOutcome::Degraded { truncation } => match &truncation.position {
                TruncationPoint::Cursor(bytes) => Some(bytes.clone()),
                TruncationPoint::LastExamined(_) => None,
            },
            PartOutcome::Complete | PartOutcome::Refused(_) | PartOutcome::Stalled => None,
        };
        Page {
            items: self.items,
            next_cursor,
            execution: Execution {
                parts: vec![part],
                coverage: Coverage {
                    entries: self.coverage,
                },
            },
        }
    }

    /// Runs the walk to its stop: batches until the kind's end, the
    /// snapshot bound, or a budget dimension expires. A stop mid-batch
    /// settles the batch tail it abandons; a complete end — the kind's
    /// end, where every pulled record was examined — settles nothing.
    ///
    /// The one kind of stop no budget dimension can produce is a stalled
    /// driver: a page that is empty yet claims a successor cursor
    /// charges nothing (the scan ceiling counts examined items only),
    /// so it would spin the walk forever. The engine bounds it here —
    /// [`EMPTY_PAGE_STALL_LIMIT`] consecutive empty continuations end
    /// the walk with what was collected, no further cursor, and the
    /// stall named in coverage.
    fn run(mut self) -> Page<RecordView> {
        let mut empty_pages = 0_usize;
        while self.stopped.is_none() {
            let (batch, batch_cursor) = self.pull();
            self.frontier = batch_cursor.or(batch.last().map(|yielded| yielded.key));
            let total = batch.len();
            if total == 0 {
                if let Some(cursor) = batch_cursor {
                    empty_pages += 1;
                    if empty_pages >= EMPTY_PAGE_STALL_LIMIT {
                        // The driver claims a successor it never yields:
                        // stop paging, mint no further cursor, and name
                        // the stall — the last record the walk examined,
                        // or the driver's own cursor anchor when nothing
                        // was (the `after` vocabulary of UncountedTail).
                        let after = self
                            .last_examined
                            .map_or_else(|| cursor.entity(), |key| key.entity());
                        self.coverage.push(CoverageEntry::DriverStall { after });
                        self.stopped = Some(PartOutcome::Stalled);
                        break;
                    }
                    self.after = Some(cursor);
                } else {
                    self.finish_counting_or_complete();
                }
                continue;
            }
            empty_pages = 0;
            for (index, yielded) in batch.into_iter().enumerate() {
                let unexamined_after = u64::try_from(total - index - 1).unwrap_or(u64::MAX);
                self.examine(yielded, unexamined_after);
                if self.stopped.is_some() {
                    break;
                }
            }
            if self.stopped.is_none() {
                match batch_cursor {
                    Some(next) => self.after = Some(next),
                    None => self.finish_counting_or_complete(),
                }
            }
        }
        self.assemble()
    }
}

/// Answers a records query within its budget: one page of the kind's
/// resident set, in the engine's total order, with every truncation named
/// and every gap covered.
///
/// The budget is admitted here, with the entry's own monotonic reading —
/// a budgetless query is invalid, and the entry is the engine's one door
/// (invariant 1). The result fails outright only on its continuation
/// cursor (invariant 3); every budget expiry is an answer shape.
///
/// # Errors
///
/// [`QueryError::Cursor`] when `continuation` is not a canonical cursor
/// encoding, or is a cursor minted under a different query (invariant 3).
pub fn records(
    store: &dyn TelemetryStore,
    query: &RecordsQuery,
    budget: QueryBudget,
    continuation: Option<&[u8]>,
) -> Result<Page<RecordView>, QueryError> {
    let fingerprint = cursor::fingerprint(&query.canonical_bytes());
    let presented = match continuation {
        None => None,
        Some(bytes) => {
            let payload = CursorPayload::decode(bytes)?;
            payload.verify(fingerprint)?;
            Some(payload)
        }
    };
    let mut session = budget.admit(Instant::now());

    // A zero results ceiling demands the empty answer: no examination, no
    // omission — the budget itself is the whole answer, and nothing was
    // left out by expiry. A continuation still names its boundary, and the
    // page is empty by budget, never eviction-blind: an anchor the store has
    // evicted is still a hole with a name, even though the walk will yield
    // nothing to resolve it — the gap names the snapshot boundary's entity,
    // the named rest, exactly as an unresolvable gap does in [`Walk::assemble`].
    if session.ledger().remaining_results() == 0 {
        let mut entries = Vec::new();
        if let Some(payload) = &presented {
            entries.push(CoverageEntry::SnapshotBoundary {
                admission: payload.snapshot(),
            });
            if !anchor_is_resident(store, query.kind(), payload.last_entity()) {
                entries.push(CoverageEntry::EvictionGap {
                    after: payload.last_entity(),
                    before: payload.snapshot().entity(),
                });
            }
        }
        return Ok(Page {
            items: Vec::new(),
            next_cursor: None,
            execution: Execution {
                parts: vec![PartOutcome::Complete],
                coverage: Coverage { entries },
            },
        });
    }

    Ok(walk_records(store, query, &mut session, presented))
}

/// Whether a continuation anchor's record is still resident: one direct
/// lookup per continuation, never a scan — checking residency costs no
/// examination. An evicted anchor is a named hole in coverage, never a
/// silent skip.
fn anchor_is_resident(store: &dyn TelemetryStore, kind: SignalKind, anchor: EntityId) -> bool {
    match kind {
        SignalKind::Spans => store.span(anchor).is_some(),
        SignalKind::LogRecords => store.log_record(anchor).is_some(),
        SignalKind::MetricPoints => store.metric_point(anchor).is_some(),
    }
}

/// The walk itself, over an already-admitted session: split from
/// [`records`] so the behavioral tests can drive the engine with a
/// session they hold, and read its ledger after the page — the honest
/// window on accounting no page shape exposes.
fn walk_records(
    store: &dyn TelemetryStore,
    query: &RecordsQuery,
    session: &mut BudgetSession,
    presented: Option<CursorPayload>,
) -> Page<RecordView> {
    let fingerprint = cursor::fingerprint(&query.canonical_bytes());
    let snapshot_bound = presented.map(|payload| payload.snapshot());
    let mut walk = Walk {
        store,
        kind: query.kind(),
        filters: query.filters(),
        fingerprint,
        session,
        snapshot_bound,
        presented,
        items: Vec::new(),
        coverage: Vec::new(),
        after: None,
        frontier: None,
        last_included: None,
        last_examined: None,
        counting: None,
        pending_gap: None,
        stopped: None,
    };
    if let Some(admission) = snapshot_bound {
        walk.coverage
            .push(CoverageEntry::SnapshotBoundary { admission });
    }
    // The one eviction a continuation can observe mechanically is its
    // anchor's: the cursor points into that record, and its absence is a
    // named hole, never a silent skip. A direct lookup, not a scan — no
    // examination is charged.
    if let Some(payload) = &presented {
        let anchor = payload.last_entity();
        if !anchor_is_resident(store, query.kind(), anchor) {
            walk.pending_gap = Some(anchor);
        }
    }
    // The resume anchor is the presented cursor's order key value plus
    // last entity: the anchor's admission key, reconstructed exactly.
    walk.after = presented.map(|payload| {
        AdmissionKey::new(
            AdmissionTime::from_unix_nano(payload.position()),
            payload.last_entity(),
        )
    });
    walk.run()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::num::NonZeroU64;
    use std::ops::Bound;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use runtime_trail_storage::{
        AdmissionKey, KeepOutcome, PointView, ScanItem, ScanPage, StoreStats, TelemetryStore,
    };
    use runtime_trail_telemetry_model::{
        Accounted, AdmissionTime, Admitted, AssignedId, Attributes, EmitterDroppedCounts, EntityId,
        InstrumentationScope, LogRecord, MetricNumber, MetricPoint, NumberPoint, Resource,
        SeverityNumber, Span, SpanId, SpanKind, SpanStatus, SpanStatusCode, StreamIdentity,
        StreamKind, TraceContext, TraceFlags, TraceId, TraceState, Value,
    };

    use super::{QueryError, RecordView, RecordsQuery, SignalKind, records, walk_records};
    use crate::budget::QueryBudget;
    use crate::cursor::{CursorError, CursorPayload};
    use crate::filters::{
        LogFilters, MetricFilters, ScopeFilter, ServiceFilter, SeverityFilter, SpanFilters,
        TimeRangeFilter,
    };
    use crate::result::{
        CoverageEntry, Dimension, Magnitude, Page, PartOutcome, Truncation, TruncationPoint,
    };

    /// A nanosecond admission reading.
    const fn at(nano: u64) -> AdmissionTime {
        AdmissionTime::from_unix_nano(nano)
    }

    /// An assigned entity id, as the ledger hands one out.
    fn assigned(serial: u64) -> EntityId {
        EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(serial).expect("fixture serials are nonzero"),
        ))
    }

    /// A span's natural wire identity.
    fn span_entity(trace: u8, span_byte: u8) -> EntityId {
        EntityId::Span {
            trace_id: TraceId::from_bytes([trace; 16]),
            span_id: SpanId::from_bytes([span_byte; 8]),
        }
    }

    /// An open budget: ten seconds, a thousand results, a terabyte, ten
    /// thousand scans — every ceiling but the one under test stays out of
    /// the way.
    fn open_budget() -> QueryBudget {
        budget_with(1_000, 1 << 40, 10_000)
    }

    /// A budget with chosen ceilings over an open deadline.
    fn budget_with(results: u64, bytes: u64, scan: u64) -> QueryBudget {
        QueryBudget::new(Duration::from_secs(10), results, bytes, scan, 4_096)
    }

    /// Five spans, admitted 100..=500 nanoseconds apart, each under its
    /// own wire identity: the pagination fixture.
    fn five_spans() -> FixtureStore {
        span_store(5)
    }

    /// `count` spans, one wire identity each, admitted 100 nanoseconds
    /// apart: big enough that a walk crosses `SCAN_BATCH` boundaries.
    fn span_store(count: u8) -> FixtureStore {
        let mut store = FixtureStore::empty();
        for index in 1_u8..=count {
            keep_span(
                &mut store,
                u64::from(index) * 100,
                span_entity(index, index),
                fixture_span(index, index, &format!("span-{index}")),
            );
        }
        store
    }

    fn keep_span(store: &mut FixtureStore, nano: u64, entity: EntityId, span: Span) {
        let outcome = store.keep_span(Admitted {
            entity,
            admitted_at: at(nano),
            record: Arc::new(span),
        });
        assert!(matches!(outcome, KeepOutcome::Kept { evicted: 0 }));
    }

    fn keep_log(store: &mut FixtureStore, nano: u64, entity: EntityId, record: LogRecord) {
        let outcome = store.keep_log_record(Admitted {
            entity,
            admitted_at: at(nano),
            record: Arc::new(record),
        });
        assert!(matches!(outcome, KeepOutcome::Kept { evicted: 0 }));
    }

    fn keep_point(
        store: &mut FixtureStore,
        nano: u64,
        entity: EntityId,
        point: MetricPoint,
        stream: StreamIdentity,
    ) {
        let outcome = store.keep_metric_point(
            Admitted {
                entity,
                admitted_at: at(nano),
                record: Arc::new(point),
            },
            Arc::new(stream),
        );
        assert!(matches!(outcome, KeepOutcome::Kept { evicted: 0 }));
    }

    /// The page's single degradation — every degraded fixture page has
    /// exactly one part.
    fn degraded_of(page: &Page<RecordView>) -> &Truncation {
        let [PartOutcome::Degraded { truncation }] = &page.execution.parts[..] else {
            panic!("the page degrades");
        };
        truncation
    }

    /// A cursor's admission-time position, for comparing anchors directly.
    fn fresh_position(payload: &CursorPayload) -> u64 {
        payload.position()
    }

    fn span_entity_of(view: &RecordView) -> Option<EntityId> {
        match view {
            RecordView::Span(span) => Some(EntityId::Span {
                trace_id: span.context.trace_id,
                span_id: span.context.span_id,
            }),
            _ => None,
        }
    }

    fn log_body_of(view: &RecordView) -> Option<&str> {
        match view {
            RecordView::LogRecord(record) => match &record.body {
                Some(Value::String(text)) => Some(text.as_str()),
                _ => None,
            },
            _ => None,
        }
    }

    fn point_value_of(view: &RecordView) -> Option<i64> {
        match view {
            RecordView::MetricPoint { point, .. } => match point.as_ref() {
                MetricPoint::Number(number) => match number.value {
                    MetricNumber::Int(value) => Some(value),
                    MetricNumber::Double(_) => None,
                },
                _ => None,
            },
            _ => None,
        }
    }

    fn stream_name_of(view: &RecordView) -> Option<&str> {
        match view {
            RecordView::MetricPoint { stream, .. } => Some(stream.name.as_str()),
            _ => None,
        }
    }

    /// One kind's resident records: the residency-order map plus the
    /// entity index — the same two structures the real in-memory shelf
    /// keeps, holding the same `Arc`ed payloads.
    struct FixtureShelf<R> {
        by_key: BTreeMap<AdmissionKey, Arc<R>>,
        by_entity: HashMap<EntityId, AdmissionKey>,
    }

    impl<R> FixtureShelf<R> {
        fn insert(&mut self, admitted: Admitted<Arc<R>>) {
            let key = AdmissionKey::new(admitted.admitted_at, admitted.entity);
            self.by_entity.insert(admitted.entity, key);
            self.by_key.insert(key, admitted.record);
        }

        fn get(&self, entity: EntityId) -> Option<Arc<R>> {
            let key = self.by_entity.get(&entity)?;
            self.by_key.get(key).cloned()
        }

        fn remove(&mut self, entity: EntityId) -> bool {
            let Some(key) = self.by_entity.remove(&entity) else {
                return false;
            };
            self.by_key.remove(&key).is_some()
        }

        fn len(&self) -> usize {
            self.by_key.len()
        }

        /// The ordered page: at most `limit` records strictly after
        /// `after`, the cursor exactly the last item's key when a record
        /// follows it — the contract's scan law, the same law the real
        /// shelf implements.
        fn scan(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<R>> {
            if limit == 0 {
                return ScanPage {
                    items: Vec::new(),
                    cursor: None,
                };
            }
            let records: Box<dyn Iterator<Item = (AdmissionKey, &Arc<R>)> + '_> = match after {
                Some(after) => Box::new(
                    self.by_key
                        .range((Bound::Excluded(after), Bound::Unbounded))
                        .map(|(key, record)| (*key, record)),
                ),
                None => Box::new(self.by_key.iter().map(|(key, record)| (*key, record))),
            };
            let mut items = Vec::new();
            let mut cursor = None;
            let mut last_in_page = None;
            for (key, record) in records {
                if items.len() == limit {
                    cursor = last_in_page;
                    break;
                }
                items.push(ScanItem {
                    key,
                    record: Arc::clone(record),
                });
                last_in_page = Some(key);
            }
            ScanPage { items, cursor }
        }
    }

    /// A resident metric point with the interned stream it was kept
    /// under.
    struct StoredPoint {
        point: Arc<MetricPoint>,
        stream: Arc<StreamIdentity>,
    }

    /// A second, contract-conforming implementation of the storage
    /// contract, defined in this crate's test code because the boundary
    /// law allows `layer-query` the contract only: the engine's seam is
    /// the trait, so its behavioral suite runs against a second
    /// conforming implementation — never against a type-shallow test
    /// double. The real driver's conformance is owned by storage-memory's
    /// contract suite; engine×driver composition stays with the
    /// composition root, where the law lets them meet.
    struct FixtureStore {
        spans: FixtureShelf<Span>,
        logs: FixtureShelf<LogRecord>,
        points: FixtureShelf<StoredPoint>,
    }

    impl FixtureStore {
        fn empty() -> Self {
            Self {
                spans: FixtureShelf {
                    by_key: BTreeMap::new(),
                    by_entity: HashMap::new(),
                },
                logs: FixtureShelf {
                    by_key: BTreeMap::new(),
                    by_entity: HashMap::new(),
                },
                points: FixtureShelf {
                    by_key: BTreeMap::new(),
                    by_entity: HashMap::new(),
                },
            }
        }

        /// Injects an eviction: retention is the driver's tested duty,
        /// and the engine's tests only need a record to disappear between
        /// scans.
        fn evict(&mut self, entity: EntityId) -> bool {
            self.spans.remove(entity) || self.logs.remove(entity) || self.points.remove(entity)
        }
    }

    impl TelemetryStore for FixtureStore {
        fn keep_span(&mut self, admitted: Admitted<Arc<Span>>) -> KeepOutcome {
            if self.spans.get(admitted.entity).is_some() {
                return KeepOutcome::Duplicate;
            }
            self.spans.insert(admitted);
            KeepOutcome::Kept { evicted: 0 }
        }

        fn keep_log_record(&mut self, admitted: Admitted<Arc<LogRecord>>) -> KeepOutcome {
            if self.logs.get(admitted.entity).is_some() {
                return KeepOutcome::Duplicate;
            }
            self.logs.insert(admitted);
            KeepOutcome::Kept { evicted: 0 }
        }

        fn keep_metric_point(
            &mut self,
            admitted: Admitted<Arc<MetricPoint>>,
            stream: Arc<StreamIdentity>,
        ) -> KeepOutcome {
            if self.points.get(admitted.entity).is_some() {
                return KeepOutcome::Duplicate;
            }
            self.points.insert(Admitted {
                entity: admitted.entity,
                admitted_at: admitted.admitted_at,
                record: Arc::new(StoredPoint {
                    point: admitted.record,
                    stream,
                }),
            });
            KeepOutcome::Kept { evicted: 0 }
        }

        fn span(&self, entity: EntityId) -> Option<Arc<Span>> {
            self.spans.get(entity)
        }

        fn log_record(&self, entity: EntityId) -> Option<Arc<LogRecord>> {
            self.logs.get(entity)
        }

        fn metric_point(&self, entity: EntityId) -> Option<PointView> {
            let stored = self.points.get(entity)?;
            Some(PointView {
                point: Arc::clone(&stored.point),
                stream: Arc::clone(&stored.stream),
            })
        }

        fn scan_spans(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<Span>> {
            self.spans.scan(after, limit)
        }

        fn scan_log_records(
            &self,
            after: Option<AdmissionKey>,
            limit: usize,
        ) -> ScanPage<Arc<LogRecord>> {
            self.logs.scan(after, limit)
        }

        fn scan_metric_points(
            &self,
            after: Option<AdmissionKey>,
            limit: usize,
        ) -> ScanPage<PointView> {
            let page = self.points.scan(after, limit);
            let items = page
                .items
                .into_iter()
                .map(|item| ScanItem {
                    key: item.key,
                    record: PointView {
                        point: Arc::clone(&item.record.point),
                        stream: Arc::clone(&item.record.stream),
                    },
                })
                .collect();
            ScanPage {
                items,
                cursor: page.cursor,
            }
        }

        fn enforce_retention(&mut self, _now: AdmissionTime) -> u64 {
            // The fixture owns no ceilings; tests evict directly.
            0
        }

        fn observe_admission_anomalies(&mut self, _total: u64) {}

        fn stats(&self) -> StoreStats {
            StoreStats {
                resident_records: u64::try_from(
                    self.spans.len() + self.logs.len() + self.points.len(),
                )
                .unwrap_or(u64::MAX),
                resident_spans: u64::try_from(self.spans.len()).unwrap_or(u64::MAX),
                resident_log_records: u64::try_from(self.logs.len()).unwrap_or(u64::MAX),
                resident_metric_points: u64::try_from(self.points.len()).unwrap_or(u64::MAX),
                ..StoreStats::default()
            }
        }

        fn mode_name(&self) -> &'static str {
            "fixture"
        }
    }

    fn fixture_span(trace: u8, span_byte: u8, name: &str) -> Span {
        Span {
            context: TraceContext {
                trace_id: TraceId::from_bytes([trace; 16]),
                span_id: SpanId::from_bytes([span_byte; 8]),
                flags: TraceFlags::new(1),
                tracestate: TraceState::default(),
            },
            parent_span_id: None,
            name: name.to_owned(),
            kind: SpanKind::Server,
            start_time_unix_nano: 10,
            end_time_unix_nano: Some(20),
            resource: Arc::new(Resource {
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            }),
            scope: Arc::new(InstrumentationScope {
                name: String::new(),
                version: None,
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            }),
            attributes: Attributes::default(),
            emitter_dropped: EmitterDroppedCounts::default(),
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: SpanStatusCode::Unset,
                message: String::new(),
            },
        }
    }

    fn fixture_log(body: &str) -> LogRecord {
        LogRecord {
            timestamp_unix_nano: Some(1),
            observed_timestamp_unix_nano: Some(2),
            severity_number: None,
            severity_text: None,
            body: Some(Value::String(body.to_owned())),
            resource: Arc::new(Resource {
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            }),
            scope: Arc::new(InstrumentationScope {
                name: String::new(),
                version: None,
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            }),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_id: None,
            span_id: None,
            trace_flags: None,
            event_name: None,
        }
    }

    fn fixture_stream(name: &str) -> StreamIdentity {
        StreamIdentity {
            resource: Resource {
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            },
            scope: InstrumentationScope {
                name: String::new(),
                version: None,
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            },
            name: name.to_owned(),
            description: None,
            unit: None,
            metadata: Attributes::default(),
            kind: StreamKind::Gauge,
            temporality: None,
        }
    }

    fn fixture_point(value: i64) -> MetricPoint {
        MetricPoint::Number(NumberPoint::measurement(
            1,
            MetricNumber::int(value),
            Attributes::default(),
            Vec::new(),
        ))
    }

    /// Five spans alternating between two services, admitted 100..=500
    /// nanoseconds apart: the filter fixtures' resident set.
    fn service_store() -> FixtureStore {
        let scope = fixture_scope("scope", None);
        let mut store = FixtureStore::empty();
        let services = ["checkout", "payments", "checkout", "payments", "checkout"];
        for (index, service) in services.iter().enumerate() {
            let index = u8::try_from(index + 1).expect("fixture indices fit");
            keep_span(
                &mut store,
                u64::from(index) * 100,
                span_entity(index, index),
                service_span(index, index, Some(service), &scope, u64::from(index) * 10),
            );
        }
        store
    }

    /// The checkout-only spans query over [`service_store`].
    fn checkout_query() -> RecordsQuery {
        RecordsQuery::spans(SpanFilters {
            service: Some(ServiceFilter::new("checkout")),
            time_range: None,
            scope: None,
        })
    }

    /// A resource whose attribute map names `service` or nothing.
    fn fixture_resource(service: Option<&str>) -> Resource {
        let attributes = match service {
            Some(name) => Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String(name.to_owned()),
            )])
            .expect("one key"),
            None => Attributes::default(),
        };
        Resource {
            attributes,
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    /// A scope with the given name and version.
    fn fixture_scope(name: &str, version: Option<&str>) -> InstrumentationScope {
        InstrumentationScope {
            name: name.to_owned(),
            version: version.map(str::to_owned),
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    /// A span under the given service, scope and start time, with its own
    /// wire identity so assertions can name it.
    fn service_span(
        trace: u8,
        span_byte: u8,
        service: Option<&str>,
        scope: &InstrumentationScope,
        start: u64,
    ) -> Span {
        let mut span = fixture_span(trace, span_byte, "filtered-span");
        span.start_time_unix_nano = start;
        span.resource = Arc::new(fixture_resource(service));
        span.scope = Arc::new(scope.clone());
        span
    }

    /// A log under the given service, scope, timestamps and severity.
    fn service_log(
        service: Option<&str>,
        scope: &InstrumentationScope,
        timestamp: Option<u64>,
        observed: Option<u64>,
        severity: Option<SeverityNumber>,
        body: &str,
    ) -> LogRecord {
        let mut record = fixture_log(body);
        record.timestamp_unix_nano = timestamp;
        record.observed_timestamp_unix_nano = observed;
        record.severity_number = severity;
        record.resource = Arc::new(fixture_resource(service));
        record.scope = Arc::new(scope.clone());
        record
    }

    /// A point under the given service and scope at a given time.
    fn service_point(
        service: Option<&str>,
        scope: &InstrumentationScope,
        time: u64,
        value: i64,
    ) -> (MetricPoint, StreamIdentity) {
        let mut stream = fixture_stream("filtered-stream");
        stream.resource = fixture_resource(service);
        stream.scope = scope.clone();
        (
            MetricPoint::Number(NumberPoint::measurement(
                time,
                MetricNumber::int(value),
                Attributes::default(),
                Vec::new(),
            )),
            stream,
        )
    }

    /// Invariant 8: the same resident set, query and budget assemble
    /// identical content — items, continuation, part shapes — across two
    /// independent runs and two independently built stores. The budget
    /// truncates at three, so a minted cursor is inside the comparison
    /// too; no deadline spend is involved, so the whole page compares.
    ///
    /// Kills the no-op where page assembly leaks run facts into content,
    /// or where the walk's order depends on the driver's iteration beyond
    /// the contract's residency order.
    #[test]
    fn identical_inputs_produce_identical_pages_across_independent_runs() {
        fn run(store: &FixtureStore) -> Page<RecordView> {
            records(
                store,
                &RecordsQuery::new(SignalKind::Spans),
                budget_with(3, 1 << 40, 10_000),
                None,
            )
            .expect("the query answers")
        }

        let first_store = five_spans();
        let second_store = five_spans();
        let first = run(&first_store);
        let second = run(&first_store);
        let third = run(&second_store);
        assert_eq!(first, second, "two runs on one store assemble one page");
        assert_eq!(first, third, "a second, identically built store too");

        let entities: Vec<EntityId> = first
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(1, 1), span_entity(2, 2), span_entity(3, 3)],
            "the page carries the first three records in residency order"
        );
        let cursor = first
            .next_cursor
            .as_deref()
            .expect("a truncated page continues");
        let payload = CursorPayload::decode(cursor).expect("the engine's own cursor");
        assert_eq!(
            payload.position(),
            300_u64,
            "the cursor's position is the anchor's admission time, not an ordinal"
        );
        assert_eq!(payload.last_entity(), span_entity(3, 3));
        assert_eq!(
            payload.snapshot(),
            AdmissionKey::new(at(500), span_entity(5, 5)),
            "the minted frontier is the pulled batch's tail: the kind's last record"
        );
    }

    /// Invariant 2: with all three kinds resident, each kind's page is
    /// totally ordered — admission time first, the entity id breaking
    /// every tie.
    ///
    /// Kills the no-op where a kind's page falls back to driver iteration
    /// order, or where a tie-break is dropped.
    #[test]
    fn each_kind_page_is_totally_ordered_with_entity_ties_broken() {
        let mut store = FixtureStore::empty();
        // Every record admits at the same nanosecond: the entity id
        // decides each page's whole order.
        keep_span(
            &mut store,
            100,
            span_entity(1, 1),
            fixture_span(1, 1, "s-one"),
        );
        keep_span(
            &mut store,
            100,
            span_entity(2, 1),
            fixture_span(2, 1, "s-two"),
        );
        keep_span(
            &mut store,
            100,
            span_entity(1, 2),
            fixture_span(1, 2, "s-three"),
        );
        keep_log(&mut store, 100, assigned(3), fixture_log("log-three"));
        keep_log(&mut store, 100, assigned(1), fixture_log("log-one"));
        keep_point(
            &mut store,
            100,
            assigned(2),
            fixture_point(2),
            fixture_stream("metric-two"),
        );
        keep_point(
            &mut store,
            100,
            assigned(1),
            fixture_point(1),
            fixture_stream("metric-one"),
        );

        let spans = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let span_entities: Vec<EntityId> = spans
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            span_entities,
            vec![span_entity(1, 1), span_entity(1, 2), span_entity(2, 1)],
            "spans tie-break by trace bytes, then span bytes"
        );
        let logs = records(
            &store,
            &RecordsQuery::new(SignalKind::LogRecords),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let log_bodies: Vec<&str> = logs
            .items
            .iter()
            .map(|view| log_body_of(view).expect("a log page"))
            .collect();
        assert_eq!(
            log_bodies,
            vec!["log-one", "log-three"],
            "logs tie-break by assigned serial"
        );
        let points = records(
            &store,
            &RecordsQuery::new(SignalKind::MetricPoints),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let point_values: Vec<i64> = points
            .items
            .iter()
            .map(|view| point_value_of(view).expect("a point page"))
            .collect();
        assert_eq!(
            point_values,
            vec![1, 2],
            "points tie-break by assigned serial"
        );
        for page in [&spans, &logs, &points] {
            assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
            assert!(page.next_cursor.is_none());
        }
    }

    /// Pagination: a set larger than one page walks to exhaustion through
    /// cursors — no record repeated, none lost, the order stable, and
    /// every continuation bounded by the first page's snapshot.
    ///
    /// Kills the stall no-op (a snapshot bound at the budget-stop record
    /// sutures every later page) and any repeat-or-loss in the cursor
    /// chain.
    #[test]
    fn pagination_walks_a_set_to_exhaustion_without_repeat_or_loss() {
        let store = five_spans();
        let query = RecordsQuery::new(SignalKind::Spans);
        let mut cursor: Option<Vec<u8>> = None;
        let mut seen: Vec<EntityId> = Vec::new();
        let mut continuations = 0;
        loop {
            let presented = cursor.is_some();
            let page = records(
                &store,
                &query,
                budget_with(2, 1 << 40, 10_000),
                cursor.as_deref(),
            )
            .expect("the query answers");
            seen.extend(
                page.items
                    .iter()
                    .map(|view| span_entity_of(view).expect("a span page")),
            );
            if let Some(next) = page.next_cursor.clone() {
                cursor = Some(next);
                continuations += 1;
                // A page that was handed a cursor is bounded by the
                // snapshot that cursor carries, and names it; the first
                // page mints the boundary instead — it travels in the
                // cursor it minted.
                if presented {
                    assert!(
                        page.execution
                            .coverage
                            .entries
                            .iter()
                            .any(|entry| matches!(entry, CoverageEntry::SnapshotBoundary { .. })),
                        "every continuation names its snapshot boundary"
                    );
                }
            } else {
                assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
                break;
            }
            assert!(continuations < 10, "the walk terminates");
        }
        assert_eq!(
            continuations, 2,
            "five records at two per page walk three pages"
        );
        assert_eq!(
            seen,
            vec![
                span_entity(1, 1),
                span_entity(2, 2),
                span_entity(3, 3),
                span_entity(4, 4),
                span_entity(5, 5),
            ],
            "every record appears exactly once, in residency order"
        );
    }

    /// Invariant 3, end to end: a cursor minted by a spans query is an
    /// error under a log-records query, and a byte string that is not a
    /// cursor is an error anywhere — never a best-effort continuation.
    ///
    /// Kills the no-op where the fingerprint covers anything but the kind
    /// (a query collision), or where a malformed cursor degrades instead
    /// of failing.
    #[test]
    fn a_cursor_presented_under_another_query_is_an_error() {
        let store = five_spans();
        let first = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        let wrong_query = records(
            &store,
            &RecordsQuery::new(SignalKind::LogRecords),
            budget_with(2, 1 << 40, 10_000),
            Some(&cursor),
        )
        .expect_err("a foreign query must not continue the cursor");
        assert_eq!(
            wrong_query,
            QueryError::Cursor(CursorError::FingerprintMismatch)
        );

        let garbage = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            Some(&[9; 10]),
        )
        .expect_err("bytes that decode to nothing are an error");
        assert_eq!(garbage, QueryError::Cursor(CursorError::Malformed));
    }

    /// The snapshot bound: records admitted after the first page's
    /// frontier stay outside every later page, and the boundary entry
    /// names the frontier exactly.
    ///
    /// Kills the no-op where a later page re-walks past its snapshot and
    /// silently picks up new records.
    #[test]
    fn records_admitted_after_the_frontier_stay_outside_every_later_page() {
        let mut store = five_spans();
        let first = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        keep_span(
            &mut store,
            600,
            span_entity(6, 6),
            fixture_span(6, 6, "span-6"),
        );
        keep_span(
            &mut store,
            700,
            span_entity(7, 7),
            fixture_span(7, 7, "span-7"),
        );

        let second = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            Some(&cursor),
        )
        .expect("the query answers");
        let entities: Vec<EntityId> = second
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(3, 3), span_entity(4, 4), span_entity(5, 5),],
            "the page walks exactly what the snapshot held — everything \
             pulled before the frontier, and none of what was admitted \
             after it",
        );
        assert_eq!(second.execution.parts, vec![PartOutcome::Complete]);
        assert_eq!(
            second.execution.coverage.entries,
            vec![CoverageEntry::SnapshotBoundary {
                admission: AdmissionKey::new(at(500), span_entity(5, 5)),
            }],
            "the boundary names the first page's pulled frontier"
        );
        assert!(second.next_cursor.is_none());
    }

    /// The evicted anchor: a continuation whose cursor points into an
    /// evicted record names the hole — the anchor, then the first
    /// resident successor — and the walk stays exact.
    ///
    /// Kills the no-op where an evicted anchor is skipped silently, and
    /// the one where the resumed walk re-walks or loses records.
    #[test]
    fn an_evicted_anchor_is_named_as_a_gap_with_its_first_resident_successor() {
        let mut store = five_spans();
        let first = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        assert!(store.evict(span_entity(2, 2)), "the anchor was resident");

        let second = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            Some(&cursor),
        )
        .expect("the query answers");
        let entities: Vec<EntityId> = second
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(3, 3), span_entity(4, 4), span_entity(5, 5),],
            "the walk resumes exactly, skipping nothing that is resident"
        );
        assert_eq!(
            second.execution.coverage.entries,
            vec![
                CoverageEntry::SnapshotBoundary {
                    admission: AdmissionKey::new(at(500), span_entity(5, 5)),
                },
                CoverageEntry::EvictionGap {
                    after: span_entity(2, 2),
                    before: span_entity(3, 3),
                },
            ],
            "the gap names the evicted anchor and its first resident successor"
        );
        assert!(second.next_cursor.is_none());
    }

    /// The byte ceiling, counted truly: the page stops including at the
    /// first record whose evidence does not fit, walks the remainder, and
    /// reports exactly how many records would not fit — anchored at the
    /// last included record.
    ///
    /// Kills both lies: reporting zero (or a guess) for the omission, and
    /// anchoring the cursor at the wall record (which would skip it
    /// forever).
    #[test]
    fn a_byte_ceiling_names_the_true_omission_count_and_anchors_behind_it() {
        let mut store = FixtureStore::empty();
        let mut sizes = Vec::new();
        for index in 1_u8..=5 {
            let name = match index {
                1 => "a".repeat(40),
                2 => "b".repeat(40),
                3 => "c".repeat(80),
                4 => "d".repeat(40),
                _ => "e".repeat(100),
            };
            let span = fixture_span(index, index, &name);
            sizes.push(u64::try_from(span.accounted_size()).expect("sizes fit"));
            keep_span(
                &mut store,
                u64::from(index) * 100,
                span_entity(index, index),
                span,
            );
        }
        // The ceiling fits the first two records and stops one byte short
        // of the third: the page includes two, then counts the remainder.
        let ceiling = sizes[0] + sizes[1] + sizes[2] - 1;
        let page = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(1_000, ceiling, 10_000),
            None,
        )
        .expect("the query answers");

        let truncation = degraded_of(&page);
        assert_eq!(truncation.dimension, Dimension::Bytes);
        assert_eq!(
            truncation.omitted, 2,
            "records three and five do not fit; four does"
        );
        let entities: Vec<EntityId> = page
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(entities, vec![span_entity(1, 1), span_entity(2, 2)]);
        let cursor = page.next_cursor.as_deref().expect("the page continues");
        let payload = CursorPayload::decode(cursor).expect("the engine's own cursor");
        assert_eq!(
            payload.position(),
            200_u64,
            "the cursor anchors at the last included record"
        );
        assert_eq!(payload.last_entity(), span_entity(2, 2));
        assert!(
            page.execution.coverage.entries.is_empty(),
            "a counted omission needs no coverage entry"
        );
    }

    /// A deadline dead before any examination refuses the first page:
    /// dimension, limit and observed spend exist only in a refusal, and
    /// no truthful degrade is expressible with nothing examined and no
    /// cursor to echo (invariant 6).
    ///
    /// Kills the no-op where a dead budget degrades into a fabricated
    /// position, or where it walks one record before noticing.
    #[test]
    fn a_dead_deadline_refuses_the_first_page_before_any_examination() {
        let store = five_spans();
        let page = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            QueryBudget::new(Duration::ZERO, 1_000, 1 << 40, 10_000, 4_096),
            None,
        )
        .expect("the query answers");

        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
        let [PartOutcome::Refused(refusal)] = &page.execution.parts[..] else {
            panic!("a dead first page refuses");
        };
        assert_eq!(refusal.dimension, Dimension::Deadline);
        assert_eq!(refusal.limit, Magnitude::Duration(Duration::ZERO));
        assert!(
            matches!(refusal.observed, Magnitude::Duration(_)),
            "the observed spend is a duration, not a count"
        );
    }

    /// An empty kind completes as an empty page: no cursor, no coverage,
    /// nothing named because nothing is missing.
    ///
    /// Kills the no-op where an empty set mints a cursor or names a
    /// truncation.
    #[test]
    fn an_empty_kind_completes_with_an_empty_page_and_no_cursor() {
        let store = FixtureStore::empty();
        let page = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            None,
        )
        .expect("the query answers");
        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
        assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
        assert!(page.execution.coverage.entries.is_empty());

        let mut logs_only = FixtureStore::empty();
        keep_log(&mut logs_only, 100, assigned(1), fixture_log("log-one"));
        let spans_of_logs = records(
            &logs_only,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            None,
        )
        .expect("the query answers");
        assert!(spans_of_logs.items.is_empty());
        assert_eq!(spans_of_logs.execution.parts, vec![PartOutcome::Complete]);
        assert!(spans_of_logs.next_cursor.is_none());
    }

    /// A counting walk cut short by its scan ceiling reports the count it
    /// had already established — the records it had confirmed as
    /// byte-omissions before the cut — and coverage names the one record
    /// the cut denied it. The reported number is a confirmed fragment,
    /// never zero pretending nothing was counted and never a partial
    /// count dressed up as complete.
    #[test]
    fn a_counting_walk_cut_short_is_uncounted_and_coverage_names_it() {
        let mut store = FixtureStore::empty();
        let mut sizes = Vec::new();
        for index in 1_u8..=4 {
            let name = match index {
                1 => "a".repeat(40),
                2 => "b".repeat(40),
                3 => "c".repeat(80),
                _ => "d".repeat(40),
            };
            let span = fixture_span(index, index, &name);
            sizes.push(u64::try_from(span.accounted_size()).expect("sizes fit"));
            keep_span(
                &mut store,
                u64::from(index) * 100,
                span_entity(index, index),
                span,
            );
        }
        // The byte ceiling fits two records and stops one byte short of
        // the third; the scan ceiling allows exactly three examinations,
        // so the counting walk is cut at the fourth record.
        let page = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(1_000, sizes[0] + sizes[1] + sizes[2] - 1, 3),
            None,
        )
        .expect("the query answers");

        let truncation = degraded_of(&page);
        assert_eq!(truncation.dimension, Dimension::Bytes);
        assert_eq!(
            truncation.omitted, 1,
            "e3 was a confirmed byte-omission before the counting walk was \
             cut — the count reports the confirmed fragment, never zero"
        );
        assert_eq!(
            page.execution.coverage.entries,
            vec![CoverageEntry::UncountedTail {
                after: span_entity(3, 3),
                dimension: Dimension::Scan,
            }],
            "coverage names where the counting walk stopped"
        );
        let entities: Vec<EntityId> = page
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(entities, vec![span_entity(1, 1), span_entity(2, 2)]);
        let cursor = page.next_cursor.as_deref().expect("the page continues");
        let payload = CursorPayload::decode(cursor).expect("the engine's own cursor");
        assert_eq!(payload.last_entity(), span_entity(2, 2));
    }

    /// A byte-ceiling counting walk cut by the scan ceiling on a
    /// continuation keeps its count: the scan stop is where the walk
    /// stops, but the omission is still byte-dimensional — the byte
    /// ceiling is what the records would not fit — and `omitted` reports
    /// the confirmed count the walk had established (e3 and e4, both
    /// confirmed byte-omissions), never a dressed-down zero. The
    /// [`CoverageEntry::UncountedTail`] names the last counted record and
    /// the scan stop for the rest.
    ///
    /// Kills the no-op where the scan cut swallows the byte-ceiling count
    /// (`omitted` reset to zero) or renames the omission to the scan
    /// dimension (`dimension: Scan`), which would misstate what was cut.
    #[test]
    fn a_byte_ceiling_count_survives_a_scan_cut_in_a_continuation() {
        let store = five_spans();
        let query = RecordsQuery::new(SignalKind::Spans);
        let first = records(&store, &query, budget_with(2, 1 << 40, 10_000), None)
            .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        // Byte ceiling 1: below every span's evidence, so the counting walk
        // opens at e3. Scan ceiling 2: e3 and e4 are examined and counted,
        // and the stop is the refusal of e5's charge.
        let second = records(&store, &query, budget_with(1_000, 1, 2), Some(&cursor))
            .expect("the query answers");
        assert!(second.items.is_empty());
        let truncation = degraded_of(&second);
        assert_eq!(
            truncation.dimension,
            Dimension::Bytes,
            "the omission stays byte-dimensional — the byte ceiling is what \
             the records would not fit"
        );
        assert_eq!(
            truncation.omitted, 2,
            "the count the walk had confirmed before the cut survives"
        );
        assert_eq!(
            second.execution.coverage.entries,
            vec![
                CoverageEntry::SnapshotBoundary {
                    admission: AdmissionKey::new(at(500), span_entity(5, 5)),
                },
                CoverageEntry::UncountedTail {
                    after: span_entity(4, 4),
                    dimension: Dimension::Scan,
                },
            ],
            "the uncounted tail names the last counted record and the scan stop"
        );
    }

    /// A continuation whose deadline is dead before its first record
    /// refuses: nothing was examined, so no cursor of its own was
    /// arrived at and the presented cursor is a fabricated position —
    /// the same honest shape a deadline-dead first page produces.
    /// The refusal names dimension (Deadline), limit and observed spend
    /// per invariant 6, and is distinguishable from a retryable echo.
    ///
    /// Kills the echo loop where a dead continuation returned a cursor
    /// identical to its input — indistinguishable from progress —
    /// creating an infinite retry cycle.
    #[test]
    fn a_dead_continuation_refuses_when_nothing_was_examined() {
        let store = five_spans();
        let first = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        let second = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            QueryBudget::new(Duration::ZERO, 1_000, 1 << 40, 10_000, 4_096),
            Some(&cursor),
        )
        .expect("the query answers");

        assert!(second.items.is_empty());
        // The continuation has no cursor to offer — the budget has no
        // scan work to do, so nothing is a truthful continuation.
        assert!(
            second.next_cursor.is_none(),
            "a refused continuation offers no cursor — the budget is spent"
        );
        // The refusal is the honest shape, identical in structure to a
        // deadline-dead first page: it names the dimension, the limit and
        // the observed spend.
        let [PartOutcome::Refused(refusal)] = &second.execution.parts[..] else {
            panic!("a dead continuation refuses, not degrades");
        };
        assert_eq!(refusal.dimension, Dimension::Deadline);
        assert_eq!(
            refusal.limit,
            Magnitude::Duration(Duration::ZERO),
            "the admitted deadline was zero"
        );
        assert!(
            matches!(refusal.observed, Magnitude::Duration(_)),
            "the observed spend is a duration"
        );
        assert_eq!(
            second.execution.coverage.entries,
            vec![CoverageEntry::SnapshotBoundary {
                admission: AdmissionKey::new(at(500), span_entity(5, 5)),
            }],
            "the snapshot boundary is still named"
        );
    }

    /// A dead continuation refuses, but a *live* continuation cut by the
    /// scan ceiling mid-walk is a different shape: the walk examined and
    /// included records, so the degrade anchors at the last included
    /// record and mints a FRESH cursor from it — never a blank echo of
    /// the presented cursor. The fresh cursor is strictly past the
    /// presented one (the anchor moved past it), so the caller's position
    /// genuinely advances and the continuation is what makes endless
    /// retry impossible — the anti-echo sibling of MINOR-4.
    #[test]
    fn an_examined_continuation_cut_by_scan_mints_a_fresh_continuation_cursor() {
        let store = five_spans();
        let query = RecordsQuery::new(SignalKind::Spans);
        let first = records(&store, &query, budget_with(2, 1 << 40, 10_000), None)
            .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        // Beyond the ceiling, this continuation WOULD include more. First
        // page minted at e2, snapshot e5. The walk resumes from e2
        // strictly past with only one scan unit: it examines and includes
        // e3, then the charge to examine e4 is refused. The stop anchors
        // at the last *included* record (e3), which is past the presented
        // anchor — no room for an echo that would loop forever, and the
        // next page resumes losslessly after e3.
        let second = records(
            &store,
            &query,
            budget_with(1_000, 1 << 40, 1),
            Some(&cursor),
        )
        .expect("the query answers");
        let entities: Vec<EntityId> = second
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(3, 3)],
            "the one record the scan allowance bought was examined and included"
        );
        let truncation = degraded_of(&second);
        assert_eq!(
            truncation.dimension,
            Dimension::Scan,
            "the scan ceiling is what cut this continuation"
        );
        // The truncated position is not a blank echo: it is a cursor minted
        // from the anchor the walk actually reached before the ceiling died.
        let fresh = second.next_cursor.as_deref().expect("a minted cursor");
        assert_ne!(
            fresh,
            cursor.as_slice(),
            "the continuation's fresh cursor is not a byte-for-byte echo of \
             the presented cursor — the walk moved at least one record"
        );
        let payload = CursorPayload::decode(fresh).expect("the engine's own cursor");
        assert_eq!(
            payload.last_entity(),
            span_entity(3, 3),
            "the fresh cursor anchors at the last included record"
        );
        let presented = CursorPayload::decode(&cursor).expect("the presented cursor");
        assert!(
            fresh_position(&payload) > fresh_position(&presented),
            "the fresh cursor's position is strictly past the presented one — \
             the byte anchor moved, so the caller's position advances and the \
             next page resumes losslessly"
        );
        assert_eq!(
            second.execution.coverage.entries,
            vec![CoverageEntry::SnapshotBoundary {
                admission: AdmissionKey::new(at(500), span_entity(5, 5)),
            }],
            "the continuation still names its snapshot boundary"
        );
    }

    /// A gap with no resident successor: when the walk yields nothing
    /// before the snapshot bound, the gap names the boundary's entity —
    /// the boundary is what names the rest.
    ///
    /// Kills the no-op where an unresolved gap is dropped, or names a
    /// successor that does not exist.
    #[test]
    fn a_gap_with_no_resident_successor_names_the_snapshot_boundary() {
        let mut store = five_spans();
        let first = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        // Everything at and past the anchor goes: the resumed walk can
        // yield nothing before the bound.
        assert!(store.evict(span_entity(2, 2)));
        assert!(store.evict(span_entity(3, 3)));
        assert!(store.evict(span_entity(4, 4)));
        assert!(store.evict(span_entity(5, 5)));

        let second = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            Some(&cursor),
        )
        .expect("the query answers");
        assert!(second.items.is_empty());
        assert_eq!(second.execution.parts, vec![PartOutcome::Complete]);
        assert_eq!(
            second.execution.coverage.entries,
            vec![
                CoverageEntry::SnapshotBoundary {
                    admission: AdmissionKey::new(at(500), span_entity(5, 5)),
                },
                CoverageEntry::EvictionGap {
                    after: span_entity(2, 2),
                    before: span_entity(5, 5),
                },
            ],
            "the boundary's entity names the rest when nothing resident remains"
        );
        assert!(second.next_cursor.is_none());
    }

    /// MAJOR-1, the reviewer's measurement shape: `max_scan` = 1 over a
    /// mid-batch stop with the whole kind pulled as one batch. The driver
    /// yielded five records; the walk examined one before the scan ceiling
    /// died at the second. The pulled-but-unexamined tail (e2..e5) settles
    /// against what remains of the ceiling — zero here, the grant was already
    /// drained — so the spend pins at the ceiling exactly: never past it, no
    /// fabricated units, and the page shape is the same degrade it always
    /// was.
    #[test]
    fn a_scan_stop_on_a_full_batch_spends_the_ceiling_and_nothing_more() {
        let store = five_spans();
        let query = RecordsQuery::new(SignalKind::Spans);
        let budget = budget_with(1_000, 1 << 40, 1);
        let mut session = budget.admit(Instant::now());
        let page = walk_records(&store, &query, &mut session, None);
        assert_eq!(
            page.items.len(),
            1,
            "one record passed every gate before the scan ceiling died"
        );
        let truncation = degraded_of(&page);
        assert_eq!(truncation.dimension, Dimension::Scan);
        assert!(page.next_cursor.is_some(), "the page continues");
        // MAJOR-1: four pulled-unexamined records were yielded by the driver
        // for this query, but the ceiling is spent — the settle charges
        // nothing rather than breaching it or inventing units.
        assert_eq!(session.ledger().remaining_scan(), 0);
    }

    /// MAJOR-1's ceiling pin at scale: a walk whose first batch alone is
    /// larger than a tiny scan ceiling stops mid-batch and spends exactly the
    /// ceiling — the sixty-two-record tail behind the refused charge settles
    /// to zero against a drained dimension, and the walk never reaches the
    /// second batch.
    #[test]
    fn a_tiny_scan_ceiling_is_never_breached_by_batch_pull_ahead() {
        let store = span_store(70);
        let query = RecordsQuery::new(SignalKind::Spans);
        let budget = budget_with(1_000, 1 << 40, 2);
        let ceiling = budget.max_scan();
        let mut session = budget.admit(Instant::now());
        let page = walk_records(&store, &query, &mut session, None);
        assert_eq!(
            page.items.len(),
            2,
            "both examined records were returned before the ceiling died"
        );
        let truncation = degraded_of(&page);
        assert_eq!(truncation.dimension, Dimension::Scan);
        // Two examined units are the whole grant; the batch's other
        // sixty-two records — pulled whole, never examined — settle to zero
        // against the drained ceiling.
        assert_eq!(session.ledger().remaining_scan(), 0);
        assert_eq!(
            ceiling - session.ledger().remaining_scan(),
            2,
            "spend equals the ceiling exactly"
        );
    }

    /// MAJOR-1's settle bites where allowance remains: a snapshot-bound
    /// continuation whose bound stop lands mid-batch charges the records the
    /// batch pulled past the bound — the walk examined three and the batch
    /// dragged two more behind the bound, so the ledger carries five units,
    /// not three.
    #[test]
    fn a_bound_stop_mid_batch_settles_the_records_past_it() {
        let mut store = five_spans();
        let query = RecordsQuery::new(SignalKind::Spans);
        let first = records(&store, &query, budget_with(2, 1 << 40, 10_000), None)
            .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");
        let presented = CursorPayload::decode(&cursor).expect("the first page minted it");

        // Admitted after the minting page: the continuation's snapshot
        // stays at the first page's pulled tail, and the new records sit
        // past the bound.
        keep_span(
            &mut store,
            600,
            span_entity(6, 6),
            fixture_span(6, 6, "span-6"),
        );
        keep_span(
            &mut store,
            700,
            span_entity(7, 7),
            fixture_span(7, 7, "span-7"),
        );

        let budget = open_budget();
        let mut session = budget.admit(Instant::now());
        let page = walk_records(&store, &query, &mut session, Some(presented));
        let entities: Vec<EntityId> = page
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(3, 3), span_entity(4, 4), span_entity(5, 5),]
        );
        assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
        // Three examined in-snapshot records, plus the two the batch pulled
        // past the bound (e6, e7) settled at the stop: five units, ceiling
        // intact.
        assert_eq!(session.ledger().remaining_scan(), 10_000 - 5);
        assert!(page.next_cursor.is_none());
    }

    /// MINOR-1's pinned path: a byte-ceiling continuation where nothing fits
    /// includes no record, so its only anchor is the presented cursor — and
    /// the echo is byte-identical, not a re-mint.
    #[test]
    fn a_byte_ceiling_continuation_that_returns_nothing_echoes_its_cursor() {
        let store = five_spans();
        let query = RecordsQuery::new(SignalKind::Spans);
        let first = records(&store, &query, budget_with(2, 1 << 40, 10_000), None)
            .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        // A byte ceiling below the smallest record's evidence: the
        // continuation returns nothing and counts the rest truthfully.
        let second = records(&store, &query, budget_with(1_000, 1, 10_000), Some(&cursor))
            .expect("the query answers");
        assert!(second.items.is_empty());
        let truncation = degraded_of(&second);
        assert_eq!(truncation.dimension, Dimension::Bytes);
        assert_eq!(truncation.omitted, 3, "e3, e4 and e5 all miss the ceiling");
        assert_eq!(
            second.next_cursor.as_deref(),
            Some(cursor.as_slice()),
            "the presented cursor is echoed byte for byte"
        );
    }

    /// A metric point's evidence is its point's accounted size only: the
    /// stream identity — far larger here than the whole ceiling — is
    /// residency bookkeeping, and a byte ceiling sized to the points
    /// alone still returns both points.
    ///
    /// Kills the no-op where a point's evidence includes its stream
    /// identity, crowding every point out of a byte-budgeted page.
    #[test]
    fn a_metric_points_evidence_is_its_point_only_never_its_stream() {
        let mut store = FixtureStore::empty();
        let first_point = fixture_point(1);
        let second_point = fixture_point(2);
        let ceiling = u64::try_from(first_point.accounted_size() + second_point.accounted_size())
            .expect("sizes fit");
        keep_point(
            &mut store,
            100,
            assigned(1),
            first_point,
            fixture_stream("requests"),
        );
        keep_point(
            &mut store,
            200,
            assigned(2),
            second_point,
            fixture_stream("requests"),
        );

        let page = records(
            &store,
            &RecordsQuery::new(SignalKind::MetricPoints),
            budget_with(1_000, ceiling, 10_000),
            None,
        )
        .expect("the query answers");
        let values: Vec<i64> = page
            .items
            .iter()
            .map(|view| point_value_of(view).expect("a point page"))
            .collect();
        assert_eq!(
            values,
            vec![1, 2],
            "both points fit: identity bytes were never charged"
        );
        let streams: Vec<&str> = page
            .items
            .iter()
            .map(|view| stream_name_of(view).expect("a point page"))
            .collect();
        assert_eq!(
            streams,
            vec!["requests", "requests"],
            "every view carries its stream identity"
        );
        assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
    }

    /// A zero results ceiling demands the empty answer on any page: no
    /// examination, no omission — nothing was left out by expiry — and a
    /// continuation still names its boundary.
    ///
    /// Kills the no-op where a zero ceiling reports a truncation, mints a
    /// cursor, or walks at all.
    #[test]
    fn a_zero_results_budget_demands_the_empty_answer() {
        let store = five_spans();
        let first = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(0, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        assert!(first.items.is_empty());
        assert!(first.next_cursor.is_none());
        assert_eq!(first.execution.parts, vec![PartOutcome::Complete]);
        assert!(first.execution.coverage.entries.is_empty());

        let page_one = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = page_one.next_cursor.expect("a truncated page continues");
        let continuation = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(0, 1 << 40, 10_000),
            Some(&cursor),
        )
        .expect("the query answers");
        assert!(continuation.items.is_empty());
        assert!(continuation.next_cursor.is_none());
        assert_eq!(continuation.execution.parts, vec![PartOutcome::Complete]);
        assert_eq!(
            continuation.execution.coverage.entries,
            vec![CoverageEntry::SnapshotBoundary {
                admission: AdmissionKey::new(at(500), span_entity(5, 5)),
            }],
        );
    }

    /// A zero-results continuation whose cursor anchor the store has
    /// evicted is empty-but-not-eviction-blind: the budget demands the
    /// empty answer and the early return never walks, so the anchor's
    /// residency is checked directly and the hole is named in coverage —
    /// the evicted record, with the snapshot boundary's entity as the
    /// named rest, exactly as an unresolvable gap names it in the walk.
    ///
    /// Kills the no-op where a zero-results budget returns an empty page
    /// with no [`CoverageEntry::EvictionGap`] for an evicted anchor — silent
    /// shrinkage.
    #[test]
    fn a_zero_results_continuation_names_an_evicted_anchor_as_a_gap() {
        let mut store = five_spans();
        let first = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        assert!(store.evict(span_entity(2, 2)), "the anchor was resident");

        let continuation = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(0, 1 << 40, 10_000),
            Some(&cursor),
        )
        .expect("the query answers");
        assert!(continuation.items.is_empty());
        assert!(continuation.next_cursor.is_none());
        assert_eq!(continuation.execution.parts, vec![PartOutcome::Complete]);
        assert_eq!(
            continuation.execution.coverage.entries,
            vec![
                CoverageEntry::SnapshotBoundary {
                    admission: AdmissionKey::new(at(500), span_entity(5, 5)),
                },
                CoverageEntry::EvictionGap {
                    after: span_entity(2, 2),
                    before: span_entity(5, 5),
                },
            ],
            "the evicted anchor is a named hole even though the walk never \
             runs to resolve it"
        );

        // The complement: the same zero-results continuation with a resident
        // anchor stays empty and Complete with only the boundary — never a
        // phantom gap.
        let resident = five_spans();
        let page_one = records(
            &resident,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(2, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let resident_cursor = page_one.next_cursor.expect("a truncated page continues");
        let empty = records(
            &resident,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(0, 1 << 40, 10_000),
            Some(&resident_cursor),
        )
        .expect("the query answers");
        assert!(empty.items.is_empty());
        assert_eq!(
            empty.execution.coverage.entries,
            vec![CoverageEntry::SnapshotBoundary {
                admission: AdmissionKey::new(at(500), span_entity(5, 5)),
            }],
            "a resident anchor produces no gap"
        );
    }

    // ------------------------------------------------------------------
    // Content filters: service, time range, severity, scope identity.
    // ------------------------------------------------------------------

    /// The service filter returns only matching records, and the
    /// filtered-out ones are still examined: the scan ledger carries all
    /// five records while the results ledger carries the three matches
    /// (F4). A filter is a predicate over the walk, never an order and
    /// never a cheaper scan.
    ///
    /// Kills the no-op where a filter quietly narrows the scan charge
    /// (making `max_scan` mean "matching records examined") or where it
    /// skips examination altogether.
    #[test]
    fn a_service_filter_returns_only_matches_and_still_examines_the_rest() {
        let store = service_store();
        let query = checkout_query();
        let budget = budget_with(1_000, 1 << 40, 10);
        let mut session = budget.admit(Instant::now());
        let page = walk_records(&store, &query, &mut session, None);

        let entities: Vec<EntityId> = page
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(1, 1), span_entity(3, 3), span_entity(5, 5)],
            "only the checkout records, in residency order"
        );
        assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
        assert_eq!(
            session.ledger().remaining_scan(),
            10 - 5,
            "all five records were examined — the two payments records pay max_scan too"
        );
        assert_eq!(
            session.ledger().remaining_results(),
            1_000 - 3,
            "results are charged only for records the page returns"
        );
    }

    /// The time range is half-open on both ends: `from` included, `to`
    /// excluded, against the span's `start_time_unix_nano` (F3).
    ///
    /// Kills the closed-interval no-op on either end.
    #[test]
    fn a_time_range_is_half_open_on_both_ends() {
        let scope = fixture_scope("scope", None);
        let mut store = FixtureStore::empty();
        for (index, start) in [100_u64, 200, 300].into_iter().enumerate() {
            let index = u8::try_from(index + 1).expect("fixture indices fit");
            keep_span(
                &mut store,
                u64::from(index) * 100,
                span_entity(index, index),
                service_span(index, index, None, &scope, start),
            );
        }
        let query = RecordsQuery::spans(SpanFilters {
            service: None,
            time_range: Some(TimeRangeFilter::new(200, 300)),
            scope: None,
        });
        let page = records(&store, &query, open_budget(), None).expect("the query answers");
        let entities: Vec<EntityId> = page
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(2, 2)],
            "start 100 is before `from`; start 300 sits on the excluded `to`"
        );
        assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
        assert!(page.next_cursor.is_none());
    }

    /// Each kind filters on its own model timestamp: a log record on its
    /// event time — falling back to its observation time, and outside
    /// entirely when the emitter sent neither — and a metric point on its
    /// `time_unix_nano` (F3).
    ///
    /// Kills the no-op where logs filter on the observation time while an
    /// event time is present, or where a record with no time key slips
    /// into a bounded window.
    #[test]
    fn each_kind_filters_on_its_own_model_timestamp() {
        let scope = fixture_scope("scope", None);
        let mut store = FixtureStore::empty();
        keep_log(
            &mut store,
            100,
            assigned(1),
            service_log(None, &scope, None, None, None, "neither-time"),
        );
        keep_log(
            &mut store,
            200,
            assigned(2),
            service_log(None, &scope, None, Some(150), None, "observation-time-only"),
        );
        keep_log(
            &mut store,
            300,
            assigned(3),
            service_log(
                None,
                &scope,
                Some(50),
                Some(150),
                None,
                "event-time-outside",
            ),
        );
        let (first_point, first_stream) = service_point(None, &scope, 150, 1);
        keep_point(&mut store, 400, assigned(4), first_point, first_stream);
        let (second_point, second_stream) = service_point(None, &scope, 250, 2);
        keep_point(&mut store, 500, assigned(5), second_point, second_stream);

        let window = TimeRangeFilter::new(100, 200);
        let logs = records(
            &store,
            &RecordsQuery::logs(LogFilters {
                service: None,
                time_range: Some(window),
                severity: None,
                scope: None,
            }),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let bodies: Vec<&str> = logs
            .items
            .iter()
            .map(|view| log_body_of(view).expect("a log page"))
            .collect();
        assert_eq!(
            bodies,
            vec!["observation-time-only"],
            "the record with no time key is outside; the event timestamp \
             outranks the in-window observation time"
        );

        let points = records(
            &store,
            &RecordsQuery::metric_points(MetricFilters {
                service: None,
                time_range: Some(window),
                scope: None,
            }),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let values: Vec<i64> = points
            .items
            .iter()
            .map(|view| point_value_of(view).expect("a point page"))
            .collect();
        assert_eq!(values, vec![1], "the point's time_unix_nano is its key");
    }

    /// The severity floor: logs at or above the threshold match, and a log
    /// with no severity number is outside a severity-filtered answer (F3).
    /// Severity for spans and metric points is not a runtime refusal — it
    /// is unexpressible at the type level (F2; asserted by construction in
    /// `filters.rs`, where the span and metric filter structs carry no
    /// severity field).
    ///
    /// Kills the no-op where an absent severity is treated as any value
    /// (`None >= threshold`) or where `severity_text` sneaks into the
    /// comparison.
    #[test]
    fn a_severity_floor_filters_logs_and_absent_severity_is_outside() {
        let scope = fixture_scope("scope", None);
        let mut store = FixtureStore::empty();
        let nine = SeverityNumber::try_new(9).expect("in domain");
        let seventeen = SeverityNumber::try_new(17).expect("in domain");
        keep_log(
            &mut store,
            100,
            assigned(1),
            service_log(None, &scope, None, None, Some(nine), "info"),
        );
        keep_log(
            &mut store,
            200,
            assigned(2),
            service_log(None, &scope, None, None, Some(seventeen), "error"),
        );
        keep_log(
            &mut store,
            300,
            assigned(3),
            service_log(None, &scope, None, None, None, "unnumbered"),
        );

        let at_floor = records(
            &store,
            &RecordsQuery::logs(LogFilters {
                service: None,
                time_range: None,
                severity: Some(SeverityFilter::new(seventeen)),
                scope: None,
            }),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let at_floor_bodies: Vec<&str> = at_floor
            .items
            .iter()
            .map(|view| log_body_of(view).expect("a log page"))
            .collect();
        assert_eq!(
            at_floor_bodies,
            vec!["error"],
            "at-threshold matches; below and absent do not"
        );

        let below_floor = records(
            &store,
            &RecordsQuery::logs(LogFilters {
                service: None,
                time_range: None,
                severity: Some(SeverityFilter::new(nine)),
                scope: None,
            }),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let bodies: Vec<&str> = below_floor
            .items
            .iter()
            .map(|view| log_body_of(view).expect("a log page"))
            .collect();
        assert_eq!(
            bodies,
            vec!["info", "error"],
            "both numbered logs at or above the floor, in residency order"
        );
    }

    /// The scope filter is exact name plus exact version, presence
    /// included (F3): the same name under a different version is a
    /// different scope, and an absent version never equals the empty
    /// string.
    ///
    /// Kills the no-op where the filter matches on name alone or where
    /// `None` means "any version".
    #[test]
    fn a_scope_filter_matches_name_and_version_exactly() {
        let mut store = FixtureStore::empty();
        let versioned = fixture_scope("io.runtime-trail.scope", Some("1.2.3"));
        let unversioned = fixture_scope("io.runtime-trail.scope", None);
        let other = fixture_scope("io.runtime-trail.other", Some("1.2.3"));
        keep_span(
            &mut store,
            100,
            span_entity(1, 1),
            service_span(1, 1, None, &versioned, 10),
        );
        keep_span(
            &mut store,
            200,
            span_entity(2, 2),
            service_span(2, 2, None, &unversioned, 10),
        );
        keep_span(
            &mut store,
            300,
            span_entity(3, 3),
            service_span(3, 3, None, &other, 10),
        );

        let exact = records(
            &store,
            &RecordsQuery::spans(SpanFilters {
                service: None,
                time_range: None,
                scope: Some(ScopeFilter::new(
                    "io.runtime-trail.scope",
                    Some("1.2.3".to_owned()),
                )),
            }),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let exact_entities: Vec<EntityId> = exact
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(exact_entities, vec![span_entity(1, 1)]);

        let absent = records(
            &store,
            &RecordsQuery::spans(SpanFilters {
                service: None,
                time_range: None,
                scope: Some(ScopeFilter::new("io.runtime-trail.scope", None)),
            }),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let absent_entities: Vec<EntityId> = absent
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            absent_entities,
            vec![span_entity(2, 2)],
            "an absent filter version matches only an absent record version"
        );
    }

    /// Invariant 3 under filters: a cursor minted under one filter set is
    /// an error under any other filter set — a different service, or a
    /// service filter where none was minted — and continues only under the
    /// filters that minted it (F5).
    ///
    /// Kills the no-op where the fingerprint covers the kind but not the
    /// filter fields, letting one answer set continue under another.
    #[test]
    fn a_cursor_continues_only_under_the_filters_that_minted_it() {
        let store = service_store();
        let first = records(
            &store,
            &checkout_query(),
            budget_with(1, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        let foreign_service = records(
            &store,
            &RecordsQuery::spans(SpanFilters {
                service: Some(ServiceFilter::new("payments")),
                time_range: None,
                scope: None,
            }),
            budget_with(1, 1 << 40, 10_000),
            Some(&cursor),
        )
        .expect_err("a different service is a different query");
        assert_eq!(
            foreign_service,
            QueryError::Cursor(CursorError::FingerprintMismatch)
        );

        let unfiltered = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            budget_with(1, 1 << 40, 10_000),
            Some(&cursor),
        )
        .expect_err("no filter set is also a different query");
        assert_eq!(
            unfiltered,
            QueryError::Cursor(CursorError::FingerprintMismatch)
        );

        let same = records(&store, &checkout_query(), open_budget(), Some(&cursor))
            .expect("the identical filter set continues");
        let entities: Vec<EntityId> = same
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(3, 3), span_entity(5, 5)],
            "the continuation carries the rest of the same answer set"
        );
    }

    /// A filtered set larger than one page walks to exhaustion through
    /// cursors — no matching record repeated, none lost, the order stable,
    /// and the filtered-out records never enter a page (invariants 2+8).
    ///
    /// Kills the no-op where the cursor lands on a filtered-out record and
    /// the next page restarts before it, repeating or losing records.
    #[test]
    fn pagination_under_filters_walks_to_exhaustion_without_repeat_or_loss() {
        let store = service_store();
        let query = checkout_query();
        let mut cursor: Option<Vec<u8>> = None;
        let mut seen: Vec<EntityId> = Vec::new();
        let mut continuations = 0;
        loop {
            let presented = cursor.is_some();
            let page = records(
                &store,
                &query,
                budget_with(2, 1 << 40, 10_000),
                cursor.as_deref(),
            )
            .expect("the query answers");
            seen.extend(
                page.items
                    .iter()
                    .map(|view| span_entity_of(view).expect("a span page")),
            );
            if let Some(next) = page.next_cursor.clone() {
                cursor = Some(next);
                continuations += 1;
                if presented {
                    assert!(
                        page.execution
                            .coverage
                            .entries
                            .iter()
                            .any(|entry| matches!(entry, CoverageEntry::SnapshotBoundary { .. })),
                        "every continuation names its snapshot boundary"
                    );
                }
            } else {
                assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
                break;
            }
            assert!(continuations < 10, "the walk terminates");
        }
        assert_eq!(continuations, 1, "five residents, three matches, two pages");
        assert_eq!(
            seen,
            vec![span_entity(1, 1), span_entity(3, 3), span_entity(5, 5)],
            "every matching record exactly once, in residency order"
        );
    }

    /// A filtered continuation stays within its first page's snapshot:
    /// matching records admitted after the minting frontier are outside
    /// every later page, and the boundary entry names the frontier (F9).
    ///
    /// Kills the no-op where a filter re-evaluates records the snapshot
    /// never held — newer matching records silently joining the page.
    #[test]
    fn records_admitted_after_the_frontier_stay_outside_a_filtered_continuation() {
        let mut store = service_store();
        let first = records(
            &store,
            &checkout_query(),
            budget_with(1, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");
        let payload = CursorPayload::decode(&cursor).expect("the engine's own cursor");
        assert_eq!(
            payload.snapshot(),
            AdmissionKey::new(at(500), span_entity(5, 5)),
            "the frontier is the minting page's pulled batch tail"
        );

        let scope = fixture_scope("scope", None);
        keep_span(
            &mut store,
            600,
            span_entity(6, 6),
            service_span(6, 6, Some("checkout"), &scope, 60),
        );

        let second = records(&store, &checkout_query(), open_budget(), Some(&cursor))
            .expect("the query answers");
        let entities: Vec<EntityId> = second
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(3, 3), span_entity(5, 5)],
            "the page walks exactly the snapshot — the newer matching record \
             stays outside"
        );
        assert_eq!(second.execution.parts, vec![PartOutcome::Complete]);
        assert_eq!(
            second.execution.coverage.entries,
            vec![CoverageEntry::SnapshotBoundary {
                admission: AdmissionKey::new(at(500), span_entity(5, 5)),
            }],
        );
        assert!(second.next_cursor.is_none());
    }

    /// An evicted anchor under filters is named as a gap exactly as wave
    /// 2 names it: the anchor is always a matching record (only matches
    /// mint cursors), and the gap's `before` is the first resident
    /// successor the walk examines — even when that successor is itself
    /// filtered out of the answer (F8, F4).
    ///
    /// Kills the no-op where the gap names the next *matching* record,
    /// hiding the residency position where the hole actually ends.
    #[test]
    fn an_evicted_anchor_under_filters_names_the_gap_and_walks_exact() {
        let mut store = service_store();
        let first = records(
            &store,
            &checkout_query(),
            budget_with(1, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");
        let payload = CursorPayload::decode(&cursor).expect("the engine's own cursor");
        assert_eq!(
            payload.last_entity(),
            span_entity(1, 1),
            "the anchor matches"
        );

        assert!(store.evict(span_entity(1, 1)), "the anchor was resident");

        let second = records(&store, &checkout_query(), open_budget(), Some(&cursor))
            .expect("the query answers");
        let entities: Vec<EntityId> = second
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(3, 3), span_entity(5, 5)],
            "the walk resumes exactly, skipping nothing matching that is resident"
        );
        assert_eq!(
            second.execution.coverage.entries,
            vec![
                CoverageEntry::SnapshotBoundary {
                    admission: AdmissionKey::new(at(500), span_entity(5, 5)),
                },
                CoverageEntry::EvictionGap {
                    after: span_entity(1, 1),
                    before: span_entity(2, 2),
                },
            ],
            "the gap names the evicted anchor and the first examined successor — \
             the filtered-out record where the hole ends"
        );
        assert!(second.next_cursor.is_none());
    }

    /// The byte ceiling under filters counts only matching records of the
    /// remainder: a filtered-out record is never evidence-charged while
    /// included and never counted into `omitted` when behind the wall (F6,
    /// F4).
    ///
    /// Kills both lies: counting filtered-out records into the omission,
    /// and charging their evidence on the way past.
    #[test]
    fn a_byte_ceiling_under_filters_counts_only_matching_omissions() {
        let scope = fixture_scope("scope", None);
        let mut store = FixtureStore::empty();
        let mut sizes = Vec::new();
        for index in 1_u8..=5_u8 {
            let service = match index {
                2 | 4 => "payments",
                _ => "checkout",
            };
            let name = match index {
                1 => "a".repeat(10),
                2 => "b".repeat(10),
                3 => "c".repeat(40),
                4 => "d".repeat(10),
                _ => "e".repeat(30),
            };
            let mut span = fixture_span(index, index, &name);
            span.resource = Arc::new(fixture_resource(Some(service)));
            span.scope = Arc::new(scope.clone());
            sizes.push(u64::try_from(span.accounted_size()).expect("sizes fit"));
            keep_span(
                &mut store,
                u64::from(index) * 100,
                span_entity(index, index),
                span,
            );
        }
        // The ceiling fits e1 and e3 with e2's evidence left over — but
        // e2 was never charged, so e5 is the wall — and stops one byte
        // short of it.
        let ceiling = sizes[0] + sizes[2] + sizes[1] - 1;
        let query = checkout_query();
        let budget = budget_with(1_000, ceiling, 10_000);
        let mut session = budget.admit(Instant::now());
        let page = walk_records(&store, &query, &mut session, None);

        let truncation = degraded_of(&page);
        assert_eq!(truncation.dimension, Dimension::Bytes);
        assert_eq!(
            truncation.omitted, 1,
            "only e5 — the matching record behind the wall; e4 is filtered, \
             not omitted"
        );
        let entities: Vec<EntityId> = page
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(entities, vec![span_entity(1, 1), span_entity(3, 3)]);
        assert_eq!(
            session.ledger().remaining_bytes(),
            ceiling - sizes[0] - sizes[2],
            "the filtered-out records' evidence was never charged"
        );
        let cursor = page.next_cursor.as_deref().expect("the page continues");
        let payload = CursorPayload::decode(cursor).expect("the engine's own cursor");
        assert_eq!(
            payload.last_entity(),
            span_entity(3, 3),
            "the cursor anchors at the last included record"
        );
        assert!(
            page.execution.coverage.entries.is_empty(),
            "a counted omission needs no coverage entry"
        );
    }

    /// F6's placement, pinned from behind the wall: a filtered-out record
    /// that sits *after* the byte wall — inside the counting region — is
    /// still examined and scan-charged, but is never counted into
    /// `omitted`. Only matching records of the remainder are.
    ///
    /// The store puts the wall at e2 (a matching checkout span that does
    /// not fit), drops e3 and e5 — payments, filtered — directly behind
    /// it, and resumes with a matching e4 that also does not fit. The
    /// count is e2 plus e4; e3 and e5 contribute nothing. This is the
    /// companion to the fixture above, whose filtered-out records all sit
    /// before the wall and so never enter the counting region: a walk
    /// that counted before matching would pass that fixture and lie only
    /// here.
    #[test]
    fn a_filtered_record_behind_the_byte_wall_is_examined_and_never_counted() {
        let scope = fixture_scope("scope", None);
        let mut store = FixtureStore::empty();
        let mut sizes = Vec::new();
        for index in 1_u8..=5_u8 {
            let service = match index {
                3 | 5 => "payments",
                _ => "checkout",
            };
            let name = match index {
                1 => "a".repeat(10),
                _ => format!("s{index:0>3}").repeat(20),
            };
            let mut span = fixture_span(index, index, &name);
            span.resource = Arc::new(fixture_resource(Some(service)));
            span.scope = Arc::new(scope.clone());
            sizes.push(u64::try_from(span.accounted_size()).expect("sizes fit"));
            keep_span(
                &mut store,
                u64::from(index) * 100,
                span_entity(index, index),
                span,
            );
        }
        // The ceiling fits e1 exactly, so e2 — matching, and not fitting —
        // is the wall that opens the counting region. e3 and e5 are
        // filtered out inside that region; e4 is the second matching
        // record that does not fit.
        let ceiling = sizes[0];
        let query = checkout_query();
        let budget = budget_with(1_000, ceiling, 5);
        let mut session = budget.admit(Instant::now());
        let page = walk_records(&store, &query, &mut session, None);

        let truncation = degraded_of(&page);
        assert_eq!(truncation.dimension, Dimension::Bytes);
        assert_eq!(
            truncation.omitted, 2,
            "only e2 and e4 — the matching records behind the wall; the \
             filtered-out e3 and e5 are never counted"
        );
        let entities: Vec<EntityId> = page
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(entities, vec![span_entity(1, 1)]);
        assert_eq!(
            session.ledger().remaining_bytes(),
            0,
            "nothing behind the wall was evidence-charged — counted \
             records are examined, never charged"
        );
        assert_eq!(
            session.ledger().remaining_scan(),
            0,
            "all five residents were examined — the filtered-out e3 and \
             e5 were scan-charged like the rest"
        );
        let cursor = page.next_cursor.as_deref().expect("the page continues");
        let payload = CursorPayload::decode(cursor).expect("the engine's own cursor");
        assert_eq!(
            payload.last_entity(),
            span_entity(1, 1),
            "the cursor anchors at the last included record — e1, the only \
             record that fit"
        );
    }

    /// A dead continuation under filters refuses exactly like one without
    /// filters: the deadline was spent before the page examined anything,
    /// so no cursor of its own was arrived at and the presented cursor is a
    /// fabricated position — the same honest shape a deadline-dead first
    /// page produces, filters or none (invariant 6). Only matches mint
    /// cursors (F7), and under a dead deadline no match is ever examined,
    /// so there is nothing to anchor at.
    ///
    /// Kills the no-op where a filtered walk moves the caller without
    /// returning anything — and the echo loop where a dead continuation
    /// returned a cursor identical to its input.
    #[test]
    fn a_dead_continuation_under_filters_refuses_at_the_dead_deadline() {
        let store = service_store();
        let first = records(
            &store,
            &checkout_query(),
            budget_with(1, 1 << 40, 10_000),
            None,
        )
        .expect("the query answers");
        let cursor = first.next_cursor.expect("a truncated page continues");

        let second = records(
            &store,
            &checkout_query(),
            QueryBudget::new(Duration::ZERO, 1_000, 1 << 40, 10_000, 4_096),
            Some(&cursor),
        )
        .expect("the query answers");

        assert!(second.items.is_empty());
        assert!(
            second.next_cursor.is_none(),
            "a refused continuation offers no cursor — an echo would be a \
             fabricated position, indistinguishable from progress"
        );
        let [PartOutcome::Refused(refusal)] = &second.execution.parts[..] else {
            panic!("a dead continuation refuses, not degrades");
        };
        assert_eq!(refusal.dimension, Dimension::Deadline);
        assert_eq!(
            refusal.limit,
            Magnitude::Duration(Duration::ZERO),
            "the admitted deadline was zero"
        );
        assert!(
            matches!(refusal.observed, Magnitude::Duration(_)),
            "the observed spend is a duration"
        );
        assert_eq!(
            second.execution.coverage.entries,
            vec![CoverageEntry::SnapshotBoundary {
                admission: AdmissionKey::new(at(500), span_entity(5, 5)),
            }],
            "the continuation still names its snapshot boundary"
        );
    }

    /// A scan-ceiling stop under filters anchors at the last examined
    /// record — which under a filter is here a filtered-out one — and the
    /// continuation stays lossless: it resumes strictly after that
    /// residency position and returns exactly the matching rest (F4, F7).
    ///
    /// The ceiling of two examines e1 (checkout, included) and e2
    /// (payments, filtered out) and refuses e3's charge: the cursor
    /// anchors at e2, the record the walk actually stopped at. Anchoring
    /// at the last *included* record instead would make every page
    /// re-examine the filtered-out span between the anchors; anchoring at
    /// or past the refused record would lose it.
    #[test]
    fn a_scan_stop_under_filters_anchors_at_the_last_examined_and_stays_lossless() {
        let store = service_store();
        let query = checkout_query();
        let budget = budget_with(1_000, 1 << 40, 2);
        let mut session = budget.admit(Instant::now());
        let page = walk_records(&store, &query, &mut session, None);

        assert_eq!(
            page.items.len(),
            1,
            "only e1 was examined and included before the ceiling expired"
        );
        let truncation = degraded_of(&page);
        assert_eq!(truncation.dimension, Dimension::Scan);
        let cursor = page.next_cursor.as_deref().expect("the page continues");
        let payload = CursorPayload::decode(cursor).expect("the engine's own cursor");
        assert_eq!(
            payload.last_entity(),
            span_entity(2, 2),
            "the anchor is the last examined record — filtered out, but where \
             the walk actually stopped"
        );
        assert_eq!(session.ledger().remaining_scan(), 0);

        let second =
            records(&store, &query, open_budget(), Some(cursor)).expect("the query answers");
        let entities: Vec<EntityId> = second
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(3, 3), span_entity(5, 5)],
            "the continuation resumes after the stop and loses nothing"
        );
    }

    /// Invariant 8 under filters: the same resident set, the same filtered
    /// query and the same budget assemble identical content parts across
    /// two runs and two independently built stores — the minted cursor
    /// included.
    ///
    /// Kills the no-op where filter evaluation leaks run facts (a clock
    /// read, an iteration order) into the page.
    #[test]
    fn identical_filtered_queries_assemble_identical_pages() {
        fn run(store: &FixtureStore) -> Page<RecordView> {
            records(
                store,
                &checkout_query(),
                budget_with(2, 1 << 40, 10_000),
                None,
            )
            .expect("the query answers")
        }

        let first_store = service_store();
        let second_store = service_store();
        let first = run(&first_store);
        let second = run(&first_store);
        let third = run(&second_store);
        assert_eq!(first, second, "two runs on one store assemble one page");
        assert_eq!(first, third, "a second, identically built store too");

        let entities: Vec<EntityId> = first
            .items
            .iter()
            .map(|view| span_entity_of(view).expect("a span page"))
            .collect();
        assert_eq!(
            entities,
            vec![span_entity(1, 1), span_entity(3, 3)],
            "the page carries the first two matches in residency order"
        );
        let cursor = first
            .next_cursor
            .as_deref()
            .expect("a truncated page continues");
        let payload = CursorPayload::decode(cursor).expect("the engine's own cursor");
        assert_eq!(
            payload.last_entity(),
            span_entity(3, 3),
            "the minted cursor anchors at the last included record"
        );
    }

    /// An all-`None` filter set is byte-identical to a kind-only query
    /// (F2): the same canonical bytes, the same fingerprint, the same
    /// pages — items, continuation and execution — under both a complete
    /// and a truncating budget, for every kind.
    ///
    /// Kills the no-op where the empty filter set perturbs the encoding or
    /// the walk, breaking every wave-2 byte.
    #[test]
    fn all_none_filters_are_byte_identical_to_kind_only_queries() {
        let scope = fixture_scope("scope", None);
        let mut store = FixtureStore::empty();
        keep_span(
            &mut store,
            100,
            span_entity(1, 1),
            service_span(1, 1, Some("checkout"), &scope, 10),
        );
        keep_span(
            &mut store,
            200,
            span_entity(2, 2),
            service_span(2, 2, None, &scope, 10),
        );
        keep_log(
            &mut store,
            300,
            assigned(3),
            service_log(
                Some("checkout"),
                &scope,
                Some(50),
                Some(60),
                Some(SeverityNumber::try_new(9).expect("in domain")),
                "log-one",
            ),
        );
        let (point, stream) = service_point(None, &scope, 70, 1);
        keep_point(&mut store, 400, assigned(4), point, stream);

        let plain = [
            RecordsQuery::new(SignalKind::Spans),
            RecordsQuery::new(SignalKind::LogRecords),
            RecordsQuery::new(SignalKind::MetricPoints),
        ];
        let emptied = [
            RecordsQuery::spans(SpanFilters::all_none()),
            RecordsQuery::logs(LogFilters::all_none()),
            RecordsQuery::metric_points(MetricFilters::all_none()),
        ];
        for (plain, emptied) in plain.iter().zip(emptied.iter()) {
            assert_eq!(
                plain.canonical_bytes(),
                emptied.canonical_bytes(),
                "the all-None encoding is the kind-only encoding"
            );
            assert_eq!(plain, emptied, "the queries are equal");
            for results in [1_000_u64, 1] {
                let budget = budget_with(results, 1 << 40, 10_000);
                let plain_page =
                    records(&store, plain, budget_with(results, 1 << 40, 10_000), None)
                        .expect("the query answers");
                let emptied_page =
                    records(&store, emptied, budget, None).expect("the query answers");
                assert_eq!(
                    plain_page, emptied_page,
                    "identical pages under a {results}-result budget"
                );
            }
        }
    }

    /// Filters are predicates, never an order (F4): a page whose matching
    /// records' timestamps deliberately run *against* the residency order
    /// comes back in residency order — no re-sort by the filter key, and
    /// the log's observation-time fallback participates in a timestamp
    /// scheme the emitter never sorted.
    ///
    /// Kills the no-op where filtering re-orders the page (or where the
    /// fallback key is consulted only when it happens to be ascending).
    #[test]
    fn a_time_filtered_page_stays_in_scan_order_not_time_order() {
        let scope = fixture_scope("scope", None);
        let mut store = FixtureStore::empty();
        // Scan order 100..400; the time keys descend: 900, 700, 500, 300.
        keep_log(
            &mut store,
            100,
            assigned(1),
            service_log(None, &scope, None, Some(900), None, "latest"),
        );
        keep_log(
            &mut store,
            200,
            assigned(2),
            service_log(None, &scope, Some(700), Some(950), None, "second"),
        );
        keep_log(
            &mut store,
            300,
            assigned(3),
            service_log(None, &scope, Some(500), Some(800), None, "third"),
        );
        keep_log(
            &mut store,
            400,
            assigned(4),
            service_log(None, &scope, Some(300), Some(310), None, "earliest"),
        );

        let page = records(
            &store,
            &RecordsQuery::logs(LogFilters {
                service: None,
                time_range: Some(TimeRangeFilter::new(400, 1_000)),
                severity: None,
                scope: None,
            }),
            open_budget(),
            None,
        )
        .expect("the query answers");
        let bodies: Vec<&str> = page
            .items
            .iter()
            .map(|view| log_body_of(view).expect("a log page"))
            .collect();
        assert_eq!(
            bodies,
            vec!["latest", "second", "third"],
            "scan order, not time order — and the observation-time-only \
             record participates through its fallback key"
        );
        assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
        assert!(page.next_cursor.is_none());
    }

    /// Per-page budget re-admission: a continuation page is a NEW
    /// execution with the caller-set budget — page 2 does not inherit
    /// page 1's remaining allowance. Page 1 saturates its `max_results`
    /// of two and mints a cursor; page 2 continues that same
    /// fingerprint-valid cursor under its own budget, enforces its own
    /// `max_results` of one, and carries its own page-2 facts: a fresh
    /// `max_results` degradation and the snapshot boundary the
    /// continuation names.
    ///
    /// Kills the no-op where a continuation inherits the first page's
    /// remaining allowance instead of re-admitting the caller's budget
    /// (a second page then over-returns, or under-enforces its own
    /// limits).
    #[test]
    fn a_continuation_page_readmits_the_callers_budget() {
        let store = five_spans();
        let query = RecordsQuery::new(SignalKind::Spans);

        let first = records(&store, &query, budget_with(2, 1 << 40, 10_000), None)
            .expect("the query answers");
        assert_eq!(
            first.items.len(),
            2,
            "page 1 saturates its own max_results of two"
        );
        let [PartOutcome::Degraded { truncation }] = &first.execution.parts[..] else {
            panic!("page 1 degrades under its own max_results");
        };
        assert_eq!(truncation.dimension, Dimension::Results);
        let cursor = first.next_cursor.expect("a saturated page continues");

        let second = records(
            &store,
            &query,
            budget_with(1, 1 << 40, 10_000),
            Some(&cursor),
        )
        .expect("the fingerprint-valid cursor continues");
        assert_eq!(
            second.items.len(),
            1,
            "page 2 re-admits the caller's budget: its own max_results \
             of one bounds the page, not page 1's remaining allowance"
        );
        let [PartOutcome::Degraded { truncation }] = &second.execution.parts[..] else {
            panic!("page 2 degrades under its own max_results");
        };
        assert_eq!(truncation.dimension, Dimension::Results);
        assert_eq!(
            truncation.omitted, 0,
            "a results cut leaves nothing uncounted"
        );
        assert!(
            matches!(truncation.position, TruncationPoint::Cursor(_)),
            "page 2's truncation names its own continuation cursor"
        );
        assert!(
            second.next_cursor.is_some(),
            "page 2 continues under its own budget"
        );
        assert_eq!(
            second.execution.coverage.entries,
            vec![CoverageEntry::SnapshotBoundary {
                admission: AdmissionKey::new(at(500), span_entity(5, 5)),
            }],
            "page 2's coverage is its own page-2 fact: the boundary the \
             first page minted, named again by the continuation"
        );

        // The full walk under per-page budgets: every page bounds itself
        assert_eq!(
            second
                .items
                .iter()
                .map(|view| span_entity_of(view).expect("a span page"))
                .collect::<Vec<_>>(),
            vec![span_entity(3, 3)],
            "page 2 returns its one entity under its own max_results"
        );
        // The full walk under per-page budgets: every page bounds itself
        // and the chain stays lossless — five records, none repeated.
        let mut entities = vec![span_entity(1, 1), span_entity(2, 2), span_entity(3, 3)];
        let mut cursor = second.next_cursor;
        while let Some(next) = cursor {
            let page = records(&store, &query, budget_with(1, 1 << 40, 10_000), Some(&next))
                .expect("the query answers");
            entities.extend(
                page.items
                    .iter()
                    .map(|view| span_entity_of(view).expect("a span page")),
            );
            cursor = page.next_cursor;
        }
        assert_eq!(
            entities,
            vec![
                span_entity(1, 1),
                span_entity(2, 2),
                span_entity(3, 3),
                span_entity(4, 4),
                span_entity(5, 5),
            ],
            "per-page budgets page the same set to exhaustion without \
             repeat or loss"
        );
    }

    /// How a TEST-ONLY, contract-violating span scan misbehaves.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum StallPolicy {
        /// Every span scan yields an empty page with a successor cursor.
        Always,
        /// The first span scan serves real records; every later scan
        /// stalls (a walk stopped mid-set).
        AfterFirst,
        /// The first span scan stalls; every later scan serves real
        /// records (a one-page boundary blip).
        FirstOnly,
    }

    /// A TEST-ONLY, contract-violating store: its span scan yields empty
    /// pages while claiming a successor cursor, per [`StallPolicy`]. A
    /// conforming driver can never do this — an empty page carries no
    /// cursor — so the engine's suite needs a deliberate violator to
    /// prove the guard. Defined in test code, never in lib code: the
    /// engine's seam is the [`TelemetryStore`] trait, and only a
    /// nonconforming impl can exercise the stall.
    struct StallingStore {
        inner: FixtureStore,
        /// Span scans served so far (interior mutability: scans are
        /// `&self`, and the store must stay `Sync`).
        served: AtomicUsize,
        /// The successor cursor every stalled page claims.
        stall_cursor: AdmissionKey,
        /// How the span scan misbehaves.
        policy: StallPolicy,
    }

    impl StallingStore {
        fn new(inner: FixtureStore, policy: StallPolicy) -> Self {
            Self {
                inner,
                served: AtomicUsize::new(0),
                stall_cursor: AdmissionKey::new(at(101 * 100), span_entity(101, 101)),
                policy,
            }
        }

        /// With a chosen successor cursor for stalled pages (the blip
        /// test resumes real scans from it).
        fn with_cursor(mut self, stall_cursor: AdmissionKey) -> Self {
            self.stall_cursor = stall_cursor;
            self
        }
    }

    impl TelemetryStore for StallingStore {
        fn keep_span(&mut self, admitted: Admitted<Arc<Span>>) -> KeepOutcome {
            self.inner.keep_span(admitted)
        }

        fn keep_log_record(&mut self, admitted: Admitted<Arc<LogRecord>>) -> KeepOutcome {
            self.inner.keep_log_record(admitted)
        }

        fn keep_metric_point(
            &mut self,
            admitted: Admitted<Arc<MetricPoint>>,
            stream: Arc<StreamIdentity>,
        ) -> KeepOutcome {
            self.inner.keep_metric_point(admitted, stream)
        }

        fn span(&self, entity: EntityId) -> Option<Arc<Span>> {
            self.inner.span(entity)
        }

        fn log_record(&self, entity: EntityId) -> Option<Arc<LogRecord>> {
            self.inner.log_record(entity)
        }

        fn metric_point(&self, entity: EntityId) -> Option<PointView> {
            self.inner.metric_point(entity)
        }

        fn scan_spans(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<Span>> {
            let served = self.served.load(Ordering::Relaxed);
            self.served.store(served + 1, Ordering::Relaxed);
            let stalls = match self.policy {
                StallPolicy::Always => true,
                StallPolicy::AfterFirst => served >= 1,
                StallPolicy::FirstOnly => served == 0,
            };
            if stalls {
                return ScanPage {
                    items: Vec::new(),
                    cursor: Some(self.stall_cursor),
                };
            }
            self.inner.scan_spans(after, limit)
        }

        fn scan_log_records(
            &self,
            after: Option<AdmissionKey>,
            limit: usize,
        ) -> ScanPage<Arc<LogRecord>> {
            self.inner.scan_log_records(after, limit)
        }

        fn scan_metric_points(
            &self,
            after: Option<AdmissionKey>,
            limit: usize,
        ) -> ScanPage<PointView> {
            self.inner.scan_metric_points(after, limit)
        }

        fn enforce_retention(&mut self, now: AdmissionTime) -> u64 {
            self.inner.enforce_retention(now)
        }

        fn observe_admission_anomalies(&mut self, total: u64) {
            self.inner.observe_admission_anomalies(total);
        }

        fn stats(&self) -> StoreStats {
            self.inner.stats()
        }

        fn mode_name(&self) -> &'static str {
            "test-stalling"
        }
    }

    /// The empty-continuation guard: a TEST-ONLY driver that yields an
    /// empty page while claiming a successor cursor must not spin the
    /// engine. The walk stops paging after `EMPTY_PAGE_STALL_LIMIT`
    /// consecutive empty continuations, presents what was collected,
    /// mints no further cursor, and names the stall honestly in
    /// coverage — `DriverStall` at the last examined record and a
    /// `Stalled` part outcome.
    #[test]
    fn a_driver_stall_ends_the_walk_with_honest_coverage() {
        let store = StallingStore::new(span_store(100), StallPolicy::AfterFirst);
        let page = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            None,
        )
        .expect("the query answers");

        assert_eq!(
            page.items.len(),
            64,
            "the one real batch is collected before the stall"
        );
        assert!(
            page.next_cursor.is_none(),
            "a stalled walk mints no further cursor"
        );
        assert_eq!(page.execution.parts, vec![PartOutcome::Stalled]);
        assert_eq!(
            page.execution.coverage.entries,
            vec![CoverageEntry::DriverStall {
                after: span_entity(64, 64),
            }],
            "the stall names the last record the walk examined"
        );
    }

    /// A stall from the very first pull — nothing examined yet — still
    /// names the stall: the driver's own successor-cursor anchor is the
    /// position, and the page is empty with a `Stalled` outcome.
    #[test]
    fn a_driver_stall_before_any_examination_names_its_cursor_anchor() {
        let store = StallingStore::new(span_store(5), StallPolicy::Always);
        let page = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            None,
        )
        .expect("the query answers");

        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
        assert_eq!(page.execution.parts, vec![PartOutcome::Stalled]);
        assert_eq!(
            page.execution.coverage.entries,
            vec![CoverageEntry::DriverStall {
                after: span_entity(101, 101),
            }],
            "with nothing examined, the stall names the driver's cursor \
             anchor"
        );
    }

    /// The guard counts *consecutive* empty continuations: a lone empty
    /// page with a successor cursor (a boundary blip) resumes the walk
    /// from that cursor, completes normally, and names no stall.
    #[test]
    fn one_empty_continuation_is_absorbed_without_stalling() {
        // The blip claims "more records after (0, span 0)" — before the
        // first real record — so the resumed walk still yields the whole
        // set: a blip that points past data would be the driver's own
        // data loss, which the engine cannot see either way.
        let store = StallingStore::new(span_store(100), StallPolicy::FirstOnly)
            .with_cursor(AdmissionKey::new(at(0), span_entity(0, 0)));
        let page = records(
            &store,
            &RecordsQuery::new(SignalKind::Spans),
            open_budget(),
            None,
        )
        .expect("the query answers");

        assert_eq!(
            page.items.len(),
            100,
            "the whole set is collected after the absorbed blip"
        );
        assert_eq!(page.execution.parts, vec![PartOutcome::Complete]);
        assert!(
            page.execution
                .coverage
                .entries
                .iter()
                .all(|entry| !matches!(entry, CoverageEntry::DriverStall { .. })),
            "a one-page blip is not a stall"
        );
        assert!(page.next_cursor.is_none());
    }
}
