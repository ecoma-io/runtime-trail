//! Budget admission: the five dimensions every query admits with.
//!
//! A query without a budget is invalid - the budget is a required value,
//! never a default ([query-model.md](../../docs/architecture/query-model.md),
//! invariant 1). This module owns admission against a budget: the
//! at-admission monotonic deadline capture, the per-dimension allowances
//! and their expiry semantics.
//!
//! [`QueryBudget`] carries the contract's five dimensions and cannot be
//! built without all five: there is deliberately no `Default` and no
//! argument-free constructor, because a budgetless query is invalid, not
//! "unbudgeted" — the missing default is the point.
//!
//! [`QueryBudget::admit`] consumes the budget into a [`BudgetSession`]
//! that pins the monotonic reading the deadline works against
//! ("Deadlines"): a wall-clock deadline would let a paused VM eat the
//! budget invisibly, so the remaining time is always the admitted
//! duration less the time elapsed since the instant admission captured.
//!
//! The session is the engine's one entry point per work shape: a
//! traversal-shaped ask degrades through an allowance
//! ([`ScanAllowance`](crate::spend::ScanAllowance)), an aggregation-shaped
//! charge refuses, and both are checked against the remaining deadline
//! first — expiry follows the shape of the work in flight ("Every query
//! carries a budget", "Refuse or degrade").

use std::time::{Duration, Instant};

use crate::result::{BudgetRefusal, Dimension, Magnitude};
use crate::spend::{ScanAllowance, SpendLedger};

/// The budget every query admits with: the contract's five dimensions.
///
/// **Owner: [query-model.md](../../docs/architecture/query-model.md),
/// "Every query carries a budget."** Every field is required — the type
/// has no `Default` and no argument-free constructor, because a
/// budgetless query is invalid (invariant 1). A caller that has not
/// chosen its ceilings has not thought about what its question may cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryBudget {
    deadline: Duration,
    max_results: u64,
    max_bytes: u64,
    max_scan: u64,
    max_aggregation_memory: u64,
}

impl QueryBudget {
    /// A budget with all five ceilings chosen. There is no other way to
    /// build one, and no default: choosing a budget is choosing what the
    /// question may cost (invariant 1).
    #[must_use]
    pub const fn new(
        deadline: Duration,
        max_results: u64,
        max_bytes: u64,
        max_scan: u64,
        max_aggregation_memory: u64,
    ) -> Self {
        Self {
            deadline,
            max_results,
            max_bytes,
            max_scan,
            max_aggregation_memory,
        }
    }

    /// The `deadline` dimension: the monotonic duration the engine works
    /// against, captured at admission (see [`BudgetSession`]).
    #[must_use]
    pub const fn deadline(&self) -> Duration {
        self.deadline
    }

    /// The `max_results` ceiling: entities returned.
    #[must_use]
    pub const fn max_results(&self) -> u64 {
        self.max_results
    }

    /// The `max_bytes` ceiling: the canonical encoding of the answer's
    /// evidence. The envelope's execution, coverage and limits parts sit
    /// outside it ("Every query carries a budget").
    #[must_use]
    pub const fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// The `max_scan` ceiling: entities examined, one per scan unit
    /// ("Scan work is driver-symmetric").
    #[must_use]
    pub const fn max_scan(&self) -> u64 {
        self.max_scan
    }

    /// The `max_aggregation_memory` ceiling: memory an aggregation may
    /// hold. Expiry refuses; it never spills to disk (invariant 7).
    #[must_use]
    pub const fn max_aggregation_memory(&self) -> u64 {
        self.max_aggregation_memory
    }

    /// Admits the query: consumes the budget into a session that measures
    /// the deadline against `at`, the monotonic reading captured at
    /// admission ("Deadlines").
    ///
    /// The session is the budget's only spend path — the engine never
    /// works the ceilings except through it (invariant 1: the engine is
    /// the budget's only enforcer).
    #[must_use]
    pub fn admit(self, at: Instant) -> BudgetSession {
        BudgetSession {
            budget: self,
            admitted_at: at,
            ledger: SpendLedger::new(
                self.max_results,
                self.max_bytes,
                self.max_scan,
                self.max_aggregation_memory,
            ),
        }
    }
}

/// One admitted query's budget in flight: the at-admission deadline
/// capture plus the [`SpendLedger`] the four finite dimensions spend
/// against.
///
/// Constructed only by [`QueryBudget::admit`]. Not `Clone`: a session is
/// one query's spend, and a copy would be a second, unaccounted spend of
/// the same budget.
#[derive(Debug)]
pub struct BudgetSession {
    budget: QueryBudget,
    admitted_at: Instant,
    ledger: SpendLedger,
}

impl BudgetSession {
    /// Time left before the deadline: the admitted duration less what has
    /// elapsed since the admission capture, floored at zero. A paused VM
    /// burns the remainder visibly — never invisibly ("Deadlines").
    #[must_use]
    pub fn deadline_remaining(&self, now: Instant) -> Duration {
        self.budget
            .deadline()
            .saturating_sub(now.saturating_duration_since(self.admitted_at))
    }

    /// Whether the deadline dimension has expired at `now`.
    #[must_use]
    pub fn deadline_exhausted(&self, now: Instant) -> bool {
        self.deadline_remaining(now).is_zero()
    }

    /// The budget the query admitted with, ceilings included — the
    /// envelope's limits block reads them from here.
    #[must_use]
    pub const fn budget(&self) -> QueryBudget {
        self.budget
    }

    /// A read-only handle on the spend ledger, for honest reads of what
    /// remains. Spending happens only through the shape methods below.
    #[must_use]
    pub const fn ledger(&self) -> &SpendLedger {
        &self.ledger
    }

    /// Traversal-shaped charge against `max_results`, deadline-checked:
    /// an expired deadline grants nothing and degrades with the deadline
    /// refusal; otherwise the ask proceeds through the ledger's allowance.
    pub fn allow_results(&mut self, now: Instant, want: u64) -> ScanAllowance {
        if self.deadline_exhausted(now) {
            return ScanAllowance::Partial {
                granted: 0,
                refusal: self.deadline_refusal(now),
            };
        }
        self.ledger.allow_results(want)
    }

    /// Traversal-shaped charge against `max_bytes` — the answer's
    /// evidence encoding — deadline-checked like [`Self::allow_results`].
    pub fn allow_bytes(&mut self, now: Instant, want: u64) -> ScanAllowance {
        if self.deadline_exhausted(now) {
            return ScanAllowance::Partial {
                granted: 0,
                refusal: self.deadline_refusal(now),
            };
        }
        self.ledger.allow_bytes(want)
    }

    /// Traversal-shaped charge against `max_scan`, deadline-checked.
    pub fn allow_scan(&mut self, now: Instant, want: u64) -> ScanAllowance {
        if self.deadline_exhausted(now) {
            return ScanAllowance::Partial {
                granted: 0,
                refusal: self.deadline_refusal(now),
            };
        }
        self.ledger.allow_scan(want)
    }

    /// Aggregation-shaped charge against `max_aggregation_memory`,
    /// deadline first: an expired deadline refuses the charge outright —
    /// aggregation-shaped work never proceeds past its deadline, and it
    /// never degrades.
    ///
    /// # Errors
    ///
    /// Returns the [`BudgetRefusal`] naming whichever dimension expired
    /// first: the deadline (with the elapsed time as the observed spend)
    /// or the memory ceiling.
    pub fn charge_aggregation_memory(
        &mut self,
        now: Instant,
        bytes: u64,
    ) -> Result<(), BudgetRefusal> {
        if self.deadline_exhausted(now) {
            return Err(self.deadline_refusal(now));
        }
        self.ledger.charge_aggregation_memory(bytes)
    }

    /// The strict scan charge for aggregation-shaped work, deadline
    /// first, exactly like [`Self::charge_aggregation_memory`].
    ///
    /// # Errors
    ///
    /// Returns the [`BudgetRefusal`] naming whichever dimension expired
    /// first: the deadline (with the elapsed time as the observed spend)
    /// or the scan ceiling.
    pub fn charge_scan_strict(&mut self, now: Instant, entities: u64) -> Result<(), BudgetRefusal> {
        if self.deadline_exhausted(now) {
            return Err(self.deadline_refusal(now));
        }
        self.ledger.charge_scan_strict(entities)
    }

    /// The deadline refusal: the dimension is time, the limit is the
    /// admitted duration, the observed spend is what the work burned
    /// between admission and this check.
    fn deadline_refusal(&self, now: Instant) -> BudgetRefusal {
        BudgetRefusal {
            dimension: Dimension::Deadline,
            limit: Magnitude::Duration(self.budget.deadline()),
            observed: Magnitude::Duration(now.saturating_duration_since(self.admitted_at)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    /// A budget with distinct ceilings, so a mixed-up dimension shows in
    /// the numbers: deadline 10 ms, results 10, bytes 512, scan 100,
    /// aggregation memory 4096.
    fn budget() -> QueryBudget {
        QueryBudget::new(Duration::from_millis(10), 10, 512, 100, 4_096)
    }

    /// The deadline is captured at admission: two sessions of the same
    /// budget admitted 5 ms apart report exactly 5 ms apart at the same
    /// reading, and one session's remainder shrinks as the reading moves.
    ///
    /// Kills the wall-clock / re-anchored no-op — a remainder that ignores
    /// the admission instant (a constant, or time re-read per call)
    /// reports the same number for both sessions and for both readings.
    #[test]
    fn admission_captures_the_deadline_and_remaining_is_measured_from_it() {
        let budget = budget();
        let earlier = Instant::now();
        let later = earlier + Duration::from_millis(5);
        let first = budget.admit(earlier);
        let second = budget.admit(later);

        let reading = later + Duration::from_millis(1);
        let first_remaining = first.deadline_remaining(reading);
        let second_remaining = second.deadline_remaining(reading);
        assert_eq!(first_remaining, Duration::from_millis(4));
        assert_eq!(second_remaining, Duration::from_millis(9));
        assert_eq!(
            second_remaining.checked_sub(first_remaining),
            Some(Duration::from_millis(5))
        );
        assert!(!first.deadline_exhausted(reading));
        assert_eq!(
            first.deadline_remaining(reading + Duration::from_millis(2)),
            Duration::from_millis(2)
        );
    }

    /// A session's refusals name dimension, limit and observed spend in
    /// the dimension's own magnitude (invariant 6): the deadline speaks
    /// durations, with the observed spend the time actually burned since
    /// admission.
    ///
    /// Kills the no-op where a deadline refusal borrows a count magnitude
    /// (`Units`/`Bytes`), or hard-codes its observed spend to the limit.
    #[test]
    fn deadline_refusals_name_the_dimension_limit_and_elapsed_spend() {
        let budget = budget();
        let admitted_at = Instant::now();
        let mut session = budget.admit(admitted_at);
        let late = admitted_at + Duration::from_millis(25);

        let ScanAllowance::Partial { granted, refusal } = session.allow_results(late, 5) else {
            panic!("work past the deadline must not be granted");
        };
        assert_eq!(granted, 0);
        assert_eq!(refusal.dimension, Dimension::Deadline);
        assert_eq!(
            refusal.limit,
            Magnitude::Duration(Duration::from_millis(10))
        );
        assert_eq!(
            refusal.observed,
            Magnitude::Duration(Duration::from_millis(25))
        );
        assert_eq!(session.ledger().remaining_results(), 10);

        let Err(refusal) = session.charge_aggregation_memory(late, 1) else {
            panic!("aggregation past the deadline must refuse");
        };
        assert_eq!(refusal.dimension, Dimension::Deadline);
        assert_eq!(
            refusal.limit,
            Magnitude::Duration(Duration::from_millis(10))
        );
        assert_eq!(
            refusal.observed,
            Magnitude::Duration(Duration::from_millis(25))
        );
        assert_eq!(session.ledger().remaining_aggregation_memory(), 4_096);
    }

    /// A real 10 ms deadline really expires after 30 ms of wall time on
    /// the monotonic clock, and the remainder reads zero — not a
    /// constant.
    ///
    /// Kills the never-expires no-op: a deadline check that always answers
    /// "time remains" would leave both assertions about `now` failing.
    #[test]
    fn a_real_deadline_expires_after_real_elapsed_time() {
        let budget = budget();
        let admitted_at = Instant::now();
        let session = budget.admit(admitted_at);
        assert_eq!(
            session.deadline_remaining(admitted_at),
            Duration::from_millis(10)
        );
        assert!(!session.deadline_exhausted(admitted_at));

        thread::sleep(Duration::from_millis(30));
        let now = Instant::now();
        assert!(session.deadline_exhausted(now));
        assert_eq!(session.deadline_remaining(now), Duration::ZERO);
    }

    /// Expiry names whichever dimension expired first: a session whose
    /// deadline lives but whose ledger is spent refuses strictly on the
    /// ledger's dimension, and a session whose deadline died refuses on
    /// the deadline in every shape, spending nothing.
    ///
    /// Kills the no-op where the session checks only one dimension, or
    /// lets the deadline and ledger refusals blur into one.
    #[test]
    fn expiry_names_whichever_dimension_expired_first() {
        let budget = budget();
        let admitted_at = Instant::now();

        let mut spent = budget.admit(admitted_at);
        assert_eq!(
            spent.allow_scan(admitted_at, 100),
            ScanAllowance::Exact(100)
        );
        let Err(refusal) = spent.charge_scan_strict(admitted_at, 1) else {
            panic!("a strict charge past the spent scan ceiling must refuse");
        };
        assert_eq!(refusal.dimension, Dimension::Scan);
        assert_eq!(refusal.observed, Magnitude::Units(100));

        let late = admitted_at + Duration::from_millis(11);
        let mut expired = budget.admit(admitted_at);
        let Err(refusal) = expired.charge_scan_strict(late, 1) else {
            panic!("a strict charge past the deadline must refuse");
        };
        assert_eq!(refusal.dimension, Dimension::Deadline);
        let Err(refusal) = expired.charge_aggregation_memory(late, 1) else {
            panic!("a memory charge past the deadline must refuse");
        };
        assert_eq!(refusal.dimension, Dimension::Deadline);
        let ScanAllowance::Partial { granted, refusal } = expired.allow_bytes(late, 1) else {
            panic!("a byte ask past the deadline must degrade to nothing");
        };
        assert_eq!(granted, 0);
        assert_eq!(refusal.dimension, Dimension::Deadline);
        assert_eq!(expired.ledger().remaining_bytes(), 512);
    }
}
