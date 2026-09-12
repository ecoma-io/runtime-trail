//! The spend ledger: per-dimension spend tracking and the
//! refuse-or-degrade policy's arithmetic.
//!
//! Traversal degrades truthfully (a subset of a set answer is still a true
//! set answer); aggregation refuses (a partial aggregate is a false
//! number) - the choice is pinned per dimension in
//! [query-model.md](../../docs/architecture/query-model.md) ("Refuse or
//! degrade", and the expiry column of the budget table); this module owns
//! the arithmetic that enforces the pinning.
//!
//! [`SpendLedger`] tracks the remaining allowance of the four finite
//! dimensions (`max_results`, `max_bytes`, `max_scan`,
//! `max_aggregation_memory`). The deadline is not ledger arithmetic: it is
//! monotonic time, owned by the session
//! ([`BudgetSession`](crate::budget::BudgetSession)) that composes this
//! ledger with the at-admission capture. Two charge shapes, exactly per
//! the contract:
//!
//! - *Traversal-shaped* charges take an **allowance**: whatever fits is
//!   granted and the shortfall comes back as a truthful
//!   [`BudgetRefusal`] — degrade, never refuse work that could degrade.
//! - *Aggregation-shaped* charges are **strict**: all-or-nothing, and a
//!   refusal names dimension, limit and observed spend (invariant 6) in
//!   the dimension's own [`Magnitude`].
//!
//! Every refusal's `observed` is the dimension's total spend at the
//! moment it expired — the spend already on the books for a refused
//! charge, plus what a partial grant actually took.

use crate::result::{BudgetRefusal, Dimension, Magnitude};

/// What a traversal-shaped charge may take.
///
/// Traversal-shaped work degrades rather than refuses
/// ([query-model.md](../../docs/architecture/query-model.md), "Refuse or
/// degrade"): the caller proceeds with what was granted and names the
/// shortfall through the refusal. Zero is a legal grant — a dimension
/// with nothing left grants exactly nothing, and an ask of zero is never
/// refused.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanAllowance {
    /// The whole ask fits; the value is what was asked for and granted.
    Exact(u64),
    /// Only part of the ask fits: `granted` was taken from the dimension
    /// (draining it to zero) and `refusal` names the expired dimension,
    /// its limit, and the total spend when it expired.
    Partial {
        /// What the dimension actually granted.
        granted: u64,
        /// The truthful refusal for the `want - granted` shortfall.
        refusal: BudgetRefusal,
    },
}

/// One dimension's allowance: the caller's limit and what is left of it.
#[derive(Debug)]
struct Allowance {
    limit: u64,
    remaining: u64,
}

impl Allowance {
    const fn new(limit: u64) -> Self {
        Self {
            limit,
            remaining: limit,
        }
    }

    /// Traversal-shaped degrade: grant what fits of `want`, refuse the
    /// shortfall. A grant takes exactly what remains when the ask does
    /// not fit, so the dimension is drained to zero and the refusal's
    /// observed spend is exactly the limit.
    fn degrade(
        &mut self,
        dimension: Dimension,
        want: u64,
        magnitude: fn(u64) -> Magnitude,
    ) -> ScanAllowance {
        if want <= self.remaining {
            self.remaining -= want;
            return ScanAllowance::Exact(want);
        }
        let granted = self.remaining;
        self.remaining = 0;
        ScanAllowance::Partial {
            granted,
            refusal: BudgetRefusal {
                dimension,
                limit: magnitude(self.limit),
                observed: magnitude(self.limit - self.remaining),
            },
        }
    }

    /// Aggregation-shaped refuse: all-or-nothing, never a partial charge.
    ///
    /// Even a zero charge refuses on an exhausted dimension: an
    /// aggregation may not continue against an expired ceiling at all, so
    /// "charge nothing more" is still answered with the truthful refusal.
    fn charge_strict(
        &mut self,
        dimension: Dimension,
        charge: u64,
        magnitude: fn(u64) -> Magnitude,
    ) -> Result<(), BudgetRefusal> {
        if self.remaining == 0 || charge > self.remaining {
            return Err(BudgetRefusal {
                dimension,
                limit: magnitude(self.limit),
                observed: magnitude(self.limit - self.remaining),
            });
        }
        self.remaining -= charge;
        Ok(())
    }
}

/// The spend ledger: the remaining allowance of the four finite budget
/// dimensions, and the shape-pinned arithmetic charged against it.
///
/// Built from a budget's four finite dimensions; the deadline is time,
/// not ledger arithmetic, and lives with the session
/// ([`BudgetSession`](crate::budget::BudgetSession)). The engine reads
/// what remains and charges per shape: allowances for traversal
/// ([`ScanAllowance`]), strict charges for aggregation.
#[derive(Debug)]
pub struct SpendLedger {
    results: Allowance,
    bytes: Allowance,
    scan: Allowance,
    aggregation_memory: Allowance,
}

impl SpendLedger {
    /// A ledger fresh from a budget: every dimension's remaining
    /// allowance equals its limit.
    #[must_use]
    pub const fn new(
        max_results: u64,
        max_bytes: u64,
        max_scan: u64,
        max_aggregation_memory: u64,
    ) -> Self {
        Self {
            results: Allowance::new(max_results),
            bytes: Allowance::new(max_bytes),
            scan: Allowance::new(max_scan),
            aggregation_memory: Allowance::new(max_aggregation_memory),
        }
    }

    /// Traversal-shaped charge against `max_results`: grants what fits of
    /// `want` and degrades with a truthful refusal for the shortfall
    /// (query-model.md, budget table row `max_results`).
    pub fn allow_results(&mut self, want: u64) -> ScanAllowance {
        self.results
            .degrade(Dimension::Results, want, Magnitude::Units)
    }

    /// Traversal-shaped charge against `max_bytes` — the canonical
    /// encoding of the answer's evidence (budget table row `max_bytes`).
    pub fn allow_bytes(&mut self, want: u64) -> ScanAllowance {
        self.bytes.degrade(Dimension::Bytes, want, Magnitude::Bytes)
    }

    /// Traversal-shaped charge against `max_scan`, in scan units — one
    /// entity examined per unit, driver-symmetric ("Scan work is
    /// driver-symmetric").
    pub fn allow_scan(&mut self, want: u64) -> ScanAllowance {
        self.scan.degrade(Dimension::Scan, want, Magnitude::Units)
    }

    /// Aggregation-shaped charge against `max_aggregation_memory`.
    ///
    /// All-or-nothing: a partial aggregate would be a false number
    /// ("Refuse or degrade"), so the charge either fits entirely or is
    /// refused with the dimension, limit and observed spend named.
    ///
    /// # Errors
    ///
    /// Returns the [`BudgetRefusal`] when the charge does not fit —
    /// including a zero charge against an exhausted dimension.
    pub fn charge_aggregation_memory(&mut self, bytes: u64) -> Result<(), BudgetRefusal> {
        self.aggregation_memory
            .charge_strict(Dimension::AggregationMemory, bytes, Magnitude::Bytes)
    }

    /// The strict scan charge for aggregation-shaped work: the same
    /// all-or-nothing contract as
    /// [`Self::charge_aggregation_memory`], against `max_scan` in scan
    /// units (budget table row `max_scan`: aggregation refuses).
    ///
    /// # Errors
    ///
    /// Returns the [`BudgetRefusal`] when the charge does not fit —
    /// including a zero charge against an exhausted dimension.
    pub fn charge_scan_strict(&mut self, entities: u64) -> Result<(), BudgetRefusal> {
        self.scan
            .charge_strict(Dimension::Scan, entities, Magnitude::Units)
    }

    /// What is left of `max_results`.
    #[must_use]
    pub const fn remaining_results(&self) -> u64 {
        self.results.remaining
    }

    /// What is left of `max_bytes`.
    #[must_use]
    pub const fn remaining_bytes(&self) -> u64 {
        self.bytes.remaining
    }

    /// What is left of `max_scan`.
    #[must_use]
    pub const fn remaining_scan(&self) -> u64 {
        self.scan.remaining
    }

    /// What is left of `max_aggregation_memory`.
    #[must_use]
    pub const fn remaining_aggregation_memory(&self) -> u64 {
        self.aggregation_memory.remaining
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ledger with distinct limits, so a mixed-up dimension shows in the
    /// numbers: results 10, bytes 512, scan 100, aggregation memory 4096.
    fn ledger() -> SpendLedger {
        SpendLedger::new(10, 512, 100, 4_096)
    }

    /// A refusal names dimension, limit and observed spend in the
    /// dimension's own magnitude (invariant 6): scan speaks units, bytes
    /// speaks bytes, and `observed` is the spend on the books when the
    /// dimension expired — not the limit, not the ask.
    ///
    /// Kills the no-op where a refusal is built with the wrong dimension,
    /// the magnitudes swapped (`Units` for `Bytes`), or `observed`
    /// hard-coded to the limit or to the refused charge.
    #[test]
    fn refusals_name_dimension_limit_and_observed_in_the_right_magnitude() {
        let mut ledger = ledger();
        let Err(refusal) = ledger.charge_scan_strict(150) else {
            panic!("a scan charge past an untouched limit must refuse");
        };
        assert_eq!(refusal.dimension, Dimension::Scan);
        assert_eq!(refusal.limit, Magnitude::Units(100));
        assert_eq!(refusal.observed, Magnitude::Units(0));

        assert_eq!(ledger.charge_scan_strict(60), Ok(()));
        let Err(refusal) = ledger.charge_scan_strict(50) else {
            panic!("a scan charge past the remaining allowance must refuse");
        };
        assert_eq!(refusal.dimension, Dimension::Scan);
        assert_eq!(refusal.limit, Magnitude::Units(100));
        assert_eq!(refusal.observed, Magnitude::Units(60));

        assert_eq!(ledger.allow_bytes(512), ScanAllowance::Exact(512));
        let ScanAllowance::Partial { refusal, .. } = ledger.allow_bytes(1) else {
            panic!("a byte ask past the exhausted dimension must degrade");
        };
        assert_eq!(refusal.dimension, Dimension::Bytes);
        assert_eq!(refusal.limit, Magnitude::Bytes(512));
        assert_eq!(refusal.observed, Magnitude::Bytes(512));
    }

    /// A traversal ask past the remaining allowance grants exactly what
    /// remains — not the whole ask, not nothing — and the refusal names
    /// the drained dimension.
    ///
    /// Kills the no-op where the partial clamp is dropped (the whole ask
    /// granted, or zero granted always).
    #[test]
    fn traversal_partial_grants_take_exactly_the_remaining_allowance() {
        let mut ledger = ledger();
        assert_eq!(ledger.allow_results(4), ScanAllowance::Exact(4));
        assert_eq!(ledger.remaining_results(), 6);
        let ScanAllowance::Partial { granted, refusal } = ledger.allow_results(25) else {
            panic!("an ask past the remaining allowance must degrade");
        };
        assert_eq!(granted, 6);
        assert_eq!(refusal.dimension, Dimension::Results);
        assert_eq!(refusal.limit, Magnitude::Units(10));
        assert_eq!(refusal.observed, Magnitude::Units(10));
        assert_eq!(ledger.remaining_results(), 0);

        assert_eq!(ledger.allow_scan(15), ScanAllowance::Exact(15));
        let ScanAllowance::Partial { granted, .. } = ledger.allow_scan(1_000) else {
            panic!("an ask past the remaining allowance must degrade");
        };
        assert_eq!(granted, 85);
        assert_eq!(ledger.remaining_scan(), 0);
    }

    /// An aggregation-shaped charge past the remaining allowance refuses
    /// AND leaves the ledger untouched — never taking what fits and
    /// reporting success.
    ///
    /// Kills the degrade-aggregation no-op: the expiry table pins
    /// `max_aggregation_memory` and aggregation-shaped scan to refuse,
    /// never to partial-charge.
    #[test]
    fn aggregation_charges_refuse_and_leave_the_ledger_unchanged() {
        let mut ledger = ledger();
        assert_eq!(ledger.charge_aggregation_memory(4_000), Ok(()));
        assert_eq!(ledger.remaining_aggregation_memory(), 96);
        let Err(refusal) = ledger.charge_aggregation_memory(97) else {
            panic!("a memory charge past the remaining allowance must refuse");
        };
        assert_eq!(refusal.dimension, Dimension::AggregationMemory);
        assert_eq!(refusal.limit, Magnitude::Bytes(4_096));
        assert_eq!(refusal.observed, Magnitude::Bytes(4_000));
        assert_eq!(ledger.remaining_aggregation_memory(), 96);

        assert_eq!(ledger.charge_scan_strict(60), Ok(()));
        let Err(refusal) = ledger.charge_scan_strict(41) else {
            panic!("a strict scan charge past the remaining allowance must refuse");
        };
        assert_eq!(refusal.dimension, Dimension::Scan);
        assert_eq!(refusal.limit, Magnitude::Units(100));
        assert_eq!(refusal.observed, Magnitude::Units(60));
        assert_eq!(ledger.remaining_scan(), 40);
    }

    /// The same charge sequence against the same budget produces the same
    /// outcomes every run.
    ///
    /// Kills any environment- or order-dependent accounting: the ledger is
    /// plain integer arithmetic, and a replay of the same script must be
    /// byte-for-byte identical, twice over.
    #[test]
    fn identical_charge_sequences_produce_identical_outcomes() {
        fn run_script() -> (Vec<ScanAllowance>, Vec<Result<(), BudgetRefusal>>, [u64; 4]) {
            let mut ledger = SpendLedger::new(10, 512, 100, 4_096);
            let allowances = vec![
                ledger.allow_results(4),
                ledger.allow_bytes(400),
                ledger.allow_scan(60),
                ledger.allow_results(25),
                ledger.allow_bytes(200),
                ledger.allow_scan(1_000),
                ledger.allow_results(0),
            ];
            let charges = vec![
                ledger.charge_aggregation_memory(4_000),
                ledger.charge_scan_strict(20),
                ledger.charge_aggregation_memory(200),
                ledger.charge_scan_strict(1_000),
            ];
            let remaining = [
                ledger.remaining_results(),
                ledger.remaining_bytes(),
                ledger.remaining_scan(),
                ledger.remaining_aggregation_memory(),
            ];
            (allowances, charges, remaining)
        }

        assert_eq!(run_script(), run_script());
        assert_eq!(run_script(), run_script());
    }

    /// Zero asks stay truthful per shape: a traversal ask of zero on an
    /// exhausted dimension is granted as nothing, while an
    /// aggregation-shaped zero charge against an exhausted dimension
    /// refuses naming `observed == limit`.
    ///
    /// Kills the zero-ask early-out no-op (`if charge == 0 { succeed }`),
    /// which gets one of the two shapes wrong whichever way it is written.
    #[test]
    fn a_zero_ask_on_an_exhausted_dimension_is_truthful_per_shape() {
        let mut ledger = ledger();
        assert_eq!(ledger.allow_bytes(512), ScanAllowance::Exact(512));
        assert_eq!(ledger.remaining_bytes(), 0);
        assert_eq!(ledger.allow_bytes(0), ScanAllowance::Exact(0));
        assert_eq!(ledger.remaining_bytes(), 0);
        let ScanAllowance::Partial { granted, refusal } = ledger.allow_bytes(7) else {
            panic!("a positive ask past the exhausted dimension must degrade");
        };
        assert_eq!(granted, 0);
        assert_eq!(refusal.dimension, Dimension::Bytes);
        assert_eq!(refusal.limit, Magnitude::Bytes(512));
        assert_eq!(refusal.observed, Magnitude::Bytes(512));

        assert_eq!(ledger.charge_scan_strict(100), Ok(()));
        let Err(refusal) = ledger.charge_scan_strict(0) else {
            panic!("aggregation may not continue on an exhausted ceiling, even for zero");
        };
        assert_eq!(refusal.dimension, Dimension::Scan);
        assert_eq!(refusal.limit, Magnitude::Units(100));
        assert_eq!(refusal.observed, Magnitude::Units(100));
    }
}
