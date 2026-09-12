//! Budget admission: the five dimensions every query admits with.
//!
//! A query without a budget is invalid - the budget is a required value,
//! never a default ([query-model.md](../../docs/architecture/query-model.md),
//! invariant 1). This module owns admission against a budget: the
//! at-admission monotonic deadline capture, the per-dimension allowances
//! and their expiry semantics.
