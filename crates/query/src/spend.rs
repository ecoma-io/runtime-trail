//! The spend ledger: per-dimension spend tracking and the
//! refuse-or-degrade policy's arithmetic.
//!
//! Traversal degrades truthfully (a subset of a set answer is still a true
//! set answer); aggregation refuses (a partial aggregate is a false
//! number) - the choice is pinned per dimension in
//! [query-model.md](../../docs/architecture/query-model.md); this module
//! owns the arithmetic that enforces the pinning.
