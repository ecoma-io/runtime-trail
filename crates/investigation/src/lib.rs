//! The Investigation API: the single contract both surfaces consume.
//!
//! The Loom UI (over HTTP, via the server) and the MCP server (in-process)
//! are two equal clients of this crate and of nothing else in the core —
//! `layer-api` depends on the query and correlation engines and the
//! telemetry model, and never on storage drivers, ingestion, or any
//! presentation or distribution surface. The contract is
//! `docs/architecture/investigation-model.md`; the dependency law is
//! `docs/architecture/boundaries.md`; the two-surface decision is
//! `docs/decisions/0002-investigation-first-architecture.md`.
//!
//! [`layer-api`]: ../../docs/architecture/boundaries.md
//!
//! # Status
//!
//! The envelope is implemented (Phase 2, M3 — issue #12): every answer is
//! one [`Investigation`] envelope with five parts — subject, execution,
//! correlated, evidence, limits — and the seven invariants of
//! `docs/architecture/investigation-model.md` are mechanically checkable
//! through [`envelope::invariants`]. The committed flow — the trace
//! investigation composing the query engine — is implemented in the
//! [`flow`] module: given a root span and a caller budget, it composes the
//! engine's pages into one envelope, decomposing the budget into fresh
//! per-page engine budgets and reporting the chain-level limits it owns.
//! The correlation engine's committed strategies — span identity, trace
//! identity, temporal co-activity — are wired into the flow (issue #28):
//! the envelope's `correlated` part is populated from the engine's run
//! under the caller's budget, narrowed to evidence-resident endpoints.
//!

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod correlated;
pub mod envelope;
pub mod evidence;
pub mod execution;
pub mod flow;
pub mod limits;
pub mod subject;

pub use correlated::Correlated;
pub use envelope::{FIELD_PATHS, Investigation};
pub use evidence::Evidence;
pub use execution::Execution;
pub use flow::{
    ChainBudget, FlowError, InvestigationBudget, TraceInvestigationRequest, investigate_trace,
    investigate_trace_bounded,
};
pub use limits::Limits;
pub use subject::Subject;

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    #[test]
    fn depends_on_the_engines_and_the_model() {
        assert!(!runtime_trail_query::VERSION.is_empty());
        assert!(!runtime_trail_correlation::VERSION.is_empty());
        assert!(!runtime_trail_telemetry_model::VERSION.is_empty());
    }
}
