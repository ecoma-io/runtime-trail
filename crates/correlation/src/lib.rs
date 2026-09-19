//! The Correlation Engine: cross-signal identity and adjacency.
//!
//! Which log records belong to which trace, which metric windows contain
//! which traces, which spans share a request — every correlation strategy
//! lives here and nowhere else ([`layer-correlation`]). The contract is
//! `docs/architecture/correlation-model.md`; the dependency law is
//! `docs/architecture/boundaries.md`.
//!
//! [`layer-correlation`]: ../../docs/architecture/boundaries.md
//!
//! # Committed strategies
//!
//! * **`SpanIdentity`** — a log record attaches to the exact span its
//!   trace context names.
//! * **`TraceIdentity`** — a log record attaches to the resident spans of
//!   its trace (suppressed in the exact relation's favor when the exact
//!   span is resident; a trace named by a log but absent from the resident
//!   set is accounted as completeness coverage, never invented into a
//!   relation).
//! * **`ParentChild`** — a span attaches to the span its `parent_span_id`
//!   names, when that span is resident.
//! * **`ResourceContext`** — records that share one resource identity,
//!   evidenced by the shared resource's own attributes.
//! * **`ExemplarAttachment`** — a metric data point attaches to the span
//!   an exemplar's trace context names, when that span is resident.
//! * **`TemporalCoActivity`** — the subject's spans and the data points
//!   co-active in a caller-supplied window.
//!
//! The committed set is machine-readable ([`bounds::COMMITTED_STRATEGIES`])
//! and versioned ([`bounds::STRATEGY_SET_VERSION`]) so the Investigation
//! flow can pin the strategy set an investigation ran under. `Inferred`
//! remains a reserved type with zero instances: no strategy emits it.

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The bounds a run is produced under, and the truth it reports.
pub mod bounds;
/// The engine: runs the committed strategies over a store.
pub mod engine;
/// The correlation taxonomy: relation types, tiers, strategies, evidence.
pub mod relations;

/// The engine's entry point.
pub use engine::correlate;

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    #[test]
    fn depends_on_the_telemetry_model() {
        assert!(!runtime_trail_telemetry_model::VERSION.is_empty());
    }
}
