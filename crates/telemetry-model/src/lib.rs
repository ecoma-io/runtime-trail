//! The Telemetry Model: `runtime-trail`'s single, presentation-independent
//! representation of OpenTelemetry logs, traces and metrics.
//!
//! This crate is the leaf of the core ([`layer-model`] in the boundary law):
//! it depends on nothing else in this repository — std only — and nothing
//! above it may redefine what telemetry is. The contract these types
//! implement is `docs/architecture/telemetry-model.md`, rule by rule; the
//! dependency law is `docs/architecture/boundaries.md`; the budget numbers
//! are owned by `docs/architecture/runtime-constraints.md` and mirrored in
//! [`budgets::BudgetLimits`]; refusal-over-truncation is ADR 0006.
//!
//! [`layer-model`]: ../../docs/architecture/boundaries.md
//!
//! # The shape of the model
//!
//! - [`values`] — the value union and the attribute maps it is built from.
//! - [`context`] — trace ids, span ids, full-width flags, tracestate.
//! - [`spans`] — spans with their resource and scope, events, links,
//!   status and dropped counts.
//! - [`logs`] — log records: two timestamps, two severities, a full value
//!   body, three independent optional trace-context facts, an event name.
//! - [`metrics`] — metric streams and data points: five kinds, per-stream
//!   temporality, metadata, exemplars carrying two optional trace ids.
//! - [`resources`] — resource and scope identity (attribute maps only;
//!   `schema_url` is preserved metadata), `schema_url` at both levels.
//! - [`identity`] — entity ids, admission metadata, the admission outcome.
//! - [`ledger`] — the admission ledger: idempotent admission, collapse
//!   only where `OTel` defines identity, conflicts recorded, log records
//!   never collapsed, payloads shared through `Arc` (ADR 0008).
//! - [`size`] — accounted size, the single definition byte ceilings count.
//! - [`budgets`] — the budget taxonomy, the numbers behind it
//!   ([`budgets::BudgetLimits`]), and the admission-gate checks.
//!
//! # Immutability is procedural
//!
//! Admitted data is complete and immutable — a contract fact (ADR 0006).
//! It is held here by **procedure**, not by the type system: the types
//! above are plain value types with no interior mutability, and the
//! convention every consumer follows is that an admitted record is never
//! rewritten. Nothing in this crate mechanically prevents a consumer from
//! rebuilding a record with different fields; the ledger hands out
//! `Arc`-shared payloads so the *honest* path shares one allocation, and
//! the rule lives in the contract documents and in review, not in
//! `unsafe`-free but coercible types.
//!
//! No serialisation format is normative inside the model (contract rule 4):
//! OTLP is ingestion's wire format, translated _into_ these types at
//! admission, and nothing here carries a transport or storage dependency.

pub mod budgets;
pub mod context;
pub mod identity;
pub mod ledger;
pub mod logs;
pub mod metrics;
pub mod resources;
pub mod size;
pub mod spans;
pub mod values;

pub use budgets::{BudgetLimits, BudgetName, BudgetRejection};
pub use context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState, TraceStateEntry};
pub use identity::{AdmissionOutcome, AdmissionTime, Admitted, AssignedId, EntityId};
pub use ledger::{AdmissionAnomalies, AdmissionLedger, PointAdmission, SpanAdmission};
pub use logs::{LogRecord, SeverityNumber, SeverityOutOfRange};
pub use metrics::{
    DATA_POINT_FLAG_NO_RECORDED_VALUE, Exemplar, ExponentialBuckets, ExponentialHistogramPoint,
    HistogramPoint, MetricNumber, MetricPoint, MetricStream, NumberPoint, PointIdentity,
    PointShape, QuantileValue, StreamIdentity, StreamKind, StreamShapeError, Temporality,
};
pub use resources::{InstrumentationScope, Resource};
pub use size::{
    ATTRIBUTE_MAP_NODE_BYTES, Accounted, KEYED_ENTRY_BYTES, STRING_ALLOCATION_CHUNK_BYTES,
    STRUCTURE_FIXED_BYTES, attribute_entry_size, heap_string_bytes, slot_bytes,
};
pub use spans::{
    EmitterDroppedCounts, Span, SpanEvent, SpanKind, SpanLink, SpanStatus, SpanStatusCode,
};
pub use values::{
    Attributes, DuplicateKey, Float, HomogeneousArray, KeyValueList, MixedKindArray, Value,
    ValueKind,
};

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    #[test]
    fn the_model_kinds_are_reachable_from_the_crate_root() {
        // A smoke pass over the public surface: the types every other
        // layer builds on are exported and constructible without
        // reaching into private modules.
        assert_eq!(super::Value::Int(1), super::Value::Int(1));
        assert_ne!(super::SpanKind::Server, super::SpanKind::Client);
        assert_ne!(
            super::StreamKind::Sum { monotonic: true },
            super::StreamKind::Sum { monotonic: false }
        );
        assert_eq!(
            super::BudgetName::AttributeValueSize.slug(),
            "attribute_value_size"
        );
        let _id = super::EntityId::Assigned(super::AssignedId::from_serial(
            std::num::NonZeroU64::new(1).expect("1 is nonzero"),
        ));
    }

    #[test]
    fn the_budget_numbers_are_reachable_from_the_crate_root() {
        let limits = super::BudgetLimits::default();
        assert_eq!(limits.key_value_list_depth, 8);
        assert_eq!(super::KEYED_ENTRY_BYTES, 160);
        assert_eq!(super::ATTRIBUTE_MAP_NODE_BYTES, 768);
    }

    #[test]
    fn the_ledger_return_types_are_reachable_from_the_crate_root() {
        let ledger = super::AdmissionLedger::new(super::BudgetLimits::default());
        assert_eq!(ledger.anomalies().total(), 0);
        assert_eq!(ledger.limits().key_value_list_depth, 8);
    }
}
