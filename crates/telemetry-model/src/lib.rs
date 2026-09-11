//! The Telemetry Model: `runtime-trail`'s single, presentation-independent
//! representation of OpenTelemetry logs, traces and metrics.
//!
//! This crate is the leaf of the core ([`layer-model`] in the boundary law):
//! it depends on nothing else in this repository — std only — and nothing
//! above it may redefine what telemetry is. The contract these types
//! implement is `docs/architecture/telemetry-model.md`, rule by rule; the
//! dependency law is `docs/architecture/boundaries.md`; the budget numbers
//! are owned by `docs/architecture/runtime-constraints.md` and mirrored in
//! [`budgets::limits`]; refusal-over-truncation is ADR 0006.
//!
//! [`layer-model`]: ../../docs/architecture/boundaries.md
//!
//! # The shape of the model
//!
//! - [`values`] — the value union and the attribute maps it is built from.
//! - [`context`] — trace ids, span ids, flags, tracestate.
//! - [`spans`] — spans with events, links, status and dropped counts.
//! - [`logs`] — log records: two timestamps, two severities, a full value
//!   body, optional trace context.
//! - [`metrics`] — metric streams and data points: five kinds, per-stream
//!   temporality, exemplars.
//! - [`resources`] — resource and scope identity, `schema_url` at both
//!   levels.
//! - [`identity`] — entity ids, admission metadata, the admission outcome.
//! - [`ledger`] — the admission ledger: idempotent admission, collapse
//!   only where `OTel` defines identity, conflicts recorded, log records
//!   never collapsed.
//! - [`size`] — accounted size, the single definition byte ceilings count.
//! - [`budgets`] — the budget taxonomy, the numbers behind it, and the
//!   admission-gate checks.
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

pub use budgets::{BudgetName, BudgetRejection};
pub use context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState, TraceStateEntry};
pub use identity::{AdmissionOutcome, AdmissionTime, Admitted, AssignedId, EntityId};
pub use ledger::{AdmissionAnomalies, AdmissionLedger};
pub use logs::{LogRecord, SeverityNumber, SeverityOutOfRange};
pub use metrics::{
    Exemplar, ExponentialBuckets, ExponentialHistogramPoint, HistogramPoint, MetricNumber,
    MetricPoint, MetricStream, NumberPoint, PointIdentity, PointShape, QuantileValue,
    StreamIdentity, StreamKind, StreamShapeError, Temporality,
};
pub use resources::{InstrumentationScope, Resource};
pub use size::{Accounted, STRUCTURE_FIXED_BYTES, attribute_entry_size};
pub use spans::{
    EmitterDroppedCounts, Span, SpanEvent, SpanKind, SpanLink, SpanStatus, SpanStatusCode,
};
pub use values::{
    Attributes, Float, HomogeneousArray, KeyValueList, MixedKindArray, PrimitiveKind,
    PrimitiveValue, Value,
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
}
