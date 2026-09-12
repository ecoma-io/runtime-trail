//! OTLP ingestion: the one door telemetry bytes come in through.
//!
//! The pipeline per export is: decode (prost, OTLP types checked in under
//! `src/otlp/`) → translate wire → model, refusing what no model record can
//! carry → admit through the
//! [`AdmissionLedger`](runtime_trail_telemetry_model::AdmissionLedger)
//! (shape law, information budgets, duplicate-delivery semantics) → hand
//! each newly admitted record to a bounded queue accounted in bytes.
//! Overload is a designed-for state: the queue's overflow **rejects the
//! producer** — the one retryable signal — and nothing ever buffers
//! without a bound or drops to make room (`docs/architecture/
//! runtime-constraints.md`, "The backpressure architecture").
//!
//! Ingestion writes *down* through the hand-off port
//! ([`RecordSink`]), never to a concrete driver, and never sideways: no
//! transport here (the HTTP/gRPC surface is the server's, Phase 1 wave 3 —
//! ADR 0001 keeps tokio and axum out of this crate), no imports from query,
//! correlation or storage ([`layer-ingest`]). Wave 3 wires storage behind
//! the *consumer* side of the queue; this crate's port does not change
//! when that happens.
//!
//! [`layer-ingest`]: ../../docs/architecture/boundaries.md
//!
//! # Memory path
//!
//! Translation moves — strings, vectors and values are taken out of the
//! decoded wire message, never copied — and admitted records are shared
//! through the ledger's `Arc`s, so queueing a record costs a reference
//! count. The decoded wire request is dropped whole when the ingest call
//! returns; what lives on is the model record, accounted at the model's
//! accounted size wherever it is referenced.
//!
//! # Honesty
//!
//! Every refusal names its reason, positions in an export are the numbers
//! `partial_success` will name, and nothing is ever silently dropped,
//! truncated or coerced (ADR 0006). The crate implements exactly what
//! `docs/roadmap/phases.md` has started: OTLP decoding and admission — no
//! transport, no storage wiring.

pub(crate) mod decode;
pub(crate) mod otlp;
pub mod pipeline;
pub mod queue;
pub mod signal;

pub use pipeline::{ExportOutcome, Pipeline};
pub use queue::{
    BoundedQueue, PIPELINE_QUEUE_NAME, QUEUE_CEILING_BYTES, QueuedRecord, RecordSink, StoredRecord,
};
pub use signal::{AdmissionSignal, RecordOutcome, RecordRejection, Unrepresentable};

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }
}
