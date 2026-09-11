//! OTLP Ingestion: receiving OpenTelemetry traces, logs and metrics.
//!
//! Ingestion writes *down*: it decodes OTLP into the telemetry model and
//! hands it to the active storage backend through the abstraction — never to
//! a concrete driver ([`layer-ingest`]). Overload is a designed-for state:
//! bounded queues, admission control and backpressure toward emitters, and
//! never a block on persistence. Those rules live in
//! `docs/architecture/storage-model.md` and
//! `docs/architecture/runtime-constraints.md`; the dependency law is
//! `docs/architecture/boundaries.md`.
//!
//! [`layer-ingest`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! Scaffolding only — no OTLP receiver exists yet. Ingestion lands with
//! Phase 1, per `docs/roadmap/phases.md`; until then nothing here may
//! pretend to receive telemetry.

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    #[test]
    fn depends_on_the_storage_contract_and_the_model() {
        assert!(!runtime_trail_storage::VERSION.is_empty());
        assert!(!runtime_trail_telemetry_model::VERSION.is_empty());
    }
}
