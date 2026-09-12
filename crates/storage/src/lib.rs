//! The Storage Abstraction: the contract for keeping telemetry and answering
//! the query engine against it.
//!
//! This crate owns the *contract only* — it knows the telemetry model and
//! nothing else in this repository ([`layer-storage`]): no concrete backend,
//! no UI type, no wire format, no admission-ledger type (the eviction
//! lifecycle reaches the ledger through an inverted hook instead —
//! [`EvictionHook`]). Concrete modes live behind it, one crate each
//! (`storage-memory`, `storage-sqlite`); only the composition root may name
//! a driver (ADR 0003). The rules this contract honours are
//! [`docs/architecture/storage-model.md`](../../docs/architecture/storage-model.md);
//! the dependency law is
//! [`docs/architecture/boundaries.md`](../../docs/architecture/boundaries.md).
//!
//! [`layer-storage`]: ../../docs/architecture/boundaries.md
//!
//! # Storage stays storage
//!
//! The contract's retrieval surface is exactly three shapes, and every one
//! of them is a *location* fact, not a *question*:
//!
//! - **get by entity id** — one resident record, by the id admission gave
//!   it;
//! - **ordered scans** — the resident set in residency order, cursor
//!   continued ([`AdmissionKey`], [`ScanPage`]);
//! - **count and size introspection** — [`StoreStats`].
//!
//! There is no find-the-slow-trace, no related-logs, no service-error
//! search, no time-window analytics, no relation building here. Anything
//! that asks a question about the data is query or correlation work
//! (`docs/architecture/investigation-model.md`), reading these primitives
//! through the abstraction — never a method on a store.
//!
//! # What a store promises
//!
//! - **Bounded retention** in every mode: the ceilings of
//!   `docs/architecture/runtime-constraints.md`, enforced by the store,
//!   evicting the oldest record — oldest by admission time, never emitter
//!   event time — until every ceiling is satisfied again. First ceiling hit
//!   wins; the cause is counted per record ([`EvictionCause`],
//!   [`StoreStats`]).
//! - **The admitted-then-kept pipeline**: keeps are plain calls that never
//!   wait on durable I/O; a slow disk degrades durability, never the hot
//!   path.
//! - **One deterministic order** ([`AdmissionKey`]): eviction order and
//!   scan order are the same sequence — admission time, entity id as
//!   tie-break.
//! - **Identity ends with residency** (ADR 0008): the store's removal hook
//!   fires per evicted record — and once more per stream whose last
//!   resident point that record was — so the composition root can drop the
//!   record's ledger identity and the stream's interning; a re-delivery
//!   afterwards is admitted fresh.
//! - **The byte ceiling counts what residency pins**: every distinct
//!   resident stream's identity accounted size is charged to the ceiling
//!   exactly once — added with the stream's first resident point, released
//!   with its last — so single-point streams cannot park identity content
//!   under a ceiling that only saw the points
//!   ([telemetry-model.md](../../docs/architecture/telemetry-model.md),
//!   [storage-model.md](../../docs/architecture/storage-model.md)).
//!
//! # The shape of the contract
//!
//! - [`store`] — [`TelemetryStore`], the trait every mode implements, and
//!   [`PointView`].
//! - [`keep`] — [`KeepOutcome`], [`EvictionCause`], and [`EvictionHook`],
//!   the inverted dependency that ends a record's ledger identity on
//!   eviction.
//! - [`order`] — [`AdmissionKey`] and [`ScanPage`], the residency order
//!   eviction and scans share.
//! - [`stats`] — [`StoreStats`], the observability counters.

pub mod keep;
pub mod order;
pub mod stats;
pub mod store;

pub use keep::{EvictionCause, EvictionHook, KeepOutcome};
pub use order::{AdmissionKey, ScanPage};
pub use stats::StoreStats;
pub use store::{PointView, TelemetryStore};

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The bootstrap binding surface, kept from the foundation commit.
///
/// This empty trait is what the Phase 3 scaffolding (`storage-sqlite`) and
/// the smoke surfaces still bind to, so the declared boundaries stay real
/// before their drivers exist. It is **not** the storage contract — the
/// contract is [`TelemetryStore`]. The placeholder disappears when the
/// file-backed driver lands against the real contract (Phase 3,
/// `docs/roadmap/phases.md`); nothing new may implement it.
pub trait StorageBackend {}

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    /// The declared dependency edge on the telemetry model is a real
    /// compile-time fact: the manifest names it and this test makes the
    /// compiler agree. Archkeep judges the same edge statically; this is the
    /// build's side of that proof.
    #[test]
    fn depends_on_the_telemetry_model() {
        assert!(!runtime_trail_telemetry_model::VERSION.is_empty());
    }

    /// The residency order is a contract fact drivers and scans share, so
    /// the ordering types are part of the public surface this crate owns.
    ///
    /// Compile-only, and the assertion is the point: building an
    /// [`AdmissionKey`] and a generic [`ScanPage`] over `EntityId` from the
    /// crate root is the proof that the residency-order surface composes
    /// for every driver and cursor. Asserting the constructor's own output
    /// back would be a tautology, so none is made.
    #[test]
    fn the_residency_order_types_compose_from_the_crate_root() {
        use std::num::NonZeroU64;

        use runtime_trail_telemetry_model::{AdmissionTime, AssignedId, EntityId};
        let entity = EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(1).expect("1 is nonzero"),
        ));
        let key = super::AdmissionKey::new(AdmissionTime::from_unix_nano(7), entity);
        let _page = super::ScanPage::<EntityId> {
            items: vec![entity],
            cursor: Some(key),
        };
    }

    /// The counters and the keep outcomes are what observability and the
    /// hand-off are typed by; both must stay value types a composition root
    /// can report without reaching into a driver. The behavior asserted is
    /// the outcome's own law: keeps report what the retention law removed,
    /// every refusal reports zero.
    #[test]
    fn keep_outcomes_report_their_evictions_and_refusals_report_none() {
        assert_eq!(super::StoreStats::default().total_evictions(), 0);
        assert_eq!(super::KeepOutcome::Kept { evicted: 3 }.evicted(), 3);
        assert_eq!(super::KeepOutcome::Kept { evicted: 0 }.evicted(), 0);
        assert_eq!(super::KeepOutcome::Oversized.evicted(), 0);
        assert_eq!(super::KeepOutcome::Duplicate.evicted(), 0);
        assert_eq!(
            super::KeepOutcome::SeriesCapReached.evicted(),
            0,
            "a series-cap refusal never evicts to make room"
        );
        assert_eq!(
            super::KeepOutcome::IdentityOverCeiling {
                ceiling: 1024,
                identity_bytes: 4096,
            }
            .evicted(),
            0,
            "an over-ceiling identity refusal never evicts to make room"
        );
    }
}
