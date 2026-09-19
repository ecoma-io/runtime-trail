//! Snapshot leases: the per-investigation pin on a cursor continuation.
//!
//! A query's continuation is bound to the snapshot its first page minted
//! (the admission key the engine names as
//! [`CoverageEntry::SnapshotBoundary`](crate::result::CoverageEntry::SnapshotBoundary)).
//! An investigation that pages a continuation presents a **snapshot
//! lease** with every page: a token binding the investigation's identity
//! to the fingerprint of that minted boundary, minted by the compose flow
//! from the first page's boundary — never from the cursor bytes, which are
//! opaque by contract
//! ([query-model.md](../../docs/architecture/query-model.md),
//! "Snapshot leases").
//!
//! The engine's leased entry ([`crate::engine::records_leased`]) rejects
//! any page whose continuation's snapshot fingerprint differs from the
//! lease's, so a lease from one investigation can never page another
//! investigation's snapshot, and a cursor cannot be replayed under a
//! foreign lease.
//!
//! A lease is deliberately **not** `Clone`: one investigation holds one
//! pin on one snapshot, and a copy would be a second, unaccounted claim on
//! the same boundary. The [`assert_not_impl_any`](crate::probe) pin in the
//! tests below keeps it that way.

use std::num::NonZeroU64;

use crate::AdmissionKey;
use crate::cursor;

/// An investigation's identity, as the lease binds it to a snapshot.
///
/// A newtype over a nonzero serial, echoing the model's identity law —
/// investigation serials are never zero — so an unassigned id cannot
/// exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvestigationId(NonZeroU64);

impl InvestigationId {
    /// Wraps a nonzero serial.
    #[must_use]
    pub const fn new(serial: NonZeroU64) -> Self {
        Self(serial)
    }

    /// The wrapped serial.
    #[must_use]
    pub const fn get(self) -> NonZeroU64 {
        self.0
    }
}

/// The fingerprint of one snapshot boundary.
///
/// Computed over the same canonical bytes the cursor embeds for its
/// snapshot (["Snapshot leases" in query-model.md](../../docs/architecture/query-model.md)),
/// so a lease minted from a boundary speaks the identical bytes
/// [`CursorPayload::verify_lease`](crate::cursor::CursorPayload::verify_lease)
/// reads back from a cursor's own snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotFingerprint(u64);

impl SnapshotFingerprint {
    /// Wraps a raw fingerprint value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The fingerprint of `boundary`: FNV-1a over the boundary's canonical
    /// bytes, exactly as the cursor machinery encodes a snapshot.
    #[must_use]
    pub fn of(boundary: AdmissionKey) -> Self {
        Self(cursor::fingerprint(&cursor::boundary_bytes(boundary)))
    }

    /// The wrapped value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A snapshot lease: one investigation's pin on one snapshot.
///
/// Created by the investigation compose flow with the boundary its first
/// page minted ([`SnapshotLease::for_boundary`]); presented with every
/// cursor page of the same continuation; accepted by the engine only while
/// the page's continuation continues within the leased snapshot.
///
/// Deliberately not `Clone`: one lease, one pin.
#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotLease {
    investigation: InvestigationId,
    snapshot: SnapshotFingerprint,
}

impl SnapshotLease {
    /// Binds `investigation` to `snapshot`.
    ///
    /// Prefer [`SnapshotLease::for_boundary`]: the compose flow meets the
    /// snapshot as an admission key, and the fingerprint must always come
    /// from the canonical boundary bytes.
    #[must_use]
    pub const fn new(investigation: InvestigationId, snapshot: SnapshotFingerprint) -> Self {
        Self {
            investigation,
            snapshot,
        }
    }

    /// Mints the lease the compose flow presents: over the fingerprint of
    /// `boundary`, the snapshot its first page minted.
    #[must_use]
    pub fn for_boundary(investigation: InvestigationId, boundary: AdmissionKey) -> Self {
        Self::new(investigation, SnapshotFingerprint::of(boundary))
    }

    /// The investigation the lease belongs to.
    #[must_use]
    pub const fn investigation(&self) -> InvestigationId {
        self.investigation
    }

    /// The snapshot the lease pins.
    #[must_use]
    pub const fn snapshot(&self) -> SnapshotFingerprint {
        self.snapshot
    }

    /// Whether this lease governs continuations within `boundary` — the
    /// one test every leased page applies.
    ///
    /// A lease governs exactly the boundary its fingerprint was minted
    /// over; any other boundary (another investigation's snapshot, a
    /// snapshot minted before or after this one) fails.
    #[must_use]
    pub fn governs(&self, boundary: AdmissionKey) -> bool {
        self.snapshot == SnapshotFingerprint::of(boundary)
    }
}

#[cfg(test)]
mod tests {
    use runtime_trail_telemetry_model::{AdmissionTime, AssignedId, EntityId, SpanId, TraceId};
    use std::num::NonZeroU64;

    use super::*;
    use crate::cursor::{CursorError, CursorPayload};
    use crate::probe::assert_not_impl_any;

    fn inv(serial: u64) -> InvestigationId {
        InvestigationId::new(NonZeroU64::new(serial).expect("fixture investigations are nonzero"))
    }

    fn span_entity(trace: [u8; 16], span: [u8; 8]) -> EntityId {
        EntityId::Span {
            trace_id: TraceId::from_bytes(trace),
            span_id: SpanId::from_bytes(span),
        }
    }

    fn assigned(serial: u64) -> EntityId {
        EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(serial).expect("test serials are nonzero"),
        ))
    }

    /// A cursor whose snapshot is the boundary at (300, span [2;16]/[2;8])
    /// — the same fixture the cursor tests use, so the lease assertions
    /// speak to a payload the encode/decode machinery already pins.
    fn span_span_payload() -> CursorPayload {
        CursorPayload::new(
            1,
            span_entity([1; 16], [1; 8]),
            42,
            AdmissionKey::new(
                AdmissionTime::from_unix_nano(300),
                span_entity([2; 16], [2; 8]),
            ),
        )
    }

    #[test]
    fn snapshot_fingerprints_distinguish_boundaries_and_agree_with_the_cursor() {
        let boundary_a = AdmissionKey::new(
            AdmissionTime::from_unix_nano(300),
            span_entity([2; 16], [2; 8]),
        );
        let boundary_b = AdmissionKey::new(
            AdmissionTime::from_unix_nano(301),
            span_entity([2; 16], [2; 8]),
        );
        let boundary_c = AdmissionKey::new(
            AdmissionTime::from_unix_nano(300),
            span_entity([2; 16], [3; 8]),
        );
        let boundary_d = AdmissionKey::new(AdmissionTime::from_unix_nano(300), assigned(9));

        let fa = SnapshotFingerprint::of(boundary_a);
        assert_ne!(
            fa,
            SnapshotFingerprint::of(boundary_b),
            "a later snapshot has its own fingerprint"
        );
        assert_ne!(
            fa,
            SnapshotFingerprint::of(boundary_c),
            "a different entity has its own fingerprint"
        );
        assert_ne!(
            fa,
            SnapshotFingerprint::of(boundary_d),
            "a different entity kind has its own fingerprint"
        );
        assert_eq!(
            fa,
            SnapshotFingerprint::of(boundary_a),
            "the fingerprint is deterministic"
        );

        // A cursor that embeds the boundary as its snapshot speaks the
        // same bytes a lease minted from the boundary does.
        let payload = span_span_payload();
        assert_eq!(payload.snapshot(), boundary_a, "fixture boundary");
        assert_eq!(SnapshotFingerprint::of(payload.snapshot()), fa);
        let lease = SnapshotLease::for_boundary(inv(1), payload.snapshot());
        assert_eq!(lease.snapshot(), fa);
        assert_eq!(payload.verify_lease(&lease), Ok(()));
    }

    #[test]
    fn a_lease_is_one_investigations_pin_not_a_negotiable_token() {
        // The consume-once authority, pinned the same way the budget pins
        // itself: a lease that could be cloned or copied would be a second
        // claim on the same snapshot.
        assert_not_impl_any!(SnapshotLease: Clone, Copy);

        let boundary = AdmissionKey::new(
            AdmissionTime::from_unix_nano(300),
            span_entity([2; 16], [2; 8]),
        );
        let lease = SnapshotLease::for_boundary(inv(9), boundary);

        assert_eq!(lease.investigation(), inv(9));
        assert!(lease.governs(boundary), "a lease governs its own boundary");
        assert!(
            !lease.governs(AdmissionKey::new(
                AdmissionTime::from_unix_nano(301),
                span_entity([2; 16], [2; 8])
            )),
            "a lease does not govern a later snapshot"
        );
        assert!(
            !lease.governs(AdmissionKey::new(
                AdmissionTime::from_unix_nano(300),
                span_entity([2; 16], [3; 8])
            )),
            "a lease does not govern another entity's snapshot"
        );

        // The cursor side agrees: verify_lease accepts the lease minted
        // from the payload's own boundary and refuses a lease minted
        // elsewhere — even under the same investigation.
        let payload = span_span_payload();
        let foreign = SnapshotLease::for_boundary(
            inv(9),
            AdmissionKey::new(
                AdmissionTime::from_unix_nano(900),
                span_entity([9; 16], [9; 8]),
            ),
        );
        assert_eq!(payload.verify_lease(&lease), Ok(()));
        assert_eq!(
            payload.verify_lease(&foreign),
            Err(CursorError::LeaseMismatch),
            "a lease over another boundary cannot page this cursor"
        );
    }
}
