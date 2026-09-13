//! Opaque, snapshot-bound, fingerprint-bound cursors.
//!
//! A cursor encodes a quadruple — (position in the total order, last
//! entity id, query fingerprint, snapshot boundary) — is opaque to
//! callers, and is rejected when presented under a different query
//! ([query-model.md](../../docs/architecture/query-model.md),
//! "Ordering, cursors and pagination", invariant 3). The snapshot
//! boundary is the first page's residency frontier, an [`AdmissionKey`]
//! in the storage contract's residency order: the engine bounds every
//! continuation at it and names it via `CoverageEntry::SnapshotBoundary`.
//! A cursor continues within the snapshot its first page evaluated;
//! records admitted after the frontier are outside every later page — a
//! declared boundary in coverage, never a silent skip. Gaps under
//! eviction are named in coverage, never silent.
//!
//! # Byte layout
//!
//! The canonical encoding, little-endian throughout, no padding and no
//! fields beyond these:
//!
//! | offset                       | width | field                                     |
//! | ---------------------------- | ----- | ----------------------------------------- |
//! | `0..8`                       | 8     | position in the total order, `u64`        |
//! | `8..16`                      | 8     | query fingerprint, `u64`                  |
//! | `16`                         | 1     | last entity tag: `0` = span, `1` = assigned |
//! | span: `17..41`               | 24    | trace id then span id, wire byte order    |
//! | assigned: `17..25`           | 8     | session serial, `u64`, never zero         |
//! | after the last entity        | 8     | snapshot admission time, `u64` unix ns    |
//! | then                         | 1     | snapshot entity tag: same two tags        |
//! | snapshot span: next 24       | 24    | trace id then span id, wire byte order    |
//! | snapshot assigned: next 8    | 8     | session serial, `u64`, never zero         |
//!
//! Total length is 42–74 bytes depending on the two entity variants.
//! Entity ids are encoded raw — never hashed — because they are data the
//! engine reads back, and a cursor's bytes are the engine's own output,
//! opaque to callers but not to itself.
//!
//! # Cursors are not authenticated
//!
//! Beyond strict canonical decoding, the fingerprint check is the whole
//! validity test. A hand-altered cursor that still decodes continues a
//! view its page never minted. The runtime's local-trust posture covers
//! this — the caller is the operator's own process on this machine. A
//! multi-tenant surface would need an authenticator.

use runtime_trail_storage::AdmissionKey;
use runtime_trail_telemetry_model::{AdmissionTime, AssignedId, EntityId, SpanId, TraceId};
use std::fmt;
use std::num::NonZeroU64;

/// Bytes before the entity tag: position and fingerprint.
const HEADER_LEN: usize = 2 * std::mem::size_of::<u64>();

/// The tag marking a span's natural wire identity.
const SPAN_TAG: u8 = 0;

/// The tag marking an admission-assigned identity.
const ASSIGNED_TAG: u8 = 1;

/// The width of a serial in the encoding.
const SERIAL_LEN: usize = std::mem::size_of::<u64>();

/// The width of a span id's id region: trace id then span id.
const SPAN_ID_REGION: usize = TraceId::LENGTH + SpanId::LENGTH;

/// The width of the snapshot's encoded admission time.
const SNAPSHOT_TIME_LEN: usize = std::mem::size_of::<u64>();

/// What a cursor encodes
/// ([query-model.md](../../docs/architecture/query-model.md),
/// "Ordering, cursors and pagination"): the engine's position in the total
/// order where the answer continues, the entity id of the anchor record
/// the page continued after — the last *examined* record in the scanned
/// residency order, which a filter may have excluded from the answer —
/// the query fingerprint the cursor belongs to, and the snapshot
/// boundary — the first page's residency frontier, an
/// [`AdmissionKey`] in the storage contract's residency order.
///
/// The encoded form is opaque to callers — they carry the bytes and hand
/// them back. These fields are the engine's view, reached through the
/// accessors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorPayload {
    /// The order key value where the next page starts: in the records
    /// flow, the anchor record's admission-time nanoseconds. With
    /// [`Self::last_entity`] it reconstructs the anchor's
    /// [`AdmissionKey`] exactly, so a resume never re-walks and never
    /// needs the anchor record to still be resident.
    position: u64,
    /// The entity id of the anchor record the cursor continues after:
    /// the last examined record along the scanned residency order, which
    /// a filter may have excluded from the answer — at a scan-ceiling
    /// stop the minting page's walk stopped at a record it examined but
    /// did not return. With [`Self::position`] it reconstructs the
    /// anchor's [`AdmissionKey`] exactly, so a resume never re-walks and
    /// never needs the anchor record to still be resident.
    last_entity: EntityId,
    /// The query fingerprint this cursor is valid for (invariant 3).
    fingerprint: u64,
    /// The first page's residency frontier: every later page of the
    /// continuation stops here.
    snapshot: AdmissionKey,
}

/// The encoded width of an entity id's id region: trace id plus span id
/// for a span, the serial for an assigned id.
fn id_region_len(entity: EntityId) -> usize {
    match entity {
        EntityId::Span { .. } => SPAN_ID_REGION,
        EntityId::Assigned(_) => SERIAL_LEN,
    }
}

/// Appends one tagged entity id — the tag byte, then the id region in wire
/// byte order — to `bytes`.
fn push_entity(bytes: &mut Vec<u8>, entity: EntityId) {
    match entity {
        EntityId::Span { trace_id, span_id } => {
            bytes.push(SPAN_TAG);
            bytes.extend_from_slice(&trace_id.as_bytes());
            bytes.extend_from_slice(&span_id.as_bytes());
        }
        EntityId::Assigned(assigned) => {
            bytes.push(ASSIGNED_TAG);
            bytes.extend_from_slice(&assigned.serial().get().to_le_bytes());
        }
    }
}

/// Parses one tagged entity id at the front of `bytes`; returns the entity
/// and the bytes it consumed (tag plus id region). `None` is an unknown
/// tag or an id region that does not fit; bytes beyond the region belong
/// to the fields after it.
fn parse_entity(bytes: &[u8]) -> Option<(EntityId, usize)> {
    let (tag, rest) = bytes.split_first()?;
    let entity = match *tag {
        SPAN_TAG => {
            if rest.len() < SPAN_ID_REGION {
                return None;
            }
            let trace = &rest[..TraceId::LENGTH];
            let span = &rest[TraceId::LENGTH..SPAN_ID_REGION];
            EntityId::Span {
                trace_id: TraceId::from_bytes(trace.try_into().ok()?),
                span_id: SpanId::from_bytes(span.try_into().ok()?),
            }
        }
        ASSIGNED_TAG => {
            if rest.len() < SERIAL_LEN {
                return None;
            }
            let serial = NonZeroU64::new(u64::from_le_bytes(rest[..SERIAL_LEN].try_into().ok()?))?;
            EntityId::Assigned(AssignedId::from_serial(serial))
        }
        _ => return None,
    };
    Some((entity, 1 + id_region_len(entity)))
}

impl CursorPayload {
    /// Assembles the payload the engine mints when it truncates a page.
    /// The snapshot is the first page's residency frontier in the storage
    /// contract's residency order (an [`AdmissionKey`]): the engine bounds
    /// continuations at it and names it via `CoverageEntry::SnapshotBoundary`.
    #[must_use]
    pub const fn new(
        position: u64,
        last_entity: EntityId,
        fingerprint: u64,
        snapshot: AdmissionKey,
    ) -> Self {
        Self {
            position,
            last_entity,
            fingerprint,
            snapshot,
        }
    }

    /// The order key value where the next page starts: the anchor
    /// record's admission-time nanoseconds in the records flow, which
    /// with [`Self::last_entity`] reconstructs the anchor's admission
    /// key.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// The entity id of the anchor record the cursor continues after:
    /// the last examined record along the scanned residency order, which
    /// a filter may have excluded from the answer.
    #[must_use]
    pub const fn last_entity(&self) -> EntityId {
        self.last_entity
    }

    /// The query fingerprint this cursor is bound to.
    #[must_use]
    pub const fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// The residency frontier every later page of the continuation is
    /// bounded at — the first page's snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> AdmissionKey {
        self.snapshot
    }

    /// Encodes the payload into the canonical byte layout documented on
    /// [the module](self). The inverse is [`CursorPayload::decode`].
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let capacity = HEADER_LEN
            + 1
            + id_region_len(self.last_entity)
            + SNAPSHOT_TIME_LEN
            + 1
            + id_region_len(self.snapshot.entity());
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(&self.position.to_le_bytes());
        bytes.extend_from_slice(&self.fingerprint.to_le_bytes());
        push_entity(&mut bytes, self.last_entity);
        bytes.extend_from_slice(&self.snapshot.admitted_at().as_unix_nano().to_le_bytes());
        push_entity(&mut bytes, self.snapshot.entity());
        bytes
    }

    /// Decodes the canonical byte layout documented on
    /// [the module](self); the inverse of [`CursorPayload::encode`].
    ///
    /// Decoding is strictly canonical: the bytes must be exactly the
    /// documented layout — header, last entity, snapshot admission time,
    /// snapshot entity, and nothing else. Each tag must be one of the two
    /// defined tags, each id region must fit exactly, and an assigned
    /// serial must be nonzero. Truncated, over-long and wrong-tagged
    /// inputs are all rejected — there is no partial read, and nothing
    /// over-long is accepted as a prefix-plus-rest. Corruption that still
    /// forms a valid layout decodes to a different position: layout
    /// validity is all `decode` promises, and the fingerprint is the only
    /// query-identity test — call [`CursorPayload::verify`] after decoding.
    ///
    /// # Errors
    ///
    /// [`CursorError::Malformed`] when the bytes do not match the
    /// canonical layout exactly: a region that does not fit, an unknown
    /// tag, a zero assigned serial, or trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, CursorError> {
        Self::parse(bytes).ok_or(CursorError::Malformed)
    }

    /// The strict parser behind [`CursorPayload::decode`]; `None` is
    /// every kind of non-canonicality collapsed into one rejection.
    fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() <= HEADER_LEN {
            return None; // the last entity's tag would not fit
        }
        let header = &bytes[..HEADER_LEN];
        let position = u64::from_le_bytes(header[..SERIAL_LEN].try_into().ok()?);
        let fingerprint = u64::from_le_bytes(header[SERIAL_LEN..].try_into().ok()?);
        let mut at = HEADER_LEN;
        let (last_entity, used) = parse_entity(&bytes[at..])?;
        at += used;
        if bytes.len() - at < SNAPSHOT_TIME_LEN {
            return None; // the snapshot's admission time would not fit
        }
        let nanos = u64::from_le_bytes(bytes[at..at + SNAPSHOT_TIME_LEN].try_into().ok()?);
        at += SNAPSHOT_TIME_LEN;
        let (snapshot_entity, used) = parse_entity(&bytes[at..])?;
        at += used;
        if at != bytes.len() {
            return None; // over-long: nothing may trail the snapshot entity
        }
        Some(Self {
            position,
            last_entity,
            fingerprint,
            snapshot: AdmissionKey::new(AdmissionTime::from_unix_nano(nanos), snapshot_entity),
        })
    }

    /// Checks the cursor against the fingerprint of the query it is
    /// presented under (invariant 3): a cursor belongs to one query's
    /// result set — same shape and parameters — and presenting it anywhere
    /// else is an error, never a best-effort continuation.
    ///
    /// # Errors
    ///
    /// [`CursorError::FingerprintMismatch`] when the cursor's embedded
    /// fingerprint differs from `expected_fingerprint`.
    pub fn verify(&self, expected_fingerprint: u64) -> Result<(), CursorError> {
        if self.fingerprint == expected_fingerprint {
            Ok(())
        } else {
            Err(CursorError::FingerprintMismatch)
        }
    }
}

/// Why a cursor failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorError {
    /// The bytes are not a canonical cursor encoding: wrong length for the
    /// encoded variant, an unknown entity tag, or a zero assigned serial.
    Malformed,
    /// The cursor's fingerprint differs from the query it was presented
    /// under — a cursor is valid only for its own query (invariant 3).
    FingerprintMismatch,
}

impl fmt::Display for CursorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => write!(f, "cursor is not a canonical cursor encoding"),
            Self::FingerprintMismatch => {
                write!(
                    f,
                    "cursor belongs to a different query (fingerprint mismatch)"
                )
            }
        }
    }
}

impl std::error::Error for CursorError {}

/// FNV-1a 64 over `input` — the query fingerprint a cursor is bound to
/// ([query-model.md](../../docs/architecture/query-model.md),
/// "Ordering, cursors and pagination"). The input is the canonical bytes
/// of the query description; the caller that assembles them owns their
/// canonicality.
///
/// Hashing is acceptable here because the fingerprint is cursor
/// *rejection*, not record identity. The model's comparison law
/// (telemetry-model.md, "Comparison is byte-exact and total") forbids
/// hashes standing in for record equality and binds the ledger's identity
/// maps — a query fingerprint does neither. Its one job is making a cursor
/// presented under a different query fail loudly, and that failure is
/// honestly probabilistic: FNV-1a is not cryptographic, two distinct query
/// descriptions collide with probability ~2^-64 per pair, and a collision
/// means a wrong-query rejection is missed — a cursor continues under a
/// query it was not minted for. For a developer-local runtime that trade
/// is one to state, not one to hide; it would not be one to accept for a
/// multi-tenant surface.
#[must_use]
pub fn fingerprint(input: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for byte in input {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use runtime_trail_storage::AdmissionKey;
    use runtime_trail_telemetry_model::{AdmissionTime, AssignedId, SpanId, TraceId};
    use std::num::NonZeroU64;

    use crate::order::sort_deterministically;
    use crate::result::{
        Coverage, Dimension, Execution, Page, PartOutcome, Truncation, TruncationPoint,
    };

    use super::*;

    fn assigned(serial: u64) -> EntityId {
        EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(serial).expect("test serials are nonzero"),
        ))
    }

    fn span_entity(trace: [u8; 16], span: [u8; 8]) -> EntityId {
        EntityId::Span {
            trace_id: TraceId::from_bytes(trace),
            span_id: SpanId::from_bytes(span),
        }
    }

    fn span_payload() -> CursorPayload {
        CursorPayload::new(
            12,
            span_entity([7; 16], [3; 8]),
            0xABCD_EF01_2345_6789,
            AdmissionKey::new(AdmissionTime::from_unix_nano(100), assigned(9)),
        )
    }

    fn assigned_payload() -> CursorPayload {
        CursorPayload::new(
            4,
            assigned(6),
            0x0123_4567_89AB_CDEF,
            AdmissionKey::new(
                AdmissionTime::from_unix_nano(200),
                span_entity([9; 16], [4; 8]),
            ),
        )
    }

    /// Last entity and snapshot both spans — the longest layout.
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

    /// Last entity and snapshot both assigned — the shortest layout.
    fn assigned_assigned_payload() -> CursorPayload {
        CursorPayload::new(
            7,
            assigned(11),
            43,
            AdmissionKey::new(AdmissionTime::from_unix_nano(400), assigned(12)),
        )
    }

    /// Kills a mutation that drops any of the four fields from either
    /// encoding step, or encodes the two entity variants inconsistently —
    /// a roundtrip through every layout must reproduce every field (the
    /// snapshot frontier included), at the canonical lengths and nothing
    /// more. The four payload shapes below cover all four variant
    /// combinations and both extremes of the documented length range
    /// (42 and 74).
    #[test]
    fn roundtrip_preserves_every_field_for_both_entity_variants() {
        for (payload, total_len) in [
            (span_payload(), 58),
            (assigned_payload(), 58),
            (span_span_payload(), 74),
            (assigned_assigned_payload(), 42),
        ] {
            let encoded = payload.encode();
            assert_eq!(
                CursorPayload::decode(&encoded).expect("own encoding decodes"),
                payload,
                "encode then decode must be the identity"
            );
            assert_eq!(
                CursorPayload::decode(&encoded)
                    .expect("own encoding decodes")
                    .snapshot(),
                payload.snapshot(),
                "the snapshot frontier must survive the roundtrip"
            );
            let expected = HEADER_LEN
                + 1
                + id_region_len(payload.last_entity())
                + SNAPSHOT_TIME_LEN
                + 1
                + id_region_len(payload.snapshot().entity());
            assert_eq!(
                encoded.len(),
                expected,
                "encoded length must be exactly the parts' sum"
            );
            assert_eq!(
                encoded.len(),
                total_len,
                "the layout's canonical total length"
            );
        }
    }

    /// Kills the lenient mutations: a length check that accepts a minimum
    /// instead of the exact layout, a missing tag check, acceptance of a
    /// zero assigned serial, and any decoder that reads what is available
    /// instead of rejecting a partial input. The loop runs over all four
    /// combinations of entity variants, so a lapse in one slot's handling
    /// cannot hide behind the other slot.
    #[test]
    fn truncation_extension_and_noncanonical_inputs_are_all_rejected() {
        for payload in [
            span_payload(),
            assigned_payload(),
            span_span_payload(),
            assigned_assigned_payload(),
        ] {
            let encoded = payload.encode();
            for cut in 1..encoded.len() {
                let truncated = &encoded[..cut];
                assert_eq!(
                    CursorPayload::decode(truncated),
                    Err(CursorError::Malformed),
                    "a {}-byte prefix of a {}-byte cursor is malformed",
                    truncated.len(),
                    encoded.len()
                );
            }
            let mut too_long = encoded.clone();
            too_long.push(0);
            assert_eq!(
                CursorPayload::decode(&too_long),
                Err(CursorError::Malformed),
                "an over-long input is rejected, not read as a prefix plus rest"
            );
            // Unknown tags are malformed in both entity slots — the last
            // entity's and the snapshot's — including tags whose region
            // would fit, so a length match alone would not pass.
            let snapshot_tag_at =
                HEADER_LEN + 1 + id_region_len(payload.last_entity()) + SNAPSHOT_TIME_LEN;
            for tag_at in [HEADER_LEN, snapshot_tag_at] {
                for bad in [2_u8, u8::MAX] {
                    let mut wrong_tag = encoded.clone();
                    wrong_tag[tag_at] = bad;
                    assert_eq!(
                        CursorPayload::decode(&wrong_tag),
                        Err(CursorError::Malformed),
                        "tag byte {tag_at} set to {bad} is malformed",
                    );
                }
            }
            // Serial zero is not an assigned id: the model starts serials
            // at 1. Each assigned slot is poisoned in turn.
            if matches!(payload.last_entity(), EntityId::Assigned(_)) {
                let mut zero_serial = encoded.clone();
                zero_serial[HEADER_LEN + 1..HEADER_LEN + 1 + SERIAL_LEN]
                    .copy_from_slice(&0_u64.to_le_bytes());
                assert_eq!(
                    CursorPayload::decode(&zero_serial),
                    Err(CursorError::Malformed)
                );
            }
            if matches!(payload.snapshot().entity(), EntityId::Assigned(_)) {
                let mut zero_serial = encoded.clone();
                let serial_at = snapshot_tag_at + 1;
                zero_serial[serial_at..serial_at + SERIAL_LEN]
                    .copy_from_slice(&0_u64.to_le_bytes());
                assert_eq!(
                    CursorPayload::decode(&zero_serial),
                    Err(CursorError::Malformed)
                );
            }
        }
    }

    /// Kills a no-op `verify` that always returns `Ok(())`, and any
    /// variant that compares a field other than the fingerprint — a
    /// cursor under a foreign query must be an error, never a
    /// best-effort continuation (invariant 3).
    #[test]
    fn verify_rejects_a_foreign_fingerprint_and_accepts_its_own() {
        let payload = assigned_payload();
        assert_eq!(payload.verify(payload.fingerprint()), Ok(()));
        assert_eq!(
            payload.verify(payload.fingerprint() ^ 1),
            Err(CursorError::FingerprintMismatch)
        );
        // The end-to-end shape a caller exercises: decode the opaque bytes,
        // then present them to their query's fingerprint.
        let decoded =
            CursorPayload::decode(&span_payload().encode()).expect("own encoding decodes");
        assert_eq!(decoded.verify(span_payload().fingerprint()), Ok(()));
        assert_eq!(
            decoded.verify(0),
            Err(CursorError::FingerprintMismatch),
            "a foreign query's fingerprint is rejected after a decode too"
        );
    }

    /// Kills a constant-returning no-op, a wrong offset basis or prime,
    /// and a hash that ignores its input: the values below are the
    /// published FNV-1a 64 vectors, and the last pair shows the input's
    /// bytes all matter.
    #[test]
    fn fingerprint_is_fnv1a64_over_every_input_byte() {
        assert_eq!(fingerprint(b""), 0xcbf2_9ce4_8422_2325, "the offset basis");
        assert_eq!(fingerprint(b"foobar"), 0x8594_4171_f739_67e8);
        assert_ne!(fingerprint(b"a"), fingerprint(b"b"));
        assert_ne!(
            fingerprint(b"records:live:span"),
            fingerprint(b"records:live:spans"),
            "a one-byte difference in the query description must not collide"
        );
    }

    /// Kills hidden nondeterminism in page assembly (invariant 8:
    /// identical query + resident set + budget ⇒ an identical page) and
    /// the no-op that drops the sort inside assembly — the second build
    /// would still equal the first, so the page's items are also asserted
    /// against the expected total order. Also pins the seam: the cursor
    /// bytes produced here are what [`TruncationPoint::Cursor`] carries.
    #[test]
    fn identical_inputs_assemble_byte_identical_pages() {
        fn build_page() -> Page<EntityId> {
            let mut records: Vec<(u64, EntityId)> = vec![
                (2, assigned(5)),
                (1, span_entity([9; 16], [1; 8])),
                (1, assigned(3)),
                (2, span_entity([1; 16], [2; 8])),
            ];
            sort_deterministically(&mut records, |record| record.0, |record| record.1);
            let items: Vec<EntityId> = records.into_iter().map(|(_, entity)| entity).collect();
            let cursor = CursorPayload::new(
                4,
                assigned(3),
                fingerprint(b"flow:errors:last-hour"),
                AdmissionKey::new(AdmissionTime::from_unix_nano(300), assigned(3)),
            );
            let bytes = cursor.encode();
            Page {
                items,
                next_cursor: Some(bytes.clone()),
                execution: Execution {
                    parts: vec![PartOutcome::Degraded {
                        truncation: Truncation {
                            dimension: Dimension::Scan,
                            position: TruncationPoint::Cursor(bytes),
                            omitted: 0,
                        },
                    }],
                    coverage: Coverage {
                        entries: Vec::new(),
                    },
                },
            }
        }

        let first = build_page();
        let second = build_page();
        assert_eq!(first, second, "identical inputs assemble one page");
        let Some(cursor_bytes) = first.next_cursor.as_deref() else {
            panic!("a truncated page carries its continuation");
        };
        assert_eq!(
            first.items,
            vec![
                span_entity([9; 16], [1; 8]),
                assigned(3),
                span_entity([1; 16], [2; 8]),
                assigned(5),
            ],
            "items sit in the engine's total order: key first, entity id as tie-break"
        );
        let PartOutcome::Degraded { truncation } = &first.execution.parts[0] else {
            panic!("the page's part degraded under the scan budget");
        };
        assert_eq!(
            truncation.position,
            TruncationPoint::Cursor(cursor_bytes.to_vec()),
            "the truncation point carries exactly the minted cursor bytes"
        );
    }
}
