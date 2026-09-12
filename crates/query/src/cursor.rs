//! Opaque, fingerprint-bound cursors.
//!
//! A cursor encodes (position in the total order, last entity id, query
//! fingerprint), is opaque to callers, and is rejected when presented
//! under a different query
//! ([query-model.md](../../docs/architecture/query-model.md),
//! "Ordering, cursors and pagination", invariant 3). A cursor continues
//! within the snapshot its first page evaluated; records admitted after it
//! are outside every later page — a declared boundary in coverage, never a
//! silent skip. Gaps under eviction are named in coverage, never silent.
//!
//! # Byte layout
//!
//! The canonical encoding, little-endian throughout, no padding and no
//! fields beyond these:
//!
//! | offset              | width | field                                 |
//! | ------------------- | ----- | ------------------------------------- |
//! | `0..8`              | 8     | position in the total order, `u64`    |
//! | `8..16`             | 8     | query fingerprint, `u64`              |
//! | `16`                | 1     | entity tag: `0` = span, `1` = assigned |
//! | span: `17..33`      | 16    | trace id, wire byte order             |
//! | span: `33..41`      | 8     | span id, wire byte order              |
//! | assigned: `17..25`  | 8     | session serial, `u64`, never zero     |
//!
//! A span payload encodes to exactly 41 bytes; an assigned payload to
//! exactly 25. The last entity id is encoded raw — never hashed — because
//! it is data the engine reads back, and a cursor's bytes are the
//! engine's own output, opaque to callers but not to itself.

use runtime_trail_telemetry_model::{AssignedId, EntityId, SpanId, TraceId};
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

/// Total encoded length of a span-identity cursor.
const SPAN_LEN: usize = HEADER_LEN + 1 + TraceId::LENGTH + SpanId::LENGTH;

/// Total encoded length of an assigned-identity cursor.
const ASSIGNED_LEN: usize = HEADER_LEN + 1 + SERIAL_LEN;

/// What a cursor encodes
/// ([query-model.md](../../docs/architecture/query-model.md),
/// "Ordering, cursors and pagination"): the engine's position in the total
/// order where the answer continues, the entity id of the last record of
/// the page that minted it, and the query fingerprint the cursor belongs
/// to.
///
/// The encoded form is opaque to callers — they carry the bytes and hand
/// them back. These fields are the engine's view, reached through the
/// accessors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorPayload {
    /// The engine's ordinal in the total order where the next page starts.
    position: u64,
    /// The entity id of the last record of the page that minted the
    /// cursor.
    last_entity: EntityId,
    /// The query fingerprint this cursor is valid for (invariant 3).
    fingerprint: u64,
}

impl CursorPayload {
    /// Assembles the payload the engine mints when it truncates a page.
    #[must_use]
    pub const fn new(position: u64, last_entity: EntityId, fingerprint: u64) -> Self {
        Self {
            position,
            last_entity,
            fingerprint,
        }
    }

    /// The ordinal in the total order where the next page starts.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// The entity id of the last record of the minting page.
    #[must_use]
    pub const fn last_entity(&self) -> EntityId {
        self.last_entity
    }

    /// The query fingerprint this cursor is bound to.
    #[must_use]
    pub const fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Encodes the payload into the canonical byte layout documented on
    /// [the module](self). The inverse is [`CursorPayload::decode`].
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let capacity = match self.last_entity {
            EntityId::Span { .. } => SPAN_LEN,
            EntityId::Assigned(_) => ASSIGNED_LEN,
        };
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(&self.position.to_le_bytes());
        bytes.extend_from_slice(&self.fingerprint.to_le_bytes());
        match self.last_entity {
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
        bytes
    }

    /// Decodes the canonical byte layout documented on
    /// [the module](self); the inverse of [`CursorPayload::encode`].
    ///
    /// Decoding is strictly canonical: the length must be exactly the
    /// layout's length for the encoded variant, the tag must be one of the
    /// two defined tags, and an assigned serial must be nonzero. Truncated,
    /// over-long and wrong-tagged inputs are all rejected — there is no
    /// partial read, and nothing over-long is accepted as a prefix-plus-rest.
    /// Corruption that still forms a valid layout decodes to a different
    /// position: layout validity is all `decode` promises, and the
    /// fingerprint is the only query-identity test — call
    /// [`CursorPayload::verify`] after decoding.
    ///
    /// # Errors
    ///
    /// [`CursorError::Malformed`] when the bytes do not match the
    /// canonical layout exactly: wrong length for the tag, an unknown tag,
    /// or a zero assigned serial.
    pub fn decode(bytes: &[u8]) -> Result<Self, CursorError> {
        Self::parse(bytes).ok_or(CursorError::Malformed)
    }

    /// The strict parser behind [`CursorPayload::decode`]; `None` is
    /// every kind of non-canonicality collapsed into one rejection.
    fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() <= HEADER_LEN {
            return None; // the entity tag would not fit
        }
        let (header, tagged) = bytes.split_at(HEADER_LEN);
        let position = u64::from_le_bytes(header[..SERIAL_LEN].try_into().ok()?);
        let fingerprint = u64::from_le_bytes(header[SERIAL_LEN..].try_into().ok()?);
        let (tag, entity) = tagged.split_first()?;
        let last_entity = match *tag {
            SPAN_TAG => {
                if entity.len() != TraceId::LENGTH + SpanId::LENGTH {
                    return None;
                }
                let (trace, span) = entity.split_at(TraceId::LENGTH);
                EntityId::Span {
                    trace_id: TraceId::from_bytes(trace.try_into().ok()?),
                    span_id: SpanId::from_bytes(span.try_into().ok()?),
                }
            }
            ASSIGNED_TAG => {
                let serial = NonZeroU64::new(u64::from_le_bytes(entity.try_into().ok()?))?;
                EntityId::Assigned(AssignedId::from_serial(serial))
            }
            _ => return None,
        };
        Some(Self {
            position,
            last_entity,
            fingerprint,
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
    use runtime_trail_telemetry_model::{AssignedId, SpanId, TraceId};
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
        CursorPayload::new(12, span_entity([7; 16], [3; 8]), 0xABCD_EF01_2345_6789)
    }

    fn assigned_payload() -> CursorPayload {
        CursorPayload::new(4, assigned(6), 0x0123_4567_89AB_CDEF)
    }

    /// Kills a mutation that drops any of the three fields from either
    /// encoding step, or encodes the two entity variants inconsistently —
    /// a roundtrip through both layouts must reproduce every field, at
    /// the canonical lengths and nothing more.
    #[test]
    fn roundtrip_preserves_every_field_for_both_entity_variants() {
        for payload in [span_payload(), assigned_payload()] {
            let encoded = payload.encode();
            assert_eq!(
                CursorPayload::decode(&encoded).expect("own encoding decodes"),
                payload,
                "encode then decode must be the identity"
            );
        }
        assert_eq!(
            span_payload().encode().len(),
            HEADER_LEN + 1 + TraceId::LENGTH + SpanId::LENGTH,
            "a span cursor encodes to exactly the documented length"
        );
        assert_eq!(
            assigned_payload().encode().len(),
            HEADER_LEN + 1 + SERIAL_LEN,
            "an assigned cursor encodes to exactly the documented length"
        );
    }

    /// Kills the lenient mutations: a length check that accepts a minimum
    /// instead of the exact length, a missing tag check, acceptance of a
    /// zero assigned serial, and any decoder that reads what is available
    /// instead of rejecting a partial input.
    #[test]
    fn truncation_extension_and_noncanonical_inputs_are_all_rejected() {
        for payload in [span_payload(), assigned_payload()] {
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
        }
        // Unknown tags are malformed — including the tag byte of the other
        // variant's length, so a length match alone would not pass.
        let mut wrong_tag = span_payload().encode();
        wrong_tag[HEADER_LEN] = 2;
        assert_eq!(
            CursorPayload::decode(&wrong_tag),
            Err(CursorError::Malformed)
        );
        let mut high_tag = assigned_payload().encode();
        high_tag[HEADER_LEN] = u8::MAX;
        assert_eq!(
            CursorPayload::decode(&high_tag),
            Err(CursorError::Malformed)
        );
        // Serial zero is not an assigned id: the model starts serials at 1.
        let mut zero_serial = assigned_payload().encode();
        zero_serial[HEADER_LEN + 1..].copy_from_slice(&0_u64.to_le_bytes());
        assert_eq!(
            CursorPayload::decode(&zero_serial),
            Err(CursorError::Malformed)
        );
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
            let cursor = CursorPayload::new(4, assigned(3), fingerprint(b"flow:errors:last-hour"));
            let bytes = cursor.encode();
            Page {
                items,
                next_cursor: Some(bytes.clone()),
                execution: Execution {
                    parts: vec![PartOutcome::Degraded {
                        truncation: Truncation {
                            dimension: Dimension::Scan,
                            position: TruncationPoint::Cursor(bytes),
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
