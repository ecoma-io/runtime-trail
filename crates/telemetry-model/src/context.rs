//! Trace context: ids, flags and tracestate.
//!
//! `docs/architecture/telemetry-model.md`, "Trace context": 16-byte trace
//! ids, 8-byte span ids, the all-zero encoding invalid but preserved as
//! sent, a 32-bit flags field of which only the sampled bit is interpreted,
//! and an ordered tracestate whose entries are never merged, deduplicated
//! or sorted.
use serde::{Deserialize, Serialize};

/// A 16-byte trace id, preserved verbatim as the emitter sent it.
///
/// The all-zero encoding is invalid. An invalid value is never regenerated,
/// hashed into something else, or coerced — it is preserved as what the
/// emitter sent, and a span carrying one is admitted under an
/// admission-assigned entity id rather than its natural identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TraceId([u8; Self::LENGTH]);

impl TraceId {
    /// A trace id is 16 bytes.
    pub const LENGTH: usize = 16;

    /// Wraps the bytes the emitter sent, verbatim.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::LENGTH]) -> Self {
        Self(bytes)
    }

    /// The bytes exactly as wrapped.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; Self::LENGTH] {
        self.0
    }

    /// False for the all-zero encoding, which the contract calls invalid.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        let mut index = 0;
        while index < Self::LENGTH {
            if self.0[index] != 0 {
                return true;
            }
            index += 1;
        }
        false
    }
}

/// An 8-byte span id, preserved verbatim as the emitter sent it.
///
/// The all-zero encoding is invalid; see [`TraceId`] for what preservation
/// of an invalid value means.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SpanId([u8; Self::LENGTH]);

impl SpanId {
    /// A span id is 8 bytes.
    pub const LENGTH: usize = 8;

    /// Wraps the bytes the emitter sent, verbatim.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::LENGTH]) -> Self {
        Self(bytes)
    }

    /// The bytes exactly as wrapped.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; Self::LENGTH] {
        self.0
    }

    /// False for the all-zero encoding, which the contract calls invalid.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        let mut index = 0;
        while index < Self::LENGTH {
            if self.0[index] != 0 {
                return true;
            }
            index += 1;
        }
        false
    }
}

/// The wire flags field, preserved verbatim; only the sampled bit is
/// interpreted.
///
/// OTLP carries flags as a 32-bit field. Bits 0–7 are the W3C trace flags
/// (bit 0 is [`TraceFlags::SAMPLED_BIT`], the only bit the model reads);
/// bits 8 and 9 carry OTLP's remote-parent signalling and MUST survive a
/// round trip; readers MUST NOT assume bits 10–31 are zero. Every bit is
/// carried exactly as sent and none but the sampled bit is ever read as
/// meaning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceFlags(u32);

impl TraceFlags {
    /// The sampled bit — the only bit the model interprets.
    pub const SAMPLED_BIT: u32 = 0b1;

    /// Wraps the raw flags field, verbatim.
    #[must_use]
    pub const fn new(bits: u32) -> Self {
        Self(bits)
    }

    /// The raw flags field, exactly as wrapped.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Whether the sampled bit is set. The other 31 bits do not
    /// participate in any interpretation.
    #[must_use]
    pub const fn sampled(self) -> bool {
        self.0 & Self::SAMPLED_BIT == Self::SAMPLED_BIT
    }
}

/// One tracestate entry: a vendor key and its opaque value.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceStateEntry {
    /// The vendor key, as sent.
    pub vendor: String,
    /// The opaque value, as sent.
    pub value: String,
}

/// An ordered tracestate: the order is semantic.
///
/// Entries are never merged, deduplicated or sorted — a repeated vendor key
/// is two entries, and reversing the order is a different tracestate.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceState {
    entries: Vec<TraceStateEntry>,
}

impl TraceState {
    /// Builds a tracestate from the entries the emitter sent, in the order
    /// the emitter sent them.
    #[must_use]
    pub fn from_entries(entries: Vec<TraceStateEntry>) -> Self {
        Self { entries }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, TraceStateEntry> {
        self.entries.iter()
    }
}

impl<'a> IntoIterator for &'a TraceState {
    type Item = &'a TraceStateEntry;
    type IntoIter = std::slice::Iter<'a, TraceStateEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

/// The trace context a signal carries: its trace, its span, the flags and
/// the ordered tracestate.
///
/// Spans and span links carry the full context. Log records and metric
/// exemplars carry only the fields OTLP defines for them — see
/// [`crate::logs::LogRecord`] and [`crate::metrics::Exemplar`] — which is
/// why this struct exists only where the whole of it is on the wire.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceContext {
    /// The 16-byte trace id, as sent.
    pub trace_id: TraceId,
    /// The 8-byte span id, as sent.
    pub span_id: SpanId,
    /// The 32-bit flags field, as sent.
    pub flags: TraceFlags,
    /// The ordered tracestate, as sent.
    pub tracestate: TraceState,
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_TRACE: [u8; 16] = [
        0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x19,
    ];
    const VALID_SPAN: [u8; 8] = [0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28];

    #[test]
    fn the_all_zero_trace_id_is_invalid_but_preserved_as_sent() {
        let invalid = TraceId::from_bytes([0; 16]);
        assert!(!invalid.is_valid());
        assert_eq!(invalid.as_bytes(), [0; 16], "preserved, not regenerated");
    }

    #[test]
    fn a_nonzero_trace_id_is_valid_and_round_trips() {
        let id = TraceId::from_bytes(VALID_TRACE);
        assert!(id.is_valid());
        assert_eq!(id.as_bytes(), VALID_TRACE);
    }

    #[test]
    fn the_all_zero_span_id_is_invalid_but_preserved_as_sent() {
        let invalid = SpanId::from_bytes([0; 8]);
        assert!(!invalid.is_valid());
        assert_eq!(invalid.as_bytes(), [0; 8]);
        assert!(SpanId::from_bytes(VALID_SPAN).is_valid());
    }

    #[test]
    fn only_the_sampled_bit_is_interpreted() {
        assert!(TraceFlags::new(0b1).sampled());
        assert!(!TraceFlags::new(0b0).sampled());
        assert!(
            !TraceFlags::new(0xFFFF_FFFE).sampled(),
            "other bits carry no meaning"
        );
    }

    #[test]
    fn every_flag_bit_is_preserved_verbatim_across_the_full_width() {
        let flags = TraceFlags::new(0xFFFF_FFFE);
        assert_eq!(flags.bits(), 0xFFFF_FFFE);
        assert_eq!(TraceFlags::new(u32::MAX).bits(), u32::MAX);
    }

    #[test]
    fn remote_parent_bits_eight_and_nine_survive_a_round_trip() {
        // Bits 8 and 9 carry OTLP's remote-parent signalling; a model that
        // narrowed the field to one byte would destroy them.
        let remote_parent = TraceFlags::new((1 << 8) | (1 << 9) | TraceFlags::SAMPLED_BIT);
        assert_eq!(remote_parent.bits(), 0b11_0000_0001);
        assert!(remote_parent.sampled(), "the sampled bit still reads");
        let context = TraceContext {
            trace_id: TraceId::from_bytes(VALID_TRACE),
            span_id: SpanId::from_bytes(VALID_SPAN),
            flags: remote_parent,
            tracestate: TraceState::default(),
        };
        assert_eq!(context.flags.bits(), 0b11_0000_0001, "round-trips verbatim");
    }

    #[test]
    fn readers_may_not_assume_the_upper_bits_are_zero() {
        let high = TraceFlags::new(1 << 31);
        assert_eq!(high.bits(), 1 << 31, "bit 31 is preserved, not masked");
        assert!(!high.sampled());
    }

    #[test]
    fn tracestate_order_is_semantic_and_duplicates_survive() {
        let entry = |vendor: &str, value: &str| TraceStateEntry {
            vendor: vendor.to_owned(),
            value: value.to_owned(),
        };
        let first = TraceState::from_entries(vec![entry("vendor-a", "1"), entry("vendor-b", "2")]);
        let reversed =
            TraceState::from_entries(vec![entry("vendor-b", "2"), entry("vendor-a", "1")]);
        assert_ne!(first, reversed, "order is semantic");
        let duplicated =
            TraceState::from_entries(vec![entry("vendor-a", "1"), entry("vendor-a", "2")]);
        assert_eq!(
            duplicated.len(),
            2,
            "entries are never merged or deduplicated"
        );
        assert_eq!(
            duplicated.iter().next().map(|e| e.value.as_str()),
            Some("1")
        );
    }

    #[test]
    fn an_empty_tracestate_is_a_value_not_an_invention() {
        let state = TraceState::default();
        assert!(state.is_empty());
        assert_eq!(state, TraceState::from_entries(Vec::new()));
    }

    #[test]
    fn context_fields_round_trip_verbatim() {
        let context = TraceContext {
            trace_id: TraceId::from_bytes(VALID_TRACE),
            span_id: SpanId::from_bytes(VALID_SPAN),
            flags: TraceFlags::new(0b11),
            tracestate: TraceState::from_entries(vec![TraceStateEntry {
                vendor: "vendor".to_owned(),
                value: "opaque".to_owned(),
            }]),
        };
        assert_eq!(context.trace_id.as_bytes(), VALID_TRACE);
        assert_eq!(context.span_id.as_bytes(), VALID_SPAN);
        assert_eq!(context.flags.bits(), 0b11);
        assert_eq!(context.tracestate.len(), 1);
    }
}
