//! Accounted size — the single definition byte ceilings count.
//!
//! `docs/architecture/telemetry-model.md` ("Information budgets") defines a
//! record's accounted size as the bytes of its value payloads, its
//! attribute keys and structure names, plus per-structure and per-container
//! overhead, so that everything variable-length is counted and byte
//! ceilings have one definition no matter which storage mode enforces
//! them. This module is that definition, in one place. It carries no
//! budget numbers from `runtime-constraints.md` — only the counting rule
//! their ceilings consume.
//!
//! # The formula
//!
//! - booleans count 1 byte; 64-bit integers and doubles count 8 bytes;
//! - strings and byte strings count their payload byte length; a heap
//!   string held as a *named field* (a span name, a severity text, a
//!   schema URL, …) additionally counts
//!   [`STRING_ALLOCATION_CHUNK_BYTES`], the worst-case cost of one small
//!   heap allocation;
//! - arrays count, per item, [`slot_bytes::<Value>()`] — the inline slot
//!   doubled to cover a growing vector's spare capacity — plus the items;
//! - key-value lists count, per entry, [`KEYED_ENTRY_BYTES`] + key bytes +
//!   value size, at every depth;
//! - attribute maps count, per entry, [`KEYED_ENTRY_BYTES`] + key bytes +
//!   value size, plus [`ATTRIBUTE_MAP_NODE_BYTES`] once for the map itself
//!   (a B-tree node is allocated whole even for one tiny entry; an empty
//!   map allocates nothing and is charged nothing);
//! - heap vectors of records (events, links, exemplars, quantiles, bucket
//!   counts and bounds) count, per element, twice the element's inline
//!   size — the slot plus the vector's growth headroom — plus the
//!   elements' own parts;
//! - tracestate entries count [`KEYED_ENTRY_BYTES`] + vendor bytes + value
//!   bytes (the entry slot and its two heap strings);
//! - each record, event, link, exemplar, quantile, bucket layout and
//!   stream identity part counts [`STRUCTURE_FIXED_BYTES`] plus its own
//!   fixed-width fields and the variable-length parts above;
//! - a structure held through an `Arc` (`Arc<Resource>` on a span or log
//!   record) is charged **in full wherever it appears**: accounted size
//!   deliberately over-counts shared payloads rather than depend on
//!   reference counts, which would make it non-deterministic. The sharing
//!   is a heap-cost optimisation (see ADR 0008), not an accounting event.
//!
//! # Over-approximation, on purpose
//!
//! Accounted size is a **deliberate over-approximation** of real heap
//! cost. The charges above are sized from measurement — a
//! counting-global-allocator probe over the legal adversarial shapes
//! (many tiny attributes in both insertion orders, single-entry maps,
//! lists and arrays), run out of this crate (the workspace forbids
//! `unsafe`, and the probe is a custom allocator) with its numbers pinned
//! by this module's `honesty` tests — so that real heap ÷ accounted stays
//! well inside the honest ceiling (~1.5×) on those shapes; the pre-fix
//! formula (16 flat bytes per structure) measured up to 14.8× real.
//! Over-counting is the honest direction: a byte ceiling that under-counts
//! is a lie about the machine.

/// The fixed overhead charged once per structure — the constant that makes
/// the count include the structure's fixed-width fields, not only its
/// payloads.
pub const STRUCTURE_FIXED_BYTES: usize = 16;

/// The per-entry charge of a keyed container — an attribute-map entry, a
/// key-value-list entry, or a tracestate entry.
///
/// Covers the entry's share of its container's nodes (worst-case B-tree
/// occupancy is about half full; measured, not guessed), the key's heap
/// allocation including the worst-case small-allocation chunk overhead
/// (32 bytes, measured), and one level of allocator rounding.
pub const KEYED_ENTRY_BYTES: usize = 160;

/// One full B-tree node for a `(String, Value)` attribute map, charged
/// once per non-empty attribute map: a map allocates a whole node even for
/// a single tiny entry (measured 544–728 bytes per node; this is the
/// rounded-up worst node).
pub const ATTRIBUTE_MAP_NODE_BYTES: usize = 768;

/// The worst-case chunk cost of one small heap allocation, charged for
/// heap strings held as named fields (their payload bytes are counted
/// separately).
pub const STRING_ALLOCATION_CHUNK_BYTES: usize = 32;

/// The heap cost of one named string field: payload bytes plus one small
/// allocation's chunk overhead.
#[must_use]
pub fn heap_string_bytes(text: &str) -> usize {
    STRING_ALLOCATION_CHUNK_BYTES + text.len()
}

/// The per-element charge of a heap vector of `T`: the element's inline
/// slot doubled, covering the vector's growth headroom (spare capacity) as
/// well as the slots themselves.
#[must_use]
pub fn slot_bytes<T>() -> usize {
    2 * std::mem::size_of::<T>()
}

/// Anything whose accounted size the model can state.
pub trait Accounted {
    /// The accounted size in bytes, by the formula in the module docs.
    #[must_use]
    fn accounted_size(&self) -> usize;
}

/// The accounted size of one attribute entry: keyed-container overhead +
/// key bytes + value size. This is the spend the attribute-value budget
/// measures (keys and names count).
#[must_use]
pub fn attribute_entry_size(key: &str, value: &crate::values::Value) -> usize {
    KEYED_ENTRY_BYTES + key.len() + value.accounted_size()
}

/// Totals the accounted size of a work-stack of values iteratively.
///
/// This is the engine every container's accounted size delegates to. It
/// never recurses and never calls back into the container impls, so a
/// deeply nested value — legal on the wire until the depth gate refuses it
/// — walks with a bounded stack, like the nesting check and drop do.
fn value_stack_bytes(mut stack: Vec<&crate::values::Value>) -> usize {
    use crate::values::Value;
    let mut total = 0;
    while let Some(value) = stack.pop() {
        match value {
            Value::String(text) => total += text.len(),
            Value::Bool(_) => total += 1,
            Value::Int(_) | Value::Double(_) => total += 8,
            Value::Bytes(bytes) => total += bytes.len(),
            Value::Array(array) => {
                total += array.len() * slot_bytes::<Value>();
                stack.extend(array.iter());
            }
            Value::KvList(list) => {
                for (key, child) in list {
                    total += KEYED_ENTRY_BYTES + key.len();
                    stack.push(child);
                }
            }
        }
    }
    total
}

fn value_bytes(value: &crate::values::Value) -> usize {
    value_stack_bytes(vec![value])
}

impl Accounted for crate::values::Float {
    fn accounted_size(&self) -> usize {
        8
    }
}

impl Accounted for crate::values::Value {
    fn accounted_size(&self) -> usize {
        value_bytes(self)
    }
}

impl Accounted for crate::values::HomogeneousArray {
    fn accounted_size(&self) -> usize {
        self.len() * slot_bytes::<crate::values::Value>() + value_stack_bytes(self.iter().collect())
    }
}

impl Accounted for crate::values::KeyValueList {
    fn accounted_size(&self) -> usize {
        let mut total = 0;
        let mut stack: Vec<&crate::values::Value> = Vec::new();
        for (key, value) in self {
            total += KEYED_ENTRY_BYTES + key.len();
            stack.push(value);
        }
        total + value_stack_bytes(stack)
    }
}

impl Accounted for crate::values::Attributes {
    fn accounted_size(&self) -> usize {
        if self.is_empty() {
            return 0; // an empty BTreeMap allocates nothing
        }
        let mut total = ATTRIBUTE_MAP_NODE_BYTES;
        let mut stack: Vec<&crate::values::Value> = Vec::new();
        for (key, value) in self {
            total += KEYED_ENTRY_BYTES + key.len();
            stack.push(value);
        }
        total + value_stack_bytes(stack)
    }
}

impl Accounted for crate::context::TraceId {
    fn accounted_size(&self) -> usize {
        Self::LENGTH
    }
}

impl Accounted for crate::context::SpanId {
    fn accounted_size(&self) -> usize {
        Self::LENGTH
    }
}

impl Accounted for crate::context::TraceStateEntry {
    fn accounted_size(&self) -> usize {
        KEYED_ENTRY_BYTES + self.vendor.len() + self.value.len()
    }
}

impl Accounted for crate::context::TraceState {
    fn accounted_size(&self) -> usize {
        self.iter().map(Accounted::accounted_size).sum::<usize>()
    }
}

impl Accounted for crate::context::TraceContext {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.trace_id.accounted_size()
            + self.span_id.accounted_size()
            + 4 // flags: the full 32-bit wire field
            + self.tracestate.accounted_size()
    }
}

impl Accounted for crate::resources::Resource {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.attributes.accounted_size()
            + self
                .schema_url
                .as_ref()
                .map_or(0, |url| heap_string_bytes(url))
            + 4 // dropped_attributes_count
    }
}

impl Accounted for crate::resources::InstrumentationScope {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + heap_string_bytes(&self.name)
            + self
                .version
                .as_ref()
                .map_or(0, |version| heap_string_bytes(version))
            + self.attributes.accounted_size()
            + self
                .schema_url
                .as_ref()
                .map_or(0, |url| heap_string_bytes(url))
            + 4 // dropped_attributes_count
    }
}

impl Accounted for crate::spans::SpanStatus {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + 1 + heap_string_bytes(&self.message) // code + message
    }
}

impl Accounted for crate::spans::SpanEvent {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + 8 // emitter-clock time, or its absence
            + heap_string_bytes(&self.name)
            + self.attributes.accounted_size()
            + 4 // dropped_attribute_count
    }
}

impl Accounted for crate::spans::SpanLink {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + self.context.accounted_size() + self.attributes.accounted_size() + 4 // dropped_attribute_count
    }
}

impl Accounted for crate::spans::Span {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.context.accounted_size()
            + self
                .parent_span_id
                .map_or(0, |parent| parent.accounted_size())
            + heap_string_bytes(&self.name)
            + 1 // kind
            + 8 // start_time
            + self.end_time_unix_nano.map_or(0, |_| 8)
            + self.resource.accounted_size() // Arc: charged in full, deterministically
            + self.scope.accounted_size()
            + self.attributes.accounted_size()
            + 12 // three emitter-reported dropped counts, u32 each
            + self.events.len() * slot_bytes::<crate::spans::SpanEvent>()
            + self
                .events
                .iter()
                .map(Accounted::accounted_size)
                .sum::<usize>()
            + self.links.len() * slot_bytes::<crate::spans::SpanLink>()
            + self.links.iter().map(Accounted::accounted_size).sum::<usize>()
            + self.status.accounted_size()
    }
}

impl Accounted for crate::logs::LogRecord {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self
                .timestamp_unix_nano
                .map_or(0, |_| 8)
            + self
                .observed_timestamp_unix_nano
                .map_or(0, |_| 8)
            + self.severity_number.map_or(0, |_| 1)
            + self.severity_text.as_ref().map_or(0, |text| heap_string_bytes(text))
            + self.body.as_ref().map_or(0, Accounted::accounted_size)
            + self.resource.accounted_size() // Arc: charged in full
            + self.scope.accounted_size()
            + self.attributes.accounted_size()
            + 4 // dropped_attribute_count
            + self.trace_id.map_or(0, |_| crate::context::TraceId::LENGTH)
            + self.span_id.map_or(0, |_| crate::context::SpanId::LENGTH)
            + self.trace_flags.map_or(0, |_| 4)
            + self
                .event_name
                .as_ref()
                .map_or(0, |name| heap_string_bytes(name))
    }
}

impl Accounted for crate::metrics::MetricNumber {
    fn accounted_size(&self) -> usize {
        match self {
            Self::Int(_) | Self::Double(_) => 8,
        }
    }
}

impl Accounted for crate::metrics::Exemplar {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.value.accounted_size()
            + 8 // time
            + self.filtered_attributes.accounted_size()
            + self.trace_id.map_or(0, |_| crate::context::TraceId::LENGTH)
            + self.span_id.map_or(0, |_| crate::context::SpanId::LENGTH)
    }
}

impl Accounted for crate::metrics::QuantileValue {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES + 16 // quantile + value
    }
}

impl Accounted for crate::metrics::ExponentialBuckets {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + 4 // offset
            + self.bucket_counts.len() * slot_bytes::<u64>() // counts, with growth headroom
    }
}

impl Accounted for crate::metrics::MetricPoint {
    fn accounted_size(&self) -> usize {
        let exemplar_bytes = |exemplars: &[crate::metrics::Exemplar]| {
            exemplars.len() * slot_bytes::<crate::metrics::Exemplar>()
                + exemplars
                    .iter()
                    .map(Accounted::accounted_size)
                    .sum::<usize>()
        };
        match self {
            Self::Number(point) => {
                STRUCTURE_FIXED_BYTES
                    + point.attributes.accounted_size()
                    + point.start_time_unix_nano.map_or(0, |_| 8)
                    + 8 // time
                    + point.value.accounted_size()
                    + 4 // flags
                    + exemplar_bytes(&point.exemplars)
            }
            Self::Histogram(point) => {
                STRUCTURE_FIXED_BYTES
                    + point.attributes.accounted_size()
                    + 16 // start + end
                    + 8 // count
                    + point.sum.map_or(0, |_| 8)
                    + point.bucket_counts.len() * slot_bytes::<u64>()
                    + point.explicit_bounds.len() * slot_bytes::<crate::values::Float>()
                    + point.min.map_or(0, |_| 8)
                    + point.max.map_or(0, |_| 8)
                    + 4 // flags
                    + exemplar_bytes(&point.exemplars)
            }
            Self::ExponentialHistogram(point) => {
                STRUCTURE_FIXED_BYTES
                    + point.attributes.accounted_size()
                    + 16 // start + end
                    + 8 // count
                    + point.sum.map_or(0, |_| 8)
                    + 4 // scale
                    + 8 // zero_count
                    + 8 // zero_threshold
                    + point.positive.accounted_size()
                    + point.negative.accounted_size()
                    + point.min.map_or(0, |_| 8)
                    + point.max.map_or(0, |_| 8)
                    + 4 // flags
                    + exemplar_bytes(&point.exemplars)
            }
            Self::Summary(point) => {
                STRUCTURE_FIXED_BYTES
                    + point.attributes.accounted_size()
                    + 16 // start + end
                    + 8 // count
                    + point.sum.map_or(0, |_| 8)
                    + point.quantiles.len() * slot_bytes::<crate::metrics::QuantileValue>()
                    + point
                        .quantiles
                        .iter()
                        .map(Accounted::accounted_size)
                        .sum::<usize>()
                    + 4 // flags
                    + exemplar_bytes(&point.exemplars)
            }
        }
    }
}

impl Accounted for crate::metrics::StreamIdentity {
    /// The identity's own content: the resource, the scope and the metric
    /// name it carries. A stream's points reference this through one shared
    /// allocation (ADR 0008's interning), so whoever charges residency for a
    /// stream charges this **once per resident stream** — never per point —
    /// exactly like the model charges an `Arc`ed payload in full wherever
    /// it appears. `docs/architecture/telemetry-model.md` ("Byte ceilings
    /// count what residency pins") owns that rule.
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + self.resource.accounted_size()
            + self.scope.accounted_size()
            + heap_string_bytes(&self.name)
            + 1 // kind (monotonic included), temporality included
    }
}

impl Accounted for crate::metrics::MetricStream {
    fn accounted_size(&self) -> usize {
        STRUCTURE_FIXED_BYTES
            + heap_string_bytes(&self.identity.name)
            + self
                .description
                .as_ref()
                .map_or(0, |text| heap_string_bytes(text))
            + self.unit.as_ref().map_or(0, |text| heap_string_bytes(text))
            + self.metadata.accounted_size()
            + self.identity.resource.accounted_size()
            + self.identity.scope.accounted_size()
            + 1 // kind, temporality included
            + self.points.len() * slot_bytes::<crate::metrics::MetricPoint>()
            + self
                .points
                .iter()
                .map(Accounted::accounted_size)
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState, TraceStateEntry};
    use crate::logs::LogRecord;
    use crate::metrics::{Exemplar, MetricNumber, MetricPoint, NumberPoint};
    use crate::resources::{InstrumentationScope, Resource};
    use crate::spans::{EmitterDroppedCounts, Span, SpanKind, SpanStatus, SpanStatusCode};
    use crate::values::{Attributes, Float, Value};

    fn attribute(key: &str, text: &str) -> Attributes {
        Attributes::from_pairs(vec![(key.to_owned(), Value::String(text.to_owned()))]).expect("ok")
    }

    fn empty_resource() -> Resource {
        Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    fn empty_scope() -> InstrumentationScope {
        InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    #[test]
    fn scalar_sizes_follow_the_formula() {
        assert_eq!(Value::Bool(true).accounted_size(), 1);
        assert_eq!(Value::Int(-1).accounted_size(), 8);
        assert_eq!(Value::Double(Float::new(1.5)).accounted_size(), 8);
        assert_eq!(Value::String(String::new()).accounted_size(), 0);
        assert_eq!(Value::String("abcde".to_owned()).accounted_size(), 5);
        assert_eq!(Value::Bytes(vec![1, 2, 3]).accounted_size(), 3);
    }

    #[test]
    fn container_charges_cover_the_container_not_just_payloads() {
        // An empty array allocates nothing and is charged nothing.
        let empty = Value::array(Vec::new()).expect("empty is homogeneous");
        assert_eq!(empty.accounted_size(), 0);
        // A two-int array: two doubled value slots plus the payloads.
        let items = Value::array(vec![Value::Int(1), Value::Int(2)]).expect("one kind");
        assert_eq!(
            items.accounted_size(),
            2 * slot_bytes::<Value>() + 8 + 8,
            "array slots are charged doubled to cover growth headroom"
        );
        // A kvlist entry: keyed overhead + key + value.
        let list = Value::kv_list(vec![("key".to_owned(), Value::Int(7))]).expect("ok");
        assert_eq!(list.accounted_size(), KEYED_ENTRY_BYTES + "key".len() + 8);
    }

    #[test]
    fn an_attribute_map_charges_its_node_and_every_entry() {
        let value = Value::String("payload!".to_owned()); // 8 bytes
        assert_eq!(
            attribute_entry_size("service.name", &value),
            KEYED_ENTRY_BYTES + "service.name".len() + 8
        );
        let map = attribute("service.name", "payload!");
        assert_eq!(
            map.accounted_size(),
            ATTRIBUTE_MAP_NODE_BYTES + attribute_entry_size("service.name", &value),
            "one full B-tree node is charged once; the entry pays keyed overhead"
        );
        // An empty map allocates no node and is charged nothing.
        assert_eq!(Attributes::default().accounted_size(), 0);
    }

    #[test]
    fn everything_variable_length_is_counted_on_a_span() {
        let context = TraceContext {
            trace_id: TraceId::from_bytes([1; 16]),
            span_id: SpanId::from_bytes([2; 8]),
            flags: TraceFlags::new(1),
            tracestate: TraceState::from_entries(vec![TraceStateEntry {
                vendor: "vendor".to_owned(), // 6
                value: "v".to_owned(),       // 1
            }]),
        };
        let span = Span {
            parent_span_id: Some(SpanId::from_bytes([3; 8])),
            context: context.clone(),
            name: "op".to_owned(),
            kind: SpanKind::Server,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            resource: crate::resources::Resource {
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            }
            .into(),
            scope: crate::resources::InstrumentationScope {
                name: String::new(),
                version: None,
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            }
            .into(),
            attributes: attribute("k", "vv"),
            emitter_dropped: EmitterDroppedCounts {
                attributes: 1,
                events: 2,
                links: 3,
            },
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: SpanStatusCode::Error,
                message: "boom".to_owned(),
            },
        };
        let expected = STRUCTURE_FIXED_BYTES
            + (STRUCTURE_FIXED_BYTES + 16 + 8 + 4 + (KEYED_ENTRY_BYTES + 6 + 1)) // context
            + 8 // parent
            + heap_string_bytes("op") // name
            + 1 // kind
            + 8 // start
            + 8 // end
            + empty_resource().accounted_size()
            + empty_scope().accounted_size()
            + (ATTRIBUTE_MAP_NODE_BYTES + KEYED_ENTRY_BYTES + "k".len() + 2)
            + 12 // dropped counts
            + (STRUCTURE_FIXED_BYTES + 1 + heap_string_bytes("boom")); // status
        assert_eq!(span.accounted_size(), expected);
        // One more byte of name is one more accounted byte: the count moves
        // with every variable-length part.
        let longer = Span {
            name: "op2".to_owned(),
            ..span
        };
        assert_eq!(longer.accounted_size(), expected + 1);
    }

    #[test]
    fn log_record_accounting_covers_every_variable_field() {
        let bare = LogRecord {
            timestamp_unix_nano: None,
            observed_timestamp_unix_nano: None,
            severity_number: None,
            severity_text: None,
            body: None,
            resource: empty_resource().into(),
            scope: empty_scope().into(),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_id: None,
            span_id: None,
            trace_flags: None,
            event_name: None,
        };
        let bare_expected = STRUCTURE_FIXED_BYTES
            + 4 // dropped count
            + empty_resource().accounted_size()
            + empty_scope().accounted_size();
        assert_eq!(bare.accounted_size(), bare_expected);
        let fuller = LogRecord {
            timestamp_unix_nano: Some(1),
            observed_timestamp_unix_nano: Some(2),
            severity_number: crate::logs::SeverityNumber::try_new(9).ok(),
            severity_text: Some("ERROR".to_owned()),
            body: Some(Value::Int(1)),
            trace_id: Some(TraceId::from_bytes([1; 16])),
            span_id: Some(SpanId::from_bytes([2; 8])),
            trace_flags: Some(TraceFlags::new(1)),
            event_name: Some("evt".to_owned()),
            ..bare
        };
        assert_eq!(
            fuller.accounted_size(),
            bare_expected
                + 8 + 8 // timestamps
                + 1 // severity number
                + heap_string_bytes("ERROR")
                + 8 // body
                + TraceId::LENGTH
                + SpanId::LENGTH
                + 4 // flags
                + heap_string_bytes("evt")
        );
    }

    #[test]
    fn point_accounting_moves_with_exemplars() {
        let point = MetricPoint::Number(NumberPoint::measurement(
            5,
            MetricNumber::int(1),
            Attributes::default(),
            Vec::new(),
        ));
        let bare = point.accounted_size();
        assert_eq!(
            bare,
            STRUCTURE_FIXED_BYTES + 8 + 8 + 4, // shell + time + value + flags
            "an empty attribute map charges nothing"
        );
        let exemplar_unit = Exemplar {
            value: MetricNumber::int(2),
            time_unix_nano: 6,
            filtered_attributes: Attributes::default(),
            trace_id: None,
            span_id: None,
        };
        let with_exemplar = MetricPoint::Number(NumberPoint::measurement(
            5,
            MetricNumber::int(1),
            Attributes::default(),
            vec![exemplar_unit],
        ));
        let exemplar_accounted = STRUCTURE_FIXED_BYTES + 8 + 8; // shell + value + time; no ids sent
        assert_eq!(
            with_exemplar.accounted_size(),
            bare + slot_bytes::<Exemplar>() + exemplar_accounted,
            "the exemplar pays its vector slot doubled plus its own parts"
        );
    }

    #[test]
    fn bucket_and_bound_vectors_pay_doubled_slots() {
        let point = MetricPoint::Histogram(crate::metrics::HistogramPoint {
            attributes: Attributes::default(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 4,
            sum: None,
            bucket_counts: vec![0, 2, 2, 0],
            explicit_bounds: vec![Float::new(1.0), Float::new(5.0)],
            min: None,
            max: None,
            flags: 0,
            exemplars: Vec::new(),
        });
        let expected = STRUCTURE_FIXED_BYTES
            + 16 // start + end
            + 8 // count
            + 4 // flags
            + 4 * slot_bytes::<u64>()
            + 2 * slot_bytes::<Float>();
        assert_eq!(point.accounted_size(), expected);
    }

    #[test]
    fn stream_identity_accounting_covers_resource_scope_and_name() {
        let identity = crate::metrics::StreamIdentity {
            resource: empty_resource(),
            scope: empty_scope(),
            name: "requests".to_owned(),
            kind: crate::metrics::StreamKind::Sum { monotonic: true },
            temporality: Some(crate::metrics::Temporality::Cumulative),
        };
        let expected = STRUCTURE_FIXED_BYTES
            + empty_resource().accounted_size()
            + empty_scope().accounted_size()
            + heap_string_bytes("requests")
            + 1; // kind, temporality included
        assert_eq!(identity.accounted_size(), expected);
        // The count moves with every variable-length identity part: one
        // more name byte, one more accounted byte; an attribute added to
        // the resource charges its full keyed-entry cost.
        let longer = crate::metrics::StreamIdentity {
            name: "requests2".to_owned(),
            ..identity.clone()
        };
        assert_eq!(longer.accounted_size(), expected + 1);
        let attributed = crate::metrics::StreamIdentity {
            resource: Resource {
                attributes: attribute("service.name", "checkout"),
                schema_url: None,
                dropped_attributes_count: 0,
            },
            ..identity
        };
        assert_eq!(
            attributed.accounted_size(),
            expected + (ATTRIBUTE_MAP_NODE_BYTES + KEYED_ENTRY_BYTES + "service.name".len() + 8),
            "the resource's attributes are identity content, charged in full"
        );
    }

    #[test]
    fn stream_accounting_includes_identity_parts_and_points() {
        let stream = crate::metrics::MetricStream::new(
            crate::metrics::StreamIdentity {
                resource: empty_resource(),
                scope: empty_scope(),
                name: "requests".to_owned(),
                kind: crate::metrics::StreamKind::Gauge,
                temporality: None,
            },
            Some("an in-flight gauge".to_owned()),
            Some("1".to_owned()),
            Attributes::default(),
            Vec::new(),
        )
        .expect("a coherent stream");
        let bare = stream.accounted_size();
        assert!(bare > STRUCTURE_FIXED_BYTES + heap_string_bytes("requests"));
        let with_point = crate::metrics::MetricStream {
            points: vec![MetricPoint::Number(NumberPoint::measurement(
                1,
                MetricNumber::int(0),
                Attributes::default(),
                Vec::new(),
            ))],
            ..stream
        };
        assert!(with_point.accounted_size() > bare);
    }
}

/// The measurements behind the accounting constants, pinned where the
/// formula can be checked against them.
///
/// The workspace forbids `unsafe` code, and an allocation-counting probe
/// (a custom `GlobalAlloc`) cannot be written without it — so the probe
/// lives outside this crate (`target/heapprobe/`, not committed) and the
/// numbers it measured are pinned here. It counts *requested* byte sizes
/// (what `GlobalAlloc` sees) on 64-bit std; when a std bump changes
/// `BTreeMap`'s node layout, re-run the probe and update the pins below
/// together with the constants.
#[cfg(test)]
mod honesty {
    use super::*;
    use crate::values::{Attributes, Value};

    /// Builds an attribute map with `count` tiny entries (`"k<index>"` →
    /// integer), in ascending or descending key order — ascending is the
    /// worst case, leaving B-tree nodes half empty after splits.
    fn tiny_attribute_map(count: usize, descending: bool) -> Attributes {
        let pairs: Vec<(String, Value)> = (0..count)
            .map(|index| {
                (
                    format!("k{index}"),
                    Value::Int(i64::try_from(index).expect("fits")),
                )
            })
            .collect();
        let pairs = if descending {
            pairs.into_iter().rev().collect()
        } else {
            pairs
        };
        Attributes::from_pairs(pairs).expect("unique keys")
    }

    /// The honest ceiling: accounted never under-counts real heap, and the
    /// over-approximation never runs away from it.
    fn assert_honest(shape: &str, real: usize, accounted: usize) {
        assert!(
            accounted >= real,
            "{shape}: accounted {accounted} under-counts the measured real heap \
             ({real}) — the formula is now a lie"
        );
        assert!(
            accounted <= 4 * real,
            "{shape}: accounted {accounted} ran away from the measured real heap \
             ({real}) — the over-approximation became fiction"
        );
    }

    #[test]
    fn the_constants_cover_the_measured_adversarial_shapes() {
        // Probe measurements (counting allocator, requested sizes, 64-bit
        // std), the shapes that drove the constants:
        // - one-entry attribute map: one whole B-tree node, 545 B;
        // - 256 tiny entries, ascending insertion (worst occupancy): 23,780 B
        //   (descending: 23,684 B);
        // - one-entry key-value list: 50 B; one-int array: 24 B.
        // Under the old flat formula (16 B per structure, no node charge)
        // the 256-entry map accounted 16 * 256 + payloads ≈ 1.6 kB against
        // 23.8 kB real — a 14.8× lie. These pins are what the constants
        // must never regress below.
        let one_entry = tiny_attribute_map(1, false);
        assert_honest("1-entry map", 545, one_entry.accounted_size());

        let mut worst_256 = 0;
        for descending in [false, true] {
            let map = tiny_attribute_map(256, descending);
            worst_256 = worst_256.max(map.accounted_size());
        }
        assert_honest("256-entry map (worst order)", 23_780, worst_256);

        let single_list = Value::kv_list(vec![("k".to_owned(), Value::Int(0))]).expect("ok");
        assert_honest("1-entry kvlist", 50, single_list.accounted_size());

        let single_array = Value::array(vec![Value::Int(0)]).expect("one kind");
        assert_honest("1-int array", 24, single_array.accounted_size());
    }

    #[test]
    fn the_worst_measured_amplification_stays_under_the_ceiling() {
        // real ÷ accounted on the worst measured shape must stay under the
        // ~1.5× amplification ceiling the constants were tuned for. The
        // formula over-counts by design (the ceiling is a floor on
        // accounted, not an equality), so the honest assertion is the
        // direction: accounted is within 1.5× *of and above* real.
        let map = tiny_attribute_map(256, false);
        let accounted = map.accounted_size();
        let real = 23_780;
        assert!(
            accounted >= real,
            "under-count on the worst shape: accounted {accounted}, real {real}"
        );
        let ratio = f64::from(u32::try_from(real).expect("fits"))
            / f64::from(u32::try_from(accounted).expect("fits"));
        assert!(
            ratio <= 1.5,
            "the formula stopped over-approximating: real÷accounted = {ratio:.2}"
        );
        assert!(
            0.0 < ratio && ratio < 1.0,
            "the formula should still over-count: real÷accounted = {ratio:.2}"
        );
    }

    #[test]
    fn empty_containers_charge_nothing_and_named_strings_carry_a_chunk() {
        assert_eq!(Attributes::default().accounted_size(), 0);
        let empty_array = Value::array(Vec::new()).expect("empty is homogeneous");
        assert_eq!(empty_array.accounted_size(), 0);
        // A named string field pays payload + allocation chunk.
        assert_eq!(heap_string_bytes(""), STRING_ALLOCATION_CHUNK_BYTES);
        assert_eq!(heap_string_bytes("abc"), STRING_ALLOCATION_CHUNK_BYTES + 3);
    }
}
