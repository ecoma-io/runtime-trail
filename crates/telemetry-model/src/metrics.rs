//! Metric streams and data points.
//!
//! `docs/architecture/telemetry-model.md`, "Metrics": five OTLP kinds as
//! five distinct model-level shapes, the monotonicity flag preserved,
//! temporality per stream with delta and cumulative never merged or
//! converted, per-point attribute sets with `start_time` and `time`, the
//! OTLP staleness flags preserved per point, and exemplars carrying value,
//! timestamp, filtered attributes and the trace ids they were sent with.
//! Histogram layouts are preserved exactly — never re-bucketed.
//!
//! A gauge's `start_time_unix_nano` is ignored by the spec's semantics but
//! producers are encouraged to set it, so the model preserves it when sent
//! and keeps it out of gauge point identity: gauge identity is
//! `(stream, time)`, and two gauges differing only in `start_time` are the
//! same point.

use crate::context::{SpanId, TraceId};
use crate::resources::{InstrumentationScope, Resource};
use crate::values::{Attributes, Float};
use std::fmt;
use std::sync::Arc;

/// A metric number: 64-bit integer or 64-bit float, distinct and never
/// interconverted. Doubles compare by bit pattern (see [`Float`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetricNumber {
    Int(i64),
    Double(Float),
}

impl MetricNumber {
    /// The int variant, as sent.
    #[must_use]
    pub const fn int(value: i64) -> Self {
        Self::Int(value)
    }

    /// The double variant, as sent.
    #[must_use]
    pub const fn double(value: f64) -> Self {
        Self::Double(Float::new(value))
    }
}

/// The one interpreted bit of a data point's flags: OTLP's
/// `DATA_POINT_FLAG_NO_RECORDED_VALUE`, the staleness marker (a
/// Prometheus-style "no value recorded" tombstone). All 32 bits are
/// preserved verbatim on every point; no other bit is interpreted.
pub const DATA_POINT_FLAG_NO_RECORDED_VALUE: u32 = 1;

/// The stream kind — the five OTLP kinds are five distinct shapes.
///
/// No kind is collapsed into another; a sum's monotonicity flag is part of
/// the kind and is never inferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StreamKind {
    Gauge,
    Sum {
        /// Whether the sum is monotonic, as the emitter declared it.
        monotonic: bool,
    },
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl StreamKind {
    /// The point shape this kind carries.
    #[must_use]
    pub const fn point_shape(self) -> PointShape {
        match self {
            Self::Gauge | Self::Sum { .. } => PointShape::Number,
            Self::Histogram => PointShape::Histogram,
            Self::ExponentialHistogram => PointShape::ExponentialHistogram,
            Self::Summary => PointShape::Summary,
        }
    }

    /// Whether this kind carries an interval start on every point. Gauges
    /// do not require one — but a gauge that sends one has it preserved
    /// (outside identity), never refused.
    #[must_use]
    pub const fn requires_start_time(self) -> bool {
        !matches!(self, Self::Gauge)
    }
}

/// The shape of a data point, paired one-to-one with a [`StreamKind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PointShape {
    Number,
    Histogram,
    ExponentialHistogram,
    Summary,
}

/// The temporality of a data point stream: delta or cumulative.
///
/// Temporality is per stream, and a delta stream and a cumulative stream
/// with the same name are different series — never merged, split, or
/// converted. Conversion is a transformation pipeline, which the product's
/// non-goals exclude.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Temporality {
    Delta,
    Cumulative,
}

/// One metric exemplar: value, timestamp, filtered attributes, and the
/// trace ids the emitter attached — the model-level hook for
/// metrics↔trace correlation, preserved exactly as sent.
///
/// OTLP's exemplar carries `trace_id` and `span_id` as independently
/// optional fields, and nothing else (no flags, no tracestate): so does
/// this model. Neither id is ever fabricated when absent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Exemplar {
    /// The exemplar's value, as sent.
    pub value: MetricNumber,
    /// The exemplar's timestamp, in nanoseconds on the emitter's clock.
    pub time_unix_nano: u64,
    /// The attributes filtered out of the measurement, as sent.
    pub filtered_attributes: Attributes,
    /// The exemplar's trace id, when the emitter sent one.
    pub trace_id: Option<TraceId>,
    /// The exemplar's span id, when the emitter sent one.
    pub span_id: Option<SpanId>,
}

impl Exemplar {
    /// The correlation pair, only when the exemplar carries both ids.
    #[must_use]
    pub fn correlation_pair(&self) -> Option<(TraceId, SpanId)> {
        match (self.trace_id, self.span_id) {
            (Some(trace_id), Some(span_id)) => Some((trace_id, span_id)),
            _ => None,
        }
    }
}

/// A gauge or sum data point: an attribute set, `start_time`, `time`, a
/// number, the point's flags, and exemplars.
///
/// Build gauge points with [`NumberPoint::measurement`] (start absent) or
/// [`NumberPoint::measurement_with_start`] (start sent and preserved,
/// outside gauge identity), and interval points with
/// [`NumberPoint::interval`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NumberPoint {
    /// The point's attribute set.
    pub attributes: Attributes,
    /// Interval start as sent; `None` when the emitter sent none. For a
    /// gauge this is preserved metadata outside identity; for an interval
    /// kind it is part of the point's identity.
    pub start_time_unix_nano: Option<u64>,
    /// Interval end or measurement time, in nanoseconds on the emitter's
    /// clock.
    pub time_unix_nano: u64,
    /// The number, int or double as sent.
    pub value: MetricNumber,
    /// The point's flags as sent (OTLP `DataPointFlags`); bit 0 is
    /// [`DATA_POINT_FLAG_NO_RECORDED_VALUE`], the staleness marker. All 32
    /// bits preserved; a flagged point and an unflagged zero are different
    /// points.
    pub flags: u32,
    /// Exemplars attached to this point, in the order the emitter sent
    /// them.
    pub exemplars: Vec<Exemplar>,
}

impl NumberPoint {
    /// A gauge measurement with no start time sent.
    #[must_use]
    pub fn measurement(
        time_unix_nano: u64,
        value: MetricNumber,
        attributes: Attributes,
        exemplars: Vec<Exemplar>,
    ) -> Self {
        Self {
            attributes,
            start_time_unix_nano: None,
            time_unix_nano,
            value,
            flags: 0,
            exemplars,
        }
    }

    /// A gauge measurement that *did* send a start time: preserved
    /// verbatim, never refused, and never part of gauge identity.
    #[must_use]
    pub fn measurement_with_start(
        start_time_unix_nano: u64,
        time_unix_nano: u64,
        value: MetricNumber,
        attributes: Attributes,
        exemplars: Vec<Exemplar>,
    ) -> Self {
        Self {
            attributes,
            start_time_unix_nano: Some(start_time_unix_nano),
            time_unix_nano,
            value,
            flags: 0,
            exemplars,
        }
    }

    /// An interval point (a sum, histogram bucket interval, or summary
    /// window): `start_time` present.
    #[must_use]
    pub fn interval(
        start_time_unix_nano: u64,
        time_unix_nano: u64,
        value: MetricNumber,
        attributes: Attributes,
        exemplars: Vec<Exemplar>,
    ) -> Self {
        Self {
            attributes,
            start_time_unix_nano: Some(start_time_unix_nano),
            time_unix_nano,
            value,
            flags: 0,
            exemplars,
        }
    }
}

/// An explicit-bucket histogram data point, preserved exactly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HistogramPoint {
    /// The point's attribute set.
    pub attributes: Attributes,
    /// Interval start, as sent.
    pub start_time_unix_nano: u64,
    /// Interval end, as sent.
    pub time_unix_nano: u64,
    /// The count of measurements in the interval.
    pub count: u64,
    /// The sum of measurements, when the emitter sent one.
    pub sum: Option<Float>,
    /// Bucket counts, as sent — never re-bucketed.
    pub bucket_counts: Vec<u64>,
    /// Explicit bucket bounds, as sent — never re-bucketed. Bit-pattern
    /// floats (see [`Float`]), so identity stays total over them.
    pub explicit_bounds: Vec<Float>,
    /// The interval minimum, when the emitter sent one.
    pub min: Option<Float>,
    /// The interval maximum, when the emitter sent one.
    pub max: Option<Float>,
    /// The point's flags as sent; see [`NumberPoint::flags`].
    pub flags: u32,
    /// Exemplars attached to this point, in the order the emitter sent
    /// them.
    pub exemplars: Vec<Exemplar>,
}

/// One explicit-bucket layout of an exponential histogram, as sent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ExponentialBuckets {
    /// The bucket index of the first count, as sent.
    pub offset: i32,
    /// The bucket counts, as sent — never re-bucketed.
    pub bucket_counts: Vec<u64>,
}

/// An exponential histogram data point: scale, zero count, zero threshold
/// and both bucket layouts preserved exactly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ExponentialHistogramPoint {
    /// The point's attribute set.
    pub attributes: Attributes,
    /// Interval start, as sent.
    pub start_time_unix_nano: u64,
    /// Interval end, as sent.
    pub time_unix_nano: u64,
    /// The count of measurements in the interval.
    pub count: u64,
    /// The sum of measurements, when the emitter sent one.
    pub sum: Option<Float>,
    /// The scale, as sent.
    pub scale: i32,
    /// The zero count, as sent.
    pub zero_count: u64,
    /// The zero threshold, as sent, as a bit-pattern float (see
    /// [`Float`]).
    pub zero_threshold: Float,
    /// The positive bucket layout, as sent.
    pub positive: ExponentialBuckets,
    /// The negative bucket layout, as sent.
    pub negative: ExponentialBuckets,
    /// The interval minimum, when the emitter sent one.
    pub min: Option<Float>,
    /// The interval maximum, when the emitter sent one.
    pub max: Option<Float>,
    /// The point's flags as sent; see [`NumberPoint::flags`].
    pub flags: u32,
    /// Exemplars attached to this point, in the order the emitter sent
    /// them.
    pub exemplars: Vec<Exemplar>,
}

/// One quantile of a summary, as sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QuantileValue {
    /// The quantile, as sent.
    pub quantile: Float,
    /// The quantile's value, as sent.
    pub value: Float,
}

/// A summary data point: quantiles, count and sum, as sent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SummaryPoint {
    /// The point's attribute set.
    pub attributes: Attributes,
    /// Window start, as sent.
    pub start_time_unix_nano: u64,
    /// Window end, as sent.
    pub time_unix_nano: u64,
    /// The count of observations in the window.
    pub count: u64,
    /// The sum of observations, when the emitter sent one.
    pub sum: Option<Float>,
    /// The quantiles, in the order the emitter sent them.
    pub quantiles: Vec<QuantileValue>,
    /// The point's flags as sent; see [`NumberPoint::flags`].
    pub flags: u32,
    /// Exemplars attached to this point, in the order the emitter sent
    /// them.
    pub exemplars: Vec<Exemplar>,
}

/// A data point — one of the four point shapes the five stream kinds
/// carry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MetricPoint {
    Number(NumberPoint),
    Histogram(HistogramPoint),
    ExponentialHistogram(ExponentialHistogramPoint),
    Summary(SummaryPoint),
}

impl MetricPoint {
    /// The point's shape; a stream's kind must match every point it
    /// carries.
    #[must_use]
    pub const fn shape(&self) -> PointShape {
        match self {
            Self::Number(_) => PointShape::Number,
            Self::Histogram(_) => PointShape::Histogram,
            Self::ExponentialHistogram(_) => PointShape::ExponentialHistogram,
            Self::Summary(_) => PointShape::Summary,
        }
    }

    /// The point's attribute set, whichever shape it carries.
    #[must_use]
    pub fn attributes(&self) -> &Attributes {
        match self {
            Self::Number(point) => &point.attributes,
            Self::Histogram(point) => &point.attributes,
            Self::ExponentialHistogram(point) => &point.attributes,
            Self::Summary(point) => &point.attributes,
        }
    }

    /// The interval start. For a gauge measurement this is whatever the
    /// emitter sent (`None` when none) — identity reads a gauge's start
    /// time as absent regardless; see [`PointIdentity`].
    #[must_use]
    pub const fn start_time_unix_nano(&self) -> Option<u64> {
        match self {
            Self::Number(point) => point.start_time_unix_nano,
            Self::Histogram(point) => Some(point.start_time_unix_nano),
            Self::ExponentialHistogram(point) => Some(point.start_time_unix_nano),
            Self::Summary(point) => Some(point.start_time_unix_nano),
        }
    }

    /// The interval end or measurement time.
    #[must_use]
    pub const fn time_unix_nano(&self) -> u64 {
        match self {
            Self::Number(point) => point.time_unix_nano,
            Self::Histogram(point) => point.time_unix_nano,
            Self::ExponentialHistogram(point) => point.time_unix_nano,
            Self::Summary(point) => point.time_unix_nano,
        }
    }

    /// The point's flags as sent (OTLP `DataPointFlags`); bit 0 is
    /// [`DATA_POINT_FLAG_NO_RECORDED_VALUE`]. The flags participate in the
    /// point's identity: a staleness-marker point and a real zero are
    /// different points.
    #[must_use]
    pub const fn flags(&self) -> u32 {
        match self {
            Self::Number(point) => point.flags,
            Self::Histogram(point) => point.flags,
            Self::ExponentialHistogram(point) => point.flags,
            Self::Summary(point) => point.flags,
        }
    }

    /// The point's exemplars, whichever shape it carries.
    #[must_use]
    pub fn exemplars(&self) -> &[Exemplar] {
        match self {
            Self::Number(point) => &point.exemplars,
            Self::Histogram(point) => &point.exemplars,
            Self::ExponentialHistogram(point) => &point.exemplars,
            Self::Summary(point) => &point.exemplars,
        }
    }

    /// Byte-exact equality over the fields the point's *identity* reads,
    /// under `kind`.
    ///
    /// This is the comparison duplicate detection uses, not plain `==`: a
    /// gauge's `start_time` is outside gauge identity (the spec's
    /// semantics ignore it), so two gauges differing only there are the
    /// same point and must collapse, not conflict. For every other kind
    /// this is plain byte equality.
    #[must_use]
    pub fn identity_payload_eq(&self, other: &Self, kind: StreamKind) -> bool {
        if kind == StreamKind::Gauge {
            let (Self::Number(a), Self::Number(b)) = (self, other) else {
                return false; // unreachable under the shape law
            };
            return a.attributes == b.attributes
                && a.time_unix_nano == b.time_unix_nano
                && a.value == b.value
                && a.flags == b.flags
                && a.exemplars == b.exemplars;
        }
        self == other
    }
}

/// A stream was offered whose kind and point shapes do not agree, whose
/// interval start times do not agree with the kind, or whose temporality
/// does not agree with the kind. Invalid input: rejected, not coerced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamShapeError {
    /// A point does not carry the shape the stream kind requires.
    KindShapeMismatch {
        /// Position of the offending point.
        index: usize,
        /// The shape the stream kind requires.
        expected: PointShape,
        /// The shape the point carries.
        found: PointShape,
    },
    /// An interval-kind point was offered without an interval start. (A
    /// gauge point with a start time is *not* an error: it is preserved
    /// metadata outside gauge identity.)
    IntervalKindMissingStartTime {
        /// Position of the offending point.
        index: usize,
    },
    /// A temporality was offered that the stream kind cannot carry.
    TemporalityMismatch {
        /// The stream kind.
        kind: StreamKind,
        /// The offered temporality.
        temporality: Option<Temporality>,
    },
}

impl fmt::Display for StreamShapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::KindShapeMismatch {
                index,
                expected,
                found,
            } => write!(
                f,
                "point {index} carries {found:?} where the stream kind requires {expected:?}"
            ),
            Self::IntervalKindMissingStartTime { index } => {
                write!(f, "interval point {index} is missing its start_time")
            }
            Self::TemporalityMismatch { kind, temporality } => {
                write!(
                    f,
                    "stream kind {kind:?} cannot carry temporality {temporality:?}"
                )
            }
        }
    }
}

impl std::error::Error for StreamShapeError {}

/// The identity of a data point stream: resource, scope, name, description,
/// unit, metadata, kind, temporality. A delta stream and a cumulative
/// stream with the same name have different identities; so do two kinds,
/// two resources, two scopes — and so do two instruments that differ only
/// in their description, unit or metadata. The descriptor is part of the
/// identity because the contract preserves it verbatim (`absent is not
/// empty`): two same-named metrics that differ there are two distinct
/// streams, never a silent merge, and a re-delivery that differs there is
/// a stream-identity conflict recorded by the admission ledger — the first
/// descriptor stands.
///
/// The resource's identity is its attribute map: `schema_url` is preserved
/// metadata and never participates (see [`crate::resources::Resource`]).
///
/// The derived [`Ord`] orders by the same content the derived
/// [`PartialEq`] compares — resource, scope, name, descriptor, kind,
/// temporality — the total order identity keys need for ordered, never
/// hashed, maps (ADR 0008). It is a comparison order over identity bytes,
/// not a ranking of streams.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamIdentity {
    /// The stream's resource.
    pub resource: Resource,
    /// The stream's scope.
    pub scope: InstrumentationScope,
    /// The metric name, verbatim.
    pub name: String,
    /// The metric description, as sent; `None` is absent and distinct from
    /// empty. Part of the identity: it describes the stream and is
    /// preserved verbatim, so two instruments differing only here are two
    /// streams.
    pub description: Option<String>,
    /// The unit, opaque at the model level — no unit grammar is
    /// interpreted here; `None` is absent and distinct from empty. Part of
    /// the identity, like the description.
    pub unit: Option<String>,
    /// The metric's metadata (OTLP `Metric.metadata`), as sent: an
    /// attribute map whose duplicate keys were refused at construction
    /// like every other keyed container. Part of the identity.
    pub metadata: Attributes,
    /// The stream kind, monotonicity included.
    pub kind: StreamKind,
    /// The stream temporality; `None` where the kind carries none.
    pub temporality: Option<Temporality>,
}

impl StreamIdentity {
    /// The description, as sent; `None` is absent and distinct from empty.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// The unit, as sent; `None` is absent and distinct from empty.
    #[must_use]
    pub fn unit(&self) -> Option<&str> {
        self.unit.as_deref()
    }

    /// The metric's metadata, as sent.
    #[must_use]
    pub const fn metadata(&self) -> &Attributes {
        &self.metadata
    }
}

/// The collapse identity of one data point: the stream it belongs to, the
/// point's own attribute set, its interval — `start_time` and `time` — and
/// the point's flags. A re-delivered point under one of these keys is the
/// same data point, whatever entity id it was assigned on first delivery.
///
/// The flags participate: a staleness-marker point and a real zero are
/// wire-byte-different, so they are identity-different points. A gauge's
/// `start_time` does *not* participate — gauge identity is
/// `(stream, time)` per the spec's semantics, so this key carries a
/// gauge's start as `None` however the point was sent.
///
/// # Sharing, ordering, never hashing (ADR 0008)
///
/// Both payloads live here **once**, as `Arc`s shared with the ledger's
/// tables and with storage: the key holds the record, the record holds its
/// attribute set — nothing is copied into the key. The stream is the
/// interned `Arc<StreamIdentity>` every point key of the stream shares, so
/// a thousand points of one stream cost one stream payload and ten
/// thousand points cost ten thousand record payloads and zero key copies.
///
/// # The projected comparison: the descriptor is deliberately blind
///
/// [`PartialEq`], [`Eq`], [`PartialOrd`] and [`Ord`] are **manual**, and
/// they project only the *`OTel` collapse key*: the stream's resource
/// (attribute map), scope (full field set), name, kind and temporality —
/// **never the stream's descriptor** (description, unit, metadata) — plus
/// the point's attribute set, gauge-normalised `start_time`, `time`, and
/// flags. The point's *payload* (its value and exemplars) is projected out
/// just as deliberately: it is compared separately, kind-aware, by the
/// ledger's collapse decision (`MetricPoint::identity_payload_eq`), so a
/// key can look up a standing entry whose payload then decides collapse
/// versus conflict.
///
/// Descriptor-blindness is the law that makes a re-delivery under a
/// changed descriptor land on the *same* key: the ledger finds the
/// standing point, sees a different stream identity there, and records a
/// stream-identity conflict — first descriptor standing — instead of
/// silently admitting a parallel stream. A *different* point under the
/// changed descriptor projects to a different key and admits as the second
/// stream it is. The descriptor difference is still byte-exact data: it
/// lives on the interned stream identities, which the ledger compares in
/// full.
///
/// The projections skip exactly the fields the descriptor owns, and
/// `a == b` holds exactly when `a.cmp(b)` is [`Ordering::Equal`]. There is
/// no `Hash`: keys live in ordered maps keyed by content, and a partial
/// equality must never be hashed (ADR 0008).
#[derive(Clone, Debug)]
pub struct PointIdentity {
    /// The stream the point belongs to — the interned identity, shared
    /// with the ledger's stream table and with storage. Compared by full
    /// byte-exact identity in the ledger's conflict decision, and by its
    /// descriptor-less `OTel` key in this key's own ordering.
    pub stream: Arc<StreamIdentity>,
    /// The point itself, as admitted — shared, never copied. The key
    /// projects its attribute set, gauge-normalised start, time and flags;
    /// its payload stays out of the ordering for the ledger's kind-aware
    /// comparison.
    pub point: Arc<MetricPoint>,
    /// The interval start; `None` for gauge points (and for gauges it is
    /// `None` however the point was sent).
    pub start_time_unix_nano: Option<u64>,
    /// The interval end or measurement time.
    pub time_unix_nano: u64,
    /// The point's flags as sent; part of the identity.
    pub flags: u32,
}

impl PointIdentity {
    /// Builds the identity of `point` in `stream`, sharing both payloads.
    ///
    /// A gauge point's start time is normalised away here: two gauges
    /// differing only in `start_time` carry the same identity key.
    #[must_use]
    pub fn of_interned(stream: &Arc<StreamIdentity>, point: &Arc<MetricPoint>) -> Self {
        let start = if stream.kind.requires_start_time() {
            point.start_time_unix_nano()
        } else {
            None
        };
        Self {
            stream: Arc::clone(stream),
            point: Arc::clone(point),
            start_time_unix_nano: start,
            time_unix_nano: point.time_unix_nano(),
            flags: point.flags(),
        }
    }

    /// The descriptor-less `OTel` key of the stream side, in comparison
    /// order: resource attributes, scope, name, kind, temporality. The
    /// descriptor — description, unit, metadata — is skipped on purpose:
    /// this projection is what routes a re-delivery under a changed
    /// descriptor onto the standing key so the ledger can name the
    /// conflict (see the type's law above).
    fn projected_stream_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.stream
            .resource
            .cmp(&other.stream.resource)
            .then_with(|| self.stream.scope.cmp(&other.stream.scope))
            .then_with(|| self.stream.name.cmp(&other.stream.name))
            .then_with(|| self.stream.kind.cmp(&other.stream.kind))
            .then_with(|| self.stream.temporality.cmp(&other.stream.temporality))
    }
}

impl PartialEq for PointIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for PointIdentity {}

impl PartialOrd for PointIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PointIdentity {
    /// The projected collapse key: the descriptor-less stream key, then
    /// the point's attribute set, gauge-normalised start, time, flags.
    /// The descriptor and the point payload are projected out — see the
    /// type's law.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.projected_stream_cmp(other)
            .then_with(|| self.point.attributes().cmp(other.point.attributes()))
            .then_with(|| self.start_time_unix_nano.cmp(&other.start_time_unix_nano))
            .then_with(|| self.time_unix_nano.cmp(&other.time_unix_nano))
            .then_with(|| self.flags.cmp(&other.flags))
    }
}

/// One metric stream: its identity — name, description, unit and metadata
/// verbatim (absent is not empty), resource, scope, kind, temporality —
/// and its points. The descriptor lives on the identity: it is identity
/// content, interning, accounting and collapsing with the stream.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetricStream {
    /// The stream identity: name, description, unit, metadata, resource,
    /// scope, kind, temporality.
    pub identity: StreamIdentity,
    /// The stream's points, in the order the emitter sent them.
    pub points: Vec<MetricPoint>,
}

impl MetricStream {
    /// Builds a stream, rejecting a shape the contract cannot represent:
    /// points whose shape disagrees with the kind, interval points without
    /// a start time, and temporalities the kind cannot carry.
    ///
    /// # Errors
    ///
    /// Returns [`StreamShapeError`] naming the first violating point (or
    /// the kind/temporality pair). Nothing is coerced or re-bucketed.
    pub fn new(
        identity: StreamIdentity,
        points: Vec<MetricPoint>,
    ) -> Result<Self, StreamShapeError> {
        let kind = identity.kind;
        let expected_temporality = match kind {
            StreamKind::Gauge | StreamKind::Summary => None,
            StreamKind::Sum { .. } | StreamKind::Histogram | StreamKind::ExponentialHistogram => {
                Some(())
            }
        };
        if expected_temporality.is_some() != identity.temporality.is_some() {
            return Err(StreamShapeError::TemporalityMismatch {
                kind,
                temporality: identity.temporality,
            });
        }
        for (index, point) in points.iter().enumerate() {
            Self::check_point_coherence(&identity, point, index)?;
        }
        Ok(Self { identity, points })
    }

    /// The shape law for one `(identity, point)` pair — the same law
    /// [`MetricStream::new`] enforces, exposed for the admission ledger so
    /// a point cannot bypass it by arriving without its stream.
    ///
    /// # Errors
    ///
    /// Returns [`StreamShapeError`] naming the violation.
    pub fn check_point_coherence(
        identity: &StreamIdentity,
        point: &MetricPoint,
        index: usize,
    ) -> Result<(), StreamShapeError> {
        let kind = identity.kind;
        let expected = kind.point_shape();
        let found = point.shape();
        if expected != found {
            return Err(StreamShapeError::KindShapeMismatch {
                index,
                expected,
                found,
            });
        }
        if kind.requires_start_time() && point.start_time_unix_nano().is_none() {
            return Err(StreamShapeError::IntervalKindMissingStartTime { index });
        }
        Ok(())
    }

    /// The kind/temporality law for one identity — part of the same shape
    /// law, exposed for the admission ledger.
    ///
    /// # Errors
    ///
    /// Returns [`StreamShapeError::TemporalityMismatch`] when the kind
    /// cannot carry the identity's temporality.
    pub fn check_identity_coherence(identity: &StreamIdentity) -> Result<(), StreamShapeError> {
        let expected_temporality = match identity.kind {
            StreamKind::Gauge | StreamKind::Summary => None,
            StreamKind::Sum { .. } | StreamKind::Histogram | StreamKind::ExponentialHistogram => {
                Some(())
            }
        };
        if expected_temporality.is_some() != identity.temporality.is_some() {
            return Err(StreamShapeError::TemporalityMismatch {
                kind: identity.kind,
                temporality: identity.temporality,
            });
        }
        Ok(())
    }

    /// The stream identity — the collapse key every one of its points
    /// shares.
    #[must_use]
    pub const fn identity(&self) -> &StreamIdentity {
        &self.identity
    }

    /// The metric name, verbatim.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.identity.name
    }

    /// The description, as sent; `None` is absent and distinct from empty.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.identity.description()
    }

    /// The unit, as sent; `None` is absent and distinct from empty.
    #[must_use]
    pub fn unit(&self) -> Option<&str> {
        self.identity.unit()
    }

    /// The metric's metadata, as sent.
    #[must_use]
    pub const fn metadata(&self) -> &Attributes {
        self.identity.metadata()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Value;

    fn resource() -> Resource {
        Resource {
            attributes: Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String("checkout".to_owned()),
            )])
            .expect("ok"),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    fn scope() -> InstrumentationScope {
        InstrumentationScope {
            name: "scope".to_owned(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    fn attributes() -> Attributes {
        Attributes::default()
    }

    fn exemplar() -> Exemplar {
        Exemplar {
            value: MetricNumber::int(3),
            time_unix_nano: 12,
            filtered_attributes: attributes(),
            trace_id: Some(TraceId::from_bytes([4; 16])),
            span_id: Some(SpanId::from_bytes([5; 8])),
        }
    }

    fn identity(kind: StreamKind, temporality: Option<Temporality>) -> StreamIdentity {
        StreamIdentity {
            resource: resource(),
            scope: scope(),
            name: "m".to_owned(),
            description: Some(String::new()),
            unit: Some("ms".to_owned()),
            metadata: Attributes::default(),
            kind,
            temporality,
        }
    }

    fn stream(
        kind: StreamKind,
        temporality: Option<Temporality>,
        points: Vec<MetricPoint>,
    ) -> MetricStream {
        let mut id = identity(kind, temporality);
        id.name = "requests".to_owned();
        MetricStream::new(id, points).expect("a coherent stream")
    }

    #[test]
    fn the_five_kinds_are_distinct_and_the_monotonic_flag_is_part_of_the_kind() {
        let kinds = [
            StreamKind::Gauge,
            StreamKind::Sum { monotonic: true },
            StreamKind::Sum { monotonic: false },
            StreamKind::Histogram,
            StreamKind::ExponentialHistogram,
            StreamKind::Summary,
        ];
        for (index, first) in kinds.iter().enumerate() {
            for second in kinds.iter().skip(index + 1) {
                assert_ne!(first, second, "no kind collapses into another");
            }
        }
    }

    #[test]
    fn delta_and_cumulative_streams_with_one_name_are_different_series() {
        let delta = StreamIdentity {
            resource: resource(),
            scope: scope(),
            name: "requests".to_owned(),
            description: None,
            unit: None,
            metadata: Attributes::default(),
            kind: StreamKind::Sum { monotonic: true },
            temporality: Some(Temporality::Delta),
        };
        let cumulative = StreamIdentity {
            temporality: Some(Temporality::Cumulative),
            ..delta.clone()
        };
        assert_ne!(delta, cumulative, "delta ≠ cumulative: never merged");
        assert_eq!(delta, delta.clone());
    }

    #[test]
    fn point_values_are_int_or_double_never_interconverted() {
        let int_point = MetricPoint::Number(NumberPoint::measurement(
            1,
            MetricNumber::int(2),
            attributes(),
            Vec::new(),
        ));
        let double_point = MetricPoint::Number(NumberPoint::measurement(
            1,
            MetricNumber::double(2.0),
            attributes(),
            Vec::new(),
        ));
        assert_ne!(int_point, double_point);
        assert_eq!(
            MetricNumber::double(f64::NAN),
            MetricNumber::double(f64::NAN),
            "NaN is a preserved value"
        );
    }

    #[test]
    fn a_gauge_point_may_carry_no_start_time_or_a_preserved_one() {
        let bare = NumberPoint::measurement(50, MetricNumber::int(1), attributes(), Vec::new());
        assert_eq!(bare.start_time_unix_nano, None);
        let sent = NumberPoint::measurement_with_start(
            10,
            50,
            MetricNumber::int(1),
            attributes(),
            Vec::new(),
        );
        assert_eq!(
            sent.start_time_unix_nano,
            Some(10),
            "a gauge's start_time is preserved when sent, never refused"
        );
        // Out of gauge identity: the same point for collapse purposes.
        assert!(
            MetricPoint::Number(bare)
                .identity_payload_eq(&MetricPoint::Number(sent), StreamKind::Gauge),
            "start_time is outside gauge identity"
        );
        assert_eq!(
            MetricPoint::Number(NumberPoint::interval(
                10,
                50,
                MetricNumber::int(1),
                attributes(),
                Vec::new()
            )),
            MetricPoint::Number(NumberPoint::measurement_with_start(
                10,
                50,
                MetricNumber::int(1),
                attributes(),
                Vec::new()
            )),
            "a number point carries no kind: the stream identity does"
        );
    }

    #[test]
    fn gauge_identity_is_stream_and_time_by_the_point_identity() {
        let kind = StreamKind::Gauge;
        let bare = NumberPoint::measurement(50, MetricNumber::int(1), attributes(), Vec::new());
        let with_start = NumberPoint::measurement_with_start(
            10,
            50,
            MetricNumber::int(1),
            attributes(),
            Vec::new(),
        );
        let stream_id = Arc::new(identity(kind, None));
        let bare_key = PointIdentity::of_interned(&stream_id, &Arc::new(MetricPoint::Number(bare)));
        let started_key =
            PointIdentity::of_interned(&stream_id, &Arc::new(MetricPoint::Number(with_start)));
        assert_eq!(
            bare_key, started_key,
            "gauge identity is (stream, time): start_time is normalised out"
        );
        assert_eq!(bare_key.start_time_unix_nano, None);
        // And the sharing is real: both keys reference the same interned
        // stream payload, and each key carries its own point — once.
        assert!(Arc::ptr_eq(&bare_key.stream, &started_key.stream));
        assert_eq!(Arc::strong_count(&stream_id), 3); // handle + two keys
    }

    #[test]
    fn staleness_flags_are_part_of_point_identity() {
        let zero = NumberPoint::measurement(50, MetricNumber::int(0), attributes(), Vec::new());
        let stale = NumberPoint {
            flags: DATA_POINT_FLAG_NO_RECORDED_VALUE,
            ..zero.clone()
        };
        assert_eq!(zero.flags, 0);
        assert_eq!(stale.flags, DATA_POINT_FLAG_NO_RECORDED_VALUE);
        let stream_id = Arc::new(identity(StreamKind::Gauge, None));
        let zero_key = PointIdentity::of_interned(&stream_id, &Arc::new(MetricPoint::Number(zero)));
        let stale_key =
            PointIdentity::of_interned(&stream_id, &Arc::new(MetricPoint::Number(stale)));
        assert_ne!(
            zero_key, stale_key,
            "a staleness marker and a real zero are different points"
        );
    }

    #[test]
    fn the_point_identity_ordering_is_blind_to_the_descriptor_and_the_payload() {
        // The projection is the OTel collapse key: descriptor-blind and
        // payload-blind, both on purpose. A re-delivery under a changed
        // descriptor must land on the standing key (the ledger then names
        // the stream conflict), and the payload must stay out of the
        // ordering because the ledger's kind-aware comparison owns it.
        let described = {
            let mut stream = identity(StreamKind::Gauge, None);
            stream.description = Some("requests being served".to_owned());
            stream.unit = Some("1".to_owned());
            stream
        };
        let bare_stream = identity(StreamKind::Gauge, None);
        assert_ne!(
            described, bare_stream,
            "the streams themselves differ — the descriptor is full identity"
        );
        let point = MetricPoint::Number(NumberPoint::measurement(
            50,
            MetricNumber::int(1),
            attributes(),
            Vec::new(),
        ));
        let payload_changed = MetricPoint::Number(NumberPoint::measurement(
            50,
            MetricNumber::int(9),
            attributes(),
            Vec::new(),
        ));
        let described_key =
            PointIdentity::of_interned(&Arc::new(described), &Arc::new(point.clone()));
        let bare_key = PointIdentity::of_interned(&Arc::new(bare_stream), &Arc::new(point));
        let payload_key =
            PointIdentity::of_interned(&described_key.stream, &Arc::new(payload_changed));
        assert_eq!(
            described_key.cmp(&bare_key),
            std::cmp::Ordering::Equal,
            "the key's ordering is blind to the stream descriptor"
        );
        assert_eq!(
            described_key.cmp(&payload_key),
            std::cmp::Ordering::Equal,
            "the key's ordering is blind to the point payload"
        );
        // The blind projections still carry the full streams: the
        // descriptor difference is intact on the key's own handle, exactly
        // what the ledger compares to name a stream conflict.
        assert_ne!(*described_key.stream, *bare_key.stream);
    }

    #[test]
    fn float_ordering_is_the_bit_pattern_and_agrees_with_equality() {
        // The Ord exists so identity keys live in ordered maps; it must be
        // total and agree exactly with the bit-pattern equality.
        let quiet_nan = Float::new(f64::NAN);
        assert_eq!(
            quiet_nan.cmp(&quiet_nan),
            std::cmp::Ordering::Equal,
            "a NaN compares Equal to itself — equality by bits"
        );
        let signalling = Float::new(f64::from_bits(0x7FF0_0000_0000_0001_u64));
        assert_ne!(quiet_nan, signalling);
        assert_ne!(
            quiet_nan.cmp(&signalling),
            std::cmp::Ordering::Equal,
            "different NaN bit patterns are different values, totally ordered"
        );
        let negative_zero = Float::new(-0.0);
        let positive_zero = Float::new(0.0);
        assert_ne!(negative_zero, positive_zero);
        assert_ne!(
            negative_zero.cmp(&positive_zero),
            std::cmp::Ordering::Equal,
            "the sign of zero is a preserved distinction under Ord, too"
        );
        // The order is the bits read as u64 — a comparison order, never a
        // claimed numeric or canonical one.
        assert_eq!(
            negative_zero.cmp(&positive_zero),
            (-0.0_f64).to_bits().cmp(&0.0_f64.to_bits())
        );
        let infinity = Float::new(f64::INFINITY);
        let one = Float::new(1.0);
        assert_eq!(one.cmp(&infinity), std::cmp::Ordering::Less);
        assert_eq!(infinity.cmp(&one), std::cmp::Ordering::Greater);
    }

    #[test]
    fn value_and_attribute_ordering_agree_with_map_equality() {
        // Attributes compare as maps: emitter order is not semantic. The
        // derived Ord walks the map's own sorted entries, so reordered but
        // equal maps must compare Equal — Ord and Eq can never disagree.
        let first = Attributes::from_pairs(vec![
            ("a".to_owned(), Value::Int(1)),
            ("b".to_owned(), Value::Int(2)),
        ])
        .expect("unique keys");
        let second = Attributes::from_pairs(vec![
            ("b".to_owned(), Value::Int(2)),
            ("a".to_owned(), Value::Int(1)),
        ])
        .expect("unique keys");
        assert_eq!(first, second, "map equality ignores pair order");
        assert_eq!(
            first.cmp(&second),
            std::cmp::Ordering::Equal,
            "the ordering agrees: equal maps are Equal"
        );
        let third = Attributes::from_pairs(vec![
            ("a".to_owned(), Value::Int(1)),
            ("b".to_owned(), Value::Int(3)),
        ])
        .expect("unique keys");
        assert_ne!(first, third);
        assert_eq!(first.cmp(&third), std::cmp::Ordering::Less);
        // Values order by variant then content, Float by bits — total and
        // equality-consistent, a key order, never a numeric ranking.
        let string_value = Value::String("x".to_owned());
        let int_value = Value::Int(1);
        assert_ne!(string_value.cmp(&int_value), std::cmp::Ordering::Equal);
        let nan_a = Value::Double(Float::new(f64::NAN));
        let nan_b = Value::Double(Float::new(f64::from_bits(0x7FF8_0000_0000_0001_u64)));
        assert_ne!(nan_a, nan_b);
        assert_ne!(nan_a.cmp(&nan_b), std::cmp::Ordering::Equal);
    }

    #[test]
    fn all_four_point_shapes_carry_flags_verbatim() {
        let histogram = HistogramPoint {
            attributes: attributes(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 1,
            sum: None,
            bucket_counts: vec![1],
            explicit_bounds: vec![Float::new(1.0)],
            min: None,
            max: None,
            flags: u32::MAX,
            exemplars: Vec::new(),
        };
        let exponential = ExponentialHistogramPoint {
            attributes: attributes(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 1,
            sum: None,
            scale: 0,
            zero_count: 0,
            zero_threshold: Float::new(0.0),
            positive: ExponentialBuckets {
                offset: 0,
                bucket_counts: vec![1],
            },
            negative: ExponentialBuckets {
                offset: 0,
                bucket_counts: vec![],
            },
            min: None,
            max: None,
            flags: 1 << 20,
            exemplars: Vec::new(),
        };
        let summary = SummaryPoint {
            attributes: attributes(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 1,
            sum: None,
            quantiles: Vec::new(),
            flags: 3,
            exemplars: Vec::new(),
        };
        assert_eq!(MetricPoint::Histogram(histogram).flags(), u32::MAX);
        assert_eq!(
            MetricPoint::ExponentialHistogram(exponential).flags(),
            1 << 20
        );
        assert_eq!(MetricPoint::Summary(summary).flags(), 3);
    }

    #[test]
    fn exponential_histogram_layouts_are_preserved_exactly() {
        let point = ExponentialHistogramPoint {
            attributes: attributes(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 7,
            sum: Some(Float::new(3.5)),
            scale: -2,
            zero_count: 1,
            zero_threshold: Float::new(0.000_001),
            positive: ExponentialBuckets {
                offset: 4,
                bucket_counts: vec![1, 2, 3],
            },
            negative: ExponentialBuckets {
                offset: -1,
                bucket_counts: vec![0, 9],
            },
            min: None,
            max: None,
            flags: 0,
            exemplars: vec![exemplar()],
        };
        assert_eq!(point.scale, -2);
        assert_eq!(point.zero_count, 1);
        assert_eq!(point.zero_threshold, Float::new(0.000_001));
        assert_eq!(point.positive.bucket_counts, vec![1, 2, 3]);
        assert_eq!(point.negative.offset, -1);
    }

    #[test]
    fn explicit_histogram_buckets_are_preserved_never_rebucketed() {
        let point = HistogramPoint {
            attributes: attributes(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 4,
            sum: Some(Float::new(10.0)),
            bucket_counts: vec![0, 2, 2, 0],
            explicit_bounds: vec![Float::new(1.0), Float::new(5.0), Float::new(10.0)],
            min: None,
            max: None,
            flags: 0,
            exemplars: Vec::new(),
        };
        assert_eq!(point.bucket_counts, vec![0, 2, 2, 0]);
        assert_eq!(
            point.explicit_bounds,
            vec![Float::new(1.0), Float::new(5.0), Float::new(10.0)]
        );
    }

    #[test]
    fn summary_quantiles_count_and_sum_are_preserved() {
        let point = SummaryPoint {
            attributes: attributes(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 100,
            sum: Some(Float::new(997.5)),
            quantiles: vec![
                QuantileValue {
                    quantile: Float::new(0.5),
                    value: Float::new(9.0),
                },
                QuantileValue {
                    quantile: Float::new(0.99),
                    value: Float::new(21.0),
                },
            ],
            flags: 0,
            exemplars: Vec::new(),
        };
        assert_eq!(point.count, 100);
        assert_eq!(point.quantiles.len(), 2);
        assert_eq!(point.quantiles[1].quantile.bits(), Float::new(0.99).bits());
    }

    #[test]
    fn exemplars_carry_the_trace_ids_exactly_as_sent() {
        let exemplar = exemplar();
        assert_eq!(exemplar.value, MetricNumber::int(3));
        assert_eq!(exemplar.time_unix_nano, 12);
        assert!(exemplar.filtered_attributes.is_empty());
        assert_eq!(
            exemplar.correlation_pair(),
            Some((TraceId::from_bytes([4; 16]), SpanId::from_bytes([5; 8])))
        );
        let half = Exemplar {
            trace_id: Some(TraceId::from_bytes([4; 16])),
            span_id: None,
            ..exemplar
        };
        assert_eq!(
            half.correlation_pair(),
            None,
            "one id alone is not a pair; nothing is fabricated"
        );
        assert!(half.span_id.is_none(), "absent stays absent");
    }

    #[test]
    fn a_stream_rejects_points_whose_shape_disagrees_with_the_kind() {
        let number = MetricPoint::Number(NumberPoint::measurement(
            1,
            MetricNumber::int(1),
            attributes(),
            Vec::new(),
        ));
        let error = MetricStream::new(
            identity(StreamKind::Histogram, Some(Temporality::Cumulative)),
            vec![number.clone()],
        )
        .expect_err("a histogram stream cannot carry a number point");
        assert_eq!(
            error,
            StreamShapeError::KindShapeMismatch {
                index: 0,
                expected: PointShape::Histogram,
                found: PointShape::Number,
            }
        );
    }

    #[test]
    fn a_gauge_stream_admits_a_point_that_sent_a_start_time() {
        // The spec ignores a gauge's start_time; the model preserves it
        // instead of refusing it — the old GaugeWithStartTime rejection is
        // gone.
        let gauge_with_start = MetricPoint::Number(NumberPoint::measurement_with_start(
            0,
            1,
            MetricNumber::int(1),
            attributes(),
            Vec::new(),
        ));
        let stream = stream(StreamKind::Gauge, None, vec![gauge_with_start]);
        assert_eq!(stream.points.len(), 1);
    }

    #[test]
    fn a_stream_rejects_a_temporality_its_kind_cannot_carry() {
        let gauge_error = MetricStream::new(
            identity(StreamKind::Gauge, Some(Temporality::Delta)),
            Vec::new(),
        )
        .expect_err("a gauge carries no temporality");
        assert_eq!(
            gauge_error,
            StreamShapeError::TemporalityMismatch {
                kind: StreamKind::Gauge,
                temporality: Some(Temporality::Delta),
            }
        );
        let sum_error = MetricStream::new(
            identity(StreamKind::Sum { monotonic: true }, None),
            Vec::new(),
        )
        .expect_err("a sum carries a temporality");
        assert_eq!(
            sum_error,
            StreamShapeError::TemporalityMismatch {
                kind: StreamKind::Sum { monotonic: true },
                temporality: None,
            }
        );
    }

    #[test]
    fn interval_kinds_require_start_times_on_every_point() {
        // A gauge-built measurement carries no start time; offering it
        // under an interval kind is invalid input.
        let number = MetricPoint::Number(NumberPoint::measurement(
            5,
            MetricNumber::int(1),
            attributes(),
            Vec::new(),
        ));
        let error = MetricStream::new(
            identity(
                StreamKind::Sum { monotonic: false },
                Some(Temporality::Delta),
            ),
            vec![number],
        )
        .expect_err("a sum point must carry its interval start");
        assert_eq!(
            error,
            StreamShapeError::IntervalKindMissingStartTime { index: 0 }
        );
    }

    #[test]
    fn the_shape_law_checks_one_pair_without_a_stream() {
        let gauge_id = identity(StreamKind::Gauge, None);
        let sum_id = identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let gauge_point = MetricPoint::Number(NumberPoint::measurement(
            1,
            MetricNumber::int(1),
            attributes(),
            Vec::new(),
        ));
        assert!(MetricStream::check_point_coherence(&gauge_id, &gauge_point, 0).is_ok());
        assert_eq!(
            MetricStream::check_point_coherence(&sum_id, &gauge_point, 3),
            Err(StreamShapeError::IntervalKindMissingStartTime { index: 3 }),
            "the ledger reuses the same law a stream enforces"
        );
        assert!(MetricStream::check_identity_coherence(&gauge_id).is_ok());
        assert!(MetricStream::check_identity_coherence(&sum_id).is_ok());
        let naked_sum = identity(StreamKind::Sum { monotonic: true }, None);
        assert!(
            MetricStream::check_identity_coherence(&naked_sum).is_err(),
            "a sum without a temporality is incoherent"
        );
        let temporality_gauge = identity(StreamKind::Gauge, Some(Temporality::Delta));
        assert!(
            MetricStream::check_identity_coherence(&temporality_gauge).is_err(),
            "a gauge cannot carry a temporality"
        );
    }

    #[test]
    fn metadata_is_preserved_as_an_attribute_map() {
        let metadata = Attributes::from_pairs(vec![
            (
                "prometheus.io/type".to_owned(),
                Value::String("gauge".to_owned()),
            ),
            ("tier".to_owned(), Value::Int(2)),
        ])
        .expect("ok");
        let mut id = identity(StreamKind::Gauge, None);
        id.metadata = metadata.clone();
        let stream = MetricStream::new(id, Vec::new()).expect("a coherent stream");
        assert_eq!(stream.metadata(), &metadata);
        assert_eq!(
            stream.identity.metadata.get("tier"),
            Some(&Value::Int(2)),
            "metadata rides on the stream verbatim"
        );
    }

    #[test]
    fn the_descriptor_is_part_of_the_stream_identity() {
        // The description, unit and metadata describe the stream and are
        // preserved verbatim; two same-named instruments differing only
        // there are two distinct streams — never a silent merge.
        let plain = identity(StreamKind::Gauge, None);
        let described = StreamIdentity {
            description: Some("an in-flight gauge".to_owned()),
            ..plain.clone()
        };
        let united = StreamIdentity {
            unit: Some("s".to_owned()),
            ..plain.clone()
        };
        let with_metadata = StreamIdentity {
            metadata: Attributes::from_pairs(vec![("tier".to_owned(), Value::Int(1))])
                .expect("unique keys"),
            ..plain.clone()
        };
        assert_ne!(plain, described, "description is identity content");
        assert_ne!(plain, united, "unit is identity content");
        assert_ne!(plain, with_metadata, "metadata is identity content");
        // Absent and empty stay distinct in identity, as everywhere.
        let empty_described = StreamIdentity {
            description: Some(String::new()),
            ..described.clone()
        };
        assert_ne!(
            described, empty_described,
            "absent is not empty, in the identity like everywhere else"
        );
    }

    #[test]
    fn a_well_shaped_stream_preserves_its_name_description_unit_and_points() {
        let gauge = MetricPoint::Number(NumberPoint::measurement(
            99,
            MetricNumber::double(1.5),
            attributes(),
            Vec::new(),
        ));
        let stream = stream(StreamKind::Gauge, None, vec![gauge.clone()]);
        assert_eq!(stream.name(), "requests");
        assert_eq!(stream.description(), Some(""), "empty ≠ absent");
        assert_eq!(stream.unit(), Some("ms"));
        assert_eq!(stream.points, vec![gauge]);
        let identity = stream.identity();
        assert_eq!(identity.kind, StreamKind::Gauge);
        assert_eq!(identity.temporality, None);
        assert_eq!(identity.resource, resource());
        assert_eq!(identity.scope, scope());
    }

    #[test]
    fn resource_schema_url_is_not_stream_identity() {
        let plain = identity(StreamKind::Gauge, None);
        let with_url = StreamIdentity {
            resource: Resource {
                schema_url: Some("https://schema/v2".to_owned()),
                ..plain.resource.clone()
            },
            ..plain.clone()
        };
        assert_eq!(
            plain, with_url,
            "resource identity is the attribute map; schema_url is metadata"
        );
    }
}
