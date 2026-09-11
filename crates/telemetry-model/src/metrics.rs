//! Metric streams and data points.
//!
//! `docs/architecture/telemetry-model.md`, "Metrics": five OTLP kinds as
//! five distinct model-level shapes, the monotonicity flag preserved,
//! temporality per stream with delta and cumulative never merged or
//! converted, per-point attribute sets with `start_time` (absent for
//! gauges) and `time`, and exemplars carrying value, timestamp, filtered
//! attributes and trace context. Histogram layouts are preserved exactly —
//! never re-bucketed.

use crate::context::TraceContext;
use crate::resources::{InstrumentationScope, Resource};
use crate::values::{Attributes, Float};
use std::fmt;

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

/// The stream kind — the five OTLP kinds are five distinct shapes.
///
/// No kind is collapsed into another; a sum's monotonicity flag is part of
/// the kind and is never inferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Temporality {
    Delta,
    Cumulative,
}

/// One metric exemplar: value, timestamp, filtered attributes and trace
/// context — the model-level hook for metrics↔trace correlation,
/// preserved whenever sent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Exemplar {
    /// The exemplar's value, as sent.
    pub value: MetricNumber,
    /// The exemplar's timestamp, in nanoseconds on the emitter's clock.
    pub time_unix_nano: u64,
    /// The attributes filtered out of the measurement, as sent.
    pub filtered_attributes: Attributes,
    /// The trace context the exemplar carries, when the emitter sent one.
    pub trace_context: Option<TraceContext>,
}

/// A gauge or sum data point: an attribute set, `start_time` (absent for
/// gauges), `time`, a number, and exemplars.
///
/// Build gauge points with [`NumberPoint::measurement`] and interval points
/// with [`NumberPoint::interval`]; the constructors keep "absent" and
/// "present" start times unconfusable.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NumberPoint {
    /// The point's attribute set.
    pub attributes: Attributes,
    /// Interval start; `None` for a gauge measurement.
    pub start_time_unix_nano: Option<u64>,
    /// Interval end or measurement time, in nanoseconds on the emitter's
    /// clock.
    pub time_unix_nano: u64,
    /// The number, int or double as sent.
    pub value: MetricNumber,
    /// Exemplars attached to this point, in the order the emitter sent
    /// them.
    pub exemplars: Vec<Exemplar>,
}

impl NumberPoint {
    /// A gauge measurement: `time` only, `start_time` absent.
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

    /// The interval start, absent for gauge measurements.
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
}

/// A stream was offered whose kind and point shapes do not agree, whose
/// gauge/interval start times do not agree with the kind, or whose
/// temporality does not agree with the kind. Invalid input: rejected, not
/// coerced.
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
    /// A gauge measurement was offered with an interval start.
    GaugeWithStartTime {
        /// Position of the offending point.
        index: usize,
    },
    /// An interval-kind point was offered without an interval start.
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
            Self::GaugeWithStartTime { index } => {
                write!(f, "gauge measurement {index} carries a start_time")
            }
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

/// The identity of a data point stream: resource, scope, name, kind,
/// temporality. A delta stream and a cumulative stream with the same name
/// have different identities; so do two kinds, two resources, two scopes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamIdentity {
    /// The stream's resource.
    pub resource: Resource,
    /// The stream's scope.
    pub scope: InstrumentationScope,
    /// The metric name, verbatim.
    pub name: String,
    /// The stream kind, monotonicity included.
    pub kind: StreamKind,
    /// The stream temporality; `None` where the kind carries none.
    pub temporality: Option<Temporality>,
}

/// The collapse identity of one data point: the stream it belongs to, the
/// point's own attribute set, and its interval — `start_time` (absent for
/// gauges) and `time`. A re-delivered point under one of these keys is the
/// same data point, whatever entity id it was assigned on first delivery.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PointIdentity {
    /// The stream the point belongs to.
    pub stream: StreamIdentity,
    /// The point's attribute set.
    pub point_attributes: Attributes,
    /// The interval start; absent for gauge measurements.
    pub start_time_unix_nano: Option<u64>,
    /// The interval end or measurement time.
    pub time_unix_nano: u64,
}

/// One metric stream: its identity (name, description and unit verbatim —
/// absent is not empty — resource, scope, kind, temporality) and its
/// points.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetricStream {
    /// The stream identity: name, resource, scope, kind, temporality.
    pub identity: StreamIdentity,
    /// The description, as sent; `None` is absent and distinct from empty.
    pub description: Option<String>,
    /// The unit, opaque at the model level — no unit grammar is
    /// interpreted here; `None` is absent and distinct from empty.
    pub unit: Option<String>,
    /// The stream's points, in the order the emitter sent them.
    pub points: Vec<MetricPoint>,
}

impl MetricStream {
    /// Builds a stream, rejecting a shape the contract cannot represent:
    /// points whose shape disagrees with the kind, gauge points with a
    /// start time, interval points without one, and temporalities the kind
    /// cannot carry.
    ///
    /// # Errors
    ///
    /// Returns [`StreamShapeError`] naming the first violating point (or
    /// the kind/temporality pair). Nothing is coerced or re-bucketed.
    pub fn new(
        identity: StreamIdentity,
        description: Option<String>,
        unit: Option<String>,
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
            let expected = kind.point_shape();
            let found = point.shape();
            if expected != found {
                return Err(StreamShapeError::KindShapeMismatch {
                    index,
                    expected,
                    found,
                });
            }
            match (point.start_time_unix_nano(), kind) {
                (Some(_), StreamKind::Gauge) => {
                    return Err(StreamShapeError::GaugeWithStartTime { index });
                }
                (
                    None,
                    StreamKind::Sum { .. }
                    | StreamKind::Histogram
                    | StreamKind::ExponentialHistogram
                    | StreamKind::Summary,
                ) => {
                    return Err(StreamShapeError::IntervalKindMissingStartTime { index });
                }
                _ => {}
            }
        }
        Ok(Self {
            identity,
            description,
            unit,
            points,
        })
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{SpanId, TraceFlags, TraceId, TraceState};
    use crate::values::Value;

    fn resource() -> Resource {
        Resource {
            attributes: Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String("checkout".to_owned()),
            )]),
            schema_url: None,
        }
    }

    fn scope() -> InstrumentationScope {
        InstrumentationScope {
            name: "scope".to_owned(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
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
            trace_context: Some(TraceContext {
                trace_id: TraceId::from_bytes([4; 16]),
                span_id: SpanId::from_bytes([5; 8]),
                flags: TraceFlags::new(1),
                tracestate: TraceState::default(),
            }),
        }
    }

    fn identity(kind: StreamKind, temporality: Option<Temporality>) -> StreamIdentity {
        StreamIdentity {
            resource: resource(),
            scope: scope(),
            name: "m".to_owned(),
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
        MetricStream::new(id, Some(String::new()), Some("ms".to_owned()), points)
            .expect("a coherent stream")
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
    fn a_gauge_point_has_no_start_time_an_interval_point_has_one() {
        let gauge = NumberPoint::measurement(50, MetricNumber::int(1), attributes(), Vec::new());
        assert_eq!(gauge.start_time_unix_nano, None);
        assert_eq!(gauge.time_unix_nano, 50);
        let interval =
            NumberPoint::interval(10, 50, MetricNumber::int(1), attributes(), Vec::new());
        assert_eq!(interval.start_time_unix_nano, Some(10));
        assert_ne!(
            MetricPoint::Number(gauge),
            MetricPoint::Number(interval),
            "absent ≠ present start time"
        );
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
            exemplars: Vec::new(),
        };
        assert_eq!(point.count, 100);
        assert_eq!(point.quantiles.len(), 2);
        assert_eq!(point.quantiles[1].quantile.bits(), Float::new(0.99).bits());
    }

    #[test]
    fn exemplars_carry_value_timestamp_filtered_attributes_and_trace_context() {
        let exemplar = exemplar();
        assert_eq!(exemplar.value, MetricNumber::int(3));
        assert_eq!(exemplar.time_unix_nano, 12);
        assert!(exemplar.filtered_attributes.is_empty());
        let context = exemplar.trace_context.clone().expect("correlation hook");
        assert!(context.trace_id.is_valid());
        assert!(context.span_id.is_valid());
        let without_context = Exemplar {
            trace_context: None,
            ..exemplar
        };
        assert!(without_context.trace_context.is_none(), "absent is a fact");
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
            None,
            None,
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
        let with_start = MetricPoint::Number(NumberPoint::interval(
            0,
            1,
            MetricNumber::int(1),
            attributes(),
            Vec::new(),
        ));
        let error = MetricStream::new(
            identity(StreamKind::Gauge, None),
            None,
            None,
            vec![with_start],
        )
        .expect_err("a gauge measurement cannot carry a start_time");
        assert_eq!(error, StreamShapeError::GaugeWithStartTime { index: 0 });
    }

    #[test]
    fn a_stream_rejects_a_temporality_its_kind_cannot_carry() {
        let gauge_error = MetricStream::new(
            identity(StreamKind::Gauge, Some(Temporality::Delta)),
            None,
            None,
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
            None,
            None,
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
    fn a_well_shaped_stream_preserves_its_name_description_unit_and_points() {
        let gauge = MetricPoint::Number(NumberPoint::measurement(
            99,
            MetricNumber::double(1.5),
            attributes(),
            Vec::new(),
        ));
        let stream = stream(StreamKind::Gauge, None, vec![gauge.clone()]);
        assert_eq!(stream.name(), "requests");
        assert_eq!(stream.description, Some(String::new()), "empty ≠ absent");
        assert_eq!(stream.unit, Some("ms".to_owned()));
        assert_eq!(stream.points, vec![gauge]);
        let identity = stream.identity();
        assert_eq!(identity.kind, StreamKind::Gauge);
        assert_eq!(identity.temporality, None);
        assert_eq!(identity.resource, resource());
        assert_eq!(identity.scope, scope());
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
            None,
            None,
            vec![number],
        )
        .expect_err("a sum point must carry its interval start");
        assert_eq!(
            error,
            StreamShapeError::IntervalKindMissingStartTime { index: 0 }
        );
    }
}
