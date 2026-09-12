//! The metrics side of the suite: stream identities, kind coherence,
//! point duplicate delivery, exemplars, the per-export cap and the
//! partial-success walk. Spans, logs and the queue live in `tests`.

use std::sync::Arc;

use runtime_trail_telemetry_model::{
    BudgetName, DATA_POINT_FLAG_NO_RECORDED_VALUE, MetricNumber, MetricPoint, PointShape,
    StreamShapeError, Temporality, budgets::OTLP_PAYLOAD_BYTES,
};

use crate::fixtures::*;
use crate::otlp::opentelemetry::{
    common::v1 as common, metrics::v1 as metrics, resource::v1 as resource,
};
use crate::queue::{QueuedRecord, StoredRecord};
use crate::signal::{AdmissionSignal, RecordOutcome, RecordRejection, Unrepresentable};

// ---------------------------------------------------------- stream identity

#[test]
fn the_five_metric_kinds_are_five_distinct_streams() {
    let harness = Harness::new();
    let kinds = vec![
        gauge(vec![number_point(as_double(1.0))]),
        sum(
            metrics::AggregationTemporality::Cumulative as i32,
            true,
            vec![number_point(as_int(2))],
        ),
        metrics::metric::Data::Histogram(metrics::Histogram {
            data_points: vec![metrics::HistogramDataPoint {
                attributes: Vec::new(),
                start_time_unix_nano: 1,
                time_unix_nano: 10,
                count: 3,
                sum: Some(6.0),
                bucket_counts: vec![1, 2],
                explicit_bounds: vec![1.0],
                exemplars: Vec::new(),
                flags: 0,
                min: Some(1.0),
                max: Some(3.0),
            }],
            aggregation_temporality: metrics::AggregationTemporality::Cumulative as i32,
        }),
        metrics::metric::Data::ExponentialHistogram(metrics::ExponentialHistogram {
            data_points: vec![metrics::ExponentialHistogramDataPoint {
                attributes: Vec::new(),
                start_time_unix_nano: 1,
                time_unix_nano: 10,
                count: 2,
                sum: Some(4.0),
                scale: 0,
                zero_count: 0,
                positive: None,
                negative: None,
                flags: 0,
                exemplars: Vec::new(),
                min: None,
                max: None,
                zero_threshold: 0.0,
            }],
            aggregation_temporality: metrics::AggregationTemporality::Cumulative as i32,
        }),
        metrics::metric::Data::Summary(metrics::Summary {
            data_points: vec![metrics::SummaryDataPoint {
                attributes: Vec::new(),
                start_time_unix_nano: 1,
                time_unix_nano: 10,
                count: 2,
                sum: 4.0,
                quantile_values: vec![metrics::summary_data_point::ValueAtQuantile {
                    quantile: 0.5,
                    value: 2.0,
                }],
                flags: 0,
            }],
        }),
    ];
    // Same name for all five: only the kind separates them.
    let metric_list: Vec<metrics::Metric> = kinds
        .into_iter()
        .map(|data| metric("same-name", data))
        .collect();
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(Some(scope("test")), metric_list)],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 5, "one point per kind");

    let queued = harness.drain();
    let mut kinds_found = Vec::new();
    let mut shapes_found = Vec::new();
    for record in &queued {
        let StoredRecord::Point { stream, point } = &record.record else {
            panic!("expected only metric points");
        };
        kinds_found.push(stream.kind);
        shapes_found.push(point.shape());
    }
    for (index, kind) in kinds_found.iter().enumerate() {
        for other in kinds_found.iter().skip(index + 1) {
            assert_ne!(kind, other, "kinds {kind:?} and {other:?} are one stream");
        }
    }
    assert_eq!(
        shapes_found,
        vec![
            PointShape::Number,
            PointShape::Number,
            PointShape::Histogram,
            PointShape::ExponentialHistogram,
            PointShape::Summary,
        ],
        "the wire oneof pins each stream's point shape by construction"
    );
}

#[test]
fn delta_and_cumulative_sums_are_distinct_streams() {
    let harness = Harness::new();
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![
                metric(
                    "sums",
                    sum(
                        metrics::AggregationTemporality::Delta as i32,
                        true,
                        vec![number_point(as_int(1))],
                    ),
                ),
                metric(
                    "sums",
                    sum(
                        metrics::AggregationTemporality::Cumulative as i32,
                        true,
                        vec![number_point(as_int(1))],
                    ),
                ),
            ],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 2, "temporality is part of the identity");

    let queued = harness.drain();
    let temporalities: Vec<Temporality> = queued
        .iter()
        .map(|record| match &record.record {
            StoredRecord::Point { stream, .. } => {
                stream.temporality.expect("sums carry temporality")
            }
            _ => panic!("expected only metric points"),
        })
        .collect();
    assert_eq!(temporalities, [Temporality::Delta, Temporality::Cumulative]);
}

#[test]
fn a_sum_without_temporality_is_refused_by_the_shape_law() {
    let harness = Harness::new();
    // AggregationTemporality unset on the wire (0): a sum must declare one.
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric(
                "unspecified",
                sum(
                    0,
                    true,
                    vec![number_point(as_int(1)), number_point(as_int(2))],
                ),
            )],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    assert_eq!(
        outcome.rejected(),
        2,
        "every point of the incoherent stream"
    );
    for position in 0..2 {
        assert!(matches!(
            rejected_reason(&outcome, position),
            RecordRejection::Shape(StreamShapeError::TemporalityMismatch { .. })
        ));
    }
    assert!(harness.drain().is_empty());
}

#[test]
fn a_sum_point_without_a_start_time_is_refused_by_the_shape_law() {
    let harness = Harness::new();
    let mut point = number_point(as_int(1));
    point.start_time_unix_nano = 0; // the interval kind's mandatory start
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric(
                "interval",
                sum(
                    metrics::AggregationTemporality::Cumulative as i32,
                    true,
                    vec![point],
                ),
            )],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Shape(StreamShapeError::IntervalKindMissingStartTime { .. })
    ));
    assert!(harness.drain().is_empty());
}

#[test]
fn metric_streams_intern_across_envelopes() {
    let harness = Harness::new();
    // Two envelopes with an equal resource and scope, each carrying the
    // same gauge at a different point time, so both points admit.
    let envelope = |time: u64| {
        let mut point = number_point(as_double(1.0));
        point.time_unix_nano = time;
        resource_metrics(
            Some(resource(vec![attr("service.name", str_value("interned"))])),
            vec![scope_metrics(
                Some(scope("test")),
                vec![metric("one", gauge(vec![point]))],
            )],
        )
    };
    let payload = encode(&metrics_request(vec![envelope(10), envelope(11)]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(
        outcome.admitted(),
        2,
        "same stream identity, different points"
    );

    let queued = harness.drain();
    let [
        QueuedRecord {
            record: StoredRecord::Point { stream: a, .. },
            ..
        },
        QueuedRecord {
            record: StoredRecord::Point { stream: b, .. },
            ..
        },
    ] = queued.as_slice()
    else {
        panic!("expected two queued points");
    };
    assert!(
        Arc::ptr_eq(a, b),
        "equal identities intern to one Arc, whatever envelope they came from"
    );
}

// ------------------------------------------------------------- delivery

#[test]
fn a_gauges_start_time_is_normalized_out_of_its_identity() {
    let harness = Harness::new();
    let mut first = number_point(as_double(5.0));
    first.time_unix_nano = 10;
    first.start_time_unix_nano = 5;
    let mut second = number_point(as_double(5.0));
    second.time_unix_nano = 10;
    second.start_time_unix_nano = 6;

    let first_payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("gaugey", gauge(vec![first]))],
        )],
    )]));
    let second_payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("gaugey", gauge(vec![second]))],
        )],
    )]));
    let first_outcome = harness
        .pipeline
        .ingest_metrics(now(), &first_payload)
        .expect("admitted");
    let second_outcome = harness
        .pipeline
        .ingest_metrics(now(), &second_payload)
        .expect("walked");

    assert_eq!(first_outcome.admitted(), 1);
    assert!(matches!(
        &second_outcome.records[0],
        RecordOutcome::Collapsed { .. }
    ));
    let drained = harness.drain();
    assert_eq!(
        drained.len(),
        2,
        "the collapse re-offers the standing point"
    );
    assert_eq!(
        drained[0].entity, drained[1].entity,
        "one point, one entity"
    );
}

#[test]
fn point_flags_participate_in_identity() {
    let harness = Harness::new();
    let mut plain = number_point(as_double(0.0));
    plain.time_unix_nano = 10;
    let mut stale = number_point(as_double(0.0));
    stale.time_unix_nano = 10;
    stale.flags = DATA_POINT_FLAG_NO_RECORDED_VALUE;

    let first_payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("flags", gauge(vec![plain]))],
        )],
    )]));
    let second_payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("flags", gauge(vec![stale]))],
        )],
    )]));
    let first = harness
        .pipeline
        .ingest_metrics(now(), &first_payload)
        .expect("admitted");
    let second = harness
        .pipeline
        .ingest_metrics(now(), &second_payload)
        .expect("walked");

    assert_eq!(first.admitted(), 1);
    assert!(
        matches!(&second.records[0], RecordOutcome::Admitted { .. }),
        "a staleness marker is a different point, not a collapse"
    );
    assert_eq!(harness.drain().len(), 2);
}

#[test]
fn exemplar_ids_are_never_fabricated() {
    let harness = Harness::new();
    let mut point = number_point(as_int(7));
    point.exemplars = vec![metrics::Exemplar {
        filtered_attributes: Vec::new(),
        time_unix_nano: 11,
        span_id: S1.to_vec(),
        trace_id: Vec::new(),
        value: Some(metrics::exemplar::Value::AsInt(7)),
    }];
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("exemplared", gauge(vec![point]))],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 1);

    let queued = harness.drain();
    let StoredRecord::Point { point, .. } = &queued[0].record else {
        panic!()
    };
    let MetricPoint::Number(number) = point.as_ref() else {
        panic!("expected a number point")
    };
    let exemplar = &number.exemplars[0];
    assert!(
        exemplar.trace_id.is_none(),
        "an absent trace id is never fabricated"
    );
    assert_eq!(exemplar.span_id.as_ref().map(|id| id.as_bytes()), Some(S1));
    assert_eq!(exemplar.value, MetricNumber::Int(7));
    assert!(exemplar.filtered_attributes.is_empty());
}

#[test]
fn an_empty_metric_carries_nothing() {
    let harness = Harness::new();
    let mut empty = metric("hollow", gauge(vec![]));
    empty.data = None;
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(Some(scope("test")), vec![empty.clone()])],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    assert!(outcome.is_empty(), "no points, no records, nothing to name");
    assert!(harness.drain().is_empty());

    // And at the decode layer, the identity builder names the refusal.
    let envelope = crate::decode::Envelope {
        resource: crate::decode::resource(resource::Resource::default(), "")
            .expect("an empty resource translates"),
        scope: crate::decode::scope(common::InstrumentationScope::default(), "")
            .expect("an empty scope translates"),
    };
    assert!(matches!(
        crate::decode::stream_identity(&empty, &envelope),
        Err(RecordRejection::Unrepresentable(
            Unrepresentable::EmptyMetric
        ))
    ));
}

// -------------------------------------------------------------- budgets

#[test]
fn an_export_over_the_point_cap_is_refused_whole() {
    let harness = Harness::new();
    // 10,001 minimal points: one past the contract's per-export budget,
    // still far under the payload ceiling.
    let points: Vec<metrics::NumberDataPoint> = (0..=10_000)
        .map(|index| {
            let mut point = number_point(as_double(1.0));
            point.time_unix_nano = index;
            point
        })
        .collect();
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("flood", gauge(points))],
        )],
    )]));
    assert!(
        payload.len() < OTLP_PAYLOAD_BYTES,
        "the point cap fires before the payload cap"
    );

    let signal = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect_err("over the export cap");
    assert!(matches!(
        &signal,
        AdmissionSignal::ExportOverCap { rejection }
            if rejection.budget == BudgetName::DataPointsPerExport
                && rejection.limit == 10_000
                && rejection.observed == 10_001
    ));
    assert!(!signal.is_retryable(), "retrying cannot shrink an export");
    assert!(
        harness.drain().is_empty(),
        "nothing from a refused export is admitted"
    );
}

#[test]
fn a_mixed_export_comes_back_partial_naming_positions() {
    let harness = Harness::new();
    let mut fine_first = number_point(as_double(1.0));
    fine_first.time_unix_nano = 1;
    let mut oversized = number_point(as_double(2.0));
    oversized.time_unix_nano = 2;
    oversized.attributes = vec![attr("blob", str_value(&"a".repeat(5_000)))];
    let mut fine_last = number_point(as_double(3.0));
    fine_last.time_unix_nano = 3;

    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric(
                "mixed",
                gauge(vec![fine_first, oversized, fine_last]),
            )],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    assert_eq!(outcome.len(), 3);

    assert!(
        matches!(&outcome.records[0], RecordOutcome::Admitted { .. }),
        "position 0"
    );
    assert!(
        matches!(
            rejected_reason(&outcome, 1),
            RecordRejection::Budget(rejection) if rejection.budget == BudgetName::AttributeValueSize
        ),
        "position 1 names its budget"
    );
    assert!(
        matches!(&outcome.records[2], RecordOutcome::Admitted { .. }),
        "position 2"
    );

    let queued = harness.drain();
    assert_eq!(queued.len(), 2);
    assert_eq!(queued[0].entity, standing_entity(&outcome, 0));
    assert_eq!(queued[1].entity, standing_entity(&outcome, 2));
}

#[test]
fn a_number_point_without_a_value_is_refused() {
    let harness = Harness::new();
    let mut hollow = number_point(as_int(1));
    hollow.value = None;
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("hollow-point", gauge(vec![hollow]))],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::MissingValue {
            field: "data point value"
        })
    ));
    assert!(harness.drain().is_empty());
}
