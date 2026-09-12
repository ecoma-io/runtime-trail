//! The metrics side of the suite: stream identities, kind coherence,
//! point duplicate delivery, exemplars, the per-export cap and the
//! partial-success walk. Spans, logs and the queue live in `tests`.

use std::sync::Arc;

use runtime_trail_telemetry_model::{
    AdmissionTime, BudgetName, DATA_POINT_FLAG_NO_RECORDED_VALUE, Float, MetricNumber, MetricPoint,
    PointShape, StreamShapeError, Temporality, budgets::OTLP_PAYLOAD_BYTES,
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

#[test]
fn two_same_named_metrics_differing_only_in_description_are_two_streams() {
    let harness = Harness::new();
    // Same instrument name, same everything — except the description, and
    // (necessarily) the point times: the same data point re-sent under a
    // changed descriptor is a conflict, not a second stream (see the
    // conflict test below). Two *different* points under two descriptors
    // are two streams, side by side in one export.
    let mut second = number_point(as_double(1.0));
    second.time_unix_nano = 11;
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![
                described_metric(
                    "in-flight",
                    "requests being served",
                    "1",
                    vec![],
                    vec![number_point(as_double(1.0))],
                ),
                described_metric(
                    "in-flight",
                    "requests queued at the gate",
                    "1",
                    vec![],
                    vec![second],
                ),
            ],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(
        outcome.admitted(),
        2,
        "same name, different description: two streams"
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
        !Arc::ptr_eq(a, b),
        "distinct identities intern to distinct streams"
    );
    let mut descriptions: Vec<_> = [a, b].iter().map(|s| s.description().unwrap()).collect();
    descriptions.sort_unstable();
    assert_eq!(
        descriptions,
        ["requests being served", "requests queued at the gate"],
        "each stream keeps its own description intact"
    );
}

#[test]
fn a_point_redelivered_under_a_changed_descriptor_is_a_recorded_conflict() {
    let harness = Harness::new();
    // The descriptor is stream identity — but the collapse key is the
    // descriptor-less OTel key, so a re-delivery of the *same point* under
    // a changed description lands on the standing point. The law fires:
    // the first descriptor stands, the divergence is a recorded conflict,
    // and the refused delivery admits nothing — no second stream, no
    // second queue entry, no interned stream left behind.
    let point_at = |description: &str, time: u64| {
        let mut point = number_point(as_double(1.0));
        point.time_unix_nano = time;
        encode(&metrics_request(vec![resource_metrics(
            Some(resource(Vec::new())),
            vec![scope_metrics(
                Some(scope("test")),
                vec![described_metric(
                    "in-flight",
                    description,
                    "1",
                    vec![],
                    vec![point],
                )],
            )],
        )]))
    };

    let first_payload = point_at("requests being served", 10);
    let redelivery_payload = point_at("requests queued at the gate", 10);

    let first = harness
        .pipeline
        .ingest_metrics(now(), &first_payload)
        .expect("admitted");
    assert_eq!(first.admitted(), 1);

    let redelivery = harness
        .pipeline
        .ingest_metrics(AdmissionTime::from_unix_nano(99), &redelivery_payload)
        .expect("walked");
    assert!(
        matches!(&redelivery.records[0], RecordOutcome::Conflict { .. }),
        "a re-delivery under a changed descriptor is a conflict: {redelivery:?}"
    );
    assert_eq!(
        harness.drain().len(),
        1,
        "the standing entry is the whole story; the conflict queues nothing"
    );

    // The refused delivery left nothing behind: the changed descriptor is
    // not interned, and a *different point* under it admits as the second
    // stream it is — first descriptor standing beside it.
    let new_point_payload = point_at("requests queued at the gate", 11);
    let second = harness
        .pipeline
        .ingest_metrics(AdmissionTime::from_unix_nano(99), &new_point_payload)
        .expect("walked");
    assert_eq!(second.admitted(), 1, "a new point is not a re-delivery");
    let queued = harness.drain();
    assert_eq!(queued.len(), 1);
    let StoredRecord::Point { stream, .. } = &queued[0].record else {
        panic!("expected a queued point");
    };
    assert_eq!(
        stream.description(),
        Some("requests queued at the gate"),
        "the second stream carries its own descriptor"
    );
}

#[test]
fn the_descriptor_survives_decode_byte_exact() {
    let harness = Harness::new();
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![described_metric(
                "in-flight",
                "requests being served",
                "ms",
                vec![
                    attr("tier", str_value("edge")),
                    attr("team", str_value("edge")),
                ],
                vec![number_point(as_double(1.0))],
            )],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 1);

    let queued = harness.drain();
    let StoredRecord::Point { stream, .. } = &queued[0].record else {
        panic!("expected a queued point");
    };
    assert_eq!(
        stream.description(),
        Some("requests being served"),
        "the description decodes whole"
    );
    assert_eq!(stream.unit(), Some("ms"));
    let keys: Vec<&str> = stream
        .metadata()
        .iter()
        .map(|(key, _)| key.as_str())
        .collect();
    assert_eq!(keys, ["team", "tier"], "the metadata map arrives whole");
    assert_eq!(
        stream.metadata().len(),
        2,
        "every sent metadata entry is present"
    );

    // The empty string is not a description: proto3 strings have no
    // presence, so an unset description and a "" description arrive
    // identically and both read as absent — "absent is not empty" holds
    // at the decode site, not just in the model.
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![described_metric(
                "bare",
                "",
                "",
                vec![],
                vec![number_point(as_double(1.0))],
            )],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 1);
    let queued = harness.drain();
    let StoredRecord::Point { stream, .. } = &queued[0].record else {
        panic!("expected a queued point");
    };
    assert!(
        stream.description().is_none() && stream.unit().is_none(),
        "\"\" on the wire is absence, not an empty-string descriptor"
    );
    assert!(stream.metadata().is_empty());
}

#[test]
fn duplicate_metadata_keys_are_a_named_per_record_refusal() {
    let harness = Harness::new();
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![described_metric(
                "in-flight",
                "requests being served",
                "1",
                vec![
                    attr("tier", str_value("edge")),
                    attr("tier", str_value("core")),
                ],
                vec![number_point(as_double(1.0))],
            )],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    assert!(
        matches!(
            rejected_reason(&outcome, 0),
            RecordRejection::DuplicateKey(error) if error.key == "tier"
        ),
        "the refusal names the duplicated key"
    );
    assert!(
        harness.drain().is_empty(),
        "nothing of a refused record is admitted"
    );
}

#[test]
fn stream_metadata_over_budget_refuses_at_each_point_position() {
    let harness = Harness::new();
    // Metadata rides the stream identity, and the identity rides every
    // point — so an over-budget metadata map refuses every point of the
    // stream, each refusal naming the budget the payload trips. (The
    // stream-level gate is the ledger's: the whole-stream check cannot see
    // a payload that arrives point by point.)
    let oversized = described_metric(
        "in-flight",
        "requests being served",
        "1",
        vec![attr("blob", str_value(&"a".repeat(5_000)))],
        vec![number_point(as_double(1.0)), number_point(as_double(2.0))],
    );
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(Some(scope("test")), vec![oversized])],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    for position in 0..2 {
        assert!(
            matches!(
                rejected_reason(&outcome, position),
                RecordRejection::Budget(rejection)
                    if rejection.budget == BudgetName::AttributeValueSize
            ),
            "position {position} names the value-size budget"
        );
    }
    assert!(harness.drain().is_empty());

    // The refusal leaves no interned stream behind: the same stream,
    // within budget, admits cleanly afterwards.
    let within = described_metric(
        "in-flight",
        "requests being served",
        "1",
        vec![attr("tier", str_value("edge"))],
        vec![number_point(as_double(1.0))],
    );
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(Some(scope("test")), vec![within])],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(AdmissionTime::from_unix_nano(99), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 1);
    assert_eq!(harness.drain().len(), 1);
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
        .ingest_metrics(AdmissionTime::from_unix_nano(99), &second_payload)
        .expect("walked");

    assert_eq!(first_outcome.admitted(), 1);
    assert!(matches!(
        &second_outcome.records[0],
        RecordOutcome::Collapsed { .. }
    ));
    let drained = harness.drain();
    assert_eq!(
        drained.len(),
        1,
        "a collapse queues nothing: the standing point's entry is the whole story"
    );
    assert_eq!(
        drained[0].entity,
        standing_entity(&second_outcome, 0),
        "one point, one entity"
    );
    assert_eq!(
        drained[0].admitted_at,
        now(),
        "the standing point keeps its own admission time, not the retry's"
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
fn an_export_at_the_point_cap_admits_in_full() {
    let harness = Harness::new();
    // Exactly the contract's per-export budget — the cap admits; only one
    // past it refuses.
    let points: Vec<metrics::NumberDataPoint> = (0..10_000)
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
            vec![metric("cap", gauge(points))],
        )],
    )]));
    assert!(
        payload.len() < OTLP_PAYLOAD_BYTES,
        "the legal cap sits far under the payload ceiling"
    );

    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("at the cap is legal");
    assert_eq!(outcome.admitted(), 10_000);
    assert_eq!(outcome.rejected(), 0);
    assert_eq!(harness.drain().len(), 10_000, "every point handed off");
}

#[test]
fn a_point_over_the_exemplar_budget_refuses_at_its_position() {
    let harness = Harness::new();
    let exemplar = || metrics::Exemplar {
        filtered_attributes: Vec::new(),
        time_unix_nano: 11,
        span_id: Vec::new(),
        trace_id: Vec::new(),
        value: Some(metrics::exemplar::Value::AsInt(1)),
    };
    let mut fine_first = number_point(as_double(1.0));
    fine_first.time_unix_nano = 1;
    let mut over = number_point(as_double(2.0));
    over.time_unix_nano = 2;
    over.exemplars = vec![
        exemplar(),
        exemplar(),
        exemplar(),
        exemplar(),
        exemplar(), // one past the per-point budget of four
    ];
    let mut fine_last = number_point(as_double(3.0));
    fine_last.time_unix_nano = 3;

    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric(
                "exemplared",
                gauge(vec![fine_first, over, fine_last]),
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
        "position 0 carries no exemplars"
    );
    assert!(
        matches!(
            rejected_reason(&outcome, 1),
            RecordRejection::Budget(rejection)
                if rejection.budget == BudgetName::ExemplarsPerDataPoint
                    && rejection.limit == 4
                    && rejection.observed == 5
        ),
        "position 1 names the exemplar budget, its limit and the observed count"
    );
    assert!(
        matches!(&outcome.records[2], RecordOutcome::Admitted { .. }),
        "position 2 carries no exemplars"
    );
    assert_eq!(
        harness.drain().len(),
        2,
        "only the over-budget point is missing from the hand-off"
    );
}

#[test]
fn a_summary_sum_keeps_negative_zero_and_reads_positive_zero_as_absent() {
    let harness = Harness::new();

    // Negative zero, on real wire bytes: prost's own encoder skips -0.0
    // (its zero check is numeric), so the encoding a compliant emitter
    // sends is written by hand here — `sum` is fixed64 field 5 of
    // SummaryDataPoint, with the sign bit set.
    let mut point = vec![0x11]; // start_time_unix_nano, wire type 1
    point.extend_from_slice(&1_u64.to_le_bytes());
    point.push(0x19); // time_unix_nano
    point.extend_from_slice(&10_u64.to_le_bytes());
    point.push(0x21); // count
    point.extend_from_slice(&1_u64.to_le_bytes());
    point.push(0x29); // sum
    point.extend_from_slice(&(-0.0_f64).to_bits().to_le_bytes());

    let mut wire_metric = field(0x0A, b"signed"); // Metric.name
    // Metric.summary (oneof data, field 11) → Summary.data_points (field 1).
    wire_metric.extend(field(0x5A, &field(0x0A, &point)));
    let scope_metrics_bytes = field(0x12, &wire_metric); // ScopeMetrics.metrics
    let resource_metrics_bytes = field(0x12, &scope_metrics_bytes); // ResourceMetrics.scope_metrics
    let payload = field(0x0A, &resource_metrics_bytes); // Export.resource_metrics

    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    assert_eq!(outcome.admitted(), 1);

    let queued = harness.drain();
    let StoredRecord::Point {
        point: negative, ..
    } = &queued[0].record
    else {
        panic!("expected a queued summary point")
    };
    let MetricPoint::Summary(negative) = negative.as_ref() else {
        panic!("expected a summary")
    };
    assert_eq!(
        negative.sum.map(Float::bits),
        Some((-0.0_f64).to_bits()),
        "-0.0 survives with its sign"
    );

    // Positive zero through the ordinary path: indistinguishable from
    // unset on the wire (proto3), so it reads as absent.
    let summary_at = |sum: f64, time: u64| metrics::SummaryDataPoint {
        attributes: Vec::new(),
        start_time_unix_nano: 1,
        time_unix_nano: time,
        count: 1,
        sum,
        quantile_values: Vec::new(),
        flags: 0,
    };
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric(
                "unsigned",
                metrics::metric::Data::Summary(metrics::Summary {
                    data_points: vec![summary_at(0.0, 11)],
                }),
            )],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("walked");
    assert_eq!(outcome.admitted(), 1);

    let queued = harness.drain();
    let StoredRecord::Point {
        point: positive, ..
    } = &queued[0].record
    else {
        panic!("expected a queued summary point")
    };
    let MetricPoint::Summary(positive) = positive.as_ref() else {
        panic!("expected a summary")
    };
    assert_eq!(
        positive.sum, None,
        "positive zero is indistinguishable from unset on the wire"
    );
}

#[test]
fn non_finite_measurements_survive_the_round_trip() {
    let harness = Harness::new();
    let measurements = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
    let points: Vec<metrics::NumberDataPoint> = measurements
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let mut point = number_point(as_double(*value));
            point.time_unix_nano = u64::try_from(index + 10).expect("a test time fits");
            point
        })
        .collect();
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("non-finite", gauge(points))],
        )],
    )]));

    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 3);

    let queued = harness.drain();
    for (record, value) in queued.iter().zip(measurements) {
        let StoredRecord::Point { point, .. } = &record.record else {
            panic!("expected a queued point")
        };
        let MetricPoint::Number(number) = point.as_ref() else {
            panic!("expected a number point")
        };
        assert_eq!(
            number.value,
            MetricNumber::Double(Float::new(value)),
            "{value} round-trips bit-exactly (Float equality is bit pattern)"
        );
    }
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
