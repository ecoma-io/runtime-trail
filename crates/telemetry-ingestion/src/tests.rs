//! The ingestion suite: spans, logs, trace context, duplicate delivery,
//! envelope sharing, admission signals, budget limits, the bounded queue,
//! and the deep-payload guarantee. Metric fixtures live in `tests_metrics`.

use std::sync::Arc;

use runtime_trail_telemetry_model::{Accounted, BudgetName, EntityId, SpanId, TraceFlags, TraceId};

use crate::fixtures::*;
use crate::otlp::opentelemetry::{common::v1 as common, logs::v1 as logs, trace::v1 as trace};
use crate::queue::{
    PIPELINE_QUEUE_NAME, QUEUE_CEILING_BYTES, QueuedRecord, RecordSink, StoredRecord,
};
use crate::signal::{AdmissionSignal, RecordOutcome, RecordRejection, Unrepresentable};

// ------------------------------------------------------ one-record helpers

fn one_span_export(span: trace::Span) -> Vec<u8> {
    encode(&traces_request(vec![resource_spans(
        Some(resource(Vec::new())),
        vec![scope_spans(Some(scope("test")), vec![span])],
    )]))
}

fn one_log_export(record: logs::LogRecord) -> Vec<u8> {
    encode(&logs_request(vec![resource_logs(
        Some(resource(Vec::new())),
        vec![scope_logs(Some(scope("test")), vec![record])],
    )]))
}

// ---------------------------------------------------------- trace context

#[test]
fn invalid_ids_are_preserved_verbatim_and_get_assigned_ids() {
    let harness = Harness::new();
    // Both ids explicitly sent as all-zero bytes: the wire carries them,
    // the model preserves them, and nothing natural remains to identify
    // the span by.
    let payload = one_span_export(trace_span("zero", [0; 16], [0; 8]));
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");

    assert!(matches!(
        &outcome.records[0],
        RecordOutcome::Admitted { entity }
            if matches!(entity, EntityId::Assigned(_))
    ));
    let queued = harness.drain();
    let StoredRecord::Span(span) = &queued[0].record else {
        panic!("expected a span in the queue");
    };
    assert_eq!(span.context.trace_id, TraceId::from_bytes([0; 16]));
    assert_eq!(span.context.span_id.as_bytes(), [0; 8]);
}

#[test]
fn a_valid_span_keeps_its_natural_identity() {
    let harness = Harness::new();
    let payload = one_span_export(trace_span("span", T1, S1));
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    assert!(matches!(
        &outcome.records[0],
        RecordOutcome::Admitted {
            entity: EntityId::Span { .. }
        }
    ));
}

#[test]
fn absent_parent_differs_from_zero_parent() {
    let harness = Harness::new();
    let mut root = trace_span("root", T1, S1);
    root.parent_span_id = Vec::new();
    let mut child_of_zero = trace_span("child", T1, S2);
    child_of_zero.parent_span_id = vec![0; 8];

    let payload = encode(&traces_request(vec![resource_spans(
        Some(resource(Vec::new())),
        vec![scope_spans(Some(scope("test")), vec![root, child_of_zero])],
    )]));
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 2);

    let queued = harness.drain();
    let [first, second] = queued.as_slice() else {
        panic!("expected two queued spans");
    };
    let StoredRecord::Span(root) = &first.record else {
        panic!()
    };
    let StoredRecord::Span(child) = &second.record else {
        panic!()
    };
    assert!(root.parent_span_id.is_none(), "absent parent stays absent");
    assert_eq!(
        child.parent_span_id.map(SpanId::as_bytes),
        Some([0; 8]),
        "a sent zero parent stays a zero parent"
    );
}

#[test]
fn flags_carry_every_bit_the_emitter_sent() {
    let harness = Harness::new();
    let mut span = trace_span("flags", T1, S1);
    // The sampled bit plus the W3C propagator-reserved bits 8/9 and bit 31.
    span.flags = 0x8000_0301;
    let payload = one_span_export(span);
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 1);

    let StoredRecord::Span(span) = &harness.drain()[0].record else {
        panic!()
    };
    assert_eq!(span.context.flags.bits(), 0x8000_0301);
    assert!(span.context.flags.sampled());
}

#[test]
fn trace_state_entries_come_through_in_emitter_order() {
    let harness = Harness::new();
    let mut span = trace_span("state", T1, S1);
    span.trace_state = "vendor2=second,vendor1=first".to_owned();
    let payload = one_span_export(span);
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");

    let StoredRecord::Span(span) = &harness.drain()[0].record else {
        panic!()
    };
    let entries: Vec<(String, String)> = span
        .context
        .tracestate
        .iter()
        .map(|entry| (entry.vendor.clone(), entry.value.clone()))
        .collect();
    assert_eq!(
        entries,
        vec![
            ("vendor2".to_owned(), "second".to_owned()),
            ("vendor1".to_owned(), "first".to_owned()),
        ]
    );
    assert_eq!(outcome.admitted(), 1);
}

#[test]
fn a_trace_state_member_without_a_value_is_refused() {
    let harness = Harness::new();
    let mut span = trace_span("state", T1, S1);
    span.trace_state = "not-a-pair".to_owned();
    let payload = one_span_export(span);
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::TraceState { raw })
            if raw == "not-a-pair"
    ));
    assert!(harness.drain().is_empty());
}

#[test]
fn wrong_length_ids_are_refused_by_name() {
    let harness = Harness::new();
    let mut span = trace_span("short", T1, S1);
    span.trace_id = vec![0x01; 15];
    let payload = one_span_export(span);
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::IdLength {
            field: "span trace_id",
            expected: 16,
            found: 15,
        })
    ));
    assert!(harness.drain().is_empty());
}

// ----------------------------------------------------------------- spans

#[test]
fn events_and_links_keep_emitter_order_and_flags() {
    let harness = Harness::new();
    let mut span = trace_span("ordered", T1, S1);
    // Sent out of alphabetical order on purpose: order is preserved, not
    // sorted.
    span.events = vec![
        trace::span::Event {
            time_unix_nano: 5,
            name: "zulu".to_owned(),
            attributes: vec![attr("kind", str_value("start"))],
            dropped_attributes_count: 9,
        },
        trace::span::Event {
            time_unix_nano: 0,
            name: "alpha".to_owned(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        },
    ];
    span.dropped_events_count = 2;
    span.links = vec![trace::span::Link {
        trace_id: T2.to_vec(),
        span_id: vec![0; 8],
        trace_state: "k=v".to_owned(),
        attributes: Vec::new(),
        dropped_attributes_count: 4,
        flags: 0x1,
    }];
    span.dropped_links_count = 1;
    span.status = Some(trace::Status {
        message: "boom".to_owned(),
        code: trace::status::StatusCode::Unset as i32,
    });

    let payload = one_span_export(span);
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 1);

    let StoredRecord::Span(span) = &harness.drain()[0].record else {
        panic!()
    };
    let names: Vec<&str> = span
        .events
        .iter()
        .map(|event| event.name.as_str())
        .collect();
    assert_eq!(names, ["zulu", "alpha"]);
    assert_eq!(span.events[0].time_unix_nano, Some(5));
    assert_eq!(
        span.events[1].time_unix_nano, None,
        "an absent event time stays absent"
    );
    assert_eq!(span.events[0].dropped_attribute_count, 9);
    assert_eq!(span.emitter_dropped.events, 2);
    assert_eq!(span.emitter_dropped.links, 1);

    let link = &span.links[0];
    assert_eq!(link.context.trace_id, TraceId::from_bytes(T2));
    assert_eq!(
        link.context.span_id.as_bytes(),
        [0; 8],
        "zero link ids stay zero"
    );
    assert_eq!(link.context.flags.bits(), 0x1);
    assert_eq!(link.dropped_attribute_count, 4);

    assert_eq!(
        span.status.code,
        runtime_trail_telemetry_model::SpanStatusCode::Unset
    );
    assert_eq!(
        span.status.message, "boom",
        "a message survives with the Unset code"
    );
}

#[test]
fn envelope_dropped_counts_are_carried_to_every_record() {
    let harness = Harness::new();
    let mut envelope_resource = resource(Vec::new());
    envelope_resource.dropped_attributes_count = 7;
    let mut envelope_scope = scope("dropping");
    envelope_scope.dropped_attributes_count = 5;
    let mut span = trace_span("counted", T1, S1);
    span.dropped_attributes_count = 1;
    span.events = vec![trace::span::Event {
        time_unix_nano: 5,
        name: "e".to_owned(),
        attributes: Vec::new(),
        dropped_attributes_count: 9,
    }];

    let payload = encode(&traces_request(vec![resource_spans(
        Some(envelope_resource),
        vec![scope_spans(Some(envelope_scope), vec![span])],
    )]));
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 1);

    let StoredRecord::Span(span) = &harness.drain()[0].record else {
        panic!()
    };
    assert_eq!(span.resource.dropped_attributes_count, 7);
    assert_eq!(span.scope.dropped_attributes_count, 5);
    assert_eq!(span.emitter_dropped.attributes, 1);
    assert_eq!(span.events[0].dropped_attribute_count, 9);
}

// ---------------------------------------------------------- span refusal

#[test]
fn unknown_span_enums_are_refused_by_name() {
    let harness = Harness::new();
    let mut kinded = trace_span("kind", T1, S1);
    kinded.kind = 9;
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(kinded))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::UnknownEnumValue {
            field: "span kind",
            value: 9
        })
    ));

    let mut statused = trace_span("status", T1, S1);
    statused.status = Some(trace::Status {
        message: String::new(),
        code: 7,
    });
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(statused))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::UnknownEnumValue {
            field: "status code",
            value: 7
        })
    ));
    assert!(harness.drain().is_empty());
}

#[test]
fn attribute_pathologies_are_refused_by_name() {
    let harness = Harness::new();

    // Duplicate keys: keeping one would drop a value the emitter sent.
    let mut duplicated = trace_span("dup", T1, S1);
    duplicated.attributes = vec![attr("k", str_value("one")), attr("k", str_value("two"))];
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(duplicated))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::DuplicateKey(_)
    ));

    // A homogeneous array, then a mixed one.
    let mut homogeneous = trace_span("array", T1, S1);
    homogeneous.attributes = vec![attr("a", array_value(vec![str_value("x"), str_value("y")]))];
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(homogeneous))
        .expect("walked");
    assert_eq!(outcome.admitted(), 1, "a single-kind array is fine");
    harness.drain();

    let mut mixed = trace_span("mixed", T1, S1);
    mixed.attributes = vec![attr("a", array_value(vec![str_value("x"), int_value(1)]))];
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(mixed))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::MixedKindArray(_)
    ));

    // An AnyValue with no kind set, and the profiling-only strindex form.
    let mut empty = trace_span("empty", T1, S1);
    empty.attributes = vec![attr("k", empty_value())];
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(empty))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::MissingValue {
            field: "span attribute"
        })
    ));

    let mut strindex = trace_span("strindex", T1, S1);
    strindex.attributes = vec![attr(
        "k",
        common::AnyValue {
            value: Some(common::any_value::Value::StringValueStrindex(3)),
        },
    )];
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(strindex))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::MissingValue {
            field: "span attribute"
        })
    ));
    assert!(harness.drain().is_empty());
}

#[test]
fn a_resource_with_entity_refs_refuses_every_record_beneath_it() {
    let harness = Harness::new();
    let mut refs = resource(Vec::new());
    refs.entity_refs = vec![common::EntityRef::default()];
    let payload = encode(&traces_request(vec![resource_spans(
        Some(refs),
        vec![scope_spans(
            Some(scope("test")),
            vec![trace_span("a", T1, S1), trace_span("b", T1, S2)],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("walked");
    assert_eq!(outcome.rejected(), 2);
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::UnsupportedEntityRefs)
    ));
    assert!(matches!(
        rejected_reason(&outcome, 1),
        RecordRejection::Unrepresentable(Unrepresentable::UnsupportedEntityRefs)
    ));
    assert!(harness.drain().is_empty());
}

#[test]
fn an_oversized_attribute_value_names_the_budget() {
    let harness = Harness::new();
    let mut span = trace_span("big", T1, S1);
    span.attributes = vec![attr("blob", str_value(&"a".repeat(5_000)))];
    let payload = one_span_export(span);
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Budget(rejection)
            if rejection.budget == BudgetName::AttributeValueSize
    ));
    assert!(harness.drain().is_empty());
}

#[test]
fn a_deep_value_is_refused_by_the_model_depth_budget() {
    let harness = Harness::new();
    // Nine nested key-value lists: one past the model's depth budget of
    // eight. Built iteratively — the test itself must not recurse.
    let mut value = kvlist_value(vec![attr("leaf", int_value(1))]);
    for _ in 1..9 {
        value = kvlist_value(vec![attr("level", value)]);
    }
    let mut span = trace_span("deep", T1, S1);
    span.attributes = vec![attr("deep", value)];
    let payload = one_span_export(span);
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Budget(rejection)
            if rejection.budget == BudgetName::KeyValueListDepth
    ));

    // Depth eight is the budget's edge and is admitted.
    let mut value = kvlist_value(vec![attr("leaf", int_value(1))]);
    for _ in 1..8 {
        value = kvlist_value(vec![attr("level", value)]);
    }
    let mut span = trace_span("shallow-enough", T1, S2);
    span.attributes = vec![attr("deep", value)];
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(span))
        .expect("walked");
    assert_eq!(outcome.admitted(), 1);
}

// ------------------------------------------------------------------ logs

#[test]
fn the_two_severities_are_independent() {
    let harness = Harness::new();
    let mut numbered = log_record();
    numbered.severity_number = 9;
    numbered.body = Some(str_value("numbered"));
    let mut texted = log_record();
    texted.severity_text = "WARN".to_owned();
    texted.body = Some(str_value("texted"));

    let payload = encode(&logs_request(vec![resource_logs(
        Some(resource(Vec::new())),
        vec![scope_logs(Some(scope("test")), vec![numbered, texted])],
    )]));
    let outcome = harness
        .pipeline
        .ingest_logs(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 2);

    let queued = harness.drain();
    let [first, second] = queued.as_slice() else {
        panic!()
    };
    let StoredRecord::Log(numbered) = &first.record else {
        panic!()
    };
    let StoredRecord::Log(texted) = &second.record else {
        panic!()
    };
    assert_eq!(
        numbered
            .severity_number
            .map(runtime_trail_telemetry_model::SeverityNumber::get),
        Some(9)
    );
    assert_eq!(numbered.severity_text, None);
    assert_eq!(texted.severity_number, None);
    assert_eq!(texted.severity_text.as_deref(), Some("WARN"));
}

#[test]
fn severity_numbers_outside_the_domain_are_refused() {
    let harness = Harness::new();
    let mut record = log_record();
    record.severity_number = 25;
    let outcome = harness
        .pipeline
        .ingest_logs(now(), &one_log_export(record))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Severity(_)
    ));

    let mut negative = log_record();
    negative.severity_number = -3;
    let outcome = harness
        .pipeline
        .ingest_logs(now(), &one_log_export(negative))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::UnknownEnumValue {
            field: "severity_number",
            value: -3
        })
    ));
    assert!(harness.drain().is_empty());
}

#[test]
fn non_string_bodies_are_preserved() {
    let harness = Harness::new();
    let bodies = vec![
        int_value(-7),
        double_value(0.5),
        common::AnyValue {
            value: Some(common::any_value::Value::BoolValue(true)),
        },
        bytes_value(vec![0xCA, 0xFE]),
        array_value(vec![int_value(1), int_value(2)]),
        kvlist_value(vec![attr("k", str_value("v"))]),
    ];
    let records: Vec<logs::LogRecord> = bodies
        .into_iter()
        .map(|body| {
            let mut record = log_record();
            record.body = Some(body);
            record
        })
        .collect();
    let payload = encode(&logs_request(vec![resource_logs(
        Some(resource(Vec::new())),
        vec![scope_logs(Some(scope("test")), records)],
    )]));
    let outcome = harness
        .pipeline
        .ingest_logs(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 6);
    let queued = harness.drain();
    let StoredRecord::Log(int_body) = &queued[0].record else {
        panic!()
    };
    assert!(matches!(
        int_body.body,
        Some(runtime_trail_telemetry_model::Value::Int(-7))
    ));
    let StoredRecord::Log(list_body) = &queued[5].record else {
        panic!()
    };
    assert!(matches!(
        list_body.body,
        Some(runtime_trail_telemetry_model::Value::KvList(_))
    ));
}

#[test]
fn log_trace_context_facts_are_independent() {
    let harness = Harness::new();
    let mut trace_only = log_record();
    trace_only.trace_id = T1.to_vec();
    let mut span_only = log_record();
    span_only.span_id = S1.to_vec();
    let mut zero_sent = log_record();
    zero_sent.trace_id = vec![0; 16];
    zero_sent.span_id = vec![0; 8];
    let mut flagged = log_record();
    flagged.span_id = S2.to_vec();
    flagged.flags = 1;

    let payload = encode(&logs_request(vec![resource_logs(
        Some(resource(Vec::new())),
        vec![scope_logs(
            Some(scope("test")),
            vec![trace_only, span_only, zero_sent, flagged],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_logs(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 4);

    let queued = harness.drain();
    let StoredRecord::Log(trace_only) = &queued[0].record else {
        panic!()
    };
    assert_eq!(trace_only.trace_id, Some(TraceId::from_bytes(T1)));
    assert_eq!(trace_only.span_id, None);
    assert_eq!(trace_only.trace_flags, None);

    let StoredRecord::Log(span_only) = &queued[1].record else {
        panic!()
    };
    assert_eq!(
        span_only.trace_id, None,
        "an absent trace id is never fabricated"
    );
    assert_eq!(span_only.span_id.as_ref().map(|id| id.as_bytes()), Some(S1));

    let StoredRecord::Log(zero_sent) = &queued[2].record else {
        panic!()
    };
    assert_eq!(
        zero_sent.trace_id,
        Some(TraceId::from_bytes([0; 16])),
        "sent zeros stay"
    );
    assert_eq!(zero_sent.span_id.map(SpanId::as_bytes), Some([0; 8]));

    let StoredRecord::Log(flagged) = &queued[3].record else {
        panic!()
    };
    assert_eq!(flagged.trace_flags.map(TraceFlags::bits), Some(1));
}

#[test]
fn two_byte_identical_log_records_are_two_records() {
    let harness = Harness::new();
    let build = || {
        let mut record = log_record();
        record.body = Some(str_value("same"));
        record
    };
    let payload = encode(&logs_request(vec![resource_logs(
        Some(resource(Vec::new())),
        vec![scope_logs(Some(scope("test")), vec![build(), build()])],
    )]));
    let outcome = harness
        .pipeline
        .ingest_logs(now(), &payload)
        .expect("admitted");
    assert_eq!(
        outcome.admitted(),
        2,
        "log records have no natural identity"
    );
    assert_eq!(harness.drain().len(), 2);
}

// ------------------------------------------------------ duplicate delivery

#[test]
fn an_identical_span_redelivery_collapses() {
    let harness = Harness::new();
    let payload = one_span_export(trace_span("twice", T1, S1));

    let first = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    let second = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("walked");

    assert_eq!(first.admitted(), 1);
    assert!(matches!(
        &second.records[0],
        RecordOutcome::Collapsed { .. }
    ));
    let first_entity = standing_entity(&first, 0);
    assert_eq!(
        standing_entity(&second, 0),
        first_entity,
        "the first record stands"
    );
    assert_eq!(harness.pipeline.anomalies().total(), 0);

    // The collapse re-delivers the standing record: a re-delivery exists
    // only because an earlier attempt may not have reached the consumer.
    let drained = harness.drain();
    assert_eq!(drained.len(), 2, "both deliveries reach the hand-off");
    assert_eq!(drained[0].entity, first_entity);
    assert_eq!(drained[1].entity, first_entity);
    let [
        QueuedRecord {
            record: StoredRecord::Span(a),
            ..
        },
        QueuedRecord {
            record: StoredRecord::Span(b),
            ..
        },
    ] = drained.as_slice()
    else {
        panic!()
    };
    assert!(Arc::ptr_eq(a, b), "the ledger's own Arc, not a copy");
}

#[test]
fn a_differing_redelivery_is_a_recorded_conflict() {
    let harness = Harness::new();
    let first_payload = one_span_export(trace_span("original", T1, S1));
    let mut renamed = trace_span("renamed", T1, S1);
    renamed.name = "renamed".to_owned();
    let second_payload = one_span_export(renamed);

    let first = harness
        .pipeline
        .ingest_spans(now(), &first_payload)
        .expect("admitted");
    let second = harness
        .pipeline
        .ingest_spans(now(), &second_payload)
        .expect("walked");

    assert!(matches!(&second.records[0], RecordOutcome::Conflict { .. }));
    assert_eq!(standing_entity(&second, 0), standing_entity(&first, 0));
    assert_eq!(harness.pipeline.anomalies().span_identity_conflicts(), 1);
    assert_eq!(harness.queue.len(), 1, "a conflict queues nothing");
    harness.drain();
}

// ---------------------------------------------------- resource envelopes

#[test]
fn one_envelope_shares_one_resource_arc() {
    let harness = Harness::new();
    let payload = encode(&traces_request(vec![resource_spans(
        Some(resource(vec![attr("service.name", str_value("shared"))])),
        vec![scope_spans(
            Some(scope("test")),
            vec![trace_span("a", T1, S1), trace_span("b", T1, S2)],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 2);

    let queued = harness.drain();
    let [
        QueuedRecord {
            record: StoredRecord::Span(a),
            ..
        },
        QueuedRecord {
            record: StoredRecord::Span(b),
            ..
        },
    ] = queued.as_slice()
    else {
        panic!()
    };
    assert!(
        Arc::ptr_eq(&a.resource, &b.resource),
        "one envelope, one resource allocation"
    );
    assert!(Arc::ptr_eq(&a.scope, &b.scope));
}

#[test]
fn resource_equality_ignores_schema_url() {
    let harness = Harness::new();
    let mut first = resource_spans(
        Some(resource(vec![attr("service.name", str_value("same"))])),
        vec![scope_spans(
            Some(scope("test")),
            vec![trace_span("a", T1, S1)],
        )],
    );
    first.schema_url = "https://schema.first".to_owned();
    let mut second = resource_spans(
        Some(resource(vec![attr("service.name", str_value("same"))])),
        vec![scope_spans(
            Some(scope("test")),
            vec![trace_span("b", T1, S2)],
        )],
    );
    second.schema_url = "https://schema.second".to_owned();

    let payload = encode(&traces_request(vec![first, second]));
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 2);

    let queued = harness.drain();
    let [
        QueuedRecord {
            record: StoredRecord::Span(a),
            ..
        },
        QueuedRecord {
            record: StoredRecord::Span(b),
            ..
        },
    ] = queued.as_slice()
    else {
        panic!()
    };
    assert_eq!(
        a.resource, b.resource,
        "the attribute map is the identity, schema_url is not in it"
    );
    assert_ne!(
        a.resource.schema_url, b.resource.schema_url,
        "each envelope's schema_url is preserved verbatim"
    );
    assert!(
        !Arc::ptr_eq(&a.resource, &b.resource),
        "sharing is per envelope"
    );
}

// --------------------------------------------------- signals and budgets

#[test]
fn malformed_bytes_reject_without_retry_semantics() {
    let harness = Harness::new();
    for ingest in [
        |h: &Harness, p: &[u8]| h.pipeline.ingest_spans(now(), p),
        |h: &Harness, p: &[u8]| h.pipeline.ingest_logs(now(), p),
        |h: &Harness, p: &[u8]| h.pipeline.ingest_metrics(now(), p),
    ] {
        let signal = ingest(&harness, &[0xFF, 0xFF, 0xFF]).expect_err("malformed");
        assert!(matches!(signal, AdmissionSignal::MalformedRequest { .. }));
        assert!(!signal.is_retryable());
    }
    assert!(harness.drain().is_empty());
}

#[test]
fn an_oversized_payload_is_refused_before_parsing() {
    let queue = crate::queue::BoundedQueue::new(PIPELINE_QUEUE_NAME, QUEUE_CEILING_BYTES);
    let pipeline = crate::pipeline::Pipeline::with_config(
        Arc::clone(&queue) as Arc<dyn crate::queue::RecordSink>,
        runtime_trail_telemetry_model::BudgetLimits::default(),
        8,
    );
    let harness = Harness {
        pipeline: Arc::new(pipeline),
        queue,
    };

    // Legal bytes, but over a ceiling configured at startup.
    let payload = one_span_export(trace_span("fit", T1, S1));
    let signal = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect_err("over the payload ceiling");
    assert!(
        matches!(
            &signal,
            AdmissionSignal::PayloadOverCap {
                ceiling_bytes: 8,
                ..
            }
        ),
        "{signal:?}"
    );
    assert!(!signal.is_retryable());
    assert!(harness.drain().is_empty());
}

#[test]
fn draining_refuses_everything_with_the_closing_signal() {
    let harness = Harness::new();
    // Something admitted before shutdown is still in flight.
    let payload = one_span_export(trace_span("pre-drain", T1, S1));
    harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");

    harness.pipeline.begin_draining();
    assert!(harness.pipeline.is_draining());
    for ingest in [
        |h: &Harness, p: &[u8]| h.pipeline.ingest_spans(now(), p),
        |h: &Harness, p: &[u8]| h.pipeline.ingest_logs(now(), p),
        |h: &Harness, p: &[u8]| h.pipeline.ingest_metrics(now(), p),
    ] {
        let signal = ingest(&harness, &payload).expect_err("draining");
        assert_eq!(signal, AdmissionSignal::Draining);
        assert!(
            !signal.is_retryable(),
            "Draining is the closing signal, not a busy signal"
        );
    }
    assert_eq!(harness.queue.len(), 1, "records in flight still drain");
    harness.drain();
}

#[test]
fn queue_overflow_is_the_one_retryable_signal() {
    let harness = Harness::with_queue_ceiling(1);
    let payload = one_span_export(trace_span("overflow", T1, S1));
    let signal = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect_err("queue of one byte");
    assert!(matches!(
        &signal,
        AdmissionSignal::QueueSaturated {
            queue: "ingestion",
            ceiling_bytes: 1,
            attempted_bytes,
        } if *attempted_bytes > 1
    ));
    assert!(signal.is_retryable(), "overflow rejects the producer");
    assert!(harness.drain().is_empty());
}

#[test]
fn saturation_mid_export_keeps_records_already_admitted() {
    // A ceiling that fits exactly two of these spans: build one, measure
    // its accounted size, and use twice that as the ceiling.
    let measure = Harness::new();
    let payload = one_span_export(trace_span("sized", T1, S1));
    measure
        .pipeline
        .ingest_spans(now(), &payload)
        .expect("admitted");
    let one_span = measure.drain().pop().expect("one queued span");
    let ceiling = 2 * one_span.record.accounted_size();

    let harness = Harness::with_queue_ceiling(ceiling);
    let payload = encode(&traces_request(vec![resource_spans(
        Some(resource(Vec::new())),
        vec![scope_spans(
            Some(scope("test")),
            vec![
                trace_span("aaa", T1, S1),
                trace_span("bbb", T1, S2),
                trace_span("ccc", T1, [0x33; 8]),
            ],
        )],
    )]));

    let signal = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect_err("three spans, two-span queue");
    assert!(matches!(signal, AdmissionSignal::QueueSaturated { .. }));
    assert_eq!(
        harness.queue.len(),
        2,
        "the first two records stay handed off"
    );
    assert_eq!(harness.drain().len(), 2, "the consumer keeps draining");

    // The producer retries the whole export: every delivery now collapses
    // onto its standing record, and every collapse is re-offered — so the
    // retry makes exactly the progress the first attempt did and saturates
    // on the third span again. Backpressure, not loss.
    let signal = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect_err("three collapses still do not fit a two-span queue");
    assert!(matches!(signal, AdmissionSignal::QueueSaturated { .. }));
    assert_eq!(harness.drain().len(), 2);

    // The producer backs off, as the retryable signal tells it to: the
    // third span alone fits, its collapse re-offers the standing record,
    // and nothing admitted during the saturation was ever lost.
    let tail = encode(&traces_request(vec![resource_spans(
        Some(resource(Vec::new())),
        vec![scope_spans(
            Some(scope("test")),
            vec![trace_span("ccc", T1, [0x33; 8])],
        )],
    )]));
    let outcome = harness.pipeline.ingest_spans(now(), &tail).expect("walked");
    assert!(matches!(
        &outcome.records[0],
        RecordOutcome::Collapsed { .. }
    ));
    let drained = harness.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(
        drained[0].entity,
        standing_entity(&outcome, 0),
        "the record that stood through the saturation is the record delivered"
    );
}

#[test]
fn a_sixty_thousand_deep_payload_is_rejected_without_a_stack_overflow() {
    let harness = Harness::new();
    // Hand-crafted wire bytes, built iteratively: an `AnyValue.array_value`
    // chain 60,000 levels deep, wrapped in one span attribute. The protobuf
    // decoder's recursion limit must refuse it long before any recursion
    // could overflow the stack.
    // 60,000 nesting levels: each iteration adds one ArrayValue.values
    // hop and one AnyValue.array_value hop.
    let mut value: Vec<u8> = vec![0x18, 0x00]; // AnyValue{int_value: 0}
    for _ in 0..30_000 {
        let array = field(0x0A, &value); // ArrayValue.values
        value = field(0x2A, &array); // AnyValue.array_value
    }
    let mut key_value = field(0x0A, b"deep"); // KeyValue.key
    key_value.extend(field(0x12, &value)); // KeyValue.value

    let mut span = Vec::new();
    span.extend(field(0x0A, &[0x01; 16])); // Span.trace_id
    span.extend(field(0x12, &[0x11; 8])); // Span.span_id
    span.extend(field(0x2A, b"d")); // Span.name
    span.extend(field(0x4A, &key_value)); // Span.attributes

    let scope_spans_bytes = field(0x12, &span); // ScopeSpans.spans
    let resource_spans_bytes = field(0x12, &scope_spans_bytes); // ResourceSpans.scope_spans
    let payload = field(0x0A, &resource_spans_bytes); // Export.resource_spans

    let signal = harness
        .pipeline
        .ingest_spans(now(), &payload)
        .expect_err("past the recursion limit");
    assert!(matches!(signal, AdmissionSignal::MalformedRequest { .. }));
    assert!(harness.drain().is_empty());
}

// ------------------------------------------------------------ the queue

#[test]
fn the_queue_restores_headroom_on_drain() {
    let queue = crate::queue::BoundedQueue::new("test", 10_000);
    let span_record = {
        let harness = Harness::new();
        harness
            .pipeline
            .ingest_spans(now(), &one_span_export(trace_span("q", T1, S1)))
            .expect("admitted");
        harness.drain().pop().expect("one span")
    };
    queue.offer(span_record.clone()).expect("fits");
    assert_eq!(queue.accounted_bytes(), span_record.record.accounted_size());
    assert_eq!(queue.len(), 1);
    assert!(!queue.is_empty());

    let drained = queue.pop();
    assert_eq!(drained.entity, span_record.entity);
    assert_eq!(queue.accounted_bytes(), 0, "headroom is restored exactly");
    assert!(queue.is_empty());
    assert!(queue.front().is_none());
}

#[test]
fn a_metric_point_charges_its_stream_identity_in_full() {
    let harness = Harness::new();
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![metric("charge", gauge(vec![number_point(as_double(1.0))]))],
        )],
    )]));
    harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    let queued = harness.drain();
    let QueuedRecord {
        record: StoredRecord::Point { stream, point },
        ..
    } = &queued[0]
    else {
        panic!("expected a queued point")
    };

    // Full charge per point: the point, plus the identity it rides on —
    // structure fixed bytes, resource, scope, and the stream name.
    let identity_charge = stream.resource.accounted_size()
        + stream.scope.accounted_size()
        + runtime_trail_telemetry_model::STRUCTURE_FIXED_BYTES
        + runtime_trail_telemetry_model::heap_string_bytes(&stream.name);
    assert_eq!(
        queued[0].record.accounted_size(),
        point.accounted_size() + identity_charge,
        "sharing is not an accounting event"
    );
}

// ------------------------------------------------------------- the law

#[test]
fn only_queue_saturation_is_retryable_anywhere() {
    let signals = [
        AdmissionSignal::QueueSaturated {
            queue: "q",
            ceiling_bytes: 1,
            attempted_bytes: 2,
        },
        AdmissionSignal::PayloadOverCap {
            bytes: 2,
            ceiling_bytes: 1,
        },
        AdmissionSignal::ExportOverCap {
            rejection: runtime_trail_telemetry_model::BudgetRejection {
                budget: BudgetName::DataPointsPerExport,
                limit: 10_000,
                observed: 10_001,
            },
        },
        AdmissionSignal::MalformedRequest {
            detail: "no".to_owned(),
        },
        AdmissionSignal::Draining,
    ];
    let retryable: Vec<bool> = signals.iter().map(AdmissionSignal::is_retryable).collect();
    assert_eq!(retryable, [true, false, false, false, false]);

    let refusal = RecordRejection::Budget(runtime_trail_telemetry_model::BudgetRejection {
        budget: BudgetName::AttributeValueSize,
        limit: 4_096,
        observed: 5_000,
    });
    assert!(!refusal.is_retryable());
    assert!(!refusal.to_string().is_empty());
}

#[test]
fn the_crate_names_its_version() {
    assert!(!crate::VERSION.is_empty());
}
