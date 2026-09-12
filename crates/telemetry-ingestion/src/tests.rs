//! The ingestion suite: spans, logs, trace context, duplicate delivery,
//! envelope sharing, admission signals, budget limits, the bounded queue,
//! and the deep-payload guarantee. Metric fixtures live in `tests_metrics`.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use runtime_trail_telemetry_model::{
    Accounted, AdmissionTime, BudgetLimits, BudgetName, EntityId, SpanId, TraceFlags, TraceId,
    budgets::OTLP_PAYLOAD_BYTES,
};

use crate::fixtures::*;
use crate::otlp::opentelemetry::{common::v1 as common, logs::v1 as logs, trace::v1 as trace};
use crate::pipeline::Pipeline;
use crate::queue::{
    BoundedQueue, PIPELINE_QUEUE_NAME, QUEUE_CEILING_BYTES, QueuedRecord, RecordSink, StoredRecord,
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
fn a_string_table_key_is_refused_like_a_missing_value() {
    let harness = Harness::new();
    // KeyValue{key: "", key_strindex: 3, value: present}: the Profiling
    // string-table encoding. A non-Profiling receiver reads the key as
    // absent, which leaves the attribute keyless — refused, not admitted
    // with an empty key.
    let span = trace_span("keyless", T1, S1);
    let keyless = trace::Span {
        attributes: vec![common::KeyValue {
            key: String::new(),
            value: Some(str_value("present")),
            key_strindex: 3,
        }],
        ..span
    };
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(keyless))
        .expect("walked");
    assert!(matches!(
        rejected_reason(&outcome, 0),
        RecordRejection::Unrepresentable(Unrepresentable::MissingValue {
            field: "span attribute key"
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
        .ingest_spans(AdmissionTime::from_unix_nano(99), &payload)
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

    // The collapse queues nothing: the standing record's original queue
    // entry — still resident here — is the whole story. Exactly ONE queue
    // entry across the two deliveries, stamped with the FIRST delivery's
    // admission time, never the retry's.
    let drained = harness.drain();
    assert_eq!(
        drained.len(),
        1,
        "a collapse never re-offers the standing record"
    );
    assert_eq!(drained[0].entity, first_entity);
    assert_eq!(
        drained[0].admitted_at,
        now(),
        "the standing record keeps its own admission time"
    );
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

#[test]
fn a_resource_schema_url_drift_collapses_and_a_scope_drift_conflicts() {
    // The schema_url asymmetry, end to end: the resource's `schema_url` is
    // provenance outside identity — a re-delivery across the drift
    // collapses onto the standing span and the drift is counted as its own
    // anomaly. The scope's `schema_url` participates in identity — the
    // same drift there is a conflict, and the provenance counter does not
    // move.
    let harness = Harness::new();
    let first = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(trace_span("op", T1, S1)))
        .expect("admitted");
    assert_eq!(first.admitted(), 1);

    let drifted = encode(&traces_request(vec![resource_spans_with_schema_url(
        "https://schema/v2",
        Some(resource(Vec::new())),
        vec![scope_spans(
            Some(scope("test")),
            vec![trace_span("op", T1, S1)],
        )],
    )]));
    let second = harness
        .pipeline
        .ingest_spans(now(), &drifted)
        .expect("walked");
    assert!(
        matches!(&second.records[0], RecordOutcome::Collapsed { .. }),
        "equal content across the resource schema drift collapses: {second:?}"
    );
    assert_eq!(standing_entity(&second, 0), standing_entity(&first, 0));
    assert_eq!(
        harness.pipeline.anomalies().provenance_mismatches(),
        1,
        "the drift is recorded, observable"
    );
    assert_eq!(harness.pipeline.anomalies().span_identity_conflicts(), 0);

    let rescoped = encode(&traces_request(vec![resource_spans(
        Some(resource(Vec::new())),
        vec![scope_spans_with_schema_url(
            "https://scope-schema/v2",
            Some(scope("test")),
            vec![trace_span("op", T1, S1)],
        )],
    )]));
    let third = harness
        .pipeline
        .ingest_spans(now(), &rescoped)
        .expect("walked");
    assert!(
        matches!(&third.records[0], RecordOutcome::Conflict { .. }),
        "the scope's schema_url participates in identity: {third:?}"
    );
    assert_eq!(standing_entity(&third, 0), standing_entity(&first, 0));
    assert_eq!(harness.pipeline.anomalies().span_identity_conflicts(), 1);
    assert_eq!(
        harness.pipeline.anomalies().provenance_mismatches(),
        1,
        "the provenance counter is the resource level's alone"
    );

    let drained = harness.drain();
    assert_eq!(
        drained.len(),
        1,
        "collapse and conflict queue nothing: the standing span is the story"
    );
    assert_eq!(drained[0].entity, standing_entity(&first, 0));
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
    let queue = BoundedQueue::new(PIPELINE_QUEUE_NAME, QUEUE_CEILING_BYTES);
    let pipeline = Pipeline::with_config(
        Arc::clone(&queue) as Arc<dyn RecordSink>,
        BudgetLimits::default(),
        8,
    )
    .expect("the contract ceiling clears the contract bound");
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
    // At the port itself: a ceiling below the next record's accounted size
    // refuses the producer with the one retryable signal. A *pipeline* can
    // only reach saturation by accumulation — construction refuses a
    // ceiling below the legal-record bound (see
    // a_queue_ceiling_below_the_legal_maximum_refuses_construction), and
    // saturation_mid_export_keeps_records_already_admitted walks that
    // path.
    let harness = Harness::new();
    harness
        .pipeline
        .ingest_spans(now(), &one_span_export(trace_span("overflow", T1, S1)))
        .expect("admitted");
    let record = harness.drain().pop().expect("one queued span");

    let queue = BoundedQueue::new(PIPELINE_QUEUE_NAME, 1);
    let signal = queue
        .offer(record)
        .expect_err("a one-byte queue holds nothing");
    assert!(matches!(
        &signal,
        AdmissionSignal::QueueSaturated {
            queue: "ingestion",
            ceiling_bytes: 1,
            attempted_bytes,
        } if *attempted_bytes > 1
    ));
    assert!(signal.is_retryable(), "overflow rejects the producer");
}

#[test]
fn a_queue_ceiling_below_the_legal_maximum_refuses_construction() {
    let queue = BoundedQueue::new(PIPELINE_QUEUE_NAME, 1024);
    let Err(error) = Pipeline::with_config(
        Arc::clone(&queue) as Arc<dyn RecordSink>,
        BudgetLimits::default(),
        OTLP_PAYLOAD_BYTES,
    ) else {
        panic!("a one-kibibyte queue is below the legal maximum");
    };
    assert_eq!(error.ceiling_bytes, 1024);
    assert!(
        error.legal_record_bound_bytes > error.ceiling_bytes,
        "the default budgets' worst case is far above one kibibyte"
    );

    // The refusal names both numbers: an operator can read the bound they
    // must clear, at startup, not as an unadmittable record at runtime.
    let message = error.to_string();
    assert!(
        message.contains(&error.legal_record_bound_bytes.to_string()),
        "the message names the bound: {message}"
    );
    assert!(
        message.contains(&error.ceiling_bytes.to_string()),
        "the message names the offending ceiling: {message}"
    );

    // The construction gate's promise: a queue AT the bound takes every
    // single legal record.
    let at_bound = Pipeline::with_config(
        BoundedQueue::new(PIPELINE_QUEUE_NAME, error.legal_record_bound_bytes),
        BudgetLimits::default(),
        OTLP_PAYLOAD_BYTES,
    )
    .expect("a queue at the bound constructs");
    let outcome = at_bound
        .ingest_spans(now(), &one_span_export(trace_span("fits", T1, S1)))
        .expect("a legal span fits an empty queue at the bound");
    assert_eq!(outcome.admitted(), 1);
}

#[test]
fn the_default_configuration_constructs() {
    // The contract numbers: the budgets' worst-case spend sits inside the
    // contract queue ceiling, so the default pipeline starts — and a
    // default-configured export walks end to end.
    let bound = Pipeline::legal_record_bound_bytes(&BudgetLimits::default());
    assert!(
        bound < QUEUE_CEILING_BYTES,
        "the contract budgets must fit the contract queue ceiling: \
         bound {bound}, ceiling {QUEUE_CEILING_BYTES}"
    );
    let harness = Harness::new();
    let outcome = harness
        .pipeline
        .ingest_spans(now(), &one_span_export(trace_span("fits", T1, S1)))
        .expect("admitted");
    assert_eq!(outcome.admitted(), 1);
}

#[test]
fn saturation_mid_export_keeps_records_already_admitted() {
    // Shrunken budgets shrink the legal-record bound, so a queue at the
    // bound — one the construction gate accepts — can still overflow on a
    // multi-record export. How many of these spans fit is *derived* from
    // the measured record against the bound, not assumed: the bound is a
    // max over every record kind's formula, so a shape change in any one
    // formula moves it, and the fixture saturates at whatever whole
    // multiple the measurement says.
    let limits = shrunken_limits();
    let bound = Pipeline::legal_record_bound_bytes(&limits);

    // Measure one of these spans through a queue at the bound — which is
    // also the construction gate's promise in action: any single legal
    // record fits an empty queue at the bound.
    let (measure_queue, measure_pipeline) = pipeline_over(bound, limits);
    measure_pipeline
        .ingest_spans(now(), &one_span_export(sized_span("sized", S1)))
        .expect("one legal span fits an empty queue at the bound");
    let measured = measure_queue
        .front()
        .expect("one queued span")
        .record
        .accounted_size();
    let fits = bound / measured;
    assert!(
        fits >= 2,
        "the fixture needs a multi-span export to overflow: bound \
         {bound}, measured {measured}"
    );
    // The saturating export carries exactly one span too many.
    let offered = fits + 1;

    // A bound-sized queue: every single legal record fits, `fits` of these
    // spans fit, `offered` does not.
    let (queue, pipeline) = pipeline_over(bound, limits);
    let spans: Vec<_> = (1..=offered)
        .map(|index| {
            let mut span_id = [0u8; 8];
            span_id[0] = u8::try_from(index).expect("a few spans");
            sized_span(&format!("span-{index}"), span_id)
        })
        .collect();
    let payload = encode(&traces_request(vec![resource_spans(
        Some(resource(Vec::new())),
        vec![scope_spans(Some(scope("test")), spans)],
    )]));

    let signal = pipeline
        .ingest_spans(now(), &payload)
        .expect_err("one span too many for a queue at the bound");
    assert!(matches!(
        &signal,
        AdmissionSignal::QueueSaturated { ceiling_bytes, .. } if *ceiling_bytes == bound
    ));
    assert!(signal.is_retryable(), "overflow rejects the producer");
    assert_eq!(
        queue.len(),
        fits,
        "the records offered before saturation stay handed off"
    );

    // The producer retries the whole export while the queue is still
    // saturated. The re-deliveries of the first `fits` spans collapse onto
    // their standing records and queue nothing — but the refused span's
    // identity was ENDED by the refusal, so its re-delivery admits fresh,
    // offers, and is refused again: the same retryable signal, honestly,
    // for exactly as long as the queue cannot take the record. (A collapse
    // of the refused span here would have meant its entry stood behind a
    // delivery that never happened.)
    let signal = pipeline
        .ingest_spans(AdmissionTime::from_unix_nano(99), &payload)
        .expect_err("the once-refused span re-admits fresh and saturates again");
    assert!(matches!(
        &signal,
        AdmissionSignal::QueueSaturated { ceiling_bytes, .. } if *ceiling_bytes == bound
    ));
    assert_eq!(queue.len(), fits, "the retry queued nothing new");
    let drained = drain_queue(&queue);
    assert_eq!(drained.len(), fits);
    for record in &drained {
        assert_eq!(
            record.admitted_at,
            now(),
            "the standing entries keep their own admission times"
        );
    }

    // The consumer has drained everything. The re-deliveries of the first
    // `fits` spans collapse onto their standing records — draining a queue
    // entry is the consumer's half of the delivery, not the end of
    // identity — while the refused span is admitted FRESH and queued: the
    // retry the signal invited actually delivers.
    let outcome = pipeline
        .ingest_spans(AdmissionTime::from_unix_nano(99), &payload)
        .expect("the retry delivers what the refusal ended");
    assert!(
        outcome.records[..fits]
            .iter()
            .all(|record| matches!(record, RecordOutcome::Collapsed { .. })),
        "the delivered spans still collapse onto their standing records: {outcome:?}"
    );
    assert!(
        matches!(&outcome.records[fits], RecordOutcome::Admitted { .. }),
        "the refused span is admitted fresh, not collapsed onto a \
         stranded entry: {outcome:?}"
    );
    let delivered = drain_queue(&queue);
    assert_eq!(
        delivered.len(),
        1,
        "exactly the once-refused span queues now"
    );
    assert_eq!(delivered[0].entity, standing_entity(&outcome, fits));
}

/// A queue refusal ends the refused record's own identity — the same
/// lifecycle ADR 0008 gives a refused keep, applied by the pipeline — so
/// the retry the signal invites can actually deliver it.
#[test]
fn a_queue_refusal_ends_the_refused_spans_identity() {
    let limits = shrunken_limits();
    let bound = Pipeline::legal_record_bound_bytes(&limits);
    let (queue, pipeline) = pipeline_over(bound, limits);

    // Saturate the bound-sized queue with distinct single-span exports.
    // The export that saturated it was refused, and — the law under test —
    // its span's identity ended at that refusal.
    let queued = saturate_with_spans(&pipeline, &queue);
    assert!(queued >= 1, "the fixture needs queued spans in flight");

    // The refused span, re-delivered while the queue is still full: it
    // admits FRESH (nothing stands behind its delivery), offers, and is
    // refused again. A `Collapsed` outcome would mean a stranded entry.
    let refused = one_span_export(sized_span("refused", S2));
    let signal = pipeline
        .ingest_spans(AdmissionTime::from_unix_nano(99), &refused)
        .expect_err("the queue is still saturated");
    assert!(matches!(signal, AdmissionSignal::QueueSaturated { .. }));

    // The consumer drains. The identical re-delivery is ADMITTED — a
    // collapse here would strand the record forever behind an entry whose
    // delivery never happened, with the 429's promised retry useless.
    drain_queue(&queue);
    let retry = pipeline
        .ingest_spans(AdmissionTime::from_unix_nano(100), &refused)
        .expect("the retried span is admitted");
    assert!(
        matches!(&retry.records[0], RecordOutcome::Admitted { .. }),
        "the retry must deliver fresh, not collapse onto the refused \
         delivery's identity: {retry:?}"
    );
    assert_eq!(queue.len(), 1, "the retried span is queued for real");
    drain_queue(&queue);
}

/// A refusal undoes exactly what the refused admission created — and
/// nothing an earlier delivery still stands on. The collapse arm is the
/// sharp edge: B collapses onto A's standing entry, so B's export failing
/// on C's refusal must not end A's identity.
#[test]
fn a_refusal_undoes_only_what_that_admission_created() {
    let limits = shrunken_limits();
    let bound = Pipeline::legal_record_bound_bytes(&limits);
    let (queue, pipeline) = pipeline_over(bound, limits);

    // A is admitted and queued while there is room.
    let first = pipeline
        .ingest_spans(now(), &one_span_export(sized_span("queued", S1)))
        .expect("the empty queue takes A");
    let entity_a = standing_entity(&first, 0);

    // The queue saturates on the filler spans.
    let queued = saturate_with_spans(&pipeline, &queue);
    assert!(queued >= 1, "the fixture needs more than A in flight");
    let in_flight = queue.len();

    // One export, two records: B identical to A (a collapse — it must not
    // touch A), then C fresh (its offer is the one that refuses).
    let collapse_then_refuse = encode(&traces_request(vec![resource_spans(
        Some(resource(Vec::new())),
        vec![scope_spans(
            Some(scope("test")),
            vec![sized_span("queued", S1), sized_span("refused-now", S2)],
        )],
    )]));
    let signal = pipeline
        .ingest_spans(AdmissionTime::from_unix_nano(99), &collapse_then_refuse)
        .expect_err("C does not fit the saturated queue");
    assert!(matches!(signal, AdmissionSignal::QueueSaturated { .. }));
    assert_eq!(
        queue.len(),
        in_flight,
        "the collapse queued nothing and C's refusal queued nothing"
    );

    // The consumer drains: A delivers exactly once, with its own admission
    // time — B's collapse neither duplicated nor disturbed it.
    let drained = drain_queue(&queue);
    assert_eq!(
        drained
            .iter()
            .filter(|record| record.entity == entity_a)
            .count(),
        1,
        "A delivers exactly once"
    );
    assert!(
        drained
            .first()
            .is_some_and(|record| record.entity == entity_a && record.admitted_at == now()),
        "A travels first, under its original admission time"
    );

    // B left no residue — and did not end A: the re-delivery of B still
    // collapses onto A's standing entry and queues nothing.
    let again_b = pipeline
        .ingest_spans(
            AdmissionTime::from_unix_nano(100),
            &one_span_export(sized_span("queued", S1)),
        )
        .expect("walked");
    assert!(
        matches!(&again_b.records[0], RecordOutcome::Collapsed { entity } if *entity == entity_a),
        "B collapses onto the A that stands — its identity untouched: {again_b:?}"
    );
    assert!(queue.is_empty(), "a collapse never queues");

    // C's ending is single: its re-delivery is admitted fresh and queued.
    let again_c = pipeline
        .ingest_spans(
            AdmissionTime::from_unix_nano(101),
            &one_span_export(sized_span("refused-now", S2)),
        )
        .expect("the once-refused C is admitted fresh");
    assert!(
        matches!(&again_c.records[0], RecordOutcome::Admitted { .. }),
        "C's identity ended exactly once, at its refusal: {again_c:?}"
    );
    assert_eq!(queue.len(), 1);
    drain_queue(&queue);
}

/// The intern half of the undo, and its boundary: a refused point releases
/// the interned stream **only when its own admission created the intern**.
/// A stream an earlier queued delivery interned stands — its residency
/// story belongs to that delivery, not to this refusal.
#[test]
fn a_refusal_releases_only_the_intern_its_own_admission_created() {
    let limits = shrunken_limits();
    let bound = Pipeline::legal_record_bound_bytes(&limits);
    let (queue, pipeline) = pipeline_over(bound, limits);

    // Saturate the queue with points of one stream — its identity interned
    // by the deliveries that were queued.
    let mut queued = 0;
    for time in 1..1_000_u64 {
        match pipeline.ingest_metrics(now(), &one_point_export_of("base", time)) {
            Ok(_) => queued += 1,
            Err(AdmissionSignal::QueueSaturated { .. }) => break,
            Err(signal) => panic!("only saturation may refuse here: {signal:?}"),
        }
    }
    assert!(queued >= 1, "the fixture needs queued points in flight");
    assert_eq!(resident_streams(&pipeline), 1, "only the base stream");
    let in_flight = queue.len();

    // A refused point of the SAME standing stream: the record's identity
    // ends, but the intern does not — it is the queued deliveries' intern.
    // (A release here would free a stream whose points are still in
    // flight, and the count would drop to zero.)
    let shared = pipeline
        .ingest_metrics(
            AdmissionTime::from_unix_nano(99),
            &one_point_export_of("base", 5_000),
        )
        .expect_err("the queue is saturated");
    assert!(matches!(shared, AdmissionSignal::QueueSaturated { .. }));
    assert_eq!(
        resident_streams(&pipeline),
        1,
        "the shared stream's intern is not this refusal's to release"
    );
    assert_eq!(queue.len(), in_flight);

    // The consumer drains. The queued base points' entries still stand —
    // draining is the consumer's half of the delivery, not the end of
    // identity — so their intern stands too, and a re-delivery collapses.
    drain_queue(&queue);
    let again_base = pipeline
        .ingest_metrics(
            AdmissionTime::from_unix_nano(100),
            &one_point_export_of("base", 1),
        )
        .expect("walked");
    assert!(
        matches!(&again_base.records[0], RecordOutcome::Collapsed { .. }),
        "the queued delivery's point still stands: {again_base:?}"
    );
    assert_eq!(
        resident_streams(&pipeline),
        1,
        "drain never ends identity: the intern survives its queue entry"
    );
    assert!(queue.is_empty(), "a collapse never queues");
}

/// The saturation storm: N refused exports leave the ledger exactly as
/// they found it — every refused record's entry forgotten, every freshly
/// interned stream released, no anomalies counted — so the ledger's
/// overhead tracks the deliveries that happened, and each refused record
/// re-delivers fresh. Read through the existing surfaces: the interned
/// stream count, the outcomes a re-delivery produces, and the anomaly
/// counter. No new observability was added for this.
#[test]
fn a_saturation_storm_leaves_no_ledger_residue() {
    let limits = shrunken_limits();
    let bound = Pipeline::legal_record_bound_bytes(&limits);
    let (queue, pipeline) = pipeline_over(bound, limits);

    // Pre-storm: the queue filled with points of one stream.
    let mut queued = 0;
    for time in 1..1_000_u64 {
        match pipeline.ingest_metrics(now(), &one_point_export_of("base", time)) {
            Ok(_) => queued += 1,
            Err(AdmissionSignal::QueueSaturated { .. }) => break,
            Err(signal) => panic!("only saturation may refuse here: {signal:?}"),
        }
    }
    assert!(queued >= 1);
    let before = resident_streams(&pipeline);
    assert_eq!(before, 1);

    // The storm: every export carries one point of a NEW stream — admission
    // creates the entry and interns the stream, the queue refuses, and the
    // undo must put both back. Eight storms, asserted one by one: the
    // interned count returns to `before` after every refusal.
    const STORM: u64 = 8;
    for storm in 0..STORM {
        let signal = pipeline
            .ingest_metrics(
                AdmissionTime::from_unix_nano(1_000 + storm),
                &one_point_export_of(&format!("storm-{storm}"), 2_000 + storm),
            )
            .expect_err("the saturated queue refuses every storm record");
        assert!(matches!(signal, AdmissionSignal::QueueSaturated { .. }));
        assert_eq!(
            resident_streams(&pipeline),
            before,
            "storm {storm}: the refused point's fresh intern was released"
        );
    }

    // The residue check, record by record: drain, then re-deliver every
    // storm point. An `Admitted` outcome means no entry stood behind it —
    // the storm's ledger entries are all gone. (A collapse would be a
    // stranded entry per storm.)
    drain_queue(&queue);
    for storm in 0..STORM {
        let outcome = pipeline
            .ingest_metrics(
                AdmissionTime::from_unix_nano(3_000 + storm),
                &one_point_export_of(&format!("storm-{storm}"), 2_000 + storm),
            )
            .expect("the re-delivery walks");
        assert!(
            matches!(&outcome.records[0], RecordOutcome::Admitted { .. }),
            "storm {storm} left no ledger entry: {outcome:?}"
        );
    }
    assert_eq!(
        resident_streams(&pipeline),
        before + STORM,
        "each re-delivery re-interned its stream, for real residency now"
    );
    assert_eq!(
        pipeline.anomalies().total(),
        0,
        "nothing in the storm was recorded as a conflict"
    );
    drain_queue(&queue);
}

/// Drains a queue from the consumer side, front to back.
fn drain_queue(queue: &BoundedQueue) -> Vec<QueuedRecord> {
    let mut drained = Vec::new();
    while let Some(record) = queue.pop_timeout(Duration::ZERO) {
        drained.push(record);
    }
    drained
}

/// A pipeline over a fresh [`BoundedQueue`] with its ceiling at
/// `ceiling_bytes`, sharing the queue through the [`RecordSink`] port.
fn pipeline_over(ceiling_bytes: usize, limits: BudgetLimits) -> (Arc<BoundedQueue>, Arc<Pipeline>) {
    let queue = BoundedQueue::new(PIPELINE_QUEUE_NAME, ceiling_bytes);
    let pipeline = Arc::new(
        Pipeline::with_config(
            Arc::clone(&queue) as Arc<dyn RecordSink>,
            limits,
            OTLP_PAYLOAD_BYTES,
        )
        .expect("the ceiling clears the bound"),
    );
    (queue, pipeline)
}

/// The shrunken budgets the saturation fixtures share: small enough that a
/// queue at the derived legal-record bound saturates within a few records.
fn shrunken_limits() -> BudgetLimits {
    BudgetLimits {
        attributes_per_signal: 1,
        attributes_per_nested_set: 1,
        attribute_value_bytes: 256,
        span_events_per_span: 0,
        span_links_per_span: 0,
        exemplars_per_data_point: 0,
        key_value_list_depth: 8,
        data_points_per_export: 10_000,
    }
}

/// A span carrying one attribute at the per-entry cap of
/// [`shrunken_limits`] (`160 + 1 + 95 == 256 == attribute_value_bytes`):
/// attribute payload is what keeps each span a healthy fraction of the
/// bound, so the saturating exports stay small.
fn sized_span(name: &str, span_id: [u8; 8]) -> trace::Span {
    let mut span = trace_span(name, T1, span_id);
    span.attributes = vec![attr("k", str_value(&"a".repeat(95)))];
    span
}

/// One single-point export of the named gauge stream, its only point at
/// `time` — the point's identity is the stream plus that time.
fn one_point_export_of(name: &str, time: u64) -> Vec<u8> {
    let mut point = number_point(as_double(1.0));
    point.time_unix_nano = time;
    let metric = described_metric(name, "d", "s", Vec::new(), vec![point]);
    encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(Some(scope("test")), vec![metric])],
    )]))
}

/// How many stream identities are interned in `pipeline`'s ledger right
/// now — the observable for the intern half of the identity lifecycle.
fn resident_streams(pipeline: &Pipeline) -> u64 {
    pipeline
        .ledger()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .resident_streams()
}

/// Fills `queue` to saturation through `pipeline`, one fresh span per
/// export, and returns the number of queued records. The export that
/// saturated the queue was refused — and, by the law the saturation tests
/// build on, its span's identity was ended by the pipeline.
///
/// # Panics
///
/// Panics if the queue never saturates within 1_000 exports, or if any
/// export is refused by anything but saturation — a misbuilt fixture.
fn saturate_with_spans(pipeline: &Pipeline, queue: &BoundedQueue) -> usize {
    let mut queued = 0;
    for index in 0..1_000_u32 {
        let mut span_id = [0_u8; 8];
        span_id[..4].copy_from_slice(&index.to_be_bytes());
        match pipeline.ingest_spans(now(), &one_span_export(sized_span("filler", span_id))) {
            Ok(_) => queued += 1,
            Err(AdmissionSignal::QueueSaturated { .. }) => return queued,
            Err(signal) => panic!("only saturation may refuse here: {signal:?}"),
        }
    }
    panic!("a bound-sized queue never saturated within 1_000 spans");
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
fn pop_timeout_does_not_rearm_the_deadline_on_wakeups_without_work() {
    // A waker hammers the queue's condition variable every millisecond
    // without ever adding a record — the noise real consumers make when a
    // notify's record is taken by someone else first. The drain deadline
    // is one absolute bound: pop_timeout returns around 50 ms after it was
    // called, not 50 ms after the LAST wakeup. (The old behaviour re-armed
    // the full deadline on every wake and only returned once the waker
    // stopped — ~450 ms here.)
    let queue = BoundedQueue::new("noisy", QUEUE_CEILING_BYTES);
    let stop = Arc::new(AtomicBool::new(false));
    let waker_stop = Arc::clone(&stop);
    let waker_queue = Arc::clone(&queue);
    let waker = std::thread::spawn(move || {
        let ends = Instant::now() + Duration::from_millis(400);
        while !waker_stop.load(Ordering::Acquire) && Instant::now() < ends {
            waker_queue.wake_waiters_without_work_for_test();
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let started = Instant::now();
    let popped = queue.pop_timeout(Duration::from_millis(50));
    let elapsed = started.elapsed();

    stop.store(true, Ordering::Release);
    waker.join().expect("the waker thread finishes");

    assert!(popped.is_none(), "no record was ever offered");
    assert!(
        elapsed < Duration::from_millis(200),
        "woken every millisecond, pop_timeout(50ms) took {elapsed:?}: \
         the deadline re-armed on a wakeup without work"
    );
}

#[test]
fn identical_points_share_one_stream_and_a_distinct_stream_pays_its_own_charge() {
    let harness = Harness::new();
    let point_at = |time: u64| {
        let mut point = number_point(as_double(1.0));
        point.time_unix_nano = time;
        point
    };
    // One stream, two points at different times; then a second stream
    // identical but for its name — the same byte length, so an equal-shaped
    // identity must charge equally.
    let payload = encode(&metrics_request(vec![resource_metrics(
        Some(resource(Vec::new())),
        vec![scope_metrics(
            Some(scope("test")),
            vec![
                metric("aaaa", gauge(vec![point_at(10), point_at(11)])),
                metric("bbbb", gauge(vec![point_at(12)])),
            ],
        )],
    )]));
    let outcome = harness
        .pipeline
        .ingest_metrics(now(), &payload)
        .expect("admitted");
    assert_eq!(outcome.admitted(), 3);

    let queued = harness.drain();
    let [first, second, other] = queued.as_slice() else {
        panic!("expected three queued points")
    };
    let StoredRecord::Point {
        stream: first_stream,
        ..
    } = &first.record
    else {
        panic!()
    };
    let StoredRecord::Point {
        stream: second_stream,
        ..
    } = &second.record
    else {
        panic!()
    };
    let StoredRecord::Point {
        stream: other_stream,
        ..
    } = &other.record
    else {
        panic!()
    };

    assert!(
        Arc::ptr_eq(first_stream, second_stream),
        "both points of one stream share one interned identity"
    );
    assert!(
        !Arc::ptr_eq(first_stream, other_stream),
        "a distinct name is a distinct stream"
    );
    assert_eq!(
        first.record.accounted_size(),
        second.record.accounted_size(),
        "equal points of one stream charge equally"
    );
    assert_eq!(
        other.record.accounted_size(),
        first.record.accounted_size(),
        "an equal-shaped identity charges equally"
    );

    // The whole hand-off carries three point charges, each including a
    // full charge of its stream: two distinct streams, so the stream
    // payload is carried twice across the queue — once per stream, not
    // once per process.
    let total: usize = queued
        .iter()
        .map(|record| record.record.accounted_size())
        .sum();
    assert_eq!(total, 3 * first.record.accounted_size());
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
