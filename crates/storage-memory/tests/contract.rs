//! The contract behaviors the retention suite does not cover: eviction ends
//! a record's identity (ADR 0008, wired through the real admission ledger),
//! entity-id retrieval after eviction, deterministic scans with cursor
//! continuation, Arc sharing, duplicate keeps and the anomaly pass-through.

mod common;

use common::{admitted, at, boxed, gauge_point, log_record, span, stream};
use runtime_trail_storage::{AdmissionKey, EvictionHook, KeepOutcome, TelemetryStore};
use runtime_trail_storage_memory::{InMemoryStore, MemoryConfig};
use runtime_trail_telemetry_model::{AdmissionLedger, AdmissionOutcome, Admitted, EntityId, Span};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// The composition root's half of ADR 0008: the hook implemented over the
/// real ledger's `forget`, so eviction ends identity exactly as the
/// decision requires. The ledger is shared behind a handle because the
/// hook slot is `'static` — the same shape a session-owned wiring takes.
#[derive(Clone)]
struct LedgerForgetter(Rc<RefCell<AdmissionLedger>>);

impl EvictionHook for LedgerForgetter {
    fn evicted(&mut self, entity: EntityId) {
        self.0.borrow_mut().forget(entity);
    }
}

/// A fresh shared ledger, the way the tests below hold one.
fn shared_ledger() -> Rc<RefCell<AdmissionLedger>> {
    Rc::new(RefCell::new(AdmissionLedger::default()))
}

/// Admits a span through the ledger and returns its entity id with the
/// shared payload wrapped for a store hand-off — admission's own `Arc`, no
/// copy in between.
fn admit_span_to(ledger: &mut AdmissionLedger, s: Span) -> (EntityId, Admitted<Arc<Span>>) {
    let admission = ledger.admit_span(s);
    let entity = admission.outcome.entity().expect("the fixture span admits");
    let record = admission.record.expect("the admitted payload is shared");
    (
        entity,
        Admitted {
            entity,
            admitted_at: at(100),
            record,
        },
    )
}

/// ADR 0008, end to end: a span evicted from residency has its ledger
/// identity forgotten, so its re-delivery is admitted FRESH — never
/// collapsed onto the record the store no longer holds.
#[test]
fn an_evicted_spans_redelivery_is_admitted_fresh_through_the_real_ledger() {
    let ledger = shared_ledger();
    let (first_entity, first_handoff) =
        admit_span_to(&mut ledger.borrow_mut(), span([7; 16], [8; 8], "op"));
    let second_entity = EntityId::Span {
        trace_id: runtime_trail_telemetry_model::TraceId::from_bytes([9; 16]),
        span_id: runtime_trail_telemetry_model::SpanId::from_bytes([9; 8]),
    };
    {
        let mut store = boxed(
            MemoryConfig {
                max_records: 1,
                ..MemoryConfig::default()
            },
            Some(Box::new(LedgerForgetter(Rc::clone(&ledger)))),
        );
        assert_eq!(
            store.keep_span(first_handoff),
            KeepOutcome::Kept { evicted: 0 }
        );
        // The second keep evicts the first, and the hook must have run:
        // residency ended, so identity ends.
        let _ = store.keep_span(admitted(second_entity, 200, span([9; 16], [9; 8], "later")));
    }
    // The store is gone and the hook with it; re-deliver the first span.
    let redelivery = ledger.borrow_mut().admit_span(span([7; 16], [8; 8], "op"));
    assert_eq!(
        redelivery.outcome,
        AdmissionOutcome::Admitted {
            entity: first_entity
        },
        "a span re-admits under its natural identity, but FRESH — not collapsed"
    );
}

/// Same law for a metric point: after eviction forgot its assigned id, the
/// same bytes re-admit under a NEW assigned serial.
#[test]
fn an_evicted_points_redelivery_gets_a_new_serial_through_the_real_ledger() {
    let identity = stream();
    let ledger = shared_ledger();

    let first_admission = ledger
        .borrow_mut()
        .admit_metric_point(&identity, gauge_point(50, 1));
    let first = first_admission.outcome.entity().expect("the point admits");
    let first_stream = first_admission.stream.expect("interned");
    let first_record = first_admission.record.expect("shared");

    let second_admission = ledger
        .borrow_mut()
        .admit_metric_point(&identity, gauge_point(60, 2));
    let second = second_admission.outcome.entity().expect("the point admits");
    let second_record = second_admission.record.expect("shared");

    {
        let mut store = boxed(
            MemoryConfig {
                max_records: 1,
                ..MemoryConfig::default()
            },
            Some(Box::new(LedgerForgetter(Rc::clone(&ledger)))),
        );
        let _ = store.keep_metric_point(
            Admitted {
                entity: first,
                admitted_at: at(100),
                record: first_record,
            },
            Arc::clone(&first_stream),
        );
        // Evicts the first point; the hook forgets its assigned id.
        let _ = store.keep_metric_point(
            Admitted {
                entity: second,
                admitted_at: at(200),
                record: second_record,
            },
            first_stream,
        );
    }
    let redelivery = ledger
        .borrow_mut()
        .admit_metric_point(&identity, gauge_point(50, 1))
        .outcome;
    let AdmissionOutcome::Admitted { entity: fresh } = redelivery else {
        panic!("after the forget, the same bytes admit fresh");
    };
    assert_ne!(
        fresh, first,
        "a forgotten point's identity is gone; the re-delivery is a new entity"
    );
}

/// Retrieval is by entity id and only while resident: after eviction, the
/// id names nothing — for every kind.
#[test]
fn retrieval_after_eviction_is_absent_for_every_kind() {
    let identity = stream();
    let mut store = boxed(
        MemoryConfig {
            max_records: 1,
            ..MemoryConfig::default()
        },
        None,
    );
    let span_entity = {
        let s = span([3; 16], [4; 8], "gone");
        s.natural_identity()
            .map(|(trace_id, span_id)| EntityId::Span { trace_id, span_id })
            .expect("fixture ids are valid")
    };
    let _ = store.keep_span(admitted(span_entity, 100, span([3; 16], [4; 8], "gone")));
    assert!(store.span(span_entity).is_some(), "resident while resident");

    let log_entity = common::assigned(1);
    let _ = store.keep_log_record(admitted(log_entity, 200, log_record("gone")));
    assert!(store.span(span_entity).is_none(), "evicted by the keep");
    assert!(store.log_record(log_entity).is_some());

    let point_entity = common::assigned(2);
    let _ = store.keep_metric_point(
        admitted(point_entity, 300, gauge_point(70, 3)),
        Arc::clone(&identity),
    );
    assert!(
        store.log_record(log_entity).is_none(),
        "evicted by the keep"
    );
    assert!(store.metric_point(point_entity).is_some());

    // An id that never lived here is absent too.
    let stranger = common::assigned(999);
    assert!(store.span(stranger).is_none());
    assert!(store.log_record(stranger).is_none());
    assert!(store.metric_point(stranger).is_none());
}

/// The scan order is the residency order, a cursor continues strictly after
/// its key, nothing is skipped or repeated, and the walk terminates with a
/// `None` cursor.
#[test]
fn scans_walk_the_residency_order_and_a_cursor_continues_it() {
    let mut store = boxed(MemoryConfig::default(), None);
    // Admit out of order on purpose; residency order sorts by admission
    // time, ties by serial.
    let serials = [5_u64, 1, 4, 2, 3];
    let nanos = [500_u64, 100, 400, 200, 300];
    for (serial, nano) in serials.into_iter().zip(nanos) {
        let entity = common::assigned(serial);
        let body = format!("body-{serial}");
        let _ = store.keep_log_record(admitted(entity, nano, log_record(&body)));
    }

    let expected_order = ["body-1", "body-2", "body-3", "body-4", "body-5"];
    let mut seen: Vec<String> = Vec::new();
    let mut cursor = None;
    for _ in 0..10 {
        let page = store.scan_log_records(cursor, 2);
        for record in &page.items {
            let Some(runtime_trail_telemetry_model::Value::String(body)) = &record.body else {
                panic!("fixture bodies are strings");
            };
            seen.push(body.clone());
        }
        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(
        seen, expected_order,
        "the walk yields every record once, in residency order"
    );

    // A cursor resumes strictly after its key: nothing before it, and the
    // record the cursor names, again.
    let middle = AdmissionKey::new(at(300), common::assigned(3));
    let page = store.scan_log_records(Some(middle), 10);
    let seen_after: Vec<String> = page
        .items
        .iter()
        .map(|record| match &record.body {
            Some(runtime_trail_telemetry_model::Value::String(body)) => body.clone(),
            _ => panic!("fixture bodies are strings"),
        })
        .collect();
    assert_eq!(seen_after, vec!["body-4", "body-5"]);
    assert_eq!(page.cursor, None, "the end of the set closes the walk");

    // A limit of zero yields an empty page and no cursor.
    let empty = store.scan_log_records(None, 0);
    assert!(empty.items.is_empty() && empty.cursor.is_none());

    // Determinism: a fresh store given the same admissions walks the same
    // sequence.
    let mut again = boxed(MemoryConfig::default(), None);
    for (serial, nano) in serials.into_iter().zip(nanos) {
        let entity = common::assigned(serial);
        let body = format!("body-{serial}");
        let _ = again.keep_log_record(admitted(entity, nano, log_record(&body)));
    }
    let first_page = store.scan_log_records(None, 3);
    let replay = again.scan_log_records(None, 3);
    let bodies =
        |page: &runtime_trail_storage::ScanPage<Arc<runtime_trail_telemetry_model::LogRecord>>| {
            page.items
                .iter()
                .map(|record| match &record.body {
                    Some(runtime_trail_telemetry_model::Value::String(body)) => body.clone(),
                    _ => panic!("fixture bodies are strings"),
                })
                .collect::<Vec<_>>()
        };
    assert_eq!(bodies(&first_page), bodies(&replay));
    assert_eq!(first_page.cursor, replay.cursor);
}

/// A scan is a location primitive: spans scan in the same order their
/// shelf evicts, so what a cursor walks and what retention removes are one
/// sequence.
#[test]
fn span_scans_order_like_eviction_at_tied_admission_times() {
    let mut store = boxed(MemoryConfig::default(), None);
    for (trace, nano) in [([9_u8; 16], 100_u64), ([1; 16], 100), ([5; 16], 50)] {
        let s = span(trace, [1; 8], "scanned");
        let entity = s
            .natural_identity()
            .map(|(trace_id, span_id)| EntityId::Span { trace_id, span_id })
            .expect("fixture ids are valid");
        let _ = store.keep_span(admitted(entity, nano, s));
    }
    let page = store.scan_spans(None, 10);
    let traces: Vec<[u8; 16]> = page
        .items
        .iter()
        .map(|s| s.context.trace_id.as_bytes())
        .collect();
    assert_eq!(
        traces,
        vec![[5; 16], [1; 16], [9; 16]],
        "admission time first, trace-id bytes as the tie-break"
    );
}

/// Retrieval shares the stored allocation: no copy on the read path, for
/// every kind — and a metric point comes back with the interned stream it
/// was admitted under.
#[test]
fn retrieval_shares_the_stored_allocations() {
    let identity = stream();
    let mut store = boxed(MemoryConfig::default(), None);

    let s = span([2; 16], [3; 8], "shared");
    let span_entity = s
        .natural_identity()
        .map(|(trace_id, span_id)| EntityId::Span { trace_id, span_id })
        .expect("fixture ids are valid");
    let _ = store.keep_span(admitted(span_entity, 100, s));

    let log_entity = common::assigned(1);
    let _ = store.keep_log_record(admitted(log_entity, 200, log_record("shared")));

    let point_entity = common::assigned(2);
    let _ = store.keep_metric_point(
        admitted(point_entity, 300, gauge_point(80, 4)),
        Arc::clone(&identity),
    );

    let fetched_span = store.span(span_entity).expect("resident");
    let expected_span = store
        .scan_spans(None, 1)
        .items
        .into_iter()
        .next()
        .expect("the span scans back");
    assert!(
        Arc::ptr_eq(&fetched_span, &expected_span),
        "get-by-id and scan share one allocation with the shelf"
    );

    let fetched_log = store.log_record(log_entity).expect("resident");
    assert_eq!(&*fetched_log, &log_record("shared"));

    let view = store.metric_point(point_entity).expect("resident");
    assert!(
        Arc::ptr_eq(&view.stream, &identity),
        "the interned stream identity comes back shared as stored"
    );
}

/// A keep under an id that is already resident is a named Duplicate: the
/// resident record stands, nothing is rewritten, the attempt is counted.
#[test]
fn a_duplicate_keep_leaves_the_resident_record_standing() {
    let mut store = boxed(MemoryConfig::default(), None);
    let s = span([6; 16], [7; 8], "original");
    let entity = s
        .natural_identity()
        .map(|(trace_id, span_id)| EntityId::Span { trace_id, span_id })
        .expect("fixture ids are valid");
    let _ = store.keep_span(admitted(entity, 100, s));
    let before = store.stats();

    let outcome = store.keep_span(admitted(entity, 400, span([6; 16], [7; 8], "original")));
    assert_eq!(outcome, KeepOutcome::Duplicate);
    let after = store.stats();
    assert_eq!(after.resident_records, before.resident_records);
    assert_eq!(after.accounted_bytes, before.accounted_bytes);
    assert_eq!(after.total_evictions(), 0);
    assert_eq!(after.duplicate_keeps, before.duplicate_keeps + 1);

    // The resident record is still the first one — a scan yields it and
    // only it.
    let page = store.scan_spans(None, 10);
    assert_eq!(page.items.len(), 1);
}

/// The anomaly pass-through: the composition root pushes the ledger's
/// recorded conflicts through, and stats report the latest total — without
/// the store ever naming the ledger.
#[test]
fn admission_anomalies_pass_through_as_the_latest_total() {
    let mut store = boxed(MemoryConfig::default(), None);
    assert_eq!(store.stats().admission_anomalies, 0);
    store.observe_admission_anomalies(3);
    assert_eq!(store.stats().admission_anomalies, 3);
    store.observe_admission_anomalies(7);
    assert_eq!(store.stats().admission_anomalies, 7, "latest value wins");
}

/// The mode name the driver reports matches the bootstrap surface's name,
/// so a store's self-description stays stable across the seam.
#[test]
fn the_driver_reports_its_mode_name() {
    let store = InMemoryStore::new(MemoryConfig::default(), None);
    assert_eq!(store.mode_name(), "memory");
    let boxed_store = boxed(MemoryConfig::default(), None);
    assert_eq!(TelemetryStore::mode_name(boxed_store.as_ref()), "memory");
}
