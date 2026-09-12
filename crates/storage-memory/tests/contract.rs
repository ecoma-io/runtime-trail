//! The contract behaviors the retention suite does not cover: eviction ends
//! a record's identity (ADR 0008, wired through the real admission ledger),
//! entity-id retrieval after eviction, deterministic scans with cursor
//! continuation, Arc sharing, duplicate keeps and the anomaly pass-through.

mod common;

use common::{admitted, at, boxed, gauge_point, log_record, span, stream};
use runtime_trail_storage::{AdmissionKey, EvictionHook, KeepOutcome, TelemetryStore};
use runtime_trail_storage_memory::{InMemoryStore, MemoryConfig};
use runtime_trail_telemetry_model::{
    Accounted, AdmissionLedger, AdmissionOutcome, Admitted, EntityId, Span,
};
use std::sync::Arc;
use std::sync::Mutex;

/// The composition root's half of ADR 0008: the hook implemented over the
/// real ledger's `forget` and `release_stream`, so identity ends exactly as
/// the decision requires. The ledger is shared behind a `Send + Sync`
/// handle because the hook must be both, the same way the store is: the
/// runtime holds one across its tasks, and the bounds ride along.
struct LedgerHook(Arc<Mutex<AdmissionLedger>>);

impl EvictionHook for LedgerHook {
    fn evicted(&mut self, entity: EntityId) {
        self.0.lock().expect("ledger lock poisoned").forget(entity);
    }

    fn stream_released(&mut self, stream: &Arc<runtime_trail_telemetry_model::StreamIdentity>) {
        self.0
            .lock()
            .expect("ledger lock poisoned")
            .release_stream(stream);
    }
}

/// A fresh shared ledger, the way the tests below hold one.
fn shared_ledger() -> Arc<Mutex<AdmissionLedger>> {
    Arc::new(Mutex::new(AdmissionLedger::default()))
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
    let (first_entity, first_handoff) = admit_span_to(
        &mut ledger.lock().expect("ledger lock poisoned"),
        span([7; 16], [8; 8], "op"),
    );
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
            Some(Box::new(LedgerHook(Arc::clone(&ledger)))),
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
    let redelivery = ledger
        .lock()
        .expect("ledger lock poisoned")
        .admit_span(span([7; 16], [8; 8], "op"));
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
        .lock()
        .expect("ledger lock poisoned")
        .admit_metric_point(&identity, gauge_point(50, 1));
    let first = first_admission.outcome.entity().expect("the point admits");
    let first_stream = first_admission.stream.expect("interned");
    let first_record = first_admission.record.expect("shared");

    let second_admission = ledger
        .lock()
        .expect("ledger lock poisoned")
        .admit_metric_point(&identity, gauge_point(60, 2));
    let second = second_admission.outcome.entity().expect("the point admits");
    let second_record = second_admission.record.expect("shared");

    {
        let mut store = boxed(
            MemoryConfig {
                max_records: 1,
                ..MemoryConfig::default()
            },
            Some(Box::new(LedgerHook(Arc::clone(&ledger)))),
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
        .lock()
        .expect("ledger lock poisoned")
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

/// Admits a metric point through the shared ledger and returns its entity
/// id, the interned stream, and the shared payload for a store hand-off.
fn admit_point_to(
    ledger: &Mutex<AdmissionLedger>,
    identity: &runtime_trail_telemetry_model::StreamIdentity,
    point: runtime_trail_telemetry_model::MetricPoint,
    nano: u64,
) -> (
    EntityId,
    Arc<runtime_trail_telemetry_model::StreamIdentity>,
    Admitted<Arc<runtime_trail_telemetry_model::MetricPoint>>,
) {
    let admission = ledger
        .lock()
        .expect("ledger lock poisoned")
        .admit_metric_point(identity, point);
    let entity = admission
        .outcome
        .entity()
        .expect("the fixture point admits");
    let interned = admission.stream.expect("the stream interns");
    let record = admission.record.expect("the admitted payload is shared");
    (
        entity,
        interned,
        Admitted {
            entity,
            admitted_at: at(nano),
            record,
        },
    )
}

/// The store tells the hook when a stream's residency ends: the ledger
/// keeps the identity while any point of the stream stands, drops it
/// exactly with the last point, and a re-delivery after that re-interns
/// fresh.
#[test]
fn the_store_releases_a_stream_only_when_its_last_point_leaves() {
    let identity = stream();
    let ledger = shared_ledger();
    let mut store = boxed(
        MemoryConfig {
            max_records: 1,
            ..MemoryConfig::default()
        },
        Some(Box::new(LedgerHook(Arc::clone(&ledger)))),
    );
    let (first, interned, first_handoff) =
        admit_point_to(&ledger, &identity, gauge_point(10, 1), 100);
    let (second_entity, interned_again, second_handoff) =
        admit_point_to(&ledger, &identity, gauge_point(20, 2), 200);
    // One content, one interning: the second admission collapsed onto the
    // first stream's Arc.
    assert!(Arc::ptr_eq(&interned, &interned_again));
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        1
    );

    let _ = store.keep_metric_point(first_handoff, interned);
    let _ = store.keep_metric_point(second_handoff, interned_again);
    // The second keep evicted the first point; the stream survives it,
    // because its last point is still resident.
    assert!(store.metric_point(first).is_none());
    assert!(store.metric_point(second_entity).is_some());
    assert_eq!(store.stats().total_evictions(), 1);
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        1,
        "no release while a point stands"
    );

    // A retention pass a window past the last point's admission ends the
    // stream: the hook releases the identity out of the ledger.
    let window_nanos =
        u64::try_from(MemoryConfig::default().admission_window.as_nanos()).expect("window fits");
    assert_eq!(store.enforce_retention(at(200 + window_nanos)), 1);
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        0,
        "the last point's exit released the stream"
    );
    assert!(store.metric_point(second_entity).is_none());

    // A re-delivery re-interns the stream: a fresh allocation, not the
    // one the release dropped.
    let (_, re_interned, _) = admit_point_to(&ledger, &identity, gauge_point(30, 3), 300);
    assert!(
        !Arc::ptr_eq(&re_interned, &identity),
        "the re-delivery interns fresh, it does not resurrect the released Arc"
    );
}

/// A hook that panics on its first delivery and answers the rest cleanly,
/// so one test holds both the broken hook and the control. The store's
/// own removal completes BEFORE the hook runs, and a panic inside the
/// hook propagates only after the store is consistent: the record is
/// gone, the eviction is counted, the delivery is not.
struct PanickingHook {
    panicked: bool,
}

impl EvictionHook for PanickingHook {
    fn evicted(&mut self, _entity: EntityId) {
        if self.panicked {
            return; // the control delivery: quiet, clean
        }
        self.panicked = true;
        panic!("the record hook is broken");
    }

    fn stream_released(&mut self, _stream: &Arc<runtime_trail_telemetry_model::StreamIdentity>) {
        // A log record's removal retires no stream; nothing runs here.
    }
}

#[test]
fn a_panicking_hook_leaves_a_consistent_store_behind() {
    let mut store = boxed(
        MemoryConfig {
            max_records: 1,
            ..MemoryConfig::default()
        },
        Some(Box::new(PanickingHook { panicked: false })),
    );
    let first = common::assigned(1);
    let _ = store.keep_log_record(admitted(first, 100, log_record("first")));
    let second = common::assigned(2);

    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.keep_log_record(admitted(second, 200, log_record("second")))
    }));
    std::panic::set_hook(previous_hook);
    assert!(
        outcome.is_err(),
        "the hook's panic propagates to the caller"
    );

    // The removal completed before the hook ran; the delivery did not.
    // The keep's own insert had completed before the panic too, so the
    // incoming record stands.
    let stats = store.stats();
    assert_eq!(stats.total_evictions(), 1, "the eviction itself is counted");
    assert_eq!(
        stats.hook_deliveries, 0,
        "a hook that panicked delivered nothing: the divergence is observable"
    );
    assert!(store.log_record(first).is_none(), "the record is gone");
    assert_eq!(stats.resident_records, 1);
    assert_eq!(
        stats.accounted_bytes,
        u64::try_from(log_record("second").accounted_size()).expect("fits"),
        "the accounting names exactly the record the aborted keep left"
    );

    // The control: the next eviction delivers cleanly and its keep
    // completes.
    let third = common::assigned(3);
    let control = store.keep_log_record(admitted(third, 300, log_record("third")));
    assert_eq!(control, KeepOutcome::Kept { evicted: 1 });
    assert!(store.log_record(third).is_some());
    assert!(store.log_record(second).is_none());
    let stats = store.stats();
    assert_eq!(stats.total_evictions(), 2);
    assert_eq!(
        stats.hook_deliveries, 1,
        "the control delivery completed; the gap closes to one missing call"
    );
}

/// A duplicate keep of a metric point leaves the resident point standing
/// and counts the attempt; the stream's identity charge does not double.
#[test]
fn a_duplicate_metric_point_keep_leaves_the_resident_point_standing() {
    let identity = stream();
    let mut store = boxed(MemoryConfig::default(), None);
    let entity = common::assigned(1);
    let _ = store.keep_metric_point(
        admitted(entity, 100, gauge_point(90, 1)),
        Arc::clone(&identity),
    );
    let before = store.stats();

    let outcome = store.keep_metric_point(
        admitted(entity, 400, gauge_point(90, 1)),
        Arc::clone(&identity),
    );
    assert_eq!(outcome, KeepOutcome::Duplicate);
    let after = store.stats();
    assert_eq!(after.resident_metric_points, before.resident_metric_points);
    assert_eq!(
        after.accounted_bytes, before.accounted_bytes,
        "neither the point nor the stream is charged twice"
    );
    assert_eq!(after.resident_streams, before.resident_streams);
    assert_eq!(after.total_evictions(), 0);
    assert_eq!(after.duplicate_keeps, before.duplicate_keeps + 1);
}

/// The string bodies of a scan page, for the walk assertions.
fn bodies(
    page: &runtime_trail_storage::ScanPage<Arc<runtime_trail_telemetry_model::LogRecord>>,
) -> Vec<String> {
    page.items
        .iter()
        .map(|record| match &record.body {
            Some(runtime_trail_telemetry_model::Value::String(body)) => body.clone(),
            _ => panic!("fixture bodies are strings"),
        })
        .collect()
}

/// A scan is a view over a living store: a cursor taken before an
/// eviction continues from where it was, skipping what left and never
/// repeating what stayed.
#[test]
fn a_cursor_continues_across_an_eviction_without_skips_or_repeats() {
    let mut store = boxed(
        MemoryConfig {
            max_records: 3,
            ..MemoryConfig::default()
        },
        None,
    );
    for serial in 1..=3_u64 {
        let _ = store.keep_log_record(admitted(
            common::assigned(serial),
            serial * 100,
            log_record(&format!("r{serial}")),
        ));
    }
    // Page one holds the oldest record; its cursor names it.
    let first_page = store.scan_log_records(None, 1);
    assert_eq!(bodies(&first_page), vec!["r1".to_owned()]);

    // A keep evicts that same oldest record while the caller holds the
    // cursor to it.
    let _ = store.keep_log_record(admitted(common::assigned(4), 400, log_record("r4")));
    assert!(store.log_record(common::assigned(1)).is_none());

    // The walk continues strictly after the cursor's key: r1 is gone and
    // simply absent, everything resident is yielded exactly once.
    let mut seen = Vec::new();
    let mut cursor = first_page.cursor;
    loop {
        let page = store.scan_log_records(cursor, 2);
        seen.extend(bodies(&page));
        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(
        seen,
        vec!["r2".to_owned(), "r3".to_owned(), "r4".to_owned()]
    );
}
