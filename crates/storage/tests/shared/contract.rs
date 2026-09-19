// The contract behaviors the retention suite does not cover: eviction ends
// a record's identity (ADR 0008, wired through the real admission ledger),
// a keep refusal ends it too, entity-id retrieval after eviction,
// deterministic scans with cursor continuation, Arc sharing, duplicate
// keeps and the anomaly pass-through.

use common::{Config, DRIVER_NAME};
use common::{
    SharedRecordingHook, admitted, assigned, at, bare_stream, boxed, gauge_point, log_record, span,
    stream,
};
use runtime_trail_storage::{AdmissionKey, EvictionHook, KeepOutcome, TelemetryStore};
use runtime_trail_telemetry_model::{
    Accounted, AdmissionLedger, AdmissionOutcome, Admitted, EntityId, Span,
};
use std::sync::Arc;
use std::sync::Mutex;

/// The composition root's half of ADR 0008: the hook implemented over the
/// real ledger's `forget` and `release_stream`, so identity ends exactly as
/// the decision requires — for every delivery kind. An evicted record is
/// forgotten (its stream released separately, by `stream_released`, when
/// the eviction retired it); a refused keep's record is forgotten with its
/// interned stream released by the same `keep_refused` report, because a
/// refusal inserted nothing and its identity must not outlive that. The
/// ledger is shared behind a `Send + Sync` handle because the hook must be
/// both, the same way the store is: the runtime holds one across its tasks,
/// and the bounds ride along.
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

    fn keep_refused(
        &mut self,
        entity: EntityId,
        stream: Option<&Arc<runtime_trail_telemetry_model::StreamIdentity>>,
    ) {
        let mut ledger = self.0.lock().expect("ledger lock poisoned");
        ledger.forget(entity);
        if let Some(stream) = stream {
            ledger.release_stream(stream);
        }
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
            Config {
                max_records: 1,
                ..Config::default()
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
            Config {
                max_records: 1,
                ..Config::default()
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
        Config {
            max_records: 1,
            ..Config::default()
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
    let mut store = boxed(Config::default(), None);
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
        for item in &page.items {
            let Some(runtime_trail_telemetry_model::Value::String(body)) = &item.record.body else {
                panic!("fixture bodies are strings");
            };
            // The key travels with the record: each item carries the
            // residency position its record was admitted at
            // ([ADR 0009](../../docs/decisions/0009-ordered-scans-yield-residency-keys.md)).
            let serial: u64 = body["body-".len()..]
                .parse()
                .expect("fixture bodies are numbered");
            assert_eq!(
                item.key,
                AdmissionKey::new(at(serial * 100), common::assigned(serial)),
                "each item carries its true residency key"
            );
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
        .map(|item| match &item.record.body {
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
    let mut again = boxed(Config::default(), None);
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
                .map(|item| match &item.record.body {
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
    let mut store = boxed(Config::default(), None);
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
        .map(|item| item.record.context.trace_id.as_bytes())
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
    let mut store = boxed(Config::default(), None);

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
        .expect("the span scans back")
        .record;
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
    let recorded = SharedRecordingHook::default();
    let mut store = boxed(Config::default(), Some(Box::new(recorded.clone())));
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
    // A duplicate inserted nothing REFUSED: the record it names is
    // resident, so the hook hears nothing — its identity must stand.
    assert_eq!(after.refused_hook_deliveries, 0);
    let hook = recorded.0.lock().expect("hook lock poisoned");
    assert!(
        hook.refused.is_empty() && hook.evicted.is_empty() && hook.streams_released.is_empty(),
        "a duplicate keep delivers nothing to the hook"
    );

    // The resident record is still the first one — a scan yields it and
    // only it.
    let page = store.scan_spans(None, 10);
    assert_eq!(page.items.len(), 1);
}

///
/// A keep that is both a duplicate AND oversized is a Duplicate first: the
/// record it names is resident, so it reports nothing to the hook. (The
/// regression: `refuse_early` answered Oversized before checking `contains`,
/// and the kept-failed path then delivered `keep_refused` for it — a
/// refusal that ends a STILL-RESIDENT identity.) An oversized AND-new keep
/// keeps its existing behavior: Oversized, with the one hook delivery that
/// ends its own identity.
#[test]
fn an_oversized_duplicate_reports_nothing_the_record_it_names_is_resident() {
    let recorded = SharedRecordingHook::default();
    let big = span([6; 16], [7; 8], &"x".repeat(2_000));
    let ceiling = u64::try_from(big.accounted_size()).expect("fits") - 1;
    let mut store = boxed(
        Config {
            max_accounted_bytes: ceiling,
            ..Config::default()
        },
        Some(Box::new(recorded.clone())),
    );

    // Keep R: the small resident record that names the entity id.
    let entity = big
        .natural_identity()
        .map(|(trace_id, span_id)| EntityId::Span { trace_id, span_id })
        .expect("fixture ids are valid");
    let _ = store.keep_span(admitted(entity, 100, span([6; 16], [7; 8], "original")));
    let before = store.stats();

    // The same entity id, mutated past the ceiling: Duplicate, no hook
    // delivery, nothing rewritten — the resident record stands.
    let outcome = store.keep_span(admitted(entity, 400, big));
    assert_eq!(outcome, KeepOutcome::Duplicate);
    let after = store.stats();
    assert_eq!(after.resident_records, before.resident_records);
    assert_eq!(after.accounted_bytes, before.accounted_bytes);
    assert_eq!(after.duplicate_keeps, before.duplicate_keeps + 1);
    assert_eq!(after.oversized_refusals, 0);
    assert_eq!(after.refused_hook_deliveries, 0);
    {
        let hook = recorded.0.lock().expect("hook lock poisoned");
        assert!(
            hook.refused.is_empty() && hook.evicted.is_empty() && hook.streams_released.is_empty(),
            "an oversized duplicate delivers nothing to the hook"
        );
    }

    // The resident record is still the first one.
    let page = store.scan_spans(None, 10);
    assert_eq!(page.items.len(), 1);

    // A genuinely oversized AND-new keep keeps its existing behavior:
    // Oversized, with the one hook delivery that ends its own identity.
    let stranger = span([8; 16], [9; 8], &"x".repeat(2_000));
    let stranger_entity = stranger
        .natural_identity()
        .map(|(trace_id, span_id)| EntityId::Span { trace_id, span_id })
        .expect("fixture ids are valid");
    let outcome = store.keep_span(admitted(stranger_entity, 500, stranger));
    assert_eq!(outcome, KeepOutcome::Oversized);
    let after = store.stats();
    assert_eq!(after.oversized_refusals, 1);
    assert_eq!(after.refused_hook_deliveries, 1);
}

/// The anomaly pass-through: the composition root pushes the ledger's
/// recorded conflicts through, and stats report the latest total — without
/// the store ever naming the ledger.
#[test]
fn admission_anomalies_pass_through_as_the_latest_total() {
    let mut store = boxed(Config::default(), None);
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
    let store = boxed(Config::default(), None);
    assert_eq!(store.mode_name(), DRIVER_NAME);
    let boxed_store = boxed(Config::default(), None);
    assert_eq!(TelemetryStore::mode_name(boxed_store.as_ref()), DRIVER_NAME);
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
        Config {
            max_records: 1,
            ..Config::default()
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
        u64::try_from(Config::default().admission_window.as_nanos()).expect("window fits");
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

/// The review's M1 probe, as a regression. A point admitted cleanly but
/// refused at keep (`IdentityOverCeiling`) used to strand its ledger entry
/// and interned stream until process end: retention could never end what
/// residency never held, and every re-delivery collapsed onto the stranded
/// identity instead of re-offering the record. Through the documented hook
/// wiring — the composition root answers `keep_refused` with `forget` and
/// `release_stream`, exactly as for eviction — the identity ends with the
/// refusal, nothing strands, and the re-delivery admits fresh.
#[test]
fn a_keep_refused_over_the_ceiling_strands_nothing_and_its_redelivery_readmits_fresh() {
    let identity = stream();
    let identity_bytes = u64::try_from(identity.accounted_size()).expect("test sizes fit");
    let ledger = shared_ledger();
    let mut store = boxed(
        Config {
            max_accounted_bytes: identity_bytes - 1,
            ..Config::default()
        },
        Some(Box::new(LedgerHook(Arc::clone(&ledger)))),
    );

    let admission = ledger
        .lock()
        .expect("ledger lock poisoned")
        .admit_metric_point(&identity, gauge_point(10, 1));
    let entity = admission.outcome.entity().expect("the point admits");
    let interned = admission.stream.expect("admission interned the stream");
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        1,
        "admission interned the stream before any keep"
    );

    assert_eq!(
        store.keep_metric_point(
            Admitted {
                entity,
                admitted_at: at(100),
                record: admission.record.expect("shared"),
            },
            interned,
        ),
        KeepOutcome::IdentityOverCeiling {
            ceiling: identity_bytes - 1,
            identity_bytes,
        },
        "the identity alone exceeds the ceiling: refused, nothing inserted"
    );
    let stats = store.stats();
    assert_eq!(stats.identity_over_ceiling_refusals, 1);
    assert_eq!(stats.total_keep_refusals(), 1);
    assert_eq!(
        stats.refused_hook_deliveries, 1,
        "the refusal was delivered to the hook"
    );
    assert_eq!(stats.total_evictions(), 0);
    assert_eq!(stats.hook_deliveries, 0, "no eviction ever happened");
    assert_eq!(stats.resident_records, 0);
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        0,
        "the refused record's interned stream did not strand"
    );

    // Retention, run far past the window, has nothing to end — the probe's
    // original point was residency-independence: the stranding survived
    // every retention pass because it never lived in residency. It stays
    // at zero.
    let window_nanos =
        u64::try_from(Config::default().admission_window.as_nanos()).expect("window fits");
    assert_eq!(store.enforce_retention(at(100 + window_nanos)), 0);
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        0,
        "no stranding survives retention"
    );

    // The re-delivery — the same bytes — admits FRESH: the collapse
    // intercept is gone with the forgotten identity.
    let redelivery = ledger
        .lock()
        .expect("ledger lock poisoned")
        .admit_metric_point(&identity, gauge_point(10, 1));
    let AdmissionOutcome::Admitted { entity: fresh } = redelivery.outcome else {
        panic!("after the refusal released the identity, the same bytes admit fresh");
    };
    assert_ne!(fresh, entity, "a fresh serial, never a collapse");
}

/// A `SeriesCapReached` refusal ends the refused record's ledger identity,
/// so the documented re-attempt is genuinely reachable: the re-delivery
/// re-admits as a fresh admission rather than collapsing onto a stranded
/// entry, and once a slot frees it keeps cleanly.
#[test]
fn a_series_cap_refusal_ends_the_identity_so_a_re_attempt_admits_cleanly() {
    let ledger = shared_ledger();
    let mut store = boxed(
        Config {
            series_cap: 1,
            ..Config::default()
        },
        Some(Box::new(LedgerHook(Arc::clone(&ledger)))),
    );
    let alpha = bare_stream("alpha");
    let gamma = bare_stream("gamma");

    // Alpha fills the cap.
    let (alpha_entity, alpha_interned, alpha_handoff) =
        admit_point_to(&ledger, &alpha, gauge_point(10, 1), 100);
    assert_eq!(
        store.keep_metric_point(alpha_handoff, alpha_interned),
        KeepOutcome::Kept { evicted: 0 }
    );
    assert!(store.metric_point(alpha_entity).is_some());

    // Gamma is admitted — interned, the ledger now holds both streams —
    // and refused at keep: the cap is full. The refusal delivers, and
    // gamma's ledger identity ends with it.
    let (gamma_entity, gamma_interned, gamma_handoff) =
        admit_point_to(&ledger, &gamma, gauge_point(20, 1), 200);
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        2,
        "alpha and gamma are both interned before the keep"
    );
    assert_eq!(
        store.keep_metric_point(gamma_handoff, gamma_interned),
        KeepOutcome::SeriesCapReached
    );
    let stats = store.stats();
    assert_eq!(stats.kept_out_series_cap, 1);
    assert_eq!(stats.refused_hook_deliveries, 1);
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        1,
        "gamma's interned stream did not strand behind the refusal"
    );

    // The re-delivery of the same bytes admits FRESH — the pipeline's
    // collapse intercept has nothing to land on.
    let redelivery = ledger
        .lock()
        .expect("ledger lock poisoned")
        .admit_metric_point(&gamma, gauge_point(20, 1));
    let AdmissionOutcome::Admitted {
        entity: fresh_gamma,
    } = redelivery.outcome
    else {
        panic!("after the refusal released the identity, the re-attempt re-admits fresh");
    };
    let fresh_interned = redelivery.stream.expect("re-interned");
    assert_ne!(
        fresh_gamma, gamma_entity,
        "a fresh serial, never a collapse"
    );

    // A slot frees — alpha's point expires, its stream released through the
    // eviction hook — and the re-attempt, no longer shadowed by a stranded
    // identity, ADMITS cleanly.
    let window_nanos =
        u64::try_from(Config::default().admission_window.as_nanos()).expect("window fits");
    assert_eq!(store.enforce_retention(at(100 + window_nanos)), 1);
    assert_eq!(store.stats().resident_streams, 0);
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        1,
        "alpha's interned stream left with the eviction; only gamma's \
         fresh intern stands"
    );
    // Alpha's ledger identity ended with that eviction: the same bytes
    // admit FRESH now — no entry stands to collapse onto.
    let alpha_redelivery = ledger
        .lock()
        .expect("ledger lock poisoned")
        .admit_metric_point(&alpha, gauge_point(10, 1));
    assert!(
        matches!(alpha_redelivery.outcome, AdmissionOutcome::Admitted { .. }),
        "the eviction forgot alpha: its re-delivery is fresh, never collapsed"
    );
    assert_eq!(
        store.keep_metric_point(
            Admitted {
                entity: fresh_gamma,
                admitted_at: at(300),
                record: redelivery.record.expect("shared"),
            },
            fresh_interned,
        ),
        KeepOutcome::Kept { evicted: 0 },
        "the documented re-attempt of the refused stream admits cleanly"
    );
}

/// A refusal is final for the delivery, not for the identity: an oversized
/// span's keep is refused with nothing inserted, the hook reports it (with
/// no stream — spans carry none), the ledger entry ends, and the re-delivery
/// re-admits fresh under the same natural identity. Log records carry no
/// ledger entry at all, and their refusals are still delivered — the hook's
/// law is per refusal, not per kind.
#[test]
fn an_oversized_refusal_ends_the_ledger_identity_and_reports_even_entryless_kinds() {
    let ledger = shared_ledger();
    let body = "x".repeat(2_000);
    let big = span([4; 16], [5; 8], &body);
    let ceiling = u64::try_from(big.accounted_size()).expect("fits") - 1;
    let mut store = boxed(
        Config {
            max_accounted_bytes: ceiling,
            ..Config::default()
        },
        Some(Box::new(LedgerHook(Arc::clone(&ledger)))),
    );

    let (entity, handoff) = admit_span_to(&mut ledger.lock().expect("ledger lock poisoned"), big);
    assert_eq!(
        store.keep_span(handoff),
        KeepOutcome::Oversized,
        "the record alone exceeds the ceiling: refused, nothing inserted"
    );
    let stats = store.stats();
    assert_eq!(stats.oversized_refusals, 1);
    assert_eq!(stats.total_keep_refusals(), 1);
    assert_eq!(stats.refused_hook_deliveries, 1);
    assert_eq!(
        ledger
            .lock()
            .expect("ledger lock poisoned")
            .resident_streams(),
        0,
        "a span carries no stream, so nothing was ever interned"
    );

    // The re-delivery re-admits FRESH under the same natural identity —
    // the ledger entry is gone, not standing between the pipeline and the
    // store.
    let redelivery = ledger
        .lock()
        .expect("ledger lock poisoned")
        .admit_span(span([4; 16], [5; 8], &body));
    let AdmissionOutcome::Admitted { entity: fresh } = redelivery.outcome else {
        panic!("after the refusal, the same span admits fresh — never collapsed");
    };
    assert_eq!(
        fresh, entity,
        "a span re-admits under its natural identity, but FRESH"
    );

    // A log record's refusal delivers too, though the ledger holds no entry
    // for the kind at all.
    let wide = usize::try_from(ceiling).expect("fits");
    let refused_log =
        store.keep_log_record(admitted(assigned(1), 50, log_record(&"y".repeat(wide))));
    assert_eq!(refused_log, KeepOutcome::Oversized);
    let stats = store.stats();
    assert_eq!(stats.refused_hook_deliveries, 2);
    assert_eq!(stats.total_keep_refusals(), 2);
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

    fn keep_refused(
        &mut self,
        _entity: EntityId,
        _stream: Option<&Arc<runtime_trail_telemetry_model::StreamIdentity>>,
    ) {
        // This test's keeps are never refused; nothing runs here.
    }
}

#[test]
fn a_panicking_hook_leaves_a_consistent_store_behind() {
    let mut store = boxed(
        Config {
            max_records: 1,
            ..Config::default()
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
    let recorded = SharedRecordingHook::default();
    let mut store = boxed(Config::default(), Some(Box::new(recorded.clone())));
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
    // The stream identity is NOT handed to the hook either: a duplicate is
    // a resident record, not a refusal, and its identity must stand.
    assert_eq!(after.refused_hook_deliveries, 0);
    let hook = recorded.0.lock().expect("hook lock poisoned");
    assert!(
        hook.refused.is_empty() && hook.evicted.is_empty() && hook.streams_released.is_empty(),
        "a duplicate keep delivers nothing to the hook"
    );
}

/// The string bodies of a scan page, for the walk assertions.
fn bodies(
    page: &runtime_trail_storage::ScanPage<Arc<runtime_trail_telemetry_model::LogRecord>>,
) -> Vec<String> {
    page.items
        .iter()
        .map(|item| match &item.record.body {
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
        Config {
            max_records: 3,
            ..Config::default()
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
