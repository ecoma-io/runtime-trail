//! The retention law, as behavior: eviction by each ceiling, first-hit
//! wins, oldest-by-admission-time evicted first, ties deterministic, and
//! every eviction observable.

mod common;

use common::{RecordingHook, admitted, assigned, at, boxed, log_record, span};
use runtime_trail_storage::KeepOutcome;
use runtime_trail_storage_memory::MemoryConfig;
use runtime_trail_telemetry_model::{Accounted, EntityId};

fn config(max_records: u64, max_accounted_bytes: u64) -> MemoryConfig {
    MemoryConfig {
        max_records,
        max_accounted_bytes,
        ..MemoryConfig::default()
    }
}

/// A span fixture's natural identity as an entity id.
fn entity_of(s: &runtime_trail_telemetry_model::Span) -> EntityId {
    s.natural_identity()
        .map(|(trace_id, span_id)| EntityId::Span { trace_id, span_id })
        .expect("fixture ids are valid")
}

#[test]
fn the_record_ceiling_evicts_oldest_by_admission_time_first() {
    let hook = RecordingHook::default();
    let mut store = boxed(config(2, u64::MAX), Some(Box::new(hook)));
    // Admitted out of time order on purpose: the eviction must follow
    // admission times, not arrival order.
    let first = assigned(1);
    let second = assigned(2);
    let third = assigned(3);
    assert_eq!(
        store.keep_log_record(admitted(first, 900, log_record("arrived first"))),
        KeepOutcome::Kept { evicted: 0 }
    );
    let _ = store.keep_log_record(admitted(second, 500, log_record("older clock")));
    let _ = store.keep_log_record(admitted(third, 1000, log_record("newest")));
    let stats = store.stats();
    assert_eq!(stats.resident_records, 2, "the ceiling holds two");
    assert_eq!(stats.evicted_for_record_ceiling, 1);
    // The record admitted at 500 is the oldest by admission time, even
    // though it arrived second.
    assert_eq!(stats.resident_log_records, 2);
    assert!(store.log_record(first).is_some(), "kept at 900 survives");
    assert!(store.log_record(second).is_none(), "oldest evicted");
    assert!(store.log_record(third).is_some(), "newest survives");
}

#[test]
fn oldest_is_admission_time_never_emitter_event_time() {
    let mut store = boxed(config(2, u64::MAX), None);
    // Emitter start times DESCEND while admission times ascend: if
    // eviction ever looked at emitter event time, the first-kept span
    // would be the first out.
    let mut s1 = span([1; 16], [1; 8], "emitter-oldest");
    s1.start_time_unix_nano = 9_000;
    let mut s2 = span([2; 16], [2; 8], "emitter-middle");
    s2.start_time_unix_nano = 8_000;
    let mut s3 = span([3; 16], [3; 8], "emitter-newest");
    s3.start_time_unix_nano = 7_000;
    let (entity1, entity2, entity3) = (entity_of(&s1), entity_of(&s2), entity_of(&s3));
    let _ = store.keep_span(admitted(entity1, 100, s1));
    let _ = store.keep_span(admitted(entity2, 200, s2));
    let _ = store.keep_span(admitted(entity3, 300, s3));
    let stats = store.stats();
    assert_eq!(stats.evicted_for_record_ceiling, 1);
    assert!(
        store.span(entity1).is_none(),
        "the ceiling removed the OLDEST ADMISSION, the first-kept span"
    );
    assert!(store.span(entity2).is_some());
    assert!(
        store.span(entity3).is_some(),
        "the span with the OLDEST emitter time but NEWEST admission survives: \
         emitter event time never drives eviction"
    );
}

#[test]
fn the_accounted_bytes_ceiling_evicts_until_the_sum_fits() {
    // A log body is a counted value: one byte of body is one accounted
    // byte. The per-record accounted size is derived at runtime, so the
    // ceiling is expressed in whole records, not guessed constants.
    let unit = log_record("0123456789").accounted_size();
    let ceiling = u64::try_from(unit * 3 + unit / 2).expect("test sizes fit");
    let mut store = boxed(config(u64::MAX, ceiling), None);
    let kept: Vec<(EntityId, usize)> = [
        ("0123456789", 1_u64),
        ("0123456789", 2),
        ("0123456789", 3),
        ("xx", 4),
    ]
    .iter()
    .map(|(body, serial)| {
        let entity = assigned(*serial);
        let record = log_record(body);
        let size = record.accounted_size();
        let outcome = store.keep_log_record(admitted(entity, *serial, record));
        assert!(matches!(outcome, KeepOutcome::Kept { .. }));
        (entity, size)
    })
    .collect();
    let stats = store.stats();
    assert!(
        stats.accounted_bytes <= ceiling,
        "the ceiling holds: {} accounted bytes against {ceiling}",
        stats.accounted_bytes
    );
    let resident_sum: usize = kept
        .iter()
        .filter(|(entity, _)| store.log_record(*entity).is_some())
        .map(|(_, size)| *size)
        .sum();
    assert_eq!(
        u64::try_from(resident_sum).expect("test sizes fit"),
        stats.accounted_bytes,
        "the store's accounting is the sum of the resident records' accounted sizes"
    );
    assert_eq!(
        stats.resident_records, 3,
        "the newest three records fit under the ceiling"
    );
    assert_eq!(stats.evicted_for_accounted_bytes_ceiling, 1);
    assert_eq!(stats.evicted_for_record_ceiling, 0);
}

#[test]
fn the_window_ceiling_expires_without_new_admissions() {
    let window = std::time::Duration::from_secs(60);
    let mut store = boxed(
        MemoryConfig {
            admission_window: window,
            ..MemoryConfig::default()
        },
        None,
    );
    let early = assigned(1);
    let late = assigned(2);
    let _ = store.keep_log_record(admitted(early, 1_000, log_record("early")));
    let _ = store.keep_log_record(admitted(late, 2_000, log_record("late")));
    assert_eq!(store.stats().resident_records, 2);
    // The composition root alone knows time has passed; the store owns no
    // clock. One window past the early record's admission, only it is out.
    let evicted = store.enforce_retention(at(1_000 + 60_000_000_000));
    assert_eq!(evicted, 1);
    assert!(store.log_record(early).is_none(), "expired");
    assert!(store.log_record(late).is_some(), "still inside the window");
    assert_eq!(store.stats().evicted_for_admission_window, 1);
    // A full window past, the late record goes too.
    let evicted = store.enforce_retention(at(2_000 + 60_000_000_000));
    assert_eq!(evicted, 1);
    assert_eq!(store.stats().resident_records, 0);
}

#[test]
fn a_keep_enforces_the_window_against_the_kept_records_admission_time() {
    let window = std::time::Duration::from_millis(500);
    let mut store = boxed(
        MemoryConfig {
            admission_window: window,
            ..MemoryConfig::default()
        },
        None,
    );
    let first = assigned(1);
    let _ = store.keep_log_record(admitted(first, 1_000, log_record("first")));
    assert_eq!(store.stats().resident_records, 1);
    // The next keep carries a reading a window past the first record:
    // the first record expires during that keep, without an explicit pass.
    let second = assigned(2);
    let outcome =
        store.keep_log_record(admitted(second, 1_000 + 500_000_000, log_record("second")));
    assert!(matches!(outcome, KeepOutcome::Kept { evicted: 1 }));
    assert!(store.log_record(first).is_none());
    assert!(store.log_record(second).is_some());
    assert_eq!(store.stats().evicted_for_admission_window, 1);
}

#[test]
fn ties_break_deterministically_by_entity_id() {
    // Two records admitted at the same nanosecond: the residency order
    // breaks the tie by entity id, so WHICH record a ceiling removes is a
    // contract fact, not whatever the map felt like.
    let mut store = boxed(config(2, u64::MAX), None);
    let span_entity = entity_of(&span([9; 16], [9; 8], "tie-span"));
    let assigned_a = assigned(5);
    let assigned_b = assigned(7);
    let _ = store.keep_log_record(admitted(assigned_b, 500, log_record("tie-b")));
    let _ = store.keep_span(admitted(
        span_entity,
        500,
        span([9; 16], [9; 8], "tie-span"),
    ));
    let _ = store.keep_log_record(admitted(assigned_a, 500, log_record("tie-a")));
    let stats = store.stats();
    assert_eq!(stats.resident_records, 2);
    assert_eq!(stats.evicted_for_record_ceiling, 1);
    // Order at equal admission time: a span's natural identity orders
    // before assigned serials, and serials ascend. The span's key is
    // therefore the smallest of the three, so the span is what the ceiling
    // removed — the same record a scan would yield first.
    assert!(
        store.span(span_entity).is_none(),
        "at a tied admission time the span's key is smallest, so the span is evicted"
    );
    assert!(store.log_record(assigned_a).is_some());
    assert!(store.log_record(assigned_b).is_some());
}

#[test]
fn a_record_larger_than_the_ceiling_is_refused_without_evictions() {
    let unit = log_record(&"x".repeat(1_000)).accounted_size();
    let mut store = boxed(
        config(u64::MAX, u64::try_from(unit).expect("fits") - 1),
        None,
    );
    let survivor = assigned(1);
    let _ = store.keep_log_record(admitted(survivor, 1, log_record("survivor")));
    let stats_before = store.stats();

    let entity = assigned(2);
    let outcome = store.keep_log_record(admitted(entity, 2, log_record(&"x".repeat(1_000))));
    assert_eq!(outcome, KeepOutcome::Oversized, "refused, not evicted-into");
    assert!(store.log_record(entity).is_none());
    let stats_after = store.stats();
    assert_eq!(
        stats_after.resident_records, stats_before.resident_records,
        "nothing evicted to make room"
    );
    assert_eq!(
        stats_after.accounted_bytes, stats_before.accounted_bytes,
        "nothing resident changed"
    );
    assert_eq!(stats_after.total_evictions(), 0);
    assert_eq!(
        stats_after.oversized_refusals,
        stats_before.oversized_refusals + 1,
        "the refusal itself is the only change"
    );
}

#[test]
fn a_zero_record_ceiling_keeps_nothing_observably() {
    let hook = RecordingHook::default();
    let mut store = boxed(config(0, u64::MAX), Some(Box::new(hook)));
    let entity = assigned(1);
    let outcome = store.keep_log_record(admitted(entity, 1, log_record("nowhere to live")));
    // The degenerate configuration is honest: the record entered residency
    // and the law immediately removed it — nothing is silently kept.
    assert_eq!(outcome, KeepOutcome::Kept { evicted: 1 });
    assert!(store.log_record(entity).is_none());
    assert_eq!(store.stats().evicted_for_record_ceiling, 1);
    assert_eq!(store.stats().resident_records, 0);
}

#[test]
fn a_mixed_sequence_is_attributed_cause_by_cause() {
    // Records whose accounted sizes differ, admissions out of order, one
    // explicit expiry pass: the cause counters must tell the story exactly.
    let window = std::time::Duration::from_secs(10);
    let small = log_record("s").accounted_size();
    let mut store = boxed(
        MemoryConfig {
            max_records: 3,
            max_accounted_bytes: u64::try_from(small * 4).expect("fits"),
            admission_window: window,
        },
        None,
    );
    let a = assigned(1);
    let b = assigned(2);
    let c = assigned(3);
    let _ = store.keep_log_record(admitted(a, 1, log_record("s")));
    let _ = store.keep_log_record(admitted(b, 2, log_record("s")));
    let _ = store.keep_log_record(admitted(c, 3, log_record("s")));
    // Ceiling state: 3 records (at the record ceiling), 3*small accounted
    // (under the byte ceiling), all inside the window.
    assert_eq!(store.stats().total_evictions(), 0);
    // The fourth keep hits BOTH countable ceilings: one record over the
    // record ceiling, and one record's bytes over the byte ceiling. The
    // first violated ceiling (records) drives the one eviction the loop
    // needs, and the loop stops when all ceilings hold again.
    let d = assigned(4);
    let outcome = store.keep_log_record(admitted(d, 4, log_record("s")));
    let evicted_during_keep = outcome.evicted();
    assert_eq!(evicted_during_keep, 1);
    let stats = store.stats();
    assert_eq!(stats.evicted_for_record_ceiling, 1);
    assert_eq!(stats.evicted_for_accounted_bytes_ceiling, 0);
    assert_eq!(stats.resident_records, 3);
    assert!(store.log_record(a).is_none());
    assert!(store.log_record(d).is_some());
    // Now an explicit pass a full window later expires everything left.
    let expired = store.enforce_retention(at(4 + 10_000_000_000));
    assert_eq!(expired, 3);
    let stats = store.stats();
    assert_eq!(stats.evicted_for_admission_window, 3);
    assert_eq!(stats.total_evictions(), 4);
    assert_eq!(stats.resident_records, 0);
    assert_eq!(stats.accounted_bytes, 0);
}
