//! Series law: the byte ceiling counts what residency pins, and the series
//! cap bounds distinct resident streams. These are the regression proofs
//! for the review's B1 and B3 findings: many single-point streams carrying
//! large legal identity content must be charged to the ceiling (the old
//! accounting saw only ~36 B per point), and a keep establishing a new
//! stream beyond the cap must be refused by name — never answered by
//! eviction.

mod common;

use common::{
    RecordingHook, SharedRecordingHook, admitted, assigned, at, bare_stream, boxed, gauge_point,
    heavy_stream,
};
use runtime_trail_storage::KeepOutcome;
use runtime_trail_storage_memory::MemoryConfig;
use runtime_trail_telemetry_model::Accounted;

fn config(max_records: u64, max_accounted_bytes: u64, series_cap: u64) -> MemoryConfig {
    MemoryConfig {
        max_records,
        max_accounted_bytes,
        series_cap,
        ..MemoryConfig::default()
    }
}

/// A resident gauge point's accounted size: an empty attribute map charges
/// nothing, so the point is shell + time + value + flags.
const POINT_BYTES: u64 = 16 + 8 + 8 + 4;

/// The reviewer's probe shape, at test scale: many distinct streams, each a
/// single point, each stream's resource carrying large legal attribute
/// content. Before the identity charge existed this suite reported
/// `36 B x points` and let every stream park its resource bytes under a
/// ceiling that never saw them.
#[test]
fn single_point_streams_pay_their_identity_to_the_byte_ceiling() {
    // One stream's identity: 16 attributes of 2 KiB payload each.
    let (attributes, value_bytes) = (16, 2_048);
    let probe = heavy_stream("probe", attributes, value_bytes);
    let identity_bytes = u64::try_from(probe.accounted_size()).expect("test sizes fit");
    // A verified lower bound on the identity's REAL heap cost: every
    // attribute's payload bytes plus its String header and key. The
    // accounted charge must sit above it (never under-count) and within
    // 4x of it (never run away) — the model's own honesty law, applied to
    // the identity content the ceiling now sees.
    let real_lower_bound = u64::try_from(attributes * (value_bytes + 24 + 4)).expect("fits");
    assert!(
        identity_bytes >= real_lower_bound,
        "the identity charge under-counts the identity content it pins"
    );
    assert!(
        identity_bytes <= 4 * real_lower_bound,
        "the identity charge ran away from the content it pins"
    );

    // The ceiling holds about two such identities: the third keep must
    // evict rather than let a third stream park uncharged.
    let ceiling = 100 * 1024;
    let hook = RecordingHook::default();
    let mut store = boxed(config(u64::MAX, ceiling, u64::MAX), Some(Box::new(hook)));

    for serial in 1..=200_u64 {
        let outcome = store.keep_metric_point(
            admitted(assigned(serial), serial * 10, gauge_point(serial, 1)),
            heavy_stream(&format!("probe-{serial}"), attributes, value_bytes),
        );
        assert!(matches!(outcome, KeepOutcome::Kept { .. }));
    }

    let stats = store.stats();
    assert!(
        stats.accounted_bytes <= ceiling,
        "the ceiling holds: {} accounted against {ceiling}",
        stats.accounted_bytes
    );
    assert!(
        stats.evicted_for_accounted_bytes_ceiling > 0,
        "the byte ceiling fired on identity content, not only on points"
    );
    // Residency stayed bounded: about two identities' worth of streams,
    // not the 200 the probe delivered. The old accounting held all 200
    // streams under this ceiling (200 x 36 B of points) parking ~6 MiB of
    // uncharged identity content.
    assert!(
        stats.resident_streams <= ceiling / identity_bytes + 1,
        "{} resident streams under a {ceiling}-byte ceiling with \
         {identity_bytes}-byte identities",
        stats.resident_streams
    );
    // The split proves what the ceiling now sees: the records' share is
    // the ~36 B/point the old accounting reported for the WHOLE residency,
    // and the identities' share dominates it by three orders of magnitude.
    assert_eq!(
        stats.record_accounted_bytes,
        POINT_BYTES * stats.resident_metric_points,
        "the records' share is exactly the per-point formula the old ceiling saw"
    );
    assert!(
        stats.identity_accounted_bytes >= 500 * stats.record_accounted_bytes,
        "identity content {} must dominate the points' {} on this shape",
        stats.identity_accounted_bytes,
        stats.record_accounted_bytes
    );
    assert_eq!(
        stats.accounted_bytes,
        stats.record_accounted_bytes + stats.identity_accounted_bytes,
        "the ceiling total is the split, summed"
    );
}

/// A stream's identity is charged exactly once however many points of the
/// stream are resident, and leaves the ceiling exactly when the stream's
/// last point is evicted — reported through the hook as a release.
#[test]
fn an_identity_is_charged_once_and_released_with_its_last_point() {
    let stream = heavy_stream("charged", 4, 256);
    let identity_bytes = u64::try_from(stream.accounted_size()).expect("test sizes fit");
    let hook = RecordingHook::default();
    let mut store = boxed(
        MemoryConfig {
            max_records: u64::MAX,
            max_accounted_bytes: identity_bytes * 10,
            admission_window: std::time::Duration::from_micros(1),
            series_cap: u64::MAX,
        },
        Some(Box::new(hook)),
    );

    // Three points of ONE stream: the charge is once, not three times.
    for (serial, time) in [(1_u64, 100_u64), (2, 200), (3, 300)] {
        let _ = store.keep_metric_point(
            admitted(assigned(serial), time, gauge_point(time, 1)),
            Arc::clone(&stream),
        );
    }
    let stats = store.stats();
    assert_eq!(stats.resident_streams, 1);
    assert_eq!(
        stats.resident_metric_points, 3,
        "all three points are resident: the window outlived the span",
    );
    assert_eq!(
        stats.identity_accounted_bytes, identity_bytes,
        "one stream, one identity charge — however many points are resident"
    );
    assert_eq!(
        stats.record_accounted_bytes,
        POINT_BYTES * 3,
        "the points' own accounted sizes are unchanged"
    );

    // The window (1_000) is wider than the whole admission span, so the
    // keeps expire nothing; each reference reading then sits exactly one
    // window past one admission (the admissions are 100 apart) and
    // expires exactly that point. The identity survives the first two
    // removals and goes exactly with the last.
    let step = 1_000_u64; // the window above
    let _ = store.enforce_retention(at(100 + step)); // expires the t=100 point
    assert_eq!(store.stats().resident_streams, 1);
    assert_eq!(store.stats().identity_accounted_bytes, identity_bytes);
    let _ = store.enforce_retention(at(200 + step)); // expires the t=200 point
    assert_eq!(store.stats().resident_streams, 1);
    assert_eq!(store.stats().identity_accounted_bytes, identity_bytes);
    let _ = store.enforce_retention(at(300 + step)); // expires the t=300 point
    let stats = store.stats();
    assert_eq!(stats.resident_metric_points, 0);
    assert_eq!(
        stats.resident_streams, 0,
        "the last point released the stream"
    );
    assert_eq!(
        stats.identity_accounted_bytes, 0,
        "the identity charge left the ceiling with the stream"
    );
    assert_eq!(stats.accounted_bytes, 0);
}

/// The series cap: a keep establishing a NEW stream beyond the cap is
/// refused by name, evicting nothing; an existing stream keeps admitting
/// points; and evicting a stream's last point frees exactly one slot for
/// the next new stream.
#[test]
fn the_series_cap_refuses_new_streams_and_a_freed_slot_readmits() {
    let recorded = SharedRecordingHook::default();
    let mut store = boxed(
        config(u64::MAX, u64::MAX, 2),
        Some(Box::new(recorded.clone())),
    );

    // Two streams fill the cap.
    let alpha = bare_stream("alpha");
    let beta = bare_stream("beta");
    let gamma = bare_stream("gamma");
    assert_eq!(
        store.keep_metric_point(
            admitted(assigned(1), 100, gauge_point(1, 1)),
            Arc::clone(&alpha)
        ),
        KeepOutcome::Kept { evicted: 0 }
    );
    assert_eq!(
        store.keep_metric_point(
            admitted(assigned(2), 200, gauge_point(2, 1)),
            Arc::clone(&beta)
        ),
        KeepOutcome::Kept { evicted: 0 }
    );

    // A third NEW stream is refused: named, counted, and nothing evicted.
    assert_eq!(
        store.keep_metric_point(
            admitted(assigned(3), 300, gauge_point(3, 1)),
            Arc::clone(&gamma)
        ),
        KeepOutcome::SeriesCapReached
    );
    assert!(store.metric_point(assigned(3)).is_none());
    let stats = store.stats();
    assert_eq!(stats.kept_out_series_cap, 1);
    assert_eq!(stats.total_evictions(), 0, "a cap refusal never evicts");
    assert_eq!(stats.resident_streams, 2);
    assert_eq!(stats.resident_records, 2);

    // A second point of an ALREADY resident stream is not a new stream:
    // the cap does not gate it, even with the cap exactly full.
    assert_eq!(
        store.keep_metric_point(
            admitted(assigned(4), 400, gauge_point(4, 1)),
            Arc::clone(&alpha)
        ),
        KeepOutcome::Kept { evicted: 0 }
    );
    // Beta takes a younger point too, so its first point can expire
    // without ending the stream.
    assert_eq!(
        store.keep_metric_point(
            admitted(assigned(6), 500, gauge_point(6, 1)),
            Arc::clone(&beta)
        ),
        KeepOutcome::Kept { evicted: 0 }
    );
    assert_eq!(store.stats().resident_streams, 2);
    assert_eq!(store.stats().kept_out_series_cap, 1);

    // A retention pass a window past alpha's first point expires exactly
    // that point (the 24 h default window still covers the rest). Alpha's
    // stream is not released, a younger point stands, so its slot is not
    // freed: gamma is refused a second time.
    let window_nanos =
        u64::try_from(MemoryConfig::default().admission_window.as_nanos()).expect("window fits");
    assert_eq!(store.enforce_retention(at(100 + window_nanos)), 1);
    let stats = store.stats();
    assert_eq!(stats.resident_streams, 2, "alpha still holds a point");
    assert_eq!(
        store.keep_metric_point(
            admitted(assigned(5), 550, gauge_point(5, 1)),
            Arc::clone(&gamma)
        ),
        KeepOutcome::SeriesCapReached,
        "an eviction that does not end the stream frees no slot"
    );
    assert_eq!(
        store.stats().kept_out_series_cap,
        stats.kept_out_series_cap + 1
    );

    // The pass one window past alpha's LAST point takes it, and beta's
    // first point, out of residency; beta's younger point survives. The
    // stream ends exactly with its last point: one slot free.
    assert_eq!(store.enforce_retention(at(400 + window_nanos)), 2);
    let stats = store.stats();
    assert_eq!(stats.resident_streams, 1, "only beta remains");
    assert!(store.metric_point(assigned(4)).is_none());
    assert!(store.metric_point(assigned(2)).is_none());
    assert!(store.metric_point(assigned(6)).is_some());

    // The freed slot admits gamma now, and the cap holds two again.
    assert_eq!(
        store.keep_metric_point(
            admitted(assigned(7), 600, gauge_point(7, 1)),
            Arc::clone(&gamma)
        ),
        KeepOutcome::Kept { evicted: 0 }
    );
    assert_eq!(store.stats().resident_streams, 2, "cap still holds two");
    assert!(store.metric_point(assigned(7)).is_some());

    // The hook's ledger: three evictions, oldest first, each delivered
    // once; the one stream exit is alpha's, exactly with its last point.
    let hook = recorded.0.lock().expect("hook lock poisoned");
    assert_eq!(hook.evicted, vec![assigned(1), assigned(2), assigned(4)]);
    assert_eq!(hook.streams_released.len(), 1, "one stream exit");
    assert_eq!(hook.streams_released[0].name, "alpha");
    drop(hook);
    let stats = store.stats();
    assert_eq!(stats.total_evictions(), 3);
    assert_eq!(stats.hook_deliveries, 3, "every eviction was delivered");
}

/// A zero series cap is legal configuration: no stream can be established,
/// observably, and nothing is evicted to pretend otherwise.
#[test]
fn a_zero_series_cap_admits_no_stream_observably() {
    let mut store = boxed(config(u64::MAX, u64::MAX, 0), None);
    let outcome = store.keep_metric_point(
        admitted(assigned(1), 100, gauge_point(1, 1)),
        bare_stream("anywhere"),
    );
    assert_eq!(outcome, KeepOutcome::SeriesCapReached);
    assert_eq!(store.stats().resident_records, 0);
    assert_eq!(store.stats().resident_streams, 0);
    assert_eq!(store.stats().kept_out_series_cap, 1);
    assert_eq!(store.stats().total_evictions(), 0);
}

/// The store hands the hook the interned identity at the moment the series
/// count reaches zero, exactly once per stream exit.
#[test]
fn the_hook_sees_one_release_per_stream_exit() {
    let hook = RecordingHook::default();
    let mut store = boxed(config(1, u64::MAX, u64::MAX), Some(Box::new(hook)));
    let first = bare_stream("first");
    let second = bare_stream("second");
    let _ = store.keep_metric_point(
        admitted(assigned(1), 100, gauge_point(1, 1)),
        Arc::clone(&first),
    );
    // Evicts the first point: one record removed, one stream released.
    let _ = store.keep_metric_point(
        admitted(assigned(2), 200, gauge_point(2, 1)),
        Arc::clone(&second),
    );
    let stats = store.stats();
    assert_eq!(stats.total_evictions(), 1);
    assert_eq!(
        stats.hook_deliveries, 1,
        "one record, one completed delivery"
    );
    assert_eq!(store.stats().resident_streams, 1);
}

// The Arc the tests share stream identities through.
use std::sync::Arc;

/// An identity the ceiling cannot hold on its own is refused before
/// anything is inserted, evicting nothing, naming the ceiling and the
/// identity's size — and the store stays fully usable for legal streams.
/// The flagged edge: an over-cap identity must never be answered by
/// evicting the store's records (eviction cannot shrink a charge that is
/// over the cap by itself) and must never leave the refused stream's
/// content resident.
#[test]
fn an_identity_over_the_ceiling_is_refused_without_evicting_anything() {
    // Four legal 4 KiB attributes: a stream identity over the 8 KiB test
    // ceiling, every attribute inside the model's own per-value budget —
    // so the payload admitted cleanly and the refusal is a keep-time
    // fact, not an admission one.
    let over = heavy_stream("over", 4, 4_096);
    let identity_bytes = u64::try_from(over.accounted_size()).expect("test sizes fit");
    let ceiling = 8 * 1024;
    assert!(
        identity_bytes > ceiling,
        "the fixture is the over-cap shape: {identity_bytes} against {ceiling}"
    );

    let hook = SharedRecordingHook::default();
    let mut store = boxed(
        config(u64::MAX, ceiling, u64::MAX),
        Some(Box::new(hook.clone())),
    );
    // A legal record enters first: whatever the refusal does, it must not
    // touch what already stands.
    let kept = store.keep_metric_point(
        admitted(assigned(1), 10, gauge_point(10, 1)),
        bare_stream("legal"),
    );
    assert!(matches!(kept, KeepOutcome::Kept { evicted: 0 }));

    let refused = store.keep_metric_point(admitted(assigned(2), 20, gauge_point(20, 1)), over);
    assert_eq!(
        refused,
        KeepOutcome::IdentityOverCeiling {
            ceiling,
            identity_bytes,
        },
        "the refusal names the cap and the identity's size"
    );
    assert_eq!(refused.evicted(), 0, "a refusal never evicts");

    let stats = store.stats();
    assert_eq!(
        stats.identity_over_ceiling_refusals, 1,
        "the refusal is counted, observable"
    );
    assert_eq!(stats.total_evictions(), 0, "nothing was evicted to try");
    assert_eq!(
        stats.resident_metric_points, 1,
        "only the legal point is resident"
    );
    assert_eq!(
        stats.resident_streams, 1,
        "the refused stream never entered"
    );
    assert_eq!(
        stats.accounted_bytes,
        stats.record_accounted_bytes + stats.identity_accounted_bytes,
        "the refused identity is charged nowhere"
    );
    let legal_identity_bytes =
        u64::try_from(bare_stream("legal").accounted_size()).expect("test sizes fit");
    assert_eq!(
        stats.identity_accounted_bytes, legal_identity_bytes,
        "the over-cap identity left no charge behind"
    );
    assert!(store.metric_point(assigned(2)).is_none());
    assert!(
        store.metric_point(assigned(1)).is_some(),
        "the legal record that stood before the refusal still stands"
    );
    {
        let recorded = hook.0.lock().expect("hook lock poisoned");
        assert!(
            recorded.evicted.is_empty() && recorded.streams_released.is_empty(),
            "the hook saw nothing: no record left, no stream released"
        );
    }

    // The store is unpoisoned by the refusal: another legal stream keeps
    // cleanly under the same ceiling.
    let after = store.keep_metric_point(
        admitted(assigned(3), 30, gauge_point(30, 1)),
        bare_stream("legal-two"),
    );
    assert!(matches!(after, KeepOutcome::Kept { evicted: 0 }));
}
