//! The file-backed driver's own durability law: what survives a graceful
//! close and reopen, what the reopen re-checks against the configured
//! ceilings, and what the file on disk looks like at each boundary.
//!
//! The behavioral parity suite (`contract.rs`, `retention.rs`,
//! `series.rs`) already proves the file-backed store answers like the
//! memory store; these tests prove the file itself.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::{SharedRecordingHook, admitted, assigned, at, bare_stream, log_record, span};
use runtime_trail_storage::{KeepOutcome, TelemetryStore};
use runtime_trail_storage_sqlite::{FileBackedConfig, FileBackedStore, OpenError};
use runtime_trail_telemetry_model::{
    Admitted, Attributes, EntityId, LogRecord, MetricNumber, MetricPoint, NumberPoint, Span,
    StreamIdentity,
};

/// A file-backed configuration that keeps its ceilings out of the way.
fn roomy() -> FileBackedConfig {
    FileBackedConfig {
        max_records: 64,
        ..FileBackedConfig::default()
    }
}

fn open(path: &Path, config: FileBackedConfig) -> FileBackedStore {
    FileBackedStore::open(path, config, None).expect("the store opens")
}

fn span_entity(trace: [u8; 16], span_id: [u8; 8]) -> EntityId {
    EntityId::Span {
        trace_id: runtime_trail_telemetry_model::TraceId::from_bytes(trace),
        span_id: runtime_trail_telemetry_model::SpanId::from_bytes(span_id),
    }
}

fn admitted_span(serial: u8, name: &str, nano: u64) -> (EntityId, Admitted<Arc<Span>>) {
    let record = span([serial; 16], [serial; 8], name);
    let entity = span_entity([serial; 16], [serial; 8]);
    (entity, admitted(entity, nano, record))
}

fn admitted_log(serial: u8, body: &str, nano: u64) -> (EntityId, Admitted<Arc<LogRecord>>) {
    let entity = assigned(u64::from(serial));
    (entity, admitted(entity, nano, log_record(body)))
}

fn admitted_point(
    serial: u8,
    nano: u64,
    value: MetricNumber,
) -> (EntityId, Admitted<Arc<MetricPoint>>, Arc<StreamIdentity>) {
    let entity = assigned(u64::from(serial));
    let stream = bare_stream(&format!("stream-{serial}"));
    let point = MetricPoint::Number(NumberPoint::measurement(
        nano,
        value,
        Attributes::default(),
        Vec::new(),
    ));
    (entity, admitted(entity, nano, point), stream)
}

/// Every kind kept in one session is resident again, content-identical,
/// after a graceful close (which checkpoints the WAL) and a reopen.
#[test]
fn kept_kinds_survive_a_graceful_close_and_reopen() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    {
        let mut store = open(&path, roomy());
        let (_, span) = admitted_span(1, "first", 1);
        assert_eq!(store.keep_span(span), KeepOutcome::Kept { evicted: 0 });
        let (_, log) = admitted_log(2, "hello", 1);
        assert_eq!(store.keep_log_record(log), KeepOutcome::Kept { evicted: 0 });
        let (_, point, stream) = admitted_point(3, 1, MetricNumber::int(7));
        assert_eq!(
            store.keep_metric_point(point, stream),
            KeepOutcome::Kept { evicted: 0 }
        );
        assert_eq!(store.stats().resident_records, 3);
        assert_eq!(store.mode_name(), FileBackedStore::NAME);
    }
    let store = open(&path, roomy());
    let stats = store.stats();
    assert_eq!(stats.resident_records, 3);
    assert_eq!(stats.resident_spans, 1);
    assert_eq!(stats.resident_log_records, 1);
    assert_eq!(stats.resident_metric_points, 1);
    let reopened_span = store.span(span_entity([1; 16], [1; 8]));
    assert_eq!(
        reopened_span,
        Some(Arc::new(span([1; 16], [1; 8], "first")))
    );
    let reopened_log = store.log_record(assigned(2)).expect("the log survives");
    assert_eq!(reopened_log.as_ref(), &log_record("hello"));
    let reopened_point = store.metric_point(assigned(3)).expect("the point survives");
    assert_eq!(
        reopened_point.point.as_ref(),
        &MetricPoint::Number(NumberPoint::measurement(
            1,
            MetricNumber::int(7),
            Attributes::default(),
            Vec::new(),
        ))
    );
    assert_eq!(
        reopened_point.stream.as_ref(),
        bare_stream("stream-3").as_ref()
    );
}

/// Session counters start at zero on a reopen, even over a file that was
/// busy before: they count this session's retention work, like the memory
/// driver's do.
#[test]
fn a_reopened_store_starts_its_session_counters_at_zero() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    {
        let mut store = open(&path, roomy());
        for serial in 1..=3 {
            let (_, span) = admitted_span(serial, "s", u64::from(serial));
            assert_eq!(store.keep_span(span), KeepOutcome::Kept { evicted: 0 });
        }
        drop(store);
    }
    let store = open(&path, roomy());
    let stats = store.stats();
    assert_eq!(stats.resident_records, 3);
    assert_eq!(stats.total_evictions(), 0);
    assert_eq!(stats.total_keep_refusals(), 0);
    assert_eq!(stats.hook_deliveries, 0);
}

/// Reopening re-measures the resident set against the ceilings and evicts
/// down to them, reporting the open-time evictions through the hook like
/// any other eviction.
#[test]
fn the_reopen_rechecks_the_ceilings_and_delivers_the_evictions() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    {
        let mut store = open(&path, roomy());
        for serial in 1..=4 {
            let (_, span) = admitted_span(serial, &format!("row-{serial}"), u64::from(serial));
            assert_eq!(store.keep_span(span), KeepOutcome::Kept { evicted: 0 });
        }
        drop(store);
    }
    let hook = SharedRecordingHook::default();
    let config = FileBackedConfig {
        max_records: 2,
        ..FileBackedConfig::default()
    };
    let store = FileBackedStore::open(&path, config, Some(Box::new(hook.clone())))
        .expect("the reopen fits two records");
    assert_eq!(store.stats().resident_records, 2);
    assert_eq!(store.stats().total_evictions(), 2);
    {
        let hook = hook.0.lock().expect("hook lock");
        assert_eq!(hook.evicted.len(), 2, "open-time evictions deliver");
    }
    let names: Vec<String> = store
        .scan_spans(None, 128)
        .items
        .into_iter()
        .map(|item| item.record.name.clone())
        .collect();
    assert_eq!(names, vec!["row-3".to_string(), "row-4".to_string()]);
}
/// The admission window is not a clock in the file: a reopened store never
/// expires records on its own, and the window's first enforcement is the
/// composition root's first `enforce_retention` reading.
#[test]
fn the_admission_window_waits_for_the_first_enforced_reading() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    let config = FileBackedConfig {
        max_records: 8,
        admission_window: Duration::from_nanos(1),
        ..FileBackedConfig::default()
    };
    {
        let mut store = open(&path, config);
        let (_, span) = admitted_span(1, "ageing", 1);
        assert_eq!(store.keep_span(span), KeepOutcome::Kept { evicted: 0 });
        drop(store);
    }
    let mut store = open(&path, config);
    assert_eq!(
        store.stats().resident_records,
        1,
        "the window is not applied at open: the store owns no clock"
    );
    let evicted = store.enforce_retention(at(2));
    assert_eq!(evicted, 1);
    assert_eq!(store.stats().resident_records, 0);
    assert_eq!(store.stats().evicted_for_admission_window, 1);
    // The expiry is durable, not just resident-side.
    drop(store);
    let store = open(&path, config);
    assert_eq!(store.stats().resident_records, 0);
}

/// The persistence encoding is the IEEE-754 bit pattern: NaN (payload
/// included), both infinities and negative zero survive the reopen
/// bit-exactly, which plain JSON could not promise.
#[test]
fn doubles_round_trip_bit_exactly_through_the_reopen() {
    use runtime_trail_telemetry_model::Float;
    let doubles: [f64; 6] = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0, 0.0, 1.5];
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    {
        let mut store = open(&path, roomy());
        for (index, double) in doubles.iter().enumerate() {
            let serial = u8::try_from(index + 1).expect("five serials");
            let (_, point, stream) =
                admitted_point(serial, 1, MetricNumber::Double(Float::new(*double)));
            assert_eq!(
                store.keep_metric_point(point, stream),
                KeepOutcome::Kept { evicted: 0 }
            );
        }
        drop(store);
    }
    let store = open(&path, roomy());
    for (index, double) in doubles.iter().enumerate() {
        let serial = u8::try_from(index + 1).expect("five serials");
        let view = store
            .metric_point(assigned(u64::from(serial)))
            .expect("the double survives");
        let number = match view.point.as_ref() {
            MetricPoint::Number(point) => point.value,
            _ => panic!("fixtures are measurements"),
        };
        let MetricNumber::Double(reopened) = number else {
            panic!("the double comes back as a double");
        };
        assert_eq!(
            reopened.bits(),
            double.to_bits(),
            "bit-exact round trip for {double:e} (#{index})"
        );
    }
}

/// A graceful close checkpoints the WAL and closes the connection, leaving
/// exactly one file — the database — which is copyable and reopens
/// elsewhere.
#[test]
fn a_graceful_close_leaves_one_file_that_copies_and_reopens() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    {
        let mut store = open(&path, roomy());
        for serial in 1..=2 {
            let (_, span) = admitted_span(serial, "copied", u64::from(serial));
            assert_eq!(store.keep_span(span), KeepOutcome::Kept { evicted: 0 });
        }
        drop(store);
    }
    let mut files: Vec<String> = std::fs::read_dir(dir.path())
        .expect("the directory reads")
        .map(|entry| {
            entry
                .expect("the entry reads")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    files.sort();
    assert_eq!(
        files,
        vec!["store.db".to_string()],
        "no -wal or -shm remains"
    );
    let copy = dir.path().join("copy.db");
    std::fs::copy(&path, &copy).expect("a single file copies");
    let store = open(&copy, roomy());
    assert_eq!(
        store.span(span_entity([1; 16], [1; 8])),
        Some(Arc::new(span([1; 16], [1; 8], "copied")))
    );
}

/// Reopened points of one stream share one interned identity: the
/// rehydrated stream table maps content to a single Arc.
#[test]
fn reopened_points_of_the_same_stream_share_one_interned_identity() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    {
        let mut store = open(&path, roomy());
        let shared = bare_stream("shared");
        for serial in 1..=2 {
            let (_entity, point, _) = admitted_point(
                serial,
                u64::from(serial),
                MetricNumber::int(i64::from(serial)),
            );
            assert_eq!(
                store.keep_metric_point(point, Arc::clone(&shared)),
                KeepOutcome::Kept { evicted: 0 }
            );
        }
        drop(store);
    }
    let store = open(&path, roomy());
    let first = store.metric_point(assigned(1)).expect("first point");
    let second = store.metric_point(assigned(2)).expect("second point");
    assert!(
        Arc::ptr_eq(&first.stream, &second.stream),
        "one interned identity per stream content"
    );
    assert_eq!(store.stats().resident_streams, 1);
}

/// The open taxonomy never destroys: a refused open is a report, and the
/// file that refused it is exactly what the caller handed over.
#[test]
fn a_refused_open_never_touches_the_file() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    let junk = b"this page is prose, not a sqlite database at all, just padding";
    std::fs::write(&path, junk).expect("junk lands");
    let Err(error) = FileBackedStore::open(&path, roomy(), None) else {
        panic!("junk refuses the open")
    };
    assert!(matches!(error, OpenError::NotADatabase { .. }), "{error}");
    assert_eq!(
        std::fs::read(&path).expect("the file still reads"),
        junk,
        "the refused open left the bytes untouched"
    );
}
