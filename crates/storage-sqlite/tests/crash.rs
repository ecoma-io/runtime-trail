//! Ungraceful-exit durability: what survives when the process dies without
//! a `Drop` — no checkpoint, the write-ahead log left as it was. The child
//! half of the story is this same test binary re-invoked with `--exact`,
//! writing records and then calling `std::process::exit`, so no destructor
//! ever runs.
mod common;

use std::sync::Arc;

use common::{admitted, assigned, bare_stream, span};
use runtime_trail_storage::{KeepOutcome, TelemetryStore};
use runtime_trail_storage_sqlite::{FileBackedConfig, FileBackedStore};
use runtime_trail_telemetry_model::{
    Admitted, Attributes, EntityId, MetricNumber, MetricPoint, NumberPoint, Span,
};
/// The environment variable that turns this test binary into the crash
/// child, naming the database the child writes and then abandons.
const CHILD_DB_ENV: &str = "SQLITE_CRASH_CHILD_DB";

fn roomy() -> FileBackedConfig {
    FileBackedConfig {
        max_records: 64,
        ..FileBackedConfig::default()
    }
}

fn span_entity(serial: u8) -> EntityId {
    EntityId::Span {
        trace_id: runtime_trail_telemetry_model::TraceId::from_bytes([serial; 16]),
        span_id: runtime_trail_telemetry_model::SpanId::from_bytes([serial; 8]),
    }
}

fn admitted_span(serial: u8, name: &str, nano: u64) -> (EntityId, Admitted<Arc<Span>>) {
    let record = span([serial; 16], [serial; 8], name);
    (
        span_entity(serial),
        admitted(span_entity(serial), nano, record),
    )
}

// The crash child: keeps four records, asserts each keep landed, and
// dies with `std::process::exit` so no `Drop` — and therefore no WAL
// checkpoint — ever runs. Run as a normal test (no child env) it is a
// no-op; the parent test spawns it with `--exact`.
#[test]
fn crash_child_keeps_then_dies() {
    let Some(path) = std::env::var_os(CHILD_DB_ENV).map(std::path::PathBuf::from) else {
        return;
    };
    let mut store =
        FileBackedStore::open(&path, roomy(), None).expect("the child opens a fresh database");
    for serial in 1u8..=3 {
        let name = format!("crash-{serial}");
        let (_, span) = admitted_span(serial, &name, u64::from(serial));
        assert_eq!(store.keep_span(span), KeepOutcome::Kept { evicted: 0 });
    }
    let entity = assigned(4);
    let stream = bare_stream("crash-stream");
    let point = MetricPoint::Number(NumberPoint::measurement(
        4,
        MetricNumber::int(9),
        Attributes::default(),
        Vec::new(),
    ));
    assert_eq!(
        store.keep_metric_point(admitted(entity, 4, point), stream),
        KeepOutcome::Kept { evicted: 0 }
    );
    std::process::exit(71);
}

// The parent half: the child's writes hit SQLite's page cache and the
/// session is a normal one — a graceful close afterwards restores the
/// single-file invariant.
#[test]
fn committed_keeps_survive_an_ungraceful_exit_and_reopen() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    let status = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args(["--exact", "crash_child_keeps_then_dies", "--nocapture"])
        .env(CHILD_DB_ENV, &path)
        .status()
        .expect("the child test binary spawns");
    assert_eq!(
        status.code(),
        Some(71),
        "the child exited by calling process::exit"
    );

    let wal = dir.path().join("store.db-wal");
    assert!(
        wal.exists(),
        "the uncheckpointed write-ahead log is still on disk after the crash"
    );

    let store = FileBackedStore::open(&path, roomy(), None)
        .expect("the crashed session's committed data reopens");
    let snapshot = store.stats();
    assert_eq!(
        snapshot.resident_records, 4,
        "every committed keep survived"
    );
    assert_eq!(snapshot.resident_spans, 3);
    assert_eq!(snapshot.resident_metric_points, 1);
    for serial in 1u8..=3 {
        let name = format!("crash-{serial}");
        let reopened = store
            .span(span_entity(serial))
            .expect("a committed span survives");
        assert_eq!(reopened.name, name, "content survives, not just rows");
    }

    drop(store);
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
        "a graceful close after recovery leaves one file again"
    );
}
