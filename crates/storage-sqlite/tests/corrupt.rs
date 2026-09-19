//! The open taxonomy: what kinds of file refuse a store open, and the
//! guarantee that a refused open never deletes, truncates, or rewrites
//! what the caller handed over.

mod common;

use common::{admitted, span};
use runtime_trail_storage::{KeepOutcome, TelemetryStore};
use runtime_trail_storage_sqlite::{FileBackedConfig, FileBackedStore, OpenError};
use runtime_trail_telemetry_model::{EntityId, SpanId, TraceId};

fn span_entity(serial: u8) -> EntityId {
    EntityId::Span {
        trace_id: TraceId::from_bytes([serial; 16]),
        span_id: SpanId::from_bytes([serial; 8]),
    }
}

/// The refused open, as a plain error: `Result::expect_err` would demand
/// `Debug` on the store, which the file-backed handle deliberately does
/// not implement; the taxonomy is what the tests pin instead.
fn refuse(path: &std::path::Path) -> OpenError {
    match FileBackedStore::open(path, FileBackedConfig::default(), None) {
        Ok(_) => panic!("the open must refuse this path"),
        Err(error) => error,
    }
}

#[test]
fn non_database_bytes_are_refused_and_left_untouched() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    let junk = b"this page is prose, not a sqlite database at all, just padding";
    std::fs::write(&path, junk).expect("junk lands");
    let error = refuse(&path);
    assert!(matches!(error, OpenError::NotADatabase { .. }), "{error}");
    assert_eq!(
        std::fs::read(&path).expect("the file still reads"),
        junk,
        "the refused open left the bytes untouched"
    );
}

/// A directory is not a database: refused, never walked.
#[test]
fn a_directory_path_is_refused() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let error = refuse(dir.path());
    assert!(
        matches!(
            error,
            OpenError::Open { .. } | OpenError::NotADatabase { .. }
        ),
        "{error}"
    );
}

/// A missing parent directory cannot be conjured by the open, and the
/// failed open creates nothing.
#[test]
fn a_missing_parent_is_refused_and_nothing_is_created() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("missing").join("store.db");
    let error = refuse(&path);
    assert!(matches!(error, OpenError::Open { .. }), "{error}");
    assert!(!path.exists(), "the refused open created nothing");
}

/// A real database holding rows this build cannot decode refuses the open
/// as corrupt — a build that wrote them, or a file edited underneath the
/// driver, is reported, not silently reinterpreted.
#[test]
fn an_undecodable_row_refuses_the_open_as_corrupt() {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let path = dir.path().join("store.db");
    {
        let mut store = FileBackedStore::open(&path, FileBackedConfig::default(), None)
            .expect("a fresh store opens");
        let record = span([1; 16], [1; 8], "will be corrupted");
        assert_eq!(
            store.keep_span(admitted(span_entity(1), 1u64, record)),
            KeepOutcome::Kept { evicted: 0 }
        );
    }
    {
        let connection = rusqlite::Connection::open(&path).expect("the raw connection opens");
        connection
            .execute("UPDATE spans SET payload = '{this is not json'", [])
            .expect("the payload is corrupted on purpose");
    }
    let error = refuse(&path);
    assert!(matches!(error, OpenError::CorruptData { .. }), "{error}");
    let message = error.to_string();
    assert!(
        message.contains("cannot decode"),
        "the error names the failure: {message}"
    );
}
