//! The `SQLite` persistence layer of the file-backed store.
//!
//! Memory-authoritative, write-through durability
//! (`docs/architecture/storage-model.md`): the store keeps residency in
//! its in-memory shelves exactly like the memory mode, and this module
//! mirrors every residency change to a `SQLite` file so a reopened session
//! starts from what a closed one kept. The database is a write-through log
//! of residency, never a source the hot path re-reads.
//!
//! # Durability contract
//!
//! The file is opened with `journal_mode=WAL` and `synchronous=FULL`:
//! every commit is fsynced before it returns, so a "committed" change is
//! durable against process crash and power loss; a crash leaves the
//! committed tail in the write-ahead log, and the next open recovers it.
//! Each store method persists its residency change as **one transaction**,
//! so on-disk and in-memory states can never disagree by half a keep.
//! A graceful close ([`checkpoint`]) folds the WAL into the single
//! database file, so "copy one file, reopen the session"
//! ([ADR 0003](../../docs/decisions/0003-storage-strategy.md)) holds when
//! the session ends cleanly.

use std::path::Path;

use rusqlite::{Connection, params};

use runtime_trail_telemetry_model::EntityId;

/// The file's schema: one table per record kind, keyed by the entity id
/// (the admission-assigned identity the store shelves by), with the
/// admission time and the serialized payload alongside.
///
/// `STRICT` tables: `SQLite` enforces the declared column types, so a row
/// written by any build of this driver is read by any other — no silent
/// type drift between the table definition and the bindings.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS spans (
    entity        TEXT    PRIMARY KEY,
    admission_nano INTEGER NOT NULL,
    payload       TEXT    NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS logs (
    entity        TEXT    PRIMARY KEY,
    admission_nano INTEGER NOT NULL,
    payload       TEXT    NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS points (
    entity         TEXT    PRIMARY KEY,
    admission_nano INTEGER NOT NULL,
    payload        TEXT    NOT NULL,
    stream_payload TEXT    NOT NULL
) STRICT;
";

/// One residency change staged for a single transaction.
///
/// Payloads arrive already serialized (`serde_json`); the entity id is
/// serialized on binding. The store stages exactly what residency did —
/// an insert for each kept record, a delete for each eviction — and
/// [`commit`] replays the batch atomically.
#[derive(Debug)]
pub(crate) enum DbOp {
    PutSpan {
        entity: EntityId,
        admitted_at: u64,
        payload: String,
    },
    PutLog {
        entity: EntityId,
        admitted_at: u64,
        payload: String,
    },
    PutPoint {
        entity: EntityId,
        admitted_at: u64,
        payload: String,
        stream_payload: String,
    },
    DeleteSpan {
        entity: EntityId,
    },
    DeleteLog {
        entity: EntityId,
    },
    DeletePoint {
        entity: EntityId,
    },
}

/// Opens (or creates) the database at `path` and prepares it for this
/// driver: WAL journaling, full-sync durability, the schema, and a
/// proof-of-life query so a file that is not a real database fails here —
/// at construction — instead of surprising a later pass.
pub(crate) fn open(path: &Path) -> Result<Connection, rusqlite::Error> {
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.execute_batch(SCHEMA)?;
    // Belt and braces: the pragma and schema statements above already read
    // the file header, but a one-row query makes the "this file is a real
    // database" check explicit.
    connection.query_row("SELECT count(*) FROM sqlite_schema", [], |row| {
        row.get::<_, i64>(0)
    })?;
    Ok(connection)
}

/// Replays one staged batch as a single transaction.
///
/// A failure rolls the whole batch back: on-disk residency and in-memory
/// residency can never disagree by half a keep. The caller decides what a
/// failure means (the store keeps residency and logs the degradation);
/// this module only guarantees the atomicity.
pub(crate) fn commit(connection: &mut Connection, ops: &[DbOp]) -> Result<(), rusqlite::Error> {
    if ops.is_empty() {
        return Ok(());
    }
    let transaction = connection.transaction()?;
    for op in ops {
        match op {
            DbOp::PutSpan {
                entity,
                admitted_at,
                payload,
            } => {
                transaction.execute(
                    "INSERT INTO spans (entity, admission_nano, payload)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(entity) DO UPDATE SET
                       admission_nano = excluded.admission_nano,
                       payload = excluded.payload",
                    params![
                        encode_entity(*entity),
                        i64::try_from(*admitted_at).unwrap_or(i64::MAX),
                        payload
                    ],
                )?;
            }
            DbOp::PutLog {
                entity,
                admitted_at,
                payload,
            } => {
                transaction.execute(
                    "INSERT INTO logs (entity, admission_nano, payload)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(entity) DO UPDATE SET
                       admission_nano = excluded.admission_nano,
                       payload = excluded.payload",
                    params![
                        encode_entity(*entity),
                        i64::try_from(*admitted_at).unwrap_or(i64::MAX),
                        payload
                    ],
                )?;
            }
            DbOp::PutPoint {
                entity,
                admitted_at,
                payload,
                stream_payload,
            } => {
                transaction.execute(
                    "INSERT INTO points (entity, admission_nano, payload, stream_payload)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(entity) DO UPDATE SET
                       admission_nano = excluded.admission_nano,
                       payload = excluded.payload,
                       stream_payload = excluded.stream_payload",
                    params![
                        encode_entity(*entity),
                        i64::try_from(*admitted_at).unwrap_or(i64::MAX),
                        payload,
                        stream_payload
                    ],
                )?;
            }
            DbOp::DeleteSpan { entity } => {
                transaction.execute(
                    "DELETE FROM spans WHERE entity = ?1",
                    params![encode_entity(*entity)],
                )?;
            }
            DbOp::DeleteLog { entity } => {
                transaction.execute(
                    "DELETE FROM logs WHERE entity = ?1",
                    params![encode_entity(*entity)],
                )?;
            }
            DbOp::DeletePoint { entity } => {
                transaction.execute(
                    "DELETE FROM points WHERE entity = ?1",
                    params![encode_entity(*entity)],
                )?;
            }
        }
    }
    transaction.commit()
}

/// The folder side of [`commit`]: folds the committed tail of the
/// write-ahead log into the single database file
/// (`PRAGMA wal_checkpoint(TRUNCATE)`), so a graceful close leaves "one
/// file to copy" ([ADR 0003](../../docs/decisions/0003-storage-strategy.md)).
/// `TRUNCATE` — not `PASSIVE` — so the file is actually stable afterwards,
/// including the WAL file itself.
pub(crate) fn checkpoint(connection: &mut Connection) -> Result<(), rusqlite::Error> {
    connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
        row.get::<_, i64>(0)
    })?;
    Ok(())
}

/// Every row the file holds, decoded but not yet shelved: residency is
/// rebuilt from nothing by the store (which owns the stream interning and
/// the ceilings). Decoding errors are the caller's to classify.
#[derive(Debug, Default)]
pub(crate) struct LoadedRows {
    pub spans: Vec<(EntityId, u64, runtime_trail_telemetry_model::Span)>,
    pub logs: Vec<(EntityId, u64, runtime_trail_telemetry_model::LogRecord)>,
    pub points: Vec<(
        EntityId,
        u64,
        runtime_trail_telemetry_model::MetricPoint,
        runtime_trail_telemetry_model::StreamIdentity,
    )>,
}

/// Why a rehydration read failed.
#[derive(Debug)]
pub(crate) enum LoadError {
    /// The file stopped reading like a database (an I/O-level failure, not
    /// a decode one).
    Sqlite(rusqlite::Error),
    /// A stored row does not decode: content this build cannot read.
    Decode { table: &'static str, detail: String },
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Sqlite(error) => write!(f, "reading the database failed: {error}"),
            LoadError::Decode { table, detail } => {
                write!(f, "a row in table {table} could not be decoded: {detail}")
            }
        }
    }
}

/// Reads every row of every table, in no particular order; the store
/// rebuilds the residency order from the admission keys.
pub(crate) fn load(connection: &mut Connection) -> Result<LoadedRows, LoadError> {
    let mut loaded = LoadedRows::default();

    {
        let mut statement = connection
            .prepare("SELECT entity, admission_nano, payload FROM spans")
            .map_err(LoadError::Sqlite)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(LoadError::Sqlite)?;
        for row in rows {
            let (entity, nano, payload) = row.map_err(LoadError::Sqlite)?;
            let entity = decode_entity(&entity, "spans")?;
            let span = serde_json::from_str(&payload).map_err(|error| LoadError::Decode {
                table: "spans",
                detail: error.to_string(),
            })?;
            loaded
                .spans
                .push((entity, u64::try_from(nano).unwrap_or(u64::MAX), span));
        }
    }

    {
        let mut statement = connection
            .prepare("SELECT entity, admission_nano, payload FROM logs")
            .map_err(LoadError::Sqlite)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(LoadError::Sqlite)?;
        for row in rows {
            let (entity, nano, payload) = row.map_err(LoadError::Sqlite)?;
            let entity = decode_entity(&entity, "logs")?;
            let record = serde_json::from_str(&payload).map_err(|error| LoadError::Decode {
                table: "logs",
                detail: error.to_string(),
            })?;
            loaded
                .logs
                .push((entity, u64::try_from(nano).unwrap_or(u64::MAX), record));
        }
    }

    {
        let mut statement = connection
            .prepare("SELECT entity, admission_nano, payload, stream_payload FROM points")
            .map_err(LoadError::Sqlite)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(LoadError::Sqlite)?;
        for row in rows {
            let (entity, nano, payload, stream_payload) = row.map_err(LoadError::Sqlite)?;
            let entity = decode_entity(&entity, "points")?;
            let point = serde_json::from_str(&payload).map_err(|error| LoadError::Decode {
                table: "points",
                detail: error.to_string(),
            })?;
            let stream =
                serde_json::from_str(&stream_payload).map_err(|error| LoadError::Decode {
                    table: "points",
                    detail: format!("stream_payload: {error}"),
                })?;
            loaded.points.push((
                entity,
                u64::try_from(nano).unwrap_or(u64::MAX),
                point,
                stream,
            ));
        }
    }

    Ok(loaded)
}

/// The entity id as stored: JSON, like every payload. An id that does not
/// decode is a corrupt row.
fn decode_entity(json: &str, table: &'static str) -> Result<EntityId, LoadError> {
    serde_json::from_str(json).map_err(|error| LoadError::Decode {
        table,
        detail: format!("entity: {error}"),
    })
}

fn encode_entity(entity: EntityId) -> String {
    // Entity ids are a Copy enum of fixed variants; JSON serialization
    // cannot fail.
    serde_json::to_string(&entity).expect("entity ids always serialize")
}
