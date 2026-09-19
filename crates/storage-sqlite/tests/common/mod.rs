#![allow(dead_code)]
// The file-backed driver's half of the shared behavioral suite: the
// fixtures come from `crates/storage/tests/shared/`, and this module
// supplies the driver seam the suite is written against — the `Config`
// alias and the `boxed` constructor. The suite compiles once and runs
// against both drivers (`contract.rs`, `retention.rs`, `series.rs` in
// this crate's `tests/`), so parity is by construction, not by
// translation.

include!("../../../storage/tests/shared/fixtures.rs");

use runtime_trail_storage::TelemetryStore;
pub use runtime_trail_storage_sqlite::FileBackedConfig as Config;
use runtime_trail_storage_sqlite::FileBackedStore;

/// A store handle that also owns its backing file's directory: the
/// file-backed mode's identity is its file, so the directory must live
/// exactly as long as the handle. The store field is declared first, so
/// the store (which checkpoints on drop) closes before its directory is
/// taken down.
pub struct BoxedStore {
    store: Box<dyn TelemetryStore>,
    _dir: tempfile::TempDir,
}

impl std::ops::Deref for BoxedStore {
    type Target = dyn TelemetryStore;

    fn deref(&self) -> &Self::Target {
        self.store.as_ref()
    }
}

impl std::ops::DerefMut for BoxedStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.store.as_mut()
    }
}

impl AsRef<dyn TelemetryStore> for BoxedStore {
    fn as_ref(&self) -> &(dyn TelemetryStore + 'static) {
        self.store.as_ref()
    }
}

/// A store over the trait object, the shape the composition root holds,
/// in a freshly minted temp directory of its own: a test that reopens the
/// store opens its own path, and these parity tests only ever use a
/// single session. Returns the owner [`BoxedStore`], deref'd to
/// `TelemetryStore` by method resolution and by the suite's explicit
/// trait calls.
#[must_use]
pub fn boxed(config: Config, hook: Option<Box<dyn EvictionHook>>) -> BoxedStore {
    let dir = tempfile::tempdir().expect("a test tempdir");
    let store = FileBackedStore::open(dir.path().join("store.db"), config, hook)
        .expect("a test store opens");
    BoxedStore {
        store: Box::new(store),
        _dir: dir,
    }
}

/// The mode name this driver reports.
pub const DRIVER_NAME: &str = FileBackedStore::NAME;
