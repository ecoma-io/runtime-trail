#![allow(dead_code)]
// The memory driver's half of the shared behavioral suite: the fixtures
// come from `crates/storage/tests/shared/`, and this module supplies the
// driver seam the suite is written against — the `Config` alias and the
// `boxed` constructor. The suite itself compiles once and runs against
// both drivers (`contract.rs`, `retention.rs`, `series.rs` in this
// crate's `tests/`), so parity is by construction.

include!("../../../storage/tests/shared/fixtures.rs");

use runtime_trail_storage::TelemetryStore;
use runtime_trail_storage_memory::InMemoryStore;
pub use runtime_trail_storage_memory::MemoryConfig as Config;

/// A store over the trait object, the shape the composition root holds.
#[must_use]
pub fn boxed(config: Config, hook: Option<Box<dyn EvictionHook>>) -> Box<dyn TelemetryStore> {
    Box::new(InMemoryStore::new(config, hook))
}

/// The mode name this driver reports.
pub const DRIVER_NAME: &str = InMemoryStore::NAME;
