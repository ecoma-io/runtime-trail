//! Memory mode's startup configuration.
//!
//! Retention ceilings are startup configuration, never mid-session state
//! (`docs/architecture/storage-model.md`): a [`MemoryConfig`] is built once
//! and the store is immutable about it afterwards — there is no setter, no
//! grow, no mode change. [`MemoryConfig::default`] is the contract numbers,
//! not a guess.

use std::time::Duration;

/// The default resident-records ceiling: 2,000,000 records.
///
/// `docs/architecture/runtime-constraints.md`, "Memory-mode retention
/// ceilings". Changing a default is an architecture change — the PR states
/// its effect on that table.
pub const DEFAULT_MAX_RECORDS: u64 = 2_000_000;

/// The default accounted-byte ceiling: 256 MiB (2^20-byte MiB, as that
/// document counts).
pub const DEFAULT_MAX_ACCOUNTED_BYTES: u64 = 256 * 1024 * 1024;

/// The default admission window: 24 hours.
pub const DEFAULT_ADMISSION_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// The default series cap: 100,000 distinct resident streams.
///
/// `docs/architecture/runtime-constraints.md`, "Series cap (active)". Like
/// the ceilings, it is startup configuration: the store refuses the
/// establishing keep by name, and a slot frees when a stream's last point
/// is evicted.
pub const DEFAULT_SERIES_CAP: u64 = 100_000;

/// The retention ceilings and window of one in-memory store, fixed at
/// startup.
///
/// The three ceilings are checked together after every keep and on every
/// retention pass: whichever is hit first drives eviction, and eviction
/// continues until all three are satisfied ("first ceiling hit wins" —
/// `docs/architecture/storage-model.md`). The series cap is the one limit
/// that never evicts: it refuses the establishing keep instead. A zero
/// ceiling is legal configuration: nothing can then stay resident, and
/// every keep is observably refused or immediately evicted — never
/// silently kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryConfig {
    /// The maximum number of records resident at once, every kind
    /// together.
    pub max_records: u64,
    /// The maximum accounted bytes resident at once: every resident
    /// record's model-accounted size, plus each distinct resident stream's
    /// identity accounted size charged exactly once
    /// (`docs/architecture/storage-model.md`).
    pub max_accounted_bytes: u64,
    /// How long a record stays resident from its admission time. "Oldest"
    /// and window are both admission-time facts, never emitter event time.
    pub admission_window: Duration,
    /// The maximum number of distinct streams that may have resident
    /// points at once. A keep establishing a stream beyond the cap is
    /// refused (`KeepOutcome::SeriesCapReached`); the slot frees when the
    /// stream's last point is evicted. A zero cap is legal: no point can
    /// then establish a stream at all.
    pub series_cap: u64,
}

impl Default for MemoryConfig {
    /// The memory-mode retention ceilings of
    /// `docs/architecture/runtime-constraints.md`: 2,000,000 records,
    /// 256 MiB accounted, a 24 h window, a 100,000 series cap.
    fn default() -> Self {
        Self {
            max_records: DEFAULT_MAX_RECORDS,
            max_accounted_bytes: DEFAULT_MAX_ACCOUNTED_BYTES,
            admission_window: DEFAULT_ADMISSION_WINDOW,
            series_cap: DEFAULT_SERIES_CAP,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defaults are the contract numbers: a driver silently drifting
    /// from `docs/architecture/runtime-constraints.md` would be changing
    /// the architecture without the table changing first.
    #[test]
    fn the_default_ceilings_are_the_contract_numbers() {
        let config = MemoryConfig::default();
        assert_eq!(config.max_records, 2_000_000);
        assert_eq!(config.max_accounted_bytes, 256 * 1024 * 1024);
        assert_eq!(config.admission_window, Duration::from_secs(24 * 60 * 60));
        assert_eq!(config.series_cap, 100_000);
    }
}
