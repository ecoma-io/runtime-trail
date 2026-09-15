//! The limits part of the [`Investigation`](crate::Investigation) envelope.
//!
//! Limits are the machine-checkable statement of what bounded the answer:
//! the caller's budget ceilings, what the FLOW actually spent (page and
//! entity totals, and an honest `stopped` marker when the flow's own chain
//! limits stopped it), the correlation strategy versions in effect (none
//! in M3 — reported, never invented), and the store's eviction state at
//! flow admission.

use std::time::Duration;

use crate::correlated::StrategyVersion;

/// The caller's budget ceilings, exactly as admitted. The flow slices these
/// into fresh per-page engine budgets; the engine stays the sole per-page
/// enforcer. Mirrors the query engine's budget contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BudgetLimits {
    /// The deadline dimension ceiling: per page.
    pub deadline: Duration,
    /// The results dimension ceiling: max records per page.
    pub max_results: u64,
    /// The bytes dimension ceiling: max accounted record bytes per page.
    pub max_bytes: u64,
    /// The scan dimension ceiling: max residency positions per page.
    pub max_scan: u64,
    /// The aggregation-memory dimension ceiling: max bookkeeping bytes per
    /// page.
    pub max_aggregation_memory: u64,
}

/// On what chain-level basis the flow stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChainBasis {
    /// The flow stopped at its total-entities chain limit.
    TotalEntities,
    /// The flow stopped at its total-pages chain limit.
    TotalPages,
}

/// The flow's chain-level accounting: what the flow itself bounded and
/// what it spent. The engine never sees these — it only ever enforces the
/// per-page budgets sliced from [`BudgetLimits`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainLimits {
    /// The flow's own ceiling on total evidence entities across all parts.
    pub max_total_entities: u64,
    /// The flow's own ceiling on total engine pages across all parts.
    pub max_total_pages: u64,
    /// Evidence entities the flow selected, across all parts.
    pub total_entities: u64,
    /// Engine pages the flow performed, across all parts.
    pub total_pages: u64,
    /// Residency positions the flow examined naming record identities
    /// (entity-id recovery walks), across all parts.
    pub identity_examinations: u64,
    /// The basis on which the flow stopped, when it stopped on its own
    /// chain limits rather than completing. Everything before the stop is
    /// reported honestly; nothing after is claimed.
    pub stopped: Option<ChainBasis>,
}

/// The store's eviction state at flow admission: residency and eviction
/// totals the runtime reported. The envelope reports the state it saw,
/// never a projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvictionState {
    /// Resident records the store held at flow admission.
    pub resident_records: u64,
    /// Total evictions the store had performed up to flow admission.
    pub total_evictions: u64,
}

/// The envelope's limits part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The caller's budget ceilings, verbatim.
    pub budget: BudgetLimits,
    /// The flow's own chain-level limits and spend.
    pub chain: ChainLimits,
    /// The correlation strategy versions the answer's correlated part was
    /// produced under. Empty in M3 (no strategy implemented) — reported.
    pub strategy_versions: Vec<StrategyVersion>,
    /// The store's eviction state at flow admission.
    pub eviction: EvictionState,
}

impl BudgetLimits {
    /// The five budget ceilings, mirroring the engine's per-page contract.
    #[must_use]
    pub const fn new(
        deadline: Duration,
        max_results: u64,
        max_bytes: u64,
        max_scan: u64,
        max_aggregation_memory: u64,
    ) -> Self {
        Self {
            deadline,
            max_results,
            max_bytes,
            max_scan,
            max_aggregation_memory,
        }
    }
}

impl ChainLimits {
    /// The flow's chain-level ceilings and spend.
    #[must_use]
    pub const fn new(
        max_total_entities: u64,
        max_total_pages: u64,
        total_entities: u64,
        total_pages: u64,
        identity_examinations: u64,
        stopped: Option<ChainBasis>,
    ) -> Self {
        Self {
            max_total_entities,
            max_total_pages,
            total_entities,
            total_pages,
            identity_examinations,
            stopped,
        }
    }
}

impl Limits {
    /// Builds the limits part.
    #[must_use]
    pub const fn new(
        budget: BudgetLimits,
        chain: ChainLimits,
        strategy_versions: Vec<StrategyVersion>,
        eviction: EvictionState,
    ) -> Self {
        Self {
            budget,
            chain,
            strategy_versions,
            eviction,
        }
    }
}

impl EvictionState {
    /// The store's residency and eviction state.
    #[must_use]
    pub const fn new(resident_records: u64, total_evictions: u64) -> Self {
        Self {
            resident_records,
            total_evictions,
        }
    }
}
