//! Observability: what a store can be asked about itself.
//!
//! Eviction is observable where it happens (dropped-record counters) and —
//! once query lands — downstream, because query responses report gaps
//! (`docs/architecture/query-model.md`). This module is the "where it
//! happens" half: the counters a store keeps as it keeps and evicts, and
//! the admission-anomaly pass-through the composition root feeds.

/// A snapshot of a store's residency and its retention history.
///
/// Every number is a count or an accounted byte total; nothing here is a
/// measurement with a machine attached (that is `docs/benchmarks/README.md`
/// territory, never a store's claim). Eviction counters are cumulative over
/// the store's lifetime; residency fields are the current state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// Records currently resident, every kind together — the number the
    /// resident-records ceiling bounds.
    pub resident_records: u64,
    /// Resident spans.
    pub resident_spans: u64,
    /// Resident log records.
    pub resident_log_records: u64,
    /// Resident metric points.
    pub resident_metric_points: u64,
    /// Accounted bytes currently resident, by the model's single accounted
    /// size definition (per-record `accounted_size` summed). This is the
    /// number the accounted-byte ceiling bounds. A metric point's interned
    /// stream identity is shared through one allocation (ADR 0008's
    /// interning), so it is residency overhead outside the accounted
    /// ceilings — the same treatment ADR 0008 gives ledger entries — and
    /// it is not counted per point.
    pub accounted_bytes: u64,
    /// Records evicted because the resident-records ceiling was exceeded.
    pub evicted_for_record_ceiling: u64,
    /// Records evicted because the accounted-byte ceiling was exceeded.
    pub evicted_for_accounted_bytes_ceiling: u64,
    /// Records evicted because their admission time fell behind the
    /// configured admission window at the reference reading.
    pub evicted_for_admission_window: u64,
    /// Keeps refused because the record alone exceeded the accounted-byte
    /// ceiling — nothing was evicted to try to make room.
    pub oversized_refusals: u64,
    /// Keeps refused because the entity id was already resident: the
    /// resident record stood (admitted data is immutable), nothing was
    /// rewritten, and the attempt was counted here.
    pub duplicate_keeps: u64,
    /// The most recent admission-anomaly total the composition root pushed
    /// through [`TelemetryStore::observe_admission_anomalies`] — the
    /// pass-through that surfaces identity conflicts recorded by admission
    /// without storage naming the ledger.
    pub admission_anomalies: u64,
}

impl StoreStats {
    /// Every eviction this store has performed, all causes together.
    #[must_use]
    pub const fn total_evictions(&self) -> u64 {
        self.evicted_for_record_ceiling
            + self.evicted_for_accounted_bytes_ceiling
            + self.evicted_for_admission_window
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The counters are a plain report: zeroed by default, sums derived,
    /// nothing hidden behind interpretation.
    #[test]
    fn the_report_is_zeroed_and_sums_its_evictions() {
        let stats = StoreStats::default();
        assert_eq!(stats.resident_records, 0);
        assert_eq!(stats.accounted_bytes, 0);
        assert_eq!(stats.total_evictions(), 0);
        let history = StoreStats {
            evicted_for_record_ceiling: 2,
            evicted_for_accounted_bytes_ceiling: 3,
            evicted_for_admission_window: 4,
            ..stats
        };
        assert_eq!(history.total_evictions(), 9);
    }
}
