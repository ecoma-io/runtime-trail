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
    /// Distinct streams with at least one resident point — the number the
    /// series cap bounds
    /// ([runtime-constraints.md](../../docs/architecture/runtime-constraints.md)).
    /// A stream enters the count when its first point enters residency and
    /// leaves it when the last point is evicted; the eviction hook reports
    /// the exit (`EvictionHook::stream_released`).
    pub resident_streams: u64,
    /// Accounted bytes currently resident, by the model's single accounted
    /// size definition: the resident records' accounted sizes **plus** each
    /// distinct resident stream's identity accounted size, charged exactly
    /// once per stream. This is the number the accounted-byte ceiling
    /// bounds — a session of single-point streams cannot park identity
    /// content under a ceiling that only saw the points.
    pub accounted_bytes: u64,
    /// The records' share of [`StoreStats::accounted_bytes`]: per-record
    /// `accounted_size` summed over every shelf.
    pub record_accounted_bytes: u64,
    /// The identities' share of [`StoreStats::accounted_bytes`]: each
    /// distinct resident stream's identity accounted size, charged once
    /// ([telemetry-model.md](../../docs/architecture/telemetry-model.md)
    /// owns the formula). Zero when no metric point is resident.
    pub identity_accounted_bytes: u64,
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
    /// Keeps refused because the point would have established a new
    /// distinct stream while the store already held the series cap's worth
    /// (`KeepOutcome::SeriesCapReached`). Nothing was evicted to make room.
    pub kept_out_series_cap: u64,
    /// How many completed [`EvictionHook`](crate::EvictionHook) deliveries
    /// the store has made for evicted records. The store's own removal —
    /// indexes, shelves, stream table, counters — completes **before** the
    /// hook fires and is counted in
    /// [`StoreStats::total_evictions`](crate::StoreStats::total_evictions)
    /// regardless; the hook is where identity ends (ADR 0008), so a
    /// divergence between `total_evictions()` and `hook_deliveries` (with a
    /// hook wired) means identity outlived residency — a misbehaving hook,
    /// named by the counters instead of silently absorbed. With no hook
    /// wired, deliveries stay zero.
    pub hook_deliveries: u64,
    /// The most recent admission-anomaly total the composition root pushed
    /// through [`TelemetryStore::observe_admission_anomalies`] — the
    /// pass-through that surfaces identity conflicts recorded by admission
    /// without storage naming the ledger.
    pub admission_anomalies: u64,
}

impl StoreStats {
    /// Every eviction this store has performed, all causes together — the
    /// number the store's own removal work produces. Compare against
    /// [`StoreStats::hook_deliveries`] (with a hook wired) to see whether
    /// identity ended wherever residency did.
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
        assert_eq!(stats.resident_streams, 0);
        assert_eq!(stats.hook_deliveries, 0);
        assert_eq!(stats.kept_out_series_cap, 0);
        assert_eq!(stats.total_evictions(), 0);
        let history = StoreStats {
            evicted_for_record_ceiling: 2,
            evicted_for_accounted_bytes_ceiling: 3,
            evicted_for_admission_window: 4,
            ..stats
        };
        assert_eq!(history.total_evictions(), 9);
        assert_eq!(
            history.accounted_bytes,
            history.record_accounted_bytes + history.identity_accounted_bytes,
            "the ceiling's total is exactly the records' and the identities' shares"
        );
    }
}
