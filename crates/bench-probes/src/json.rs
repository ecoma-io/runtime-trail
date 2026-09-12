//! The probes' JSON records, written by hand.
//!
//! A probe's whole product is one machine-readable record on stdout, which
//! the bench harness captures verbatim beside the commit and machine that
//! produced it (`scripts/bench/run-all.sh`,
//! [benchmarks/README.md](../../docs/benchmarks/README.md)). The records
//! carry integers, a couple of floats and short fixed strings, so they are
//! emitted directly — no serialisation framework, no reflection, nothing
//! between the number the probe measured and the text the harness files.
//! Every field name says its unit (`_kib`) or its source (`accounted`
//! bytes are the model's accounting, `vm_*` are the kernel's), because a
//! record that needs a decoder ring to be read honestly is a record that
//! will be misread.

use std::fmt;
use std::fmt::Write as _;

/// One JSON value a record field can carry.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue {
    /// An unsigned integer (counts, byte totals, KiB readings).
    U64(u64),
    /// A float (durations, in seconds). Written with three decimals;
    /// NaN and infinity never reach a record — a probe's clock readings
    /// are finite or the probe has failed.
    F64(f64),
    /// A short string.
    Text(String),
    /// A flag (whether a probe phase completed under its own power).
    Flag(bool),
    /// A list of unsigned readings (the RSS samples over a run).
    U64List(Vec<u64>),
}

/// An ordered JSON object: the record format. Field order is insertion
/// order and stable, so two runs of one probe diff line by line.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JsonRecord {
    fields: Vec<(&'static str, JsonValue)>,
}

impl JsonRecord {
    /// An empty record.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a field. Duplicate keys would be a schema bug, so the second
    /// one overwrites the first in place — same key, one fact.
    pub fn field(&mut self, key: &'static str, value: JsonValue) -> &mut Self {
        if let Some(slot) = self.fields.iter_mut().find(|(name, _)| *name == key) {
            slot.1 = value;
        } else {
            self.fields.push((key, value));
        }
        self
    }

    /// Every field name, in record order — how a schema assertion checks
    /// the record without parsing it.
    pub fn field_names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.fields.iter().map(|(name, _)| *name)
    }

    /// Renders the record as one compact JSON line — the exact text the
    /// harness captures.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::from("{");
        for (index, (name, value)) in self.fields.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            write_json_string(&mut out, name);
            out.push(':');
            value.write_json(&mut out);
        }
        out.push('}');
        out
    }
}

impl fmt::Display for JsonRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

impl JsonValue {
    fn write_json(&self, out: &mut String) {
        match self {
            Self::U64(value) => {
                out.push_str(&value.to_string());
            }
            Self::F64(value) => {
                // A probe's own timings are finite by construction; a
                // non-finite value would be a bug, and `NaN` is not JSON —
                // write the failure instead of pretending it measured.
                if value.is_finite() {
                    let _ = write!(out, "{value:.3}");
                } else {
                    out.push_str("null");
                }
            }
            Self::Text(text) => write_json_string(out, text),
            Self::Flag(value) => {
                out.push_str(if *value { "true" } else { "false" });
            }
            Self::U64List(values) => {
                out.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&value.to_string());
                }
                out.push(']');
            }
        }
    }
}

/// Writes a JSON string literal with the two-character escapes the
/// probe-generated strings can actually contain (everything else is
/// plain ASCII names and numbers).
fn write_json_string(out: &mut String, text: &str) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            control if control.is_control() => {
                let _ = write!(out, "\\u{:04x}", u32::from(control));
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probes::{OverloadReport, PumpTally, RetentionReport};
    use crate::rss::RssSample;
    use runtime_trail_storage::StoreStats;
    use runtime_trail_storage_memory::MemoryConfig;

    /// A zeroed overload report: the schema tests check the record
    /// builders' shape, never a machine run.
    fn sample_overload_report() -> OverloadReport {
        OverloadReport {
            queue_ceiling_bytes: 64 * 1024 * 1024,
            store_config: MemoryConfig::default(),
            exports_ingested: 0,
            queue_saturated_exports: 0,
            payload_bytes: 0,
            admitted_records: 0,
            collapsed_records: 0,
            conflict_records: 0,
            rejected_records: 0,
            pump: PumpTally::default(),
            store_stats: StoreStats::default(),
            queue_accounted_bytes: 0,
            queue_records: 0,
            anomalies: 0,
            final_sample: RssSample {
                vm_rss_kib: 0,
                vm_hwm_kib: 0,
            },
            rss_samples_kib: Vec::new(),
            iterations: 0,
            completed_within_budget: true,
            saturation_persisted: false,
            wall_seconds: 0.0,
        }
    }

    /// A zeroed retention report, same law.
    fn sample_retention_report() -> RetentionReport {
        RetentionReport {
            store_config: MemoryConfig::default(),
            queue_ceiling_bytes: 64 * 1024 * 1024,
            exports_ingested: 0,
            admitted_records: 0,
            pump: PumpTally::default(),
            store_stats: StoreStats::default(),
            plateau_vm_rss_kib: Vec::new(),
            plateau_spread_kib: 0,
            final_sample: RssSample {
                vm_rss_kib: 0,
                vm_hwm_kib: 0,
            },
            iterations: 0,
            completed_within_budget: true,
            wall_seconds: 0.0,
        }
    }

    #[test]
    fn renders_a_stable_compact_object() {
        let mut record = JsonRecord::new();
        record
            .field("probe", JsonValue::Text("probe-idle".to_owned()))
            .field("vm_rss_kib", JsonValue::U64(4812))
            .field("settled", JsonValue::Flag(true))
            .field("elapsed_s", JsonValue::F64(1.25));
        assert_eq!(
            record.render(),
            "{\"probe\":\"probe-idle\",\"vm_rss_kib\":4812,\"settled\":true,\"elapsed_s\":1.250}"
        );
    }

    #[test]
    fn lists_and_escapes_render_as_json() {
        let mut record = JsonRecord::new();
        record
            .field("samples_kib", JsonValue::U64List(vec![1, 2, 3]))
            .field(
                "note",
                JsonValue::Text("quote \" backslash \\ newline\n".to_owned()),
            );
        assert_eq!(
            record.render(),
            "{\"samples_kib\":[1,2,3],\"note\":\"quote \\\" backslash \\\\ newline\\n\"}"
        );
    }

    #[test]
    fn a_repeated_field_keeps_one_fact() {
        let mut record = JsonRecord::new();
        record
            .field("vm_rss_kib", JsonValue::U64(1))
            .field("vm_rss_kib", JsonValue::U64(2));
        assert_eq!(record.render(), "{\"vm_rss_kib\":2}");
        assert_eq!(record.field_names().count(), 1);
    }

    /// The overload record's field contract, checked as data: every field
    /// the harness consumers compute margins from is present. The numbers
    /// here are the probe's own fixed-point values, not a machine run.
    #[test]
    fn the_overload_record_carries_its_schema() {
        let record = crate::probes::overload_record(&sample_overload_report());
        let rendered = record.render();
        assert!(
            rendered.starts_with("{\"probe\":\"probe-ingest-overload\",")
                && rendered.ends_with('}'),
            "the record renders as one compact object naming its probe: {rendered}"
        );
        let names: Vec<&str> = record.field_names().collect();
        for required in [
            "probe",
            "queue_ceiling_bytes",
            "store_max_accounted_bytes",
            "store_max_records",
            "exports_ingested",
            "queue_saturated_exports",
            "admitted_records",
            "records_kept",
            "resident_records",
            "resident_streams",
            "accounted_bytes",
            "record_accounted_bytes",
            "identity_accounted_bytes",
            "queue_accounted_bytes",
            "resident_accounted_bytes",
            "vm_rss_kib",
            "vm_hwm_kib",
            "rss_samples_kib",
            "anomaly_conflicts",
            "iterations",
        ] {
            assert!(
                names.contains(&required),
                "the overload record lost {required}: {names:?}"
            );
        }
    }

    /// The retention record's field contract, same law.
    #[test]
    fn the_retention_record_carries_its_schema() {
        let record = crate::probes::retention_record(&sample_retention_report());
        let names: Vec<&str> = record.field_names().collect();
        for required in [
            "probe",
            "store_max_accounted_bytes",
            "store_max_records",
            "plateau_vm_rss_kib",
            "plateau_spread_kib",
            "evicted_total",
            "evicted_record_ceiling",
            "evicted_accounted_bytes_ceiling",
            "resident_records",
            "accounted_bytes",
            "iterations",
        ] {
            assert!(
                names.contains(&required),
                "the retention record lost {required}: {names:?}"
            );
        }
    }
}
