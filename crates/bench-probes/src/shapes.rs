//! The concrete payload shapes the Phase 1 probes emit.
//!
//! Each shape is a *worst-case-legal* export: at the per-export point cap
//! (10,000), heavy attribute content in every entry, and a payload kept
//! under the 4 MiB wire ceiling — the heaviest export the contract admits,
//! which is what an overload probe has to send. The tuning is stated here
//! once, in one place, and asserted by the wire tests through the real
//! pipeline; the probes print the shapes they used next to the numbers
//! they measured, so a record explains its own workload.

use crate::wire::{GaugeShape, LogShape};

/// Generated attribute values are fixed-width, so one export's payload
/// size is stable across exports — a measurement invariant: if two
/// exports' payloads differed in size for no workload reason, the RSS
/// samples would move with the encoder, not with the runtime.
pub const GAUGE_VALUE_CHARS: usize = 90;

/// Fixed width of a generated log record's attribute values.
pub const LOG_VALUE_CHARS: usize = 32;

/// Fixed width of a generated log record's body.
pub const LOG_BODY_CHARS: usize = 120;

/// Fixed width of a generated resource attribute's value.
pub const RESOURCE_VALUE_CHARS: usize = 40;

/// A fixed base timestamp (2023-11-15T00:00:00Z, in nanoseconds) the
/// generated points and records count up from — constant across runs, so
/// payloads are a function of the export index alone.
pub const BASE_TIME_UNIX_NANO: u64 = 1_700_000_000 * 1_000_000_000;

/// A resource attribute value: fixed-width, unique per slot.
#[must_use]
pub fn resource_attribute_value(slot: usize) -> String {
    pad_left_padded(&format!("r{slot:02}"), RESOURCE_VALUE_CHARS)
}

/// One generated gauge point's attributes: three, fixed-width, unique per
/// (export, point) pair — every point is its own admission identity.
#[must_use]
pub fn gauge_point_attributes(export_index: u64, point_index: u64) -> Vec<(String, String)> {
    (0..3)
        .map(|slot| {
            (
                format!("bench.attr.{slot:02}"),
                pad_left_padded(
                    &format!("v{export_index:06}.{point_index:06}.{slot:02}"),
                    GAUGE_VALUE_CHARS,
                ),
            )
        })
        .collect()
}

/// One generated log record's attributes: four, fixed-width, unique per
/// record.
#[must_use]
pub fn log_record_attributes(export_index: u64, record_index: u64) -> Vec<(String, String)> {
    (0..4)
        .map(|slot| {
            (
                format!("bench.log.{slot:02}"),
                pad_left_padded(
                    &format!("{export_index:06}.{record_index:06}.{slot:02}"),
                    LOG_VALUE_CHARS,
                ),
            )
        })
        .collect()
}

/// A generated log body: fixed-width, unique per record.
#[must_use]
pub fn log_body(export_index: u64, record_index: u64) -> String {
    pad_left_padded(
        &format!("log {export_index:06}.{record_index:06}"),
        LOG_BODY_CHARS,
    )
}

/// Pads `stem` on the right with `.` to exactly `width` characters.
fn pad_left_padded(stem: &str, width: usize) -> String {
    format!("{stem:.<width$}")
}

/// The overload probe's gauge shape: the heaviest legal per-export load —
/// at-cap points, attribute-heavy resource, one stream per export.
#[must_use]
pub fn gauge_shape() -> GaugeShape {
    GaugeShape {
        resource_attributes: std::iter::once((
            "service.name".to_owned(),
            "runtime-trail-bench-probes".to_owned(),
        ))
        .chain((0..8).map(|slot| {
            (
                format!("bench.resource.{slot:02}"),
                resource_attribute_value(slot),
            )
        }))
        .collect(),
        scope_name: "runtime-trail-bench-probes".to_owned(),
        metric_name: Box::new(|export_index| format!("bench.gauge.{export_index:06}")),
        point_count: crate::wire::POINTS_PER_EXPORT_CAP,
        base_time_unix_nano: BASE_TIME_UNIX_NANO,
    }
}

/// The retention probe's log shape: plain records whose residency is
/// bounded by the retention ceilings alone (log records leave no ledger
/// entry — see the probe's own docs for why that matters to the
/// measurement).
#[must_use]
pub fn log_shape() -> LogShape {
    LogShape {
        resource_attributes: std::iter::once((
            "service.name".to_owned(),
            "runtime-trail-bench-probes".to_owned(),
        ))
        .chain((0..3).map(|slot| {
            (
                format!("bench.resource.{slot:02}"),
                resource_attribute_value(slot),
            )
        }))
        .collect(),
        scope_name: "runtime-trail-bench-probes".to_owned(),
        severity_number: 9, // INFO
        severity_text: "INFO".to_owned(),
        record_count: 4_000,
        body: Box::new(log_body),
        record_attribute: Box::new(log_record_attributes),
        base_time_unix_nano: BASE_TIME_UNIX_NANO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_strings_are_fixed_width() {
        assert_eq!(resource_attribute_value(3).len(), RESOURCE_VALUE_CHARS);
        let (_, value) = &gauge_point_attributes(1, 2)[0];
        assert_eq!(value.len(), GAUGE_VALUE_CHARS);
        let (_, log_value) = &log_record_attributes(1, 2)[0];
        assert_eq!(log_value.len(), LOG_VALUE_CHARS);
        assert_eq!(log_body(1, 2).len(), LOG_BODY_CHARS);
    }

    #[test]
    fn generated_strings_are_unique_per_record_and_per_export() {
        let a = gauge_point_attributes(0, 1);
        let b = gauge_point_attributes(0, 2);
        let c = gauge_point_attributes(1, 1);
        assert_ne!(a, b, "two points of one export differ");
        assert_ne!(a, c, "one point across two exports differs");

        let log_a = log_record_attributes(0, 1);
        let log_b = log_record_attributes(0, 2);
        assert_ne!(log_a, log_b);
        assert_ne!(log_body(0, 1), log_body(0, 2));
    }

    #[test]
    fn the_probe_shapes_are_the_documented_load() {
        let gauge = gauge_shape();
        assert_eq!(gauge.point_count, crate::wire::POINTS_PER_EXPORT_CAP);
        let logs = log_shape();
        assert_eq!(logs.record_count, 4_000);
        assert!(
            logs.resource_attributes.len() <= 256,
            "under the per-signal cap"
        );
        assert!(
            gauge.resource_attributes.len() <= 256,
            "under the per-signal cap"
        );
    }
}
