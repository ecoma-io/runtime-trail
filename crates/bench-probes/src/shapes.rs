//! The concrete payload shapes the Phase 1 probes emit.
//!
//! Each shape is a *worst-case-legal* export: at the per-export point cap
//! (10,000), heavy attribute content in every entry, and a payload kept
//! under the 4 MiB wire ceiling — the heaviest export the contract admits,
//! which is what an overload probe has to send. The tuning is stated here
//! once, in one place, and asserted by the wire tests through the real
//! pipeline; the probes print the shapes they used next to the numbers
//! they measured, so a record explains its own workload.

use crate::wire::{GaugeShape, GeneratedSpan, LogShape, TraceContext, TraceShape};

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

/// The one trace the served-runtime probes drive and investigate: its
/// 16-byte trace id, fixed so the probes know their subject.
pub const INVESTIGATED_TRACE_ID: [u8; 16] = [0x5A; 16];

/// The same trace's root span id. The root is the only parentless span of
/// the workload, so it is the waterfall's effective root — the subject the
/// investigation surfaces.
pub const INVESTIGATED_ROOT_SPAN_ID: [u8; 8] = [0x01; 8];

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
        trace_context: None,
    }
}

/// One generated span of the served-runtime trace: span 0 of export 0 is
/// the root (parentless); every later span is its child, with an id
/// distinct per (export, position) — byte 0 (`0xAA`) keeps children from
/// ever colliding with the root, so no delivery collapses.
///
/// # Panics
/// Never: the span-id bytes are masked to 8 bits, so the `try_from`
/// conversions are infallible by construction.
#[must_use]
pub fn trace_span(export_index: u64, k: u64) -> GeneratedSpan {
    let (span_id, parent_span_id, name) = if export_index == 0 && k == 0 {
        (INVESTIGATED_ROOT_SPAN_ID, [0; 8], "bench.root".to_owned())
    } else {
        (
            [
                0xAA,
                u8::try_from((k >> 8) & 0xff).expect("span id bytes stay bounded"),
                u8::try_from(k & 0xff).expect("span id bytes stay bounded"),
                u8::try_from(export_index & 0xff).expect("span id bytes stay bounded"),
                0,
                0,
                0,
                0,
            ],
            INVESTIGATED_ROOT_SPAN_ID,
            format!("bench.span.{export_index:03}.{k:04}"),
        )
    };
    GeneratedSpan {
        trace_id: INVESTIGATED_TRACE_ID,
        span_id,
        parent_span_id,
        name,
        start_time_unix_nano: BASE_TIME_UNIX_NANO + k,
        end_time_unix_nano: BASE_TIME_UNIX_NANO + k + 1_000_000,
    }
}

/// One generated span's attributes: two, fixed-width, unique per (export,
/// position) so every span entry is distinct evidence.
#[must_use]
pub fn trace_span_attributes(export_index: u64, k: u64) -> Vec<(String, String)> {
    (0..2)
        .map(|slot| {
            (
                format!("bench.span.attr.{slot:02}"),
                pad_left_padded(
                    &format!("{export_index:06}.{k:06}.{slot:02}"),
                    LOG_VALUE_CHARS,
                ),
            )
        })
        .collect()
}

/// The served-runtime probes' trace shape: one trace of `span_count` spans
/// per export (the root first on export 0, the rest its children), each
/// span carrying two fixed-width attributes, under a small resource.
#[must_use]
pub fn trace_shape(span_count: usize) -> TraceShape {
    TraceShape {
        resource_attributes: std::iter::once((
            "service.name".to_owned(),
            "runtime-trail-bench-probes".to_owned(),
        ))
        .chain((0..2).map(|slot| {
            (
                format!("bench.resource.{slot:02}"),
                resource_attribute_value(slot),
            )
        }))
        .collect(),
        scope_name: "runtime-trail-bench-probes".to_owned(),
        span_count,
        span: Box::new(trace_span),
        span_attribute: Box::new(trace_span_attributes),
        base_time_unix_nano: BASE_TIME_UNIX_NANO,
    }
}

/// The workload probe's related-log shape: the retention-shaped plain
/// records, carrying the investigated trace's context (the log record's
/// `trace_id`/`span_id`), so the investigation surfaces them as
/// related-logs evidence — the trace linkage phase-one log records
/// deliberately lack.
#[must_use]
pub fn related_log_shape(context: TraceContext, record_count: usize) -> LogShape {
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
        record_count,
        body: Box::new(log_body),
        record_attribute: Box::new(log_record_attributes),
        base_time_unix_nano: BASE_TIME_UNIX_NANO,
        trace_context: Some(context),
    }
}

/// The workload probe's gauge shape: one stream with a handful of points
/// (a *typical* session, deliberately far from the overload probe's
/// at-cap load), timed inside the investigated trace's window so the
/// waterfall's surrounding-metrics evidence is real.
#[must_use]
pub fn workload_gauge_shape(point_count: usize, base_time_unix_nano: u64) -> GaugeShape {
    GaugeShape {
        resource_attributes: std::iter::once((
            "service.name".to_owned(),
            "runtime-trail-bench-probes".to_owned(),
        ))
        .chain((0..2).map(|slot| {
            (
                format!("bench.resource.{slot:02}"),
                resource_attribute_value(slot),
            )
        }))
        .collect(),
        scope_name: "runtime-trail-bench-probes".to_owned(),
        metric_name: Box::new(|_export_index| "bench.requests".to_owned()),
        point_count,
        base_time_unix_nano,
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

    #[test]
    fn the_served_runtime_trace_has_one_root_and_distinct_children() {
        let root = trace_span(0, 0);
        assert_eq!(root.parent_span_id, [0; 8], "the root is parentless");
        assert_eq!(root.trace_id, INVESTIGATED_TRACE_ID);
        assert_eq!(root.span_id, INVESTIGATED_ROOT_SPAN_ID);
        assert_eq!(root.name, "bench.root");

        let mut seen = std::collections::BTreeSet::new();
        for export in 0..12 {
            for k in 0..1_000 {
                let span = trace_span(export, k);
                if export == 0 && k == 0 {
                    continue; // the root, asserted above
                }
                assert_eq!(
                    span.parent_span_id, INVESTIGATED_ROOT_SPAN_ID,
                    "every other span is a child of the root"
                );
                assert_ne!(
                    span.span_id, INVESTIGATED_ROOT_SPAN_ID,
                    "no child reuses the root's id"
                );
                assert!(
                    seen.insert(span.span_id),
                    "span ids are distinct per (export, position): {span:?}"
                );
                assert!(span.start_time_unix_nano < span.end_time_unix_nano);
            }
        }
    }

    #[test]
    fn the_related_log_shape_carries_the_trace_context() {
        let context = TraceContext {
            trace_id: INVESTIGATED_TRACE_ID,
            span_id: INVESTIGATED_ROOT_SPAN_ID,
        };
        let logs = related_log_shape(context, 5);
        assert_eq!(logs.record_count, 5);
        assert_eq!(logs.trace_context, Some(context));
        assert_eq!(
            log_shape().trace_context,
            None,
            "phase-one logs stay context-free"
        );

        let gauge = workload_gauge_shape(16, BASE_TIME_UNIX_NANO + 100);
        assert_eq!(gauge.point_count, 16);
        assert_eq!(gauge.base_time_unix_nano, BASE_TIME_UNIX_NANO + 100);
    }
}
