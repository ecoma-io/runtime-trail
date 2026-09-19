//! Envelope rendering: the MCP surface renders the investigation envelope
//! to JSON exactly as the HTTP surface does
//! (`crates/server/src/investigation_http.rs`), so both transports answer
//! with the same field paths, the same encodings and the same absences.
//!
//! This module is a deliberate line-by-line mirror of the HTTP surface's
//! render functions: every function, every field, every encoding here has
//! its counterpart there. Nothing is added, dropped or re-shaped — the
//! parity test pins the mirror by asserting the same field paths on the
//! rendered JSON that the HTTP tests assert on theirs, and both surfaces
//! render the same committed envelope types.
//!
//! Encoding laws (shared with the HTTP surface):
//! - entity ids render as `{"span": {"trace_id", "span_id"}} | {"assigned": n}`;
//! - ids and bytes render as lowercase hex;
//! - `Option::None` renders as JSON `null` (present-but-empty, never
//!   omitted); structured model values render verbatim — arrays and
//!   key-value lists are never flattened or nulled (evidence fidelity,
//!   issues #39/#42).

use runtime_trail_investigation::correlated::{EvidenceFact, Relation, StrategyVersion};
use runtime_trail_investigation::evidence::{
    Evidence, LogEvidence, PointEvidence, SignalKind, SignalRef, SpanEvidence,
};
use runtime_trail_investigation::execution::{
    CorrelationStop, CoverageEntry, Dimension, FlowCoverageEntry, Magnitude, Outcome, PartName,
    Refusal, RunFacts, RunGroup, TimeWindow, Truncation, TruncationPoint,
};
use runtime_trail_investigation::limits::ChainBasis;
use runtime_trail_investigation::subject::{EffectiveRoot, ResolutionNote, Subject};
use runtime_trail_investigation::telemetry_model::{
    EntityId, LogRecord, MetricNumber, MetricPoint, Span, SpanId, StreamIdentity, TraceId, Value,
};
use runtime_trail_investigation::{Execution, Investigation, Limits};
use serde_json::{Value as JsonValue, json};
/// Renders the investigation envelope as JSON, field-for-field with the
/// HTTP surface's `render_investigation`.
#[must_use]
pub fn render_investigation(investigation: &Investigation) -> JsonValue {
    json!({
        "subject": render_subject(&investigation.subject),
        "execution": render_execution(&investigation.execution),
        "correlated": render_correlated(&investigation.correlated),
        "evidence": render_evidence(&investigation.evidence),
        "limits": render_limits(&investigation.limits),
    })
}

fn render_subject(subject: &Subject) -> JsonValue {
    json!({
        "requested": { "root_span": render_entity(&subject.requested.root_span) },
        "effective": {
            "root": render_effective_root(&subject.effective.root),
            "notes": subject.effective.notes.iter().map(render_note).collect::<Vec<_>>(),
        },
    })
}

fn render_effective_root(root: &EffectiveRoot) -> JsonValue {
    json!({
        "entity": render_entity(&root.entity),
        "name": root.name,
        "trace_id": render_trace_id(root.trace_id),
        "span_id": render_span_id(root.span_id),
    })
}

fn render_note(note: &ResolutionNote) -> JsonValue {
    match note {
        ResolutionNote::RequestedSpanIsRoot => {
            json!({ "kind": "requested_span_is_root" })
        }
        ResolutionNote::TraceRootedAt {
            entity,
            name,
            span_id,
        } => json!({
            "kind": "trace_rooted_at",
            "entity": render_entity(entity),
            "name": name,
            "span_id": render_span_id(*span_id),
        }),
        ResolutionNote::NoParentlessSpanResident => {
            json!({ "kind": "no_parentless_span_resident" })
        }
        ResolutionNote::RootHasNoValidTraceIdentity => {
            json!({ "kind": "root_has_no_valid_trace_identity" })
        }
    }
}

fn render_execution(execution: &Execution) -> JsonValue {
    json!({
        "run_groups": execution.run_groups.iter().map(render_run_group).collect::<Vec<_>>(),
        "flow_coverage": execution.flow_coverage.iter().map(render_flow_coverage).collect::<Vec<_>>(),
    })
}

fn render_run_group(group: &RunGroup) -> JsonValue {
    json!({
        "part": render_part(&group.part),
        "runs": group.runs.iter().map(render_run).collect::<Vec<_>>(),
    })
}

fn render_run(run: &RunFacts) -> JsonValue {
    json!({
        "outcome": render_outcome(&run.outcome),
        "coverage": run.coverage.iter().map(render_coverage).collect::<Vec<_>>(),
        "next_cursor": run.next_cursor.as_ref().map(|cursor| {
            json!({ "hex": hex(cursor.0.as_ref()) })
        }),
    })
}

fn render_outcome(outcome: &Outcome) -> JsonValue {
    match outcome {
        Outcome::Complete => json!({ "kind": "complete" }),
        Outcome::Degraded { truncation } => json!({
            "kind": "degraded",
            "truncation": render_truncation(truncation),
        }),
        Outcome::Refused(refusal) => json!({
            "kind": "refused",
            "refusal": render_refusal(refusal),
        }),
        Outcome::Stalled => json!({ "kind": "stalled" }),
    }
}

fn render_truncation(truncation: &Truncation) -> JsonValue {
    json!({
        "dimension": render_dimension(&truncation.dimension),
        "position": render_truncation_point(&truncation.position),
        "omitted": truncation.omitted,
    })
}

fn render_truncation_point(point: &TruncationPoint) -> JsonValue {
    match point {
        TruncationPoint::Cursor(bytes) => json!({ "cursor": { "hex": hex(bytes) } }),
        TruncationPoint::LastExamined(entity) => {
            json!({ "last_examined": render_entity(entity) })
        }
    }
}

fn render_refusal(refusal: &Refusal) -> JsonValue {
    json!({
        "dimension": render_dimension(&refusal.dimension),
        "limit": render_magnitude(&refusal.limit),
        "observed": render_magnitude(&refusal.observed),
    })
}

fn render_dimension(dimension: &Dimension) -> &'static str {
    match dimension {
        Dimension::Deadline => "deadline",
        Dimension::Results => "results",
        Dimension::Bytes => "bytes",
        Dimension::Scan => "scan",
        Dimension::AggregationMemory => "aggregation_memory",
    }
}

fn render_magnitude(magnitude: &Magnitude) -> JsonValue {
    match magnitude {
        Magnitude::Duration(duration) => json!({ "duration_ms": duration.as_millis() }),
        Magnitude::Units(units) => json!({ "units": units }),
        Magnitude::Bytes(bytes) => json!({ "bytes": bytes }),
    }
}

fn render_coverage(entry: &CoverageEntry) -> JsonValue {
    match entry {
        CoverageEntry::EvictionGap { after, before } => json!({
            "kind": "eviction_gap",
            "after": render_entity(after),
            "before": render_entity(before),
        }),
        CoverageEntry::SnapshotBoundary {
            admission_time,
            entity,
        } => json!({
            "kind": "snapshot_boundary",
            "admission_time_unix_nano": admission_time.as_unix_nano(),
            "entity": render_entity(entity),
        }),
        CoverageEntry::UncountedTail { after, dimension } => json!({
            "kind": "uncounted_tail",
            "after": render_entity(after),
            "dimension": render_dimension(dimension),
        }),
        CoverageEntry::DriverStall { after } => json!({
            "kind": "driver_stall",
            "after": render_entity(after),
        }),
    }
}

fn render_flow_coverage(entry: &FlowCoverageEntry) -> JsonValue {
    match entry {
        FlowCoverageEntry::MetricWindow { asked, resident } => json!({
            "kind": "metric_window",
            "asked": render_window(asked),
            "resident": render_window(resident),
        }),
        FlowCoverageEntry::AdmissionAnomalies { total } => {
            json!({ "kind": "admission_anomalies", "total": total })
        }
        FlowCoverageEntry::ResidencyHole { part, count } => json!({
            "kind": "residency_hole",
            "part": render_part(part),
            "count": count,
        }),
        FlowCoverageEntry::SuppressedEvidence {
            log,
            span,
            suppressed,
        } => json!({
            "kind": "suppressed_evidence",
            "log": render_entity(log),
            "span": render_entity(span),
            "suppressed": suppressed,
        }),
        FlowCoverageEntry::AbsentTraceSpans { log } => json!({
            "kind": "absent_trace_spans",
            "log": render_entity(log),
        }),
        FlowCoverageEntry::RelationShrinkage { count } => {
            json!({ "kind": "relation_shrinkage", "count": count })
        }
        FlowCoverageEntry::CorrelationDegradation { at } => {
            let (kind, value): (&str, u64) = match at {
                CorrelationStop::MaxDepth { depth } => ("max_depth", u64::from(*depth)),
                CorrelationStop::ScanExhausted { examined } => ("scan_exhausted", *examined),
                CorrelationStop::MaxRelations { count } => ("max_relations", *count),
            };
            json!({ "kind": "correlation_degradation", "at": { "kind": kind, "value": value } })
        }
        FlowCoverageEntry::TemporalStrategySkipped { reason } => {
            json!({ "kind": "temporal_strategy_skipped", "reason": reason })
        }
    }
}

fn render_window(window: &TimeWindow) -> JsonValue {
    json!({ "from": window.from, "to": window.to })
}

fn render_part(part: &PartName) -> &'static str {
    match part {
        PartName::Spans => "spans",
        PartName::RelatedLogs => "related_logs",
        PartName::SurroundingMetrics => "surrounding_metrics",
    }
}

fn render_correlated(
    correlated: &runtime_trail_investigation::correlated::Correlated,
) -> JsonValue {
    json!({
        "relations": correlated.relations.iter().map(render_relation).collect::<Vec<_>>(),
    })
}

fn render_relation(relation: &Relation) -> JsonValue {
    json!({
        "type": render_relation_type(relation.relation_type),
        "from": render_signal_ref(&relation.from),
        "to": render_signal_ref(&relation.to),
        "facts": relation.facts.iter().map(render_fact).collect::<Vec<_>>(),
        "strategy": render_strategy(&relation.strategy),
        "window": relation.window.as_ref().map(|window| render_window(&TimeWindow::from(*window))),
    })
}

fn render_signal_ref(reference: &SignalRef) -> JsonValue {
    json!({
        "kind": render_signal_kind(&reference.kind),
        "entity": render_entity(&reference.entity),
    })
}

fn render_signal_kind(kind: &SignalKind) -> &'static str {
    match kind {
        SignalKind::Spans => "spans",
        SignalKind::LogRecords => "log_records",
        SignalKind::MetricPoints => "metric_points",
    }
}

fn render_relation_type(
    relation_type: runtime_trail_investigation::correlated::RelationType,
) -> &'static str {
    match relation_type {
        runtime_trail_investigation::correlated::RelationType::SpanIdentity => "span_identity",
        runtime_trail_investigation::correlated::RelationType::TraceIdentity => "trace_identity",
        runtime_trail_investigation::correlated::RelationType::ParentChild => "parent_child",
        runtime_trail_investigation::correlated::RelationType::ResourceContext => {
            "resource_context"
        }
        runtime_trail_investigation::correlated::RelationType::TemporalCoActivity => {
            "temporal_co_activity"
        }
        runtime_trail_investigation::correlated::RelationType::ExemplarAttachment => {
            "exemplar_attachment"
        }
        runtime_trail_investigation::correlated::RelationType::Inferred => "inferred",
    }
}

fn render_fact(fact: &EvidenceFact) -> JsonValue {
    json!({
        "field": fact.field,
        "value": render_model_value(&fact.value),
    })
}

fn render_strategy(strategy: &StrategyVersion) -> JsonValue {
    json!({ "name": strategy.name, "version": strategy.version })
}

fn render_evidence(evidence: &Evidence) -> JsonValue {
    json!({
        "spans": evidence.spans.iter().map(render_span_view).collect::<Vec<_>>(),
        "logs": evidence.logs.iter().map(render_log_view).collect::<Vec<_>>(),
        "points": evidence.points.iter().map(render_point_view).collect::<Vec<_>>(),
    })
}

fn render_span_view(view: &SpanEvidence) -> JsonValue {
    let span: &Span = &view.span;
    json!({
        "entity": view.entity.as_ref().map(render_entity),
        "span": {
            "trace_id": render_trace_id(span.context.trace_id),
            "span_id": render_span_id(span.context.span_id),
            "parent_span_id": span.parent_span_id.map(render_span_id),
            "name": span.name,
            "start_time_unix_nano": span.start_time_unix_nano,
            "end_time_unix_nano": span.end_time_unix_nano,
        },
    })
}

fn render_log_view(view: &LogEvidence) -> JsonValue {
    let log: &LogRecord = &view.log;
    json!({
        "entity": view.entity.as_ref().map(render_entity),
        "log": {
            "timestamp_unix_nano": log.timestamp_unix_nano,
            "observed_timestamp_unix_nano": log.observed_timestamp_unix_nano,
            "body": match &log.body {
                Some(value) => render_model_value(value),
                None => JsonValue::Null,
            },
            "trace_id": log.trace_id.map(render_trace_id),
            "span_id": log.span_id.map(render_span_id),
        },
    })
}

fn render_point_view(view: &PointEvidence) -> JsonValue {
    json!({
        "entity": view.entity.as_ref().map(render_entity),
        "point": render_point(&view.point),
        "stream": render_stream(&view.stream),
    })
}

fn render_point(point: &MetricPoint) -> JsonValue {
    match point {
        MetricPoint::Number(number) => json!({
            "shape": "number",
            "time_unix_nano": number.time_unix_nano,
            "value": render_number(number.value),
        }),
        other => json!({ "shape": render_point_shape(other) }),
    }
}

fn render_point_shape(point: &MetricPoint) -> &'static str {
    match point {
        MetricPoint::Number(_) => "number",
        MetricPoint::Histogram(_) => "histogram",
        MetricPoint::ExponentialHistogram(_) => "exponential_histogram",
        MetricPoint::Summary(_) => "summary",
    }
}

fn render_number(number: MetricNumber) -> JsonValue {
    match number {
        MetricNumber::Int(value) => json!({ "int": value }),
        MetricNumber::Double(value) => json!({ "double": value.get() }),
    }
}

fn render_stream(stream: &StreamIdentity) -> JsonValue {
    json!({ "name": stream.name })
}

fn render_limits(limits: &Limits) -> JsonValue {
    json!({
        "budget": {
            "deadline_ms": limits.budget.deadline.as_millis(),
            "max_results": limits.budget.max_results,
            "max_bytes": limits.budget.max_bytes,
            "max_scan": limits.budget.max_scan,
            "max_aggregation_memory": limits.budget.max_aggregation_memory,
        },
        "chain": {
            "max_total_entities": limits.chain.max_total_entities,
            "max_total_pages": limits.chain.max_total_pages,
            "total_entities": limits.chain.total_entities,
            "total_pages": limits.chain.total_pages,
            "identity_examinations": limits.chain.identity_examinations,
            "stopped": limits.chain.stopped.as_ref().map(render_chain_basis),
        },
        "strategy_versions": limits
            .strategy_versions
            .iter()
            .map(render_strategy)
            .collect::<Vec<_>>(),
        "eviction": {
            "resident_records": limits.eviction.resident_records,
            "total_evictions": limits.eviction.total_evictions,
        },
    })
}

fn render_chain_basis(basis: &ChainBasis) -> &'static str {
    match basis {
        ChainBasis::TotalEntities => "total_entities",
        ChainBasis::TotalPages => "total_pages",
    }
}

/// Renders a model `Value` as JSON, verbatim (evidence fidelity, #39/#42):
/// scalars pass through as JSON scalars, arrays and key-value lists render
/// recursively as JSON arrays and objects (never flattened, never nulled),
/// and bytes render as hex — the wire's id encoding. Nothing is truncated
/// here; the model's admission gates bound a value's size once at
/// ingestion, and this surface renders the admitted value whole.
fn render_model_value(value: &Value) -> JsonValue {
    match value {
        Value::String(text) => JsonValue::String(text.clone()),
        Value::Int(number) => JsonValue::from(*number),
        Value::Double(double) => JsonValue::from(double.get()),
        Value::Bool(flag) => JsonValue::from(*flag),
        Value::Bytes(bytes) => JsonValue::String(hex(bytes)),
        Value::Array(items) => JsonValue::Array(items.iter().map(render_model_value).collect()),
        Value::KvList(entries) => JsonValue::Object(
            entries
                .iter()
                .map(|(key, child)| (key.clone(), render_model_value(child)))
                .collect(),
        ),
    }
}

fn render_entity(entity: &EntityId) -> JsonValue {
    match entity {
        EntityId::Span { trace_id, span_id } => json!({
            "span": {
                "trace_id": render_trace_id(*trace_id),
                "span_id": render_span_id(*span_id),
            }
        }),
        EntityId::Assigned(id) => json!({ "assigned": id.serial().get() }),
    }
}

fn render_trace_id(trace_id: TraceId) -> String {
    hex(&trace_id.as_bytes())
}

fn render_span_id(span_id: SpanId) -> String {
    hex(&span_id.as_bytes())
}

/// The wire's id encoding: lowercase hex. Mirrors the HTTP surface's
/// `hex` helper byte-for-byte.
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

/// The human-readable subject form the surfaces' refusals name, mirroring
/// the HTTP surface's `entity_text`.
pub(crate) fn entity_text(entity: &EntityId) -> String {
    match entity {
        EntityId::Span { trace_id, span_id } => format!(
            "span {}:{}",
            render_trace_id(*trace_id),
            render_span_id(*span_id)
        ),
        EntityId::Assigned(id) => format!("assigned {}", id.serial()),
    }
}
