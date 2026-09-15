//! The Investigation API's HTTP surface (M3, ADR 0011): ONE JSON endpoint
//! composing the trace investigation flow.
//!
//! `POST /v1/investigations/traces` — the request names one root span by
//! entity id:
//!
//! ```text
//! { "root_span": { "span":     { "trace_id": "<32 lowercase hex>", "span_id": "<16 lowercase hex>" } } }
//! { "root_span": { "assigned": 7 } }
//! ```
//!
//! The answer is the investigation envelope rendered as JSON: subject
//! (requested and effective, separately), execution (run facts per part,
//! never fabricated), the correlated part (typed-but-empty in M3),
//! evidence (waterfall spans, related logs and surrounding points with
//! their entity ids), and limits (the admitted budget, the flow's chain
//! spend and stops, and the store's eviction state at admission).
//!
//! Budgets are not carried on the wire in M3 — the wire shapes of later
//! phases stay unclaimed; the adapter admits the server-side default
//! ceilings below and the envelope's limits part reports them verbatim.
//!
//! The transport-edge law ([ADR 0010]) applies unchanged: the body is
//! read under the runtime's payload ceiling and read deadline, and the
//! shared in-flight body budget is charged before any byte is buffered
//! (the inflight guard routes this path like the OTLP/HTTP endpoints).
//!
//! Answers: **200** with the envelope, **400** for a malformed or
//! unparsable subject, **404** for a well-formed subject with no resident
//! record, **413** for a declared over-ceiling body, **408** when the
//! body never arrives within the read deadline, and **500** when the flow
//! itself fails on an internal contract (a rejected cursor, an engine
//! contract violation) — each refusal a JSON error naming the reason,
//! never a fabricated envelope.
//!
//! [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md

use std::sync::Arc;
use std::time::Duration;

use crate::otlp_http::{declared_over_ceiling, read_body};
use crate::runtime::CoreRuntime;
use axum::Json;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use runtime_trail_investigation::correlated::{Relation, StrategyVersion};
use runtime_trail_investigation::evidence::{
    Evidence, LogEvidence, PointEvidence, SignalKind, SignalRef, SpanEvidence,
};
use runtime_trail_investigation::execution::{
    CoverageEntry, Dimension, FlowCoverageEntry, Magnitude, Outcome, PartName, Refusal, RunFacts,
    RunGroup, TimeWindow, Truncation, TruncationPoint,
};
use runtime_trail_investigation::limits::ChainBasis;
use runtime_trail_investigation::subject::{EffectiveRoot, ResolutionNote};
use runtime_trail_investigation::{
    FlowError, Investigation, TraceInvestigationRequest, investigate_trace_bounded,
};
use runtime_trail_telemetry_model::{
    EntityId, LogRecord, MetricNumber, MetricPoint, Span, SpanId, StreamIdentity, TraceId, Value,
};
use serde_json::{Map, Value as JsonValue, json};

/// The one investigation endpoint this phase serves.
pub(crate) const INVESTIGATION_TRACES_PATH: &str = "/v1/investigations/traces";

/// The per-page deadline the adapter admits: generous next to the
/// transport-edge 10 s body-read deadline — the flow answers from a
/// memory store, so a page is a bounded scan, not a long computation.
const PAGE_DEADLINE: Duration = Duration::from_secs(30);

/// The page ceilings the adapter admits: the same magnitudes the rest of
/// the runtime bounds buffering and admission with, per page.
const PAGE_MAX_RESULTS: u64 = 1_000;
const PAGE_MAX_SCAN: u64 = 100_000;
const PAGE_MAX_AGGREGATION_MEMORY: u64 = 4_194_304;

/// The budget the adapter admits for every investigation (M3: budgets are
/// not carried on the wire). The envelope reports these ceilings verbatim.
fn admitted_budget(
    payload_ceiling_bytes: usize,
) -> runtime_trail_investigation::InvestigationBudget {
    runtime_trail_investigation::InvestigationBudget::new(
        PAGE_DEADLINE,
        PAGE_MAX_RESULTS,
        u64::try_from(payload_ceiling_bytes).unwrap_or(u64::MAX),
        PAGE_MAX_SCAN,
        PAGE_MAX_AGGREGATION_MEMORY,
    )
}

/// The flow's own chain ceilings: its shipped defaults (see the flow
/// crate), so the envelope's limits part reports the numbers that
/// actually bounded the answer.
const CHAIN_MAX_TOTAL_ENTITIES: u64 = 10_000;
const CHAIN_MAX_TOTAL_PAGES: u64 = 16;
const CHAIN_MAX_IDENTITY_EXAMINATIONS: u64 = 100_000;

fn admitted_chain() -> runtime_trail_investigation::ChainBudget {
    runtime_trail_investigation::ChainBudget::new(
        CHAIN_MAX_TOTAL_PAGES,
        CHAIN_MAX_TOTAL_ENTITIES,
        CHAIN_MAX_IDENTITY_EXAMINATIONS,
    )
}

/// `POST /v1/investigations/traces`
///
/// # Errors
///
/// Never a transport error: every outcome, refusal included, is an HTTP
/// answer.
pub(crate) async fn investigate_trace_http(
    State(runtime): State<Arc<CoreRuntime>>,
    request: Request,
) -> Response {
    let ceiling_bytes = runtime.payload_ceiling_bytes();
    // The one over-ceiling refusal the handler makes before reading — the
    // sibling of the OTLP/HTTP 413, answering before any byte is buffered.
    if declared_over_ceiling(request.headers(), ceiling_bytes) {
        return json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "the investigation request exceeds the {ceiling_bytes}-byte ceiling; \
                 refused before reading, non-retryable"
            ),
        );
    }
    let payload = match read_body(
        request.into_body(),
        ceiling_bytes,
        runtime.body_read_timeout(),
    )
    .await
    {
        Ok(Some(payload)) => payload,
        Ok(None) => {
            return json_error(
                StatusCode::REQUEST_TIMEOUT,
                "the request body did not arrive in full within the read deadline; \
                 nothing was parsed or investigated",
            );
        }
        Err(_) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "the request body could not be read in full; \
                 nothing was parsed or investigated",
            );
        }
    };
    let root_span = match parse_subject(&payload) {
        Ok(entity) => entity,
        Err(reason) => return json_error(StatusCode::BAD_REQUEST, reason),
    };
    let budget = admitted_budget(ceiling_bytes);
    let chain = admitted_chain();
    let store = runtime.lock_store();
    match investigate_trace_bounded(
        &**store,
        &TraceInvestigationRequest::new(root_span, budget),
        chain,
    ) {
        Ok(envelope) => Json(render_investigation(&envelope)).into_response(),
        Err(FlowError::SubjectUnresolved { requested }) => json_error(
            StatusCode::NOT_FOUND,
            format!(
                "no resident record carries the requested subject \
                 ({}) — it was never admitted, or evicted",
                entity_text(&requested),
            ),
        ),
        Err(FlowError::CursorRejected) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the flow rejected an engine continuation cursor — an internal \
             contract violation; nothing was fabricated",
        ),
        Err(FlowError::EngineContract) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the engine returned an answer the envelope contract cannot hold — \
             an internal contract violation; nothing was fabricated",
        ),
    }
}

// ---------------------------------------------------------------------------
// Request parsing: one object, `{ "root_span": <entity> }`.
// ---------------------------------------------------------------------------

/// Parses the request body into the requested root-span entity id.
fn parse_subject(payload: &[u8]) -> Result<EntityId, String> {
    let value: JsonValue = serde_json::from_slice(payload)
        .map_err(|_| "the request body is not a JSON object".to_owned())?;
    let root = value
        .get("root_span")
        .ok_or_else(|| "the request must name a root_span".to_owned())?;
    parse_entity(root)
}

/// Parses one entity descriptor: `{"span": {"trace_id": hex, "span_id": hex}}`
/// or `{"assigned": serial}`.
fn parse_entity(value: &JsonValue) -> Result<EntityId, String> {
    if let Some(span) = value.get("span") {
        let trace_text = span
            .get("trace_id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "a span subject needs trace_id (32 lowercase hex)".to_owned())?;
        let span_text = span
            .get("span_id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "a span subject needs span_id (16 lowercase hex)".to_owned())?;
        let trace_id = parse_hex::<16>(trace_text, "trace_id")?;
        let span_id = parse_hex::<8>(span_text, "span_id")?;
        return Ok(EntityId::Span {
            trace_id: TraceId::from_bytes(trace_id),
            span_id: SpanId::from_bytes(span_id),
        });
    }
    if let Some(serial) = value.get("assigned").and_then(JsonValue::as_u64) {
        let serial = std::num::NonZeroU64::new(serial)
            .ok_or_else(|| "an assigned serial starts at 1".to_owned())?;
        return Ok(EntityId::Assigned(
            runtime_trail_telemetry_model::AssignedId::from_serial(serial),
        ));
    }
    Err("an entity is {\"span\": …} or {\"assigned\": <serial>}".to_owned())
}

/// Parses `width` bytes of lowercase hex.
fn parse_hex<const N: usize>(text: &str, what: &str) -> Result<[u8; N], String> {
    if text.len() != N * 2 {
        return Err(format!(
            "{what} must be {N} bytes, i.e. {} lowercase hex characters",
            N * 2
        ));
    }
    let mut out = [0u8; N];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let (high, low) = (hex_value(pair[0])?, hex_value(pair[1])?);
        out[index] = (high << 4) | low;
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(format!(
            "hex ids use 0-9 and a-f, but the value contains {:?}",
            char::from(byte)
        )),
    }
}

/// Renders bytes as lowercase hex — the wire's id encoding.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

// ---------------------------------------------------------------------------
// Envelope rendering: every part, every fact, verbatim.
// ---------------------------------------------------------------------------

fn render_investigation(investigation: &Investigation) -> JsonValue {
    json!({
        "subject": render_subject(&investigation.subject),
        "execution": render_execution(&investigation.execution),
        "correlated": render_correlated(&investigation.correlated),
        "evidence": render_evidence(&investigation.evidence),
        "limits": render_limits(&investigation.limits),
    })
}

fn render_subject(subject: &runtime_trail_investigation::subject::Subject) -> JsonValue {
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

fn render_execution(execution: &runtime_trail_investigation::execution::Execution) -> JsonValue {
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
        "type": render_relation_type(&relation.relation_type),
        "from": render_signal_ref(&relation.from),
        "to": render_signal_ref(&relation.to),
        "facts": relation.facts.iter().map(render_fact).collect::<Vec<_>>(),
        "strategy": render_strategy(&relation.strategy),
        "window": relation.window.as_ref().map(render_window),
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
    relation_type: &runtime_trail_investigation::correlated::RelationType,
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

fn render_fact(fact: &runtime_trail_investigation::correlated::EvidenceFact) -> JsonValue {
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

fn render_limits(limits: &runtime_trail_investigation::limits::Limits) -> JsonValue {
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

/// Renders a model `Value` as JSON: primitive values pass through as JSON
/// scalars; arrays and maps render as null on this surface (the envelope
/// itself carries the record verbatim).
fn render_model_value(value: &Value) -> JsonValue {
    match value {
        Value::String(text) => JsonValue::String(text.clone()),
        Value::Int(n) => JsonValue::from(*n),
        Value::Double(d) => JsonValue::from(d.get()),
        Value::Bool(b) => JsonValue::from(*b),
        // Bytes render as hex on this surface (bounded, honest);
        // arrays and kv-lists render as null — the envelope itself
        // carries the record verbatim.
        Value::Bytes(bytes) => JsonValue::String(hex(bytes)),
        Value::Array(_) | Value::KvList(_) => JsonValue::Null,
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

fn entity_text(entity: &EntityId) -> String {
    match entity {
        EntityId::Span { trace_id, span_id } => format!(
            "span {}:{}",
            render_trace_id(*trace_id),
            render_span_id(*span_id)
        ),
        EntityId::Assigned(id) => format!("assigned {}", id.serial()),
    }
}

fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    let mut object = Map::new();
    object.insert("error".to_owned(), JsonValue::String(message.into()));
    (status, Json(JsonValue::Object(object))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    use runtime_trail_telemetry_ingestion::fixtures as fx;

    use crate::runtime::CoreRuntime;
    use crate::test_support;
    use crate::{ServerConfig, build_router};

    /// T1's trace id and S1's span id as they appear on the wire.
    const TRACE_ID_HEX: &str = "01010101010101010101010101010101";
    const SPAN_ID_HEX: &str = "1111111111111111";

    fn wait_for_resident(runtime: &CoreRuntime, expected: u64) {
        for _ in 0..5_000 {
            if runtime.store_stats().resident_records == expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!(
            "residency never reached {expected}: {:?}",
            runtime.store_stats()
        );
    }

    async fn post(router: Router, path: &str, body: &str) -> (StatusCode, Value) {
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_owned()))
                    .expect("a static request builds"),
            )
            .await
            .expect("the router answers every request");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body reads")
            .to_vec();
        let value: Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| panic!("the answer is JSON: {}", String::from_utf8_lossy(&bytes)));
        (status, value)
    }

    fn one_trace_payload() -> Vec<u8> {
        let mut child = fx::trace_span("child", fx::T1, fx::S2);
        child.parent_span_id = fx::S1.to_vec();
        child.start_time_unix_nano = 12;
        child.end_time_unix_nano = 18;
        let mut root = fx::trace_span("root", fx::T1, fx::S1);
        root.start_time_unix_nano = 10;
        root.end_time_unix_nano = 20;
        fx::encode(&fx::traces_request(vec![fx::resource_spans(
            None,
            vec![fx::scope_spans(None, vec![root, child])],
        )]))
    }

    fn one_related_log_payload() -> Vec<u8> {
        let mut log = fx::log_record();
        log.trace_id = fx::T1.to_vec();
        log.span_id = fx::S1.to_vec();
        log.body = Some(fx::str_value("related"));
        fx::encode(&fx::logs_request(vec![fx::resource_logs(
            None,
            vec![fx::scope_logs(None, vec![log])],
        )]))
    }

    fn one_point_payload() -> Vec<u8> {
        let metric = fx::described_metric(
            "cpu.seconds",
            "described",
            "s",
            Vec::new(),
            vec![fx::number_point(fx::as_double(1.0))],
        );
        fx::encode(&fx::metrics_request(vec![fx::resource_metrics(
            None,
            vec![fx::scope_metrics(None, vec![metric])],
        )]))
    }

    /// Ingests a two-span trace, one related log and one in-window point,
    /// waits for residency, and returns the runtime.
    fn seeded_runtime() -> Arc<CoreRuntime> {
        let runtime = test_support::runtime();
        let pipeline = Arc::clone(runtime.pipeline());
        let now = runtime.now();
        pipeline
            .ingest_spans(now, &one_trace_payload())
            .expect("the trace ingests");
        pipeline
            .ingest_logs(now, &one_related_log_payload())
            .expect("the log ingests");
        pipeline
            .ingest_metrics(now, &one_point_payload())
            .expect("the point ingests");
        wait_for_resident(&runtime, 4);
        runtime
    }

    #[tokio::test]
    async fn the_trace_investigation_round_trips_over_http() {
        let runtime = seeded_runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());
        let body = format!(
            r#"{{"root_span": {{"span": {{"trace_id": "{TRACE_ID_HEX}", "span_id": "{SPAN_ID_HEX}"}}}}}}"#
        );

        let (status, json) = post(router, INVESTIGATION_TRACES_PATH, &body).await;

        assert_eq!(status, StatusCode::OK);
        assert!(
            json.get("error").is_none(),
            "no error in the envelope: {json}"
        );
        // Subject: requested and effective agree.
        assert_eq!(
            json["subject"]["requested"]["root_span"]["span"]["trace_id"],
            TRACE_ID_HEX
        );
        assert_eq!(json["subject"]["effective"]["root"]["name"], "root");
        // Evidence: the waterfall, one related log, one in-window point.
        let spans = json["evidence"]["spans"].as_array().expect("span views");
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0]["span"]["name"], "root");
        assert_eq!(spans[1]["span"]["name"], "child");
        assert_eq!(spans[1]["span"]["parent_span_id"], SPAN_ID_HEX);
        let logs = json["evidence"]["logs"].as_array().expect("log views");
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0]["log"]["body"], "related");
        let points = json["evidence"]["points"].as_array().expect("point views");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0]["point"]["time_unix_nano"], 10);
        assert_eq!(points[0]["stream"]["name"], "cpu.seconds");
        // Execution: three groups, one complete run each.
        let groups = json["execution"]["run_groups"]
            .as_array()
            .expect("run groups");
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0]["part"], "spans");
        assert_eq!(groups[1]["part"], "related_logs");
        assert_eq!(groups[2]["part"], "surrounding_metrics");
        for group in groups {
            let runs = group["runs"].as_array().expect("runs");
            assert_eq!(runs.len(), 1);
            assert_eq!(runs[0]["outcome"]["kind"], "complete");
        }
        // The metric window is stated.
        assert_eq!(
            json["execution"]["flow_coverage"][0]["kind"],
            "metric_window"
        );
        // Limits: the admitted budget mirrored, the chain totals honest.
        assert_eq!(json["limits"]["chain"]["total_pages"], 3);
        assert_eq!(json["limits"]["chain"]["total_entities"], 4);
        assert_eq!(json["limits"]["chain"]["stopped"], Value::Null);
        assert_eq!(json["limits"]["budget"]["max_results"], PAGE_MAX_RESULTS);
        runtime.shutdown();
    }

    #[tokio::test]
    async fn an_unresolvable_subject_is_a_404() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());
        let (status, json) = post(
            router,
            INVESTIGATION_TRACES_PATH,
            r#"{"root_span": {"span": {"trace_id": "02020202020202020202020202020202", "span_id": "2222222222222222"}}}"#,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("never admitted"),
            "the error names the reason: {}",
            json["error"]
        );
        runtime.shutdown();
    }

    #[tokio::test]
    async fn a_malformed_body_is_a_400() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());
        let (status, json) = post(router, INVESTIGATION_TRACES_PATH, "{not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            json["error"]
                .as_str()
                .unwrap_or_default()
                .contains("not a JSON object"),
            "the error names the problem: {}",
            json["error"]
        );
        runtime.shutdown();
    }
}
