//! The MCP tool surface (issue #36): the Investigation API's committed
//! flow, presented to MCP clients exactly as the HTTP surface
//! (`crates/server/src/investigation_http.rs`) presents it — the same
//! subject parsing, the same admitted budget, the same error vocabulary.
//!
//! There is one committed flow entry point in the API —
//! [`investigate_trace_bounded`] — and every tool routes through it. No
//! tool invents a capability the flow does not implement:
//!
//! - [`INVESTIGATE_TRACE`]: the HTTP surface's request itself —
//!   `{"root_span": <entity>}` — answered with the same envelope.
//! - [`INVESTIGATE_LOG`] / [`INVESTIGATE_METRIC`]: the same flow named by
//!   a resident log-record or metric-point entity; the API's subject
//!   resolution decides (an unresolvable subject answers
//!   [`ToolError::SubjectUnresolved`], named the way the HTTP surface's
//!   404 names it).
//! - [`CONTINUE_INVESTIGATION`]: same subject, cursor validated (the hex
//!   this surface reports), re-investigated under a fresh admitted budget.
//!   Answers are deterministic (query-model invariant): while residency is
//!   stable, the envelope is the same investigation the HTTP surface
//!   returns for the same subject — the flow has no continuation entry
//!   point, so nothing "next-page" is fabricated.
//!
//! Budgets are not carried in tool arguments, exactly as they are not
//! carried on the HTTP wire (M3): the adapter admits the server-side
//! default ceilings below — the same magnitudes the HTTP adapter admits —
//! and the envelope's `limits` part reports them verbatim.

use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;

use runtime_trail_investigation::telemetry_model::{AssignedId, EntityId, SpanId, TraceId};
use runtime_trail_investigation::{
    ChainBudget, FlowError, Investigation, InvestigationBudget, TelemetryStore,
    TraceInvestigationRequest, investigate_trace_bounded,
};
use serde_json::{Value as JsonValue, json};

use crate::render;

/// The per-page deadline the adapter admits: the same magnitude the HTTP
/// adapter admits (a page is a bounded scan over a memory store).
pub(crate) const PAGE_DEADLINE: Duration = Duration::from_secs(30);

/// The page ceilings the adapter admits: the same magnitudes the HTTP
/// adapter admits, per page.
pub(crate) const PAGE_MAX_RESULTS: u64 = 1_000;
pub(crate) const PAGE_MAX_SCAN: u64 = 100_000;
pub(crate) const PAGE_MAX_AGGREGATION_MEMORY: u64 = 4_194_304;
/// The bytes ceiling: the model's OTLP payload ceiling
/// (`telemetry_model::budgets::OTLP_PAYLOAD_BYTES`), the magnitude the
/// runtime bounds the HTTP body with — so both surfaces admit the same
/// `limits.budget`.
pub(crate) const PAGE_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// The flow's own chain ceilings: the shipped defaults, mirroring the HTTP
/// adapter's constants, so the envelope's `limits.chain` reports the
/// numbers that actually bounded the answer.
pub(crate) const CHAIN_MAX_TOTAL_ENTITIES: u64 = 10_000;
pub(crate) const CHAIN_MAX_TOTAL_PAGES: u64 = 16;
pub(crate) const CHAIN_MAX_IDENTITY_EXAMINATIONS: u64 = 100_000;

/// The budget the adapter admits for every tool call: same ceilings as the
/// HTTP adapter's `admitted_budget`, with `max_bytes` at the runtime's
/// payload ceiling (the model's OTLP ceiling).
fn admitted_budget() -> InvestigationBudget {
    InvestigationBudget::new(
        PAGE_DEADLINE,
        PAGE_MAX_RESULTS,
        PAGE_MAX_BYTES,
        PAGE_MAX_SCAN,
        PAGE_MAX_AGGREGATION_MEMORY,
    )
}

/// The flow's own chain ceilings.
fn admitted_chain() -> ChainBudget {
    ChainBudget::new(
        CHAIN_MAX_TOTAL_PAGES,
        CHAIN_MAX_TOTAL_ENTITIES,
        CHAIN_MAX_IDENTITY_EXAMINATIONS,
    )
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

/// The normalized tool names, referenced by `tools/call`.
pub const INVESTIGATE_TRACE: &str = "investigate_trace";
pub const INVESTIGATE_LOG: &str = "investigate_log";
pub const INVESTIGATE_METRIC: &str = "investigate_metric";
pub const CONTINUE_INVESTIGATION: &str = "continue_investigation";

/// One MCP tool: its `tools/list` advertisement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tool {
    /// The normalized tool name (`tools/call` addresses it by this).
    pub name: &'static str,
    /// The tool's `tools/list` description.
    pub description: &'static str,
    /// The tool's JSON Schema `inputSchema`, as advertised by `tools/list`.
    pub input_schema: JsonValue,
}

/// The subject descriptor schema shared by every tool.
fn entity_schema() -> JsonValue {
    json!({
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "span": {
                        "type": "object",
                        "properties": {
                            "trace_id": { "type": "string", "description": "32 lowercase hex" },
                            "span_id": { "type": "string", "description": "16 lowercase hex" },
                        },
                        "required": ["trace_id", "span_id"],
                    }
                },
                "required": ["span"],
            },
            {
                "type": "object",
                "properties": {
                    "assigned": { "type": "integer", "minimum": 1 },
                },
                "required": ["assigned"],
            },
        ]
    })
}

/// The four committed tools, in `tools/list` order. A lazy static: the
/// JSON-Schema payloads are built once, at first access (JSON construction
/// is not const-evaluable).
pub static TOOLS: LazyLock<[Tool; 4]> = LazyLock::new(|| {
    [
        Tool {
            name: INVESTIGATE_TRACE,
            description: concat!(
                "Investigates a trace: the committed investigation flow answers the envelope ",
                "for the named root span — subject (requested and effective), execution run ",
                "facts, correlated relations, evidence, limits — under the same admitted budget ",
                "the HTTP surface admits (30 s/page deadline, 1000 records/page, 4 MiB/page, ",
                "100 000 scan positions/page, 4 MiB/page aggregation memory; chain 16 pages, ",
                "10 000 entities, 100 000 identity examinations). The answer is the same ",
                "envelope the HTTP surface returns."
            ),
            input_schema: entity_schema(),
        },
        Tool {
            name: INVESTIGATE_LOG,
            description: concat!(
                "Investigates the trace a resident log record belongs to, naming the record by ",
                "its entity id. A span descriptor is investigated as-is (the investigation ",
                "subject is always a root span); an assigned serial resolves to the resident ",
                "record's own span when it carries one, and that trace is investigated — the ",
                "same flow, envelope and budget as investigate_trace. An unresolvable subject ",
                "is refused with the same wording the HTTP surface's 404 carries; nothing is ",
                "fabricated."
            ),
            input_schema: entity_schema(),
        },
        Tool {
            name: INVESTIGATE_METRIC,
            description: concat!(
                "Investigates the trace a resident metric point belongs to, naming the point ",
                "by its entity id. A span descriptor is investigated as-is. A metric point ",
                "carries no trace linkage in the committed model (the strategy set's exemplar ",
                "relation is pinned not-implemented), so a resident point is refused cleanly — ",
                "the flow investigates a root span, and nothing is fabricated."
            ),
            input_schema: entity_schema(),
        },
        Tool {
            name: CONTINUE_INVESTIGATION,
            description: concat!(
                "Re-investigates the named root span from the cursor a prior investigation ",
                "reported (its execution run facts' next_cursor hex), under a fresh admitted ",
                "budget. The flow answers deterministically (query-model invariant): while ",
                "residency is stable, the envelope is the same investigation the HTTP surface ",
                "returns for the same subject. A malformed cursor is refused cleanly; nothing ",
                "is fabricated."
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "root_span": entity_schema(),
                    "cursor": {
                        "type": "string",
                        "description": "lowercase hex, as a prior answer's next_cursor reported",
                    },
                },
                "required": ["root_span", "cursor"],
            }),
        },
    ]
});

/// A tool call's refusal. Every variant names the reason; none fabricates
/// an answer. The messages mirror the HTTP surface's error text word for
/// word, so a client sees the same vocabulary over either transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolError {
    /// The arguments do not name a subject (shape, keyword or hex
    /// problems) — the HTTP surface's 400 family.
    Parse(String),
    /// The named subject has no resident record — the HTTP surface's 404.
    SubjectUnresolved(EntityId),
    /// The flow rejected an engine continuation cursor — an internal
    /// contract violation; nothing was fabricated (the HTTP surface's 500).
    CursorRejected,
    /// The engine answered outside the envelope contract — an internal
    /// contract violation; nothing was fabricated (the HTTP surface's 500).
    EngineContract,
}

impl fmt::Display for ToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ToolError::Parse(reason) => formatter.write_str(reason),
            ToolError::SubjectUnresolved(requested) => write!(
                formatter,
                "no resident record carries the requested subject ({}) — it was never \
                 admitted, or evicted",
                render::entity_text(requested),
            ),
            ToolError::CursorRejected => formatter.write_str(
                "the flow rejected an engine continuation cursor — an internal contract \
                 violation; nothing was fabricated",
            ),
            ToolError::EngineContract => formatter.write_str(
                "the engine returned an answer the envelope contract cannot hold — an \
                 internal contract violation; nothing was fabricated",
            ),
        }
    }
}

impl std::error::Error for ToolError {}

// ---------------------------------------------------------------------------
// The four tool callbacks.
// ---------------------------------------------------------------------------

/// `investigate_trace`: the HTTP surface's own request, answered with the
/// same envelope.
///
/// # Errors
///
/// Refuses when the arguments do not name a root-span entity, or when the
/// span is not resident — the HTTP surface's 400/404 wording.
pub fn investigate_trace(
    store: &dyn TelemetryStore,
    arguments: &JsonValue,
) -> Result<Investigation, ToolError> {
    let subject = parse_subject(arguments, "root_span")?;
    run_flow(store, subject)
}

/// `investigate_log`: the same flow, entered through a resident log
/// record. A span descriptor is investigated as-is (the investigation
/// subject is always a root span); an assigned serial resolves to the
/// resident record's own span, and that trace is investigated. The API's
/// subject resolution decides, and an unresolvable subject is refused with
/// the HTTP surface's wording.
///
/// # Errors
///
/// Refuses when the arguments do not name a log entity; an unresolvable
/// record is the HTTP surface's 404 wording, and a resident record
/// without span context is refused rather than fabricated.
pub fn investigate_log(
    store: &dyn TelemetryStore,
    arguments: &JsonValue,
) -> Result<Investigation, ToolError> {
    let entity = parse_subject(arguments, "log")?;
    match entity {
        EntityId::Span { .. } => run_flow(store, entity),
        EntityId::Assigned(_) => {
            let record = store
                .log_record(entity)
                .ok_or(ToolError::SubjectUnresolved(entity))?;
            run_flow(
                store,
                subject_span(record.trace_id, record.span_id, "the resident log")?,
            )
        }
    }
}

/// `investigate_metric`: the same flow, entered through a resident
/// metric-point entity. A span descriptor is investigated as-is. A metric
/// point carries no trace linkage in the committed model (the strategy
/// set's exemplar relation is pinned not-implemented), so a resident point
/// is refused cleanly rather than fabricating a trace.
///
/// # Errors
///
/// Refuses when the arguments do not name a metric entity; an
/// unresolvable record is the HTTP surface's 404 wording, and a resident
/// point is refused because the model carries no point-to-trace linkage.
pub fn investigate_metric(
    store: &dyn TelemetryStore,
    arguments: &JsonValue,
) -> Result<Investigation, ToolError> {
    let entity = parse_subject(arguments, "metric")?;
    match entity {
        EntityId::Span { .. } => run_flow(store, entity),
        EntityId::Assigned(_) => {
            if store.metric_point(entity).is_none() {
                return Err(ToolError::SubjectUnresolved(entity));
            }
            // The point is resident but carries no trace linkage (see the
            // tool doc): the strategy set's exemplar relation is pinned
            // not-implemented, and the committed flow investigates a root
            // span — nothing to investigate, nothing fabricated.
            Err(ToolError::Parse(
                "a resident metric point carries no trace linkage — the strategy set's \
                 exemplar relation is pinned not-implemented, and the committed flow \
                 investigates a root span; nothing to investigate"
                    .to_owned(),
            ))
        }
    }
}

/// The span a resident record carries its trace context as: the subject
/// the one committed span-taking flow needs.
fn subject_span(
    trace_id: Option<runtime_trail_investigation::telemetry_model::TraceId>,
    span_id: Option<runtime_trail_investigation::telemetry_model::SpanId>,
    what: &str,
) -> Result<EntityId, ToolError> {
    match (trace_id, span_id) {
        (Some(trace_id), Some(span_id)) => Ok(EntityId::Span { trace_id, span_id }),
        _ => Err(ToolError::Parse(format!(
            "{what} carries no span context — the committed flow investigates a root span; \
             nothing to investigate"
        ))),
    }
}

/// `continue_investigation`: re-investigates the named root span from a
/// reported cursor, under a fresh admitted budget. The cursor is validated
/// (the well-formed hex this surface reports); the answer is the
/// deterministic re-investigation.
///
/// # Errors
///
/// Refuses when the root span or the cursor are malformed, or when the
/// span is not resident — the HTTP surface's 400/404 wording.
pub fn continue_investigation(
    store: &dyn TelemetryStore,
    arguments: &JsonValue,
) -> Result<Investigation, ToolError> {
    let subject = parse_subject(arguments, "root_span")?;
    parse_cursor(arguments)?;
    run_flow(store, subject)
}

/// Runs the one committed flow under the adapter's admitted budget and
/// chain, mapping the flow's refusals onto the tool vocabulary.
fn run_flow(store: &dyn TelemetryStore, subject: EntityId) -> Result<Investigation, ToolError> {
    let budget = admitted_budget();
    let chain = admitted_chain();
    investigate_trace_bounded(
        store,
        &TraceInvestigationRequest::new(subject, budget, None),
        chain,
    )
    .map_err(|error| match error {
        FlowError::SubjectUnresolved { requested } => ToolError::SubjectUnresolved(requested),
        FlowError::CursorRejected => ToolError::CursorRejected,
        FlowError::EngineContract => ToolError::EngineContract,
    })
}

// ---------------------------------------------------------------------------
// Argument parsing: one object naming a subject (and, for the continuation
// tool, a cursor). Parsing mirrors the HTTP surface's `parse_subject` /
// `parse_entity` / `parse_hex` exactly, including the error wording.
// ---------------------------------------------------------------------------

/// Parses one tool's arguments: the object must name the subject under the
/// tool's own keyword — `root_span` for the trace and continuation tools,
/// `log` / `metric` for the signal-named tools.
pub(crate) fn parse_subject(arguments: &JsonValue, keyword: &str) -> Result<EntityId, ToolError> {
    let object = arguments
        .as_object()
        .ok_or_else(|| ToolError::Parse("the request body is not a JSON object".to_owned()))?;
    let subject = object
        .get(keyword)
        .ok_or_else(|| ToolError::Parse(format!("the request must name a {keyword}")))?;
    parse_entity(subject)
}

/// The continuation tool's cursor: a non-empty string of lowercase hex, as
/// this surface reports cursors. Deeper validity is the engine's to judge
/// when a continuation entry point exists; this surface accepts what it
/// reports and refuses what it does not.
pub(crate) fn parse_cursor(arguments: &JsonValue) -> Result<(), ToolError> {
    let object = arguments
        .as_object()
        .ok_or_else(|| ToolError::Parse("the request body is not a JSON object".to_owned()))?;
    let text = object
        .get("cursor")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            ToolError::Parse(
                "a continuation needs a cursor: the hex a prior answer's next_cursor reported"
                    .to_owned(),
            )
        })?;
    if text.is_empty() || text.len() % 2 != 0 || !text.bytes().all(|byte| hex_value(byte).is_ok()) {
        return Err(ToolError::Parse(
            "the cursor must be lowercase hex of even length — a cursor this surface reported, \
             not arbitrary text"
                .to_owned(),
        ));
    }
    Ok(())
}

/// Parses one entity descriptor: `{"span": {"trace_id": hex, "span_id":
/// hex}}` or `{"assigned": serial}`.
fn parse_entity(value: &JsonValue) -> Result<EntityId, ToolError> {
    if let Some(span) = value.get("span") {
        let trace_text = span
            .get("trace_id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                ToolError::Parse("a span subject needs trace_id (32 lowercase hex)".to_owned())
            })?;
        let span_text = span
            .get("span_id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                ToolError::Parse("a span subject needs span_id (16 lowercase hex)".to_owned())
            })?;
        let trace_id = parse_hex::<16>(trace_text, "trace_id")?;
        let span_id = parse_hex::<8>(span_text, "span_id")?;
        return Ok(EntityId::Span {
            trace_id: TraceId::from_bytes(trace_id),
            span_id: SpanId::from_bytes(span_id),
        });
    }
    if let Some(serial) = value.get("assigned").and_then(JsonValue::as_u64) {
        let serial = std::num::NonZeroU64::new(serial)
            .ok_or_else(|| ToolError::Parse("an assigned serial starts at 1".to_owned()))?;
        return Ok(EntityId::Assigned(AssignedId::from_serial(serial)));
    }
    Err(ToolError::Parse(
        "an entity is {\"span\": …} or {\"assigned\": <serial>}".to_owned(),
    ))
}

/// Parses `N` bytes of lowercase hex, mirroring the HTTP surface's
/// `parse_hex` including its wording.
fn parse_hex<const N: usize>(text: &str, what: &str) -> Result<[u8; N], ToolError> {
    if text.len() != N * 2 {
        return Err(ToolError::Parse(format!(
            "{what} must be {N} bytes, i.e. {} lowercase hex characters",
            N * 2
        )));
    }
    let mut out = [0u8; N];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let (high, low) = (hex_value(pair[0])?, hex_value(pair[1])?);
        out[index] = (high << 4) | low;
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8, ToolError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(ToolError::Parse(format!(
            "hex ids use 0-9 and a-f, but the value contains {:?}",
            char::from(byte)
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(json: &str) -> JsonValue {
        serde_json::from_str(json).expect("test argument JSON")
    }

    fn hex(bytes: &[u8]) -> String {
        crate::render::hex(bytes)
    }

    #[test]
    fn trace_accepts_a_span_descriptor() {
        let arguments = object(
            r#"{"root_span": {"span": {"trace_id": "02020202020202020202020202020202", "span_id": "2222222222222222"}}}"#,
        );
        let entity = parse_subject(&arguments, "root_span").expect("parses");
        let EntityId::Span { trace_id, span_id } = entity else {
            panic!("a span descriptor must parse as a span entity");
        };
        assert_eq!(
            hex(&trace_id.as_bytes()),
            "02020202020202020202020202020202"
        );
        assert_eq!(hex(&span_id.as_bytes()), "2222222222222222");
    }

    #[test]
    fn trace_accepts_an_assigned_serial() {
        let arguments = object(r#"{"root_span": {"assigned": 7}}"#);
        let entity = parse_subject(&arguments, "root_span").expect("parses");
        let EntityId::Assigned(id) = entity else {
            panic!("an assigned descriptor must parse as an assigned entity");
        };
        assert_eq!(id.serial().get(), 7);
    }

    #[test]
    fn every_tool_enforces_its_own_keyword() {
        let trace_body = object(r#"{"root_span": {"assigned": 1}}"#);
        let log_body = object(r#"{"log": {"assigned": 1}}"#);
        let metric_body = object(r#"{"metric": {"assigned": 1}}"#);
        assert!(parse_subject(&trace_body, "root_span").is_ok());
        assert!(parse_subject(&log_body, "log").is_ok());
        assert!(parse_subject(&metric_body, "metric").is_ok());
        // The wrong keyword for the tool is refused with the tool's own wording.
        match parse_subject(&log_body, "root_span") {
            Err(ToolError::Parse(reason)) => {
                assert_eq!(reason, "the request must name a root_span");
            }
            _ => panic!("the trace tool must refuse a body that does not name root_span"),
        }
    }

    #[test]
    fn malformed_subjects_are_refused_with_the_http_wording() {
        let cases: &[(&str, &str)] = &[
            (
                r#"{"root_span": {"span": {"trace_id": "not-hex", "span_id": "2222222222222222"}}}"#,
                "trace_id must be 16 bytes, i.e. 32 lowercase hex characters",
            ),
            (
                r#"{"root_span": {"span": {"span_id": "2222222222222222"}}}"#,
                "a span subject needs trace_id (32 lowercase hex)",
            ),
            (
                r#"{"root_span": {"span": {"trace_id": "02020202020202020202020202020202"}}}"#,
                "a span subject needs span_id (16 lowercase hex)",
            ),
            (
                r#"{"root_span": {"assigned": 0}}"#,
                "an assigned serial starts at 1",
            ),
            (
                r#"{"root_span": {"trace_id": "02020202020202020202020202020202"}}"#,
                "an entity is {\"span\": …} or {\"assigned\": <serial>}",
            ),
            (
                r#"{"root_span": "02020202020202020202020202020202"}"#,
                "an entity is {\"span\": …} or {\"assigned\": <serial>}",
            ),
        ];
        for (body, expected) in cases {
            match parse_subject(&object(body), "root_span") {
                Err(ToolError::Parse(reason)) => assert_eq!(&reason, expected, "for {body}"),
                other => panic!("{body} must be refused, got {other:?}"),
            }
        }
    }

    #[test]
    fn non_object_arguments_are_refused() {
        match parse_subject(&JsonValue::Array(vec![]), "root_span") {
            Err(ToolError::Parse(reason)) => {
                assert_eq!(reason, "the request body is not a JSON object");
            }
            _ => panic!("a non-object body must be refused"),
        }
    }

    #[test]
    fn a_missing_keyword_is_refused() {
        let arguments = object(r#"{"log": {"assigned": 1}}"#);
        match parse_subject(&arguments, "metric") {
            Err(ToolError::Parse(reason)) => {
                assert_eq!(reason, "the request must name a metric");
            }
            _ => panic!("a body without the tool's keyword must be refused"),
        }
    }

    #[test]
    fn the_continuation_cursor_is_validated() {
        let valid = object(r#"{"root_span": {"assigned": 1}, "cursor": "aabbccdd"}"#);
        assert!(parse_cursor(&valid).is_ok());
        // A missing or non-string cursor is refused as needing a cursor.
        for body in [
            r#"{"root_span": {"assigned": 1}}"#,
            r#"{"root_span": {"assigned": 1}, "cursor": 7}"#,
        ] {
            match parse_cursor(&object(body)) {
                Err(ToolError::Parse(reason)) => assert!(
                    reason.contains("needs a cursor"),
                    "a {body}-style body must be refused as needing a cursor: {reason}"
                ),
                _ => panic!("{body} must be refused"),
            }
        }
        // A present but malformed hex cursor is refused on the hex wording.
        for bad in [
            r#"{"root_span": {"assigned": 1}, "cursor": "zz"}"#,
            r#"{"root_span": {"assigned": 1}, "cursor": "abc"}"#,
            r#"{"root_span": {"assigned": 1}, "cursor": ""}"#,
        ] {
            match parse_cursor(&object(bad)) {
                Err(ToolError::Parse(reason)) => assert!(
                    reason.contains("lowercase hex"),
                    "malformed cursor {bad} must name hex: {reason}"
                ),
                _ => panic!("malformed cursor {bad} must be refused"),
            }
        }
    }

    #[test]
    fn the_admitted_ceilings_mirror_the_http_adapter() {
        let budget = admitted_budget();
        assert_eq!(budget.max_results, 1_000);
        assert_eq!(budget.max_bytes, 4 * 1024 * 1024);
        assert_eq!(budget.max_scan, 100_000);
        assert_eq!(budget.max_aggregation_memory, 4_194_304);
        let chain = admitted_chain();
        assert_eq!(chain.max_total_pages, 16);
        assert_eq!(chain.max_total_entities, 10_000);
        assert_eq!(chain.max_identity_examinations, 100_000);
    }
}
