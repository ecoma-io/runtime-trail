//! The MCP transport: JSON-RPC 2.0 over stdio with LSP-style
//! `Content-Length` framing — a thin, synchronous protocol layer, so the
//! crate needs no async runtime (tokio is confined to `crates/server` by
//! AGENTS.md; the maintained MCP SDKs are tokio-based and are deliberately
//! not taken — see the report for issue #36).
//!
//! Surface:
//! - `initialize` → `{"protocolVersion", "capabilities": {"tools": {}},
//!   "serverInfo": {"name", "version"}}`; `notifications/initialized` and
//!   any other notification (no `id`) is never answered.
//! - `tools/list` → the four committed tools with their JSON-Schema
//!   `inputSchema` (see [`crate::tools`]).
//! - `tools/call` → `{"content": [{"type": "text", "text": <envelope
//!   JSON>}], "isError": false}`; a tool refusal answers `isError: true`
//!   with the refusal's message as the text content — the same vocabulary
//!   the HTTP surface uses, never a fabricated envelope.
//! - `ping` → `{}`; unknown methods answer `-32601`; a well-framed body
//!   that is not JSON answers `-32700` and the session stays alive;
//!   malformed requests answer `-32600`/`-32602`.
//!
//! Message bodies are bounded: the framing reads at most `MAX_MESSAGE_BYTES`
//! per message, so an oversized or hostile client cannot grow the process
//! without limit (the runtime's own payload ceiling is a transport-edge
//! concern of `crates/server`; here the ceiling protects the framing).

use std::io::{self, BufRead, Write};

use runtime_trail_investigation::TelemetryStore;
use serde_json::{Value as JsonValue, json};

use crate::render;
use crate::tools;

/// The MCP protocol version this server speaks (tools capability only).
pub const PROTOCOL_VERSION: &str = "2024-11-05";

/// The `serverInfo.name` reported by `initialize`.
pub const SERVER_NAME: &str = "runtime-trail-mcp";

/// The largest single JSON-RPC message this surface reads, bytes.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

// JSON-RPC error codes.
const PARSE_ERROR: i64 = -32_700;
const INVALID_REQUEST: i64 = -32_600;
const METHOD_NOT_FOUND: i64 = -32_601;
const INVALID_PARAMS: i64 = -32_602;

#[derive(Debug)]
enum ReadOutcome {
    /// The reader reached a clean EOF between messages: the session is over.
    Eof,
    /// A well-framed body that is not JSON: answer `-32700` and continue.
    ParseError,
    /// A JSON-RPC message to dispatch.
    Message(JsonValue),
}

/// Serves one session over the given streams: reads framed JSON-RPC
/// messages until EOF, answering each request in turn. A notification (a
/// message without `id`) is never answered.
///
/// The loop is synchronous and single-threaded: one request at a time,
/// answered strictly in arrival order — the MCP stdio contract over a
/// memory store needs no concurrency (the store's own contract is
/// single-threaded per call).
///
/// # Errors
///
/// Returns the I/O error that ended the session: a framing violation
/// (missing or invalid `Content-Length`, an oversized body, truncated
/// body) or an underlying stream error.
pub fn serve(
    store: &dyn TelemetryStore,
    mut reader: impl BufRead,
    mut writer: impl Write,
) -> io::Result<()> {
    loop {
        match read_message(&mut reader)? {
            ReadOutcome::Eof => return Ok(()),
            ReadOutcome::ParseError => {
                write_message(
                    &mut writer,
                    &error_response(&JsonValue::Null, PARSE_ERROR, "parse error"),
                )?;
                writer.flush()?;
            }
            ReadOutcome::Message(message) => {
                if let Some(response) = handle(&message, store) {
                    write_message(&mut writer, &response)?;
                    writer.flush()?;
                }
            }
        }
    }
}

/// Serves one session over `stdin`/`stdout` — the MCP stdio transport.
///
/// # Errors
///
/// Returns the I/O error that ended the session (see [`serve`]).
pub fn serve_stdio(store: &dyn TelemetryStore) -> io::Result<()> {
    serve(store, io::stdin().lock(), io::stdout().lock())
}

// ---------------------------------------------------------------------------
// Framing: LSP-style `Content-Length: N\r\n\r\n<body>`.
// ---------------------------------------------------------------------------

/// Reads one framed message: headers until a blank line, then exactly the
/// declared body length. `Eof` at a clean EOF between messages; a body
/// that is not JSON is a `ParseError`, not a fatal framing violation.
fn read_message(reader: &mut impl BufRead) -> io::Result<ReadOutcome> {
    let mut length: Option<usize> = None;
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            return Ok(ReadOutcome::Eof);
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if let Some(value) = line.strip_prefix("Content-Length:") {
            let value = value.trim();
            length = Some(value.parse::<usize>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid Content-Length header")
            })?);
        }
    }
    let length = length.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "missing Content-Length header")
    })?;
    if length > MAX_MESSAGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message exceeds the {MAX_MESSAGE_BYTES}-byte framing ceiling"),
        ));
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    match serde_json::from_slice(&body) {
        Ok(message) => Ok(ReadOutcome::Message(message)),
        Err(_) => Ok(ReadOutcome::ParseError),
    }
}

/// Writes one framed message.
fn write_message(writer: &mut impl Write, message: &JsonValue) -> io::Result<()> {
    let body = serde_json::to_vec(message)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "response is not encodable"))?;
    write!(writer, "Content-Length: {}\r\n\r\n", body.len())?;
    writer.write_all(&body)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// JSON-RPC dispatch.
// ---------------------------------------------------------------------------

/// Handles one client message. Notifications (no `id`) are never answered.
fn handle(message: &JsonValue, store: &dyn TelemetryStore) -> Option<JsonValue> {
    if message.get("jsonrpc").is_none() {
        // A malformed envelope: answer only if the message carries an id.
        return message
            .get("id")
            .cloned()
            .map(|id| error_response(&id, INVALID_REQUEST, "invalid request"));
    }
    let Some(id) = message.get("id").cloned() else {
        return None; // A notification: never answered.
    };
    let Some(method) = message.get("method").and_then(JsonValue::as_str) else {
        return Some(error_response(
            &id,
            INVALID_REQUEST,
            "invalid request: missing method",
        ));
    };
    let params = message.get("params");
    match method {
        "initialize" => Some(result_response(
            &id,
            &json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": SERVER_NAME,
                    "version": crate::VERSION,
                },
            }),
        )),
        "ping" => Some(result_response(&id, &json!({}))),
        "tools/list" => {
            let tools = tools::TOOLS
                .iter()
                .map(|tool| {
                    json!({
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.input_schema,
                    })
                })
                .collect::<Vec<_>>();
            Some(result_response(&id, &json!({ "tools": tools })))
        }
        "tools/call" => {
            let Some(params) = params else {
                return Some(error_response(
                    &id,
                    INVALID_PARAMS,
                    "a tools/call needs params: {name, arguments}",
                ));
            };
            let name = params.get("name").and_then(JsonValue::as_str);
            let known = name.is_some_and(|name| tools::TOOLS.iter().any(|tool| tool.name == name));
            if !known {
                return Some(error_response(
                    &id,
                    INVALID_PARAMS,
                    &format!("unknown tool {name:?} — tools/list names the available tools"),
                ));
            }
            match dispatch(store, params).map(|envelope| render::render_investigation(&envelope)) {
                Ok(rendered) => Some(result_response(
                    &id,
                    &json!({
                        "content": [ { "type": "text", "text": rendered.to_string() } ],
                        "isError": false,
                    }),
                )),
                Err(error) => Some(result_response(
                    &id,
                    &json!({
                        "content": [ { "type": "text", "text": error.to_string() } ],
                        "isError": true,
                    }),
                )),
            }
        }
        _ => Some(error_response(&id, METHOD_NOT_FOUND, "Method not found")),
    }
}

/// Routes one `tools/call` to the named tool. The caller has already
/// validated the name against [`tools::TOOLS`], so dispatch can only fail
/// with a tool's own refusal.
fn dispatch(
    store: &dyn TelemetryStore,
    params: &JsonValue,
) -> Result<runtime_trail_investigation::Investigation, tools::ToolError> {
    let name = params
        .get("name")
        .and_then(JsonValue::as_str)
        .expect("the name was validated against TOOLS");
    let arguments = params
        .get("arguments")
        .ok_or_else(|| tools::ToolError::Parse("a tools/call needs arguments".to_owned()))?;
    match name {
        tools::INVESTIGATE_TRACE => tools::investigate_trace(store, arguments),
        tools::INVESTIGATE_LOG => tools::investigate_log(store, arguments),
        tools::INVESTIGATE_METRIC => tools::investigate_metric(store, arguments),
        tools::CONTINUE_INVESTIGATION => tools::continue_investigation(store, arguments),
        _ => unreachable!("the name was validated against TOOLS"),
    }
}

fn result_response(id: &JsonValue, result: &JsonValue) -> JsonValue {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error_response(id: &JsonValue, code: i64, message: &str) -> JsonValue {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Renders one framed message verbatim (as a client would write it).
    fn framed(body: &str) -> Vec<u8> {
        format!("Content-Length: {}\r\n\r\n{}", body.len(), body).into_bytes()
    }

    /// A store that resolves nothing: enough to prove the protocol's
    /// error paths answer cleanly without a fixture.
    fn empty_store() -> &'static dyn TelemetryStore {
        struct Empty;
        impl TelemetryStore for Empty {
            fn keep_span(
                &mut self,
                _admitted: runtime_trail_investigation::telemetry_model::Admitted<
                    std::sync::Arc<runtime_trail_investigation::telemetry_model::Span>,
                >,
            ) -> runtime_trail_investigation::KeepOutcome {
                unreachable!("the protocol never keeps")
            }
            fn keep_log_record(
                &mut self,
                _admitted: runtime_trail_investigation::telemetry_model::Admitted<
                    std::sync::Arc<runtime_trail_investigation::telemetry_model::LogRecord>,
                >,
            ) -> runtime_trail_investigation::KeepOutcome {
                unreachable!("the protocol never keeps")
            }
            fn keep_metric_point(
                &mut self,
                _admitted: runtime_trail_investigation::telemetry_model::Admitted<
                    std::sync::Arc<runtime_trail_investigation::telemetry_model::MetricPoint>,
                >,
                _stream: std::sync::Arc<
                    runtime_trail_investigation::telemetry_model::StreamIdentity,
                >,
            ) -> runtime_trail_investigation::KeepOutcome {
                unreachable!("the protocol never keeps")
            }
            fn span(
                &self,
                _entity: runtime_trail_investigation::telemetry_model::EntityId,
            ) -> Option<std::sync::Arc<runtime_trail_investigation::telemetry_model::Span>>
            {
                None
            }
            fn log_record(
                &self,
                _entity: runtime_trail_investigation::telemetry_model::EntityId,
            ) -> Option<std::sync::Arc<runtime_trail_investigation::telemetry_model::LogRecord>>
            {
                None
            }
            fn metric_point(
                &self,
                _entity: runtime_trail_investigation::telemetry_model::EntityId,
            ) -> Option<runtime_trail_investigation::PointView> {
                None
            }
            fn scan_spans(
                &self,
                _after: Option<runtime_trail_investigation::AdmissionKey>,
                _limit: usize,
            ) -> runtime_trail_investigation::ScanPage<
                std::sync::Arc<runtime_trail_investigation::telemetry_model::Span>,
            > {
                runtime_trail_investigation::ScanPage {
                    items: Vec::new(),
                    cursor: None,
                }
            }
            fn scan_log_records(
                &self,
                _after: Option<runtime_trail_investigation::AdmissionKey>,
                _limit: usize,
            ) -> runtime_trail_investigation::ScanPage<
                std::sync::Arc<runtime_trail_investigation::telemetry_model::LogRecord>,
            > {
                runtime_trail_investigation::ScanPage {
                    items: Vec::new(),
                    cursor: None,
                }
            }
            fn scan_metric_points(
                &self,
                _after: Option<runtime_trail_investigation::AdmissionKey>,
                _limit: usize,
            ) -> runtime_trail_investigation::ScanPage<runtime_trail_investigation::PointView>
            {
                runtime_trail_investigation::ScanPage {
                    items: Vec::new(),
                    cursor: None,
                }
            }
            fn enforce_retention(
                &mut self,
                _now: runtime_trail_investigation::telemetry_model::AdmissionTime,
            ) -> u64 {
                0
            }
            fn observe_admission_anomalies(&mut self, _total: u64) {}
            fn stats(&self) -> runtime_trail_investigation::StoreStats {
                runtime_trail_investigation::StoreStats::default()
            }
            fn mode_name(&self) -> &'static str {
                "empty"
            }
        }
        static EMPTY: Empty = Empty;
        &EMPTY
    }

    #[test]
    fn framing_round_trips_a_message_and_honors_eof() {
        let mut reader = Cursor::new(framed(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#));
        let outcome = read_message(&mut reader).expect("reads");
        match outcome {
            ReadOutcome::Message(message) => assert_eq!(message["method"], "ping"),
            other => panic!("a framed message must read, got {other:?}"),
        }
        assert!(matches!(
            read_message(&mut reader).expect("clean EOF"),
            ReadOutcome::Eof
        ));
    }

    #[test]
    fn framing_rejects_oversized_bodies() {
        let body =
            serde_json::to_string(&json!({ "unused": "x".repeat(MAX_MESSAGE_BYTES + 1) })).unwrap();
        let mut reader = Cursor::new(framed(&body));
        let error = read_message(&mut reader).expect_err("refused");
        assert!(error.to_string().contains("ceiling"));
    }

    #[test]
    fn framing_requires_content_length() {
        // A header section that ends without declaring a length: the
        // headers loop must have read a blank line before the EOF check,
        // or it reads a clean EOF instead of the framing violation.
        let mut reader = Cursor::new(b"{\"jsonrpc\":\"2.0\"}\r\n\r\n".to_vec());
        let error = read_message(&mut reader).expect_err("refused");
        assert!(error.to_string().contains("missing Content-Length"));
    }

    #[test]
    fn a_non_json_body_answers_parse_error_and_the_session_survives() {
        let mut reader = Cursor::new(framed("this is not json"));
        let mut writer = Vec::new();
        serve(empty_store(), &mut reader, &mut writer).expect("session survives a parse error");
        // serde_json's Map serializes keys in sorted order (no
        // preserve_order feature), so the expected wire bytes are sorted.
        let expected = framed(
            r#"{"error":{"code":-32700,"message":"parse error"},"id":null,"jsonrpc":"2.0"}"#,
        );
        assert_eq!(writer, expected);
    }

    #[test]
    fn initialize_negotiates_tools() {
        let message = json!({"jsonrpc":"2.0","id":1,"method":"initialize",
            "params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test"}}});
        let response = handle(&message, empty_store()).expect("answered");
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(response["result"]["serverInfo"]["name"], SERVER_NAME);
        assert!(response["result"]["capabilities"]["tools"].is_object());
    }

    #[test]
    fn notifications_are_never_answered() {
        let notification = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        assert_eq!(handle(&notification, empty_store()), None);
    }

    #[test]
    fn tools_list_advertises_the_four_tools() {
        let message = json!({"jsonrpc":"2.0","id":2,"method":"tools/list"});
        let response = handle(&message, empty_store()).expect("answered");
        let tools = response["result"]["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), 4);
        let names = tools
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                tools::INVESTIGATE_TRACE,
                tools::INVESTIGATE_LOG,
                tools::INVESTIGATE_METRIC,
                tools::CONTINUE_INVESTIGATION,
            ]
        );
        for tool in tools {
            assert!(
                tool["description"].as_str().unwrap_or_default().len() > 20,
                "descriptions must be substantive"
            );
            assert!(tool["inputSchema"].is_object());
        }
    }

    #[test]
    fn unknown_methods_answer_method_not_found() {
        let message = json!({"jsonrpc":"2.0","id":3,"method":"resources/list"});
        let response = handle(&message, empty_store()).expect("answered");
        assert_eq!(response["error"]["code"], METHOD_NOT_FOUND);
    }

    #[test]
    fn malformed_envelopes_answer_invalid_request() {
        let message = json!({"id": 4});
        let response = handle(&message, empty_store()).expect("answered");
        assert_eq!(response["error"]["code"], INVALID_REQUEST);
    }

    #[test]
    fn an_unknown_tool_is_invalid_params_not_fabricated() {
        let message = json!({"jsonrpc":"2.0","id":5,"method":"tools/call",
            "params":{"name":"no_such_tool","arguments":{"root_span":{"assigned":1}}}});
        let response = handle(&message, empty_store()).expect("answered");
        assert_eq!(response["error"]["code"], INVALID_PARAMS);
    }

    #[test]
    fn a_refused_tool_answers_is_error_with_the_http_wording() {
        let message = json!({"jsonrpc":"2.0","id":6,"method":"tools/call",
            "params":{"name":"investigate_trace","arguments":{"root_span":{"assigned":1}}}});
        let response = handle(&message, empty_store()).expect("answered");
        assert_eq!(response["result"]["isError"], true);
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text");
        assert!(
            text.contains("no resident record carries the requested subject"),
            "the refusal must carry the HTTP wording: {text}"
        );
    }
}
