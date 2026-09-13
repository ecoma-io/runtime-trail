//! The OTLP/HTTP surface: the three protobuf export endpoints.
//!
//! `POST /v1/traces`, `/v1/metrics`, `/v1/logs` — `application/x-protobuf`
//! request bodies handed to the shared admission pipeline as raw bytes,
//! and OTLP protobuf responses (or a transport-edge refusal) back. The
//! wire behaviour of every refusal is the backpressure architecture's
//! contract (`docs/architecture/runtime-constraints.md`, "The backpressure
//! architecture", and the signal table in the ingestion crate):
//!
//! | Signal              | HTTP answer                                            |
//! | ------------------- | ------------------------------------------------------ |
//! | `QueueSaturated`    | **429** + `Retry-After` — the one retryable signal      |
//! | `Draining`          | **503** — the closing signal, answered before reading   |
//! | `PayloadOverCap`    | **413** naming the ceiling, refused before parsing      |
//! | `MalformedRequest`  | **400** naming the decode failure, non-retryable        |
//! | `ExportOverCap`     | **200** + `partial_success` naming the budget           |
//! | per-record refusals | **200** + `partial_success` naming rejected positions   |
//!
//! Two answers are protocol gates, not admission signals: a request that
//! declares a content-type this phase does not speak (anything but
//! `application/x-protobuf` — the OTLP spec's JSON encoding included) is
//! refused with **415** naming the supported encoding, before the body is
//! read; and a body that never arrives in full gets **400** naming the
//! failed read, never dressed up as an over-ceiling refusal.
//!
//! The transport-edge body bound is the same number the pipeline gates
//! with ([`CoreRuntime::payload_ceiling_bytes`]): the body is read only up
//! to the ceiling, so an over-ceiling export is refused before it is ever
//! decoded — and before its bytes are buffered past the bound.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use prost::Message;
use runtime_trail_telemetry_ingestion::{
    AdmissionSignal, ExportLogsPartialSuccess, ExportLogsServiceResponse,
    ExportMetricsPartialSuccess, ExportMetricsServiceResponse, ExportOutcome,
    ExportTracePartialSuccess, ExportTraceServiceResponse, RecordOutcome,
};

use crate::runtime::CoreRuntime;

/// The `Retry-After` hint a saturated queue answers with, in seconds.
///
/// The contract names the header, not a value; a local collector's retry
/// is cheap, so one second. It is a hint, not a rate-limit contract.
const RETRY_AFTER_SECS: &str = "1";

/// The one content-type this phase speaks: OTLP/HTTP protobuf.
const PROTOBUF_MEDIA_TYPE: &str = "application/x-protobuf";

/// How many rejected positions a `partial_success` message names in full
/// before the rest are summarized by count.
///
/// The rejected *count* is always complete; the naming is bounded so a
/// pathologically refusy export cannot turn the response message itself
/// into an unbounded write.
const MAX_NAMED_POSITIONS: usize = 64;

/// Counts the export's rejected records and names their positions.
///
/// Positions are flat document-order indexes — the numbering the OTLP
/// `partial_success` contract asks for — and every named position carries
/// its refusal reason. Returns the complete rejected count and the bounded
/// position message.
pub(crate) fn rejected_summary(outcome: &ExportOutcome) -> (usize, String) {
    let mut rejected = 0usize;
    let mut message = String::new();
    for (position, record) in outcome.records.iter().enumerate() {
        if let RecordOutcome::Rejected { reason } = record {
            rejected += 1;
            if rejected <= MAX_NAMED_POSITIONS {
                if rejected > 1 {
                    message.push_str("; ");
                }
                let _ = write!(message, "position {position}: {reason}");
            }
        }
    }
    if rejected > MAX_NAMED_POSITIONS {
        message.push_str("; ");
        let _ = write!(
            message,
            "{not_named} further rejected records not named",
            not_named = rejected - MAX_NAMED_POSITIONS,
        );
    }
    (rejected, message)
}

/// The rejected count on the wire: OTLP counts refusals in an `i64`.
///
/// A count past `i64::MAX` is unreachable under the admission budgets, and
/// saturating is the honest direction over a wrong count.
pub(crate) fn rejected_count(rejected: usize) -> i64 {
    i64::try_from(rejected).unwrap_or(i64::MAX)
}

/// `POST /v1/traces`
///
/// # Errors
///
/// Never a transport error: every outcome is an HTTP answer.
pub(crate) async fn export_traces(
    State(runtime): State<Arc<CoreRuntime>>,
    request: Request,
) -> Response {
    export(runtime, request, Signal::Traces).await
}

/// `POST /v1/metrics`
///
/// # Errors
///
/// Never a transport error: every outcome is an HTTP answer.
pub(crate) async fn export_metrics(
    State(runtime): State<Arc<CoreRuntime>>,
    request: Request,
) -> Response {
    export(runtime, request, Signal::Metrics).await
}

/// `POST /v1/logs`
///
/// # Errors
///
/// Never a transport error: every outcome is an HTTP answer.
pub(crate) async fn export_logs(
    State(runtime): State<Arc<CoreRuntime>>,
    request: Request,
) -> Response {
    export(runtime, request, Signal::Logs).await
}

/// Which of the three export endpoints a request arrived on — the signal
/// family it admits and the response message it encodes.
#[derive(Clone, Copy, Debug)]
enum Signal {
    Traces,
    Metrics,
    Logs,
}

/// The one OTLP/HTTP path: drain check, content-type gate, bounded body
/// read, admission, wire mapping.
async fn export(runtime: Arc<CoreRuntime>, request: Request, signal: Signal) -> Response {
    // The closing gate answers before the body is read: a draining runtime
    // refuses new telemetry outright and owes no buffering for it.
    if runtime.is_draining() {
        return draining_response();
    }
    // The protocol gate, also before the body is read: a request that
    // declares an encoding this phase does not speak is told so plainly.
    if let Some(refusal) = content_type_gate(request.headers()) {
        return refusal;
    }
    // The over-ceiling answer when the length is *declared*: refused
    // before a single body byte is buffered.
    let ceiling_bytes = runtime.payload_ceiling_bytes();
    if declared_over_ceiling(request.headers(), ceiling_bytes) {
        return payload_over_cap_response(None, ceiling_bytes);
    }
    // The per-request body read deadline bounds a single body's buffering
    // window — a stuck or slow-drip client is refused 408 instead of holding
    // its bytes indefinitely ([ADR 0010]).
    let timeout = runtime.body_read_timeout();
    let payload = match read_body(request.into_body(), ceiling_bytes, timeout).await {
        Ok(Some(payload)) => payload,
        Ok(None) => return body_read_timeout_response(timeout),
        Err(error) => {
            // Two honest ways a read fails: the body grew past the ceiling
            // mid-read (an undeclared over-cap — the refusal, but the size
            // was never known), or the body never arrived in full — which
            // is not an over-cap refusal and must not wear one.
            if error
                .into_inner()
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                return payload_over_cap_response(None, ceiling_bytes);
            }
            return body_read_failure_response();
        }
    };
    let now = runtime.now();
    let result = match signal {
        Signal::Traces => runtime.pipeline().ingest_spans(now, &payload),
        Signal::Metrics => runtime.pipeline().ingest_metrics(now, &payload),
        Signal::Logs => runtime.pipeline().ingest_logs(now, &payload),
    };
    match result {
        Ok(outcome) => admitted_response(signal, &outcome),
        Err(export_signal) => match export_signal {
            AdmissionSignal::QueueSaturated {
                queue,
                ceiling_bytes,
                attempted_bytes,
            } => {
                tracing::warn!(
                    queue,
                    ceiling_bytes,
                    attempted_bytes,
                    "ingestion queue saturated: refusing the producer retryably"
                );
                let mut response = text_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    format!(
                        "the {queue} ingestion queue is at its {ceiling_bytes}-byte \
                         in-flight ceiling and a record of {attempted_bytes} bytes did \
                         not fit; the only retryable admission signal — retry"
                    ),
                );
                response.headers_mut().insert(
                    "Retry-After",
                    header::HeaderValue::from_static(RETRY_AFTER_SECS),
                );
                response
            }
            AdmissionSignal::PayloadOverCap {
                bytes,
                ceiling_bytes,
            } => payload_over_cap_response(Some(bytes), ceiling_bytes),
            AdmissionSignal::MalformedRequest { detail } => text_response(
                StatusCode::BAD_REQUEST,
                format!(
                    "the payload did not parse as an OTLP export request and cannot \
                     be retried: {detail}"
                ),
            ),
            AdmissionSignal::ExportOverCap { rejection } => {
                // The transport can carry partial_success, so the refusal
                // rides the response: the whole export was refused before
                // anything was admitted, and the budget names itself.
                let body = match signal {
                    Signal::Traces => encode(&ExportTraceServiceResponse {
                        partial_success: Some(ExportTracePartialSuccess {
                            rejected_spans: rejected_count(rejection.observed),
                            error_message: rejection.to_string(),
                        }),
                    }),
                    Signal::Metrics => encode(&ExportMetricsServiceResponse {
                        partial_success: Some(ExportMetricsPartialSuccess {
                            rejected_data_points: rejected_count(rejection.observed),
                            error_message: rejection.to_string(),
                        }),
                    }),
                    Signal::Logs => encode(&ExportLogsServiceResponse {
                        partial_success: Some(ExportLogsPartialSuccess {
                            rejected_log_records: rejected_count(rejection.observed),
                            error_message: rejection.to_string(),
                        }),
                    }),
                };
                protobuf(StatusCode::OK, body)
            }
            AdmissionSignal::Draining => draining_response(),
        },
    }
}

/// The admitted-export answer: a 200 whose `partial_success` counts and
/// names the export's refused records, per family. A fully admitted export
/// carries an empty `partial_success` — the transport's standing "here is
/// what happened to everything you sent".
fn admitted_response(signal: Signal, outcome: &ExportOutcome) -> Response {
    let (rejected, message) = rejected_summary(outcome);
    let body = match signal {
        Signal::Traces => encode(&ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: rejected_count(rejected),
                error_message: message,
            }),
        }),
        Signal::Metrics => encode(&ExportMetricsServiceResponse {
            partial_success: Some(ExportMetricsPartialSuccess {
                rejected_data_points: rejected_count(rejected),
                error_message: message,
            }),
        }),
        Signal::Logs => encode(&ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: rejected_count(rejected),
                error_message: message,
            }),
        }),
    };
    protobuf(StatusCode::OK, body)
}

/// The draining answer: 503, the closing signal — an explicit refusal, not
/// a silent hang.
fn draining_response() -> Response {
    text_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "the runtime is draining and admits no new telemetry; re-delivery \
         after restart will be admitted fresh",
    )
}

/// The over-ceiling answer: 413 naming the ceiling, refused before
/// parsing, never retryable. The size is named when the runtime knows it
/// (the pipeline's gate measured the payload); the transport-edge bound
/// refused before buffering names the ceiling alone.
fn payload_over_cap_response(bytes: Option<usize>, ceiling_bytes: usize) -> Response {
    let measured = bytes
        .map(|bytes| format!(" of {bytes} bytes"))
        .unwrap_or_default();
    text_response(
        StatusCode::PAYLOAD_TOO_LARGE,
        format!(
            "the OTLP payload{measured} exceeds the {ceiling_bytes}-byte ceiling; \
             refused before parsing, non-retryable"
        ),
    )
}

/// The content-type gate: this phase speaks OTLP/HTTP protobuf only. A
/// request that *declares* another encoding — including the OTLP spec's
/// JSON encoding, which is real but not supported here — is refused with
/// 415 naming the supported one, before the body is read: a protobuf
/// decode error is not an answer a JSON emitter can act on. A request that
/// declares nothing is handed to the decode, where the payload — not the
/// header — is the authority.
fn content_type_gate(headers: &axum::http::HeaderMap) -> Option<Response> {
    let declared = headers.get(header::CONTENT_TYPE)?.to_str().ok()?;
    let media_type = declared.split(';').next()?.trim();
    if media_type.eq_ignore_ascii_case(PROTOBUF_MEDIA_TYPE) {
        return None;
    }
    tracing::debug!(
        declared,
        "non-protobuf content-type refused at the HTTP layer"
    );
    Some(text_response(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        format!(
            "this phase speaks OTLP/HTTP protobuf only ({PROTOBUF_MEDIA_TYPE}); \
             the request declared {declared}, which is refused before reading"
        ),
    ))
}

/// Whether the request declares a content length over `ceiling_bytes` —
/// the one over-cap refusal that can be made before any body byte is
/// buffered. An undeclared length is decided by the bounded read.
fn declared_over_ceiling(headers: &axum::http::HeaderMap, ceiling_bytes: usize) -> bool {
    let ceiling = u64::try_from(ceiling_bytes).unwrap_or(u64::MAX);
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|declared| declared > ceiling)
}

/// The body-read answer: the payload never arrived complete, so there is
/// nothing to parse, no ceiling was crossed, and nothing was admitted. The
/// honest answer names the failed read — never an over-cap refusal the
/// payload did not commit.
fn body_read_failure_response() -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        "the request body could not be read in full; the payload never \
         arrived complete, so nothing was parsed or admitted",
    )
}

/// The read-timeout answer: the body did not arrive in full within the
/// per-request deadline. It is a transport refusal, not an admission signal:
/// the payload never arrived, so there is nothing to parse and a retry of the
/// same slow body is not the answer ([ADR 0010]).
///
/// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
fn body_read_timeout_response(timeout: Duration) -> Response {
    text_response(
        StatusCode::REQUEST_TIMEOUT,
        format!(
            "the request body did not arrive in full within the {}-millisecond \
             read deadline; nothing was parsed or admitted, and the operation \
             was refused at the transport edge",
            timeout.as_millis(),
        ),
    )
}

/// Reads the request body with two bounds: the transport-edge byte ceiling
/// (shared with the per-request `to_bytes` contract) and the per-request read
/// timeout ([ADR 0010]).
///
/// Returns `Ok(Some(payload))` on a full read within the deadline, `Ok(None)`
/// when the deadline elapsed first, and `Err(_)` when the body errored (the
/// caller tells a length-limit error from a broken connection).
///
/// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
async fn read_body(
    body: axum::body::Body,
    ceiling_bytes: usize,
    timeout: Duration,
) -> Result<Option<bytes::Bytes>, axum::Error> {
    match tokio::time::timeout(timeout, to_bytes(body, ceiling_bytes)).await {
        Ok(result) => result.map(Some),
        Err(_elapsed) => Ok(None),
    }
}

/// The aggregate in-flight body-budget gate for the OTLP/HTTP routes
/// ([ADR 0010]).
///
/// Applied as `middleware::from_fn` in `build_router`, this runs for every
/// request but only acts on the three OTLP/HTTP export endpoints: it charges
/// the request's declared body length (≤ the payload ceiling) to the shared
/// transport-edge budget before the body is read, and releases it when the
/// handler is done. A request that would push the aggregate past its budget is
/// refused **429 + `Retry-After`** before a body byte is buffered — the honest
/// retryable answer for transient transport-edge overload, sibling to queue
/// saturation.
///
/// Requests the earlier honest gates own are passed through without charging:
/// a draining runtime (the handler answers 503 before reading) and a declared
/// over-ceiling body (the handler answers 413 before reading). Non-OTLP
/// surfaces (health, version, the UI) never buffer a request body and are
/// passed through.
pub(crate) async fn inflight_body_guard(
    State(runtime): State<Arc<CoreRuntime>>,
    request: Request,
    next: Next,
) -> Response {
    // The three OTLP/HTTP export endpoints are the only routes that buffer a
    // request body. Everything else — health, version, the UI, the gRPC
    // services (which acquire on the same budget at their own seam) — passes
    // through untouched.
    if request.method() != Method::POST || !is_otlp_http_export(request.uri().path()) {
        return next.run(request).await;
    }
    let ceiling_bytes = runtime.payload_ceiling_bytes();
    // Draining and declared-over-ceiling are answered by their own gates
    // before any body byte is buffered; charging the aggregate for them would
    // only contrive a 429 where the contracted answer is 503/413.
    if runtime.is_draining() || declared_over_ceiling(request.headers(), ceiling_bytes) {
        return next.run(request).await;
    }
    let charge = declared_length_bounded(request.headers(), ceiling_bytes);
    let Some(_guard) = runtime.inflight_body_budget().try_acquire(charge) else {
        return inflight_over_budget_response(runtime.inflight_body_budget().ceiling_bytes());
    };
    // The guard is held across the whole handler call: the body is buffered
    // inside it, and no body byte is buffered before the acquire.
    next.run(request).await
}

/// Whether the path is one of the three OTLP/HTTP export endpoints.
fn is_otlp_http_export(path: &str) -> bool {
    matches!(path, "/v1/traces" | "/v1/metrics" | "/v1/logs")
}

/// The charge a request makes against the aggregate body budget: the declared
/// `Content-Length`, bounded to the payload ceiling — the worst case the
/// bounded read can buffer. An absent or unparseable length is charged at the
/// ceiling (a chunked body with no declared size could still buffer up to it).
fn declared_length_bounded(headers: &axum::http::HeaderMap, ceiling_bytes: usize) -> usize {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .map_or(ceiling_bytes, |declared| declared.min(ceiling_bytes))
}

/// The aggregate-over-budget answer: 429 + `Retry-After`, the retryable
/// transient-overload shape, naming the budget. A request refused here holds
/// no body bytes — the refusal is the whole answer.
fn inflight_over_budget_response(budget_bytes: usize) -> Response {
    let mut response = text_response(
        StatusCode::TOO_MANY_REQUESTS,
        format!(
            "the transport edge's in-flight-body budget of {budget_bytes} bytes \
             is exhausted; buffering another body now would breach the runtime's \
             bounded-memory law. Retry when a buffered body completes"
        ),
    );
    response.headers_mut().insert(
        "Retry-After",
        header::HeaderValue::from_static(RETRY_AFTER_SECS),
    );
    response
}

/// An OTLP protobuf answer: 200, `application/x-protobuf`.
fn protobuf(status: StatusCode, body: Vec<u8>) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/x-protobuf")],
        body,
    )
        .into_response()
}

/// A refusal answer: an HTTP status and a plain-text reason.
fn text_response(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        message.into(),
    )
        .into_response()
}

/// Protobuf-encodes an OTLP response message.
fn encode<M: Message>(message: &M) -> Vec<u8> {
    message.encode_to_vec()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use prost::Message;
    use runtime_trail_storage_memory::MemoryConfig;
    use runtime_trail_telemetry_ingestion::fixtures as fx;
    use runtime_trail_telemetry_ingestion::{
        ExportLogsServiceResponse, ExportMetricsServiceResponse, ExportOutcome,
        ExportTraceServiceResponse, RecordOutcome, RecordRejection, Unrepresentable,
    };
    use runtime_trail_telemetry_model::BudgetLimits;
    use tower::ServiceExt;

    use crate::runtime::{CoreRuntime, RuntimeConfig};
    use crate::test_support;
    use crate::{ServerConfig, build_router};

    /// Polls until the pump has made `expected` records resident, with a
    /// hard bound so a stuck pump fails the test instead of hanging it.
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

    async fn post(router: Router, path: &str, body: Vec<u8>) -> axum::response::Response {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/x-protobuf")
            .body(Body::from(body))
            .expect("a static request builds");
        router
            .oneshot(request)
            .await
            .expect("the router answers every request")
    }

    async fn body_bytes(response: axum::response::Response) -> Vec<u8> {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body reads")
            .to_vec()
    }

    /// One legal span, as an emitter would put it on the wire.
    fn one_span_payload(trace_id: [u8; 16], span_id: [u8; 8]) -> Vec<u8> {
        fx::encode(&fx::traces_request(vec![fx::resource_spans(
            None,
            vec![fx::scope_spans(
                None,
                vec![fx::trace_span("handle", trace_id, span_id)],
            )],
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

    fn one_log_payload() -> Vec<u8> {
        fx::encode(&fx::logs_request(vec![fx::resource_logs(
            None,
            vec![fx::scope_logs(
                Some(fx::scope("app")),
                vec![fx::log_record()],
            )],
        )]))
    }

    /// The happy path on every endpoint: a 200 whose `partial_success`
    /// names nothing, with the records actually resident behind it — the
    /// admitted-then-kept pipeline closed over the wire.
    #[tokio::test]
    async fn export_keeps_the_records_and_answers_empty_partial_success() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let response = post(router, "/v1/traces", one_span_payload(fx::T1, fx::S1)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/x-protobuf");
        let answer = ExportTraceServiceResponse::decode(&body_bytes(response).await[..])
            .expect("the answer decodes");
        let partial = answer
            .partial_success
            .expect("every answer carries partial_success");
        assert_eq!(partial.rejected_spans, 0, "nothing was refused");
        assert_eq!(partial.error_message, "", "full success names nothing");

        let response = post(
            build_router(Arc::clone(&runtime), ServerConfig::default()),
            "/v1/metrics",
            one_point_payload(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let answer = ExportMetricsServiceResponse::decode(&body_bytes(response).await[..])
            .expect("the answer decodes");
        let partial = answer.partial_success.expect("partial_success is present");
        assert_eq!(partial.rejected_data_points, 0);
        assert_eq!(partial.error_message, "");

        let response = post(
            build_router(Arc::clone(&runtime), ServerConfig::default()),
            "/v1/logs",
            one_log_payload(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let answer = ExportLogsServiceResponse::decode(&body_bytes(response).await[..])
            .expect("the answer decodes");
        let partial = answer.partial_success.expect("partial_success is present");
        assert_eq!(partial.rejected_log_records, 0);
        assert_eq!(partial.error_message, "");

        wait_for_resident(&runtime, 3);
        runtime.shutdown();
    }

    /// The poison-position contract: exactly the refused record is named,
    /// by its flat document-order position, and the healthy rest of the
    /// export still keeps.
    #[tokio::test]
    async fn partial_success_names_exactly_the_poison_position() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let mut poison = fx::trace_span("poison", fx::T1, fx::S2);
        poison.trace_id = vec![0x01, 0x02, 0x03]; // a wrong-width id: unrepresentable
        let request = fx::traces_request(vec![fx::resource_spans(
            None,
            vec![fx::scope_spans(
                None,
                vec![
                    fx::trace_span("before", fx::T1, fx::S1),
                    poison,
                    fx::trace_span("after", fx::T2, fx::S1),
                ],
            )],
        )]);

        let response = post(router, "/v1/traces", fx::encode(&request)).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the export still answers"
        );
        let answer = ExportTraceServiceResponse::decode(&body_bytes(response).await[..])
            .expect("the answer decodes");
        let partial = answer.partial_success.expect("partial_success is present");
        assert_eq!(partial.rejected_spans, 1, "exactly the poison record");
        assert!(
            partial.error_message.contains("position 1"),
            "the refusal names the flat position: {:?}",
            partial.error_message
        );
        assert!(
            !partial.error_message.contains("position 0")
                && !partial.error_message.contains("position 2"),
            "only the poison record is named: {:?}",
            partial.error_message
        );

        wait_for_resident(&runtime, 2);
        runtime.shutdown();
    }

    /// Saturation is the one retryable refusal, and it says so on the wire:
    /// 429 with a `Retry-After` hint.
    #[tokio::test]
    /// The freeze holds a std mutex across the `post`/`call` awaits on
    /// purpose: the frozen store is the test's determinism device, and the
    /// awaited handler runs on the router, not the store.
    #[allow(clippy::await_holding_lock)]
    async fn saturated_queue_answers_429_with_retry_after() {
        let runtime = CoreRuntime::build(saturation_config()).expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // Park the pump on the frozen store before the fill so nothing
        // frees queue space mid-test: a freeze alone leaves the pump one
        // pop of headroom, and a pop after the fill frees exactly the slot
        // this export then fits (the filed flake, #9).
        let _frozen = test_support::freeze_and_park_pump(&runtime);
        let mut saturated = false;
        for i in 0..2_048_u32 {
            let mut span_id = [0_u8; 8];
            span_id[..4].copy_from_slice(&i.to_be_bytes());
            let span = fx::trace_span("saturate", fx::T1, span_id);
            let request = fx::traces_request(vec![fx::resource_spans(
                None,
                vec![fx::scope_spans(None, vec![span])],
            )]);
            if runtime
                .pipeline()
                .ingest_spans(fx::now(), &fx::encode(&request))
                .is_err()
            {
                saturated = true;
                break;
            }
        }
        assert!(saturated, "a 4-KiB queue saturates within 2 Ki spans");

        let response = post(router, "/v1/traces", one_span_payload(fx::T2, fx::S2)).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers()["retry-after"],
            "1",
            "saturation is the one retryable signal and carries a retry hint"
        );
    }

    /// Draining answers the closing signal before the body is even read.
    #[tokio::test]
    async fn draining_runtime_answers_503() {
        let runtime = test_support::runtime();
        runtime.begin_drain();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let response = post(router, "/v1/traces", one_span_payload(fx::T1, fx::S1)).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        runtime.shutdown();
    }

    /// An over-ceiling payload is refused at the transport edge, before any
    /// parsing, and the answer names the ceiling.
    #[tokio::test]
    async fn over_ceiling_payload_is_refused_413_before_parsing() {
        let runtime = CoreRuntime::build(RuntimeConfig {
            payload_ceiling_bytes: 1024,
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // Zeros are not valid protobuf: if this body were ever decoded the
        // answer would be a 400, so a 413 proves the refusal preceded parse.
        let response = post(router, "/v1/traces", vec![0_u8; 2048]).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let message = String::from_utf8_lossy(&body_bytes(response).await).to_string();
        assert!(message.contains("1024"), "the ceiling is named: {message}");
        runtime.shutdown();
    }

    /// Bytes that never parse are refused with a non-retryable 400 that
    /// names the failure.
    #[tokio::test]
    async fn malformed_payload_answers_400_and_names_the_failure() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let response = post(router, "/v1/logs", vec![0xFF, 0xFF, 0xFF, 0xFF, 0x01]).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let message = String::from_utf8_lossy(&body_bytes(response).await).to_string();
        assert!(
            message.contains("did not parse"),
            "the failure is named: {message}"
        );
        runtime.shutdown();
    }

    /// A request that declares an encoding this phase does not speak — the
    /// OTLP spec's JSON encoding included — is refused with 415 naming the
    /// supported one, before the body is read. Never a protobuf decode
    /// error the JSON emitter cannot act on.
    #[tokio::test]
    async fn json_content_type_answers_415_naming_protobuf_only() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/json")
            .body(Body::from(br#"{"resource_spans": []}"#.to_vec()))
            .expect("a static request builds");
        let response = router
            .oneshot(request)
            .await
            .expect("the router answers every request");
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let message = String::from_utf8_lossy(&body_bytes(response).await).to_string();
        assert!(
            message.contains("application/x-protobuf") && message.contains("application/json"),
            "the supported and the declared encodings are both named: {message}"
        );
        runtime.shutdown();
    }

    /// An over-ceiling *declared* length is refused before a single body
    /// byte is buffered — the request says so itself.
    #[tokio::test]
    async fn declared_over_ceiling_length_refused_413_before_reading() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .header("content-length", "9999999999")
            .body(Body::from(one_span_payload(fx::T1, fx::S1)))
            .expect("a static request builds");
        let response = router
            .oneshot(request)
            .await
            .expect("the router answers every request");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let message = String::from_utf8_lossy(&body_bytes(response).await).to_string();
        assert!(
            message.contains("exceeds") && message.contains("4194304"),
            "the ceiling is named: {message}"
        );
        runtime.shutdown();
    }

    /// A body that never arrives complete gets its own honest answer: the
    /// read failed — not a 413 dressing a broken connection up as an
    /// over-ceiling payload.
    #[tokio::test]
    async fn a_body_that_never_completes_answers_the_failed_read() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // One chunk, then the body errors: the payload never completes.
        let truncated = Body::from_stream(tonic::codegen::tokio_stream::iter(vec![
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"a partial OTLP payload")),
            Err(std::io::Error::other("the connection broke")),
        ]));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .body(truncated)
            .expect("a static request builds");
        let response = router
            .oneshot(request)
            .await
            .expect("the router answers every request");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let message = String::from_utf8_lossy(&body_bytes(response).await).to_string();
        assert!(
            message.contains("could not be read"),
            "the failed read is named: {message}"
        );
        assert!(
            !message.contains("ceiling"),
            "a broken body is not an over-ceiling payload: {message}"
        );
        runtime.shutdown();
    }

    /// The whole-export budget refusal rides `partial_success` too: the
    /// export was refused before anything was admitted, and the budget
    /// names itself with the observed count.
    #[tokio::test]
    async fn export_budget_refusal_rides_partial_success() {
        let runtime = CoreRuntime::build(RuntimeConfig {
            budgets: BudgetLimits {
                data_points_per_export: 1,
                ..BudgetLimits::default()
            },
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let two_points = fx::described_metric(
            "cpu.seconds",
            "described",
            "s",
            Vec::new(),
            vec![
                fx::number_point(fx::as_double(1.0)),
                fx::number_point(fx::as_double(2.0)),
            ],
        );
        let request = fx::metrics_request(vec![fx::resource_metrics(
            None,
            vec![fx::scope_metrics(None, vec![two_points])],
        )]);
        let response = post(router, "/v1/metrics", fx::encode(&request)).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a budget refusal still answers 200 through partial_success"
        );
        let answer = ExportMetricsServiceResponse::decode(&body_bytes(response).await[..])
            .expect("the answer decodes");
        let partial = answer.partial_success.expect("partial_success is present");
        assert_eq!(partial.rejected_data_points, 2, "the whole export");
        assert!(
            !partial.error_message.is_empty(),
            "the budget names itself: {:?}",
            partial.error_message
        );
        assert_eq!(
            runtime.store_stats().resident_records,
            0,
            "nothing from a refused export reached storage"
        );
        runtime.shutdown();
    }

    /// The saturation configuration: budgets whose legal records are
    /// small, a queue that saturates within a handful of them.
    fn saturation_config() -> RuntimeConfig {
        RuntimeConfig {
            store: MemoryConfig {
                max_records: u64::MAX,
                max_accounted_bytes: u64::MAX,
                admission_window: Duration::from_secs(3_600),
                series_cap: u64::MAX,
            },
            budgets: BudgetLimits {
                attributes_per_signal: 1,
                attributes_per_nested_set: 0,
                attribute_value_bytes: 64,
                span_events_per_span: 0,
                span_links_per_span: 0,
                exemplars_per_data_point: 0,
                key_value_list_depth: 1,
                data_points_per_export: 10,
                records_per_export: 10,
                numeric_vector_entries_per_data_point: 0,
            },
            payload_ceiling_bytes: 64 * 1024,
            queue_ceiling_bytes: 4096,
            inflight_body_ceiling_bytes: 1024 * 1024,
            body_read_timeout: Duration::from_secs(10),
            clock: Box::new(crate::runtime::SystemWallClock),
        }
    }

    /// The summary's naming bound is the message's only truncation: the
    /// first 64 rejected positions are named in full, the rest are
    /// summarized by count — and the rejected count itself is never
    /// truncated. Both transports encode through this one function.
    #[test]
    fn the_summary_names_sixty_four_positions_and_counts_the_rest() {
        use super::rejected_summary;

        let refused = |count: usize| ExportOutcome {
            records: (0..count)
                .map(|_| RecordOutcome::Rejected {
                    reason: RecordRejection::Unrepresentable(Unrepresentable::IdLength {
                        field: "trace_id",
                        expected: 16,
                        found: 3,
                    }),
                })
                .collect(),
        };

        let (rejected, message) = rejected_summary(&refused(2));
        assert_eq!(rejected, 2);
        assert_eq!(
            message,
            "position 0: trace_id carried 3 bytes where the model carries \
             16; position 1: trace_id carried 3 bytes where the model \
             carries 16"
        );

        let (rejected, message) = rejected_summary(&refused(70));
        assert_eq!(rejected, 70, "the count is complete");
        assert!(
            message.contains("position 0") && message.contains("position 63"),
            "the first and 64th positions are named: {message:?}"
        );
        assert!(
            !message.contains("position 64"),
            "the 65th position is summarized, not named: {message:?}"
        );
        assert!(
            message.contains("6 further rejected records not named"),
            "the truncation is stated: {message:?}"
        );
    }

    /// A body stream that never yields — the transport-edge drip the aggregate
    /// budget and the read deadline exist for.
    fn endless() -> Body {
        Body::from_stream(tonic::codegen::tokio_stream::pending::<
            Result<bytes::Bytes, std::io::Error>,
        >())
    }

    /// One HTTP/1.1 request over a real TCP connection; the whole response as
    /// bytes. The mirror of the crate-level helper, named differently so the
    /// two modules stay independent.
    async fn httpexchange(bind: std::net::SocketAddr, request: &str, body: &[u8]) -> Vec<u8> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(bind)
            .await
            .expect("a client connects");
        let head = format!(
            "{request}\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(head.as_bytes())
            .await
            .expect("the head writes");
        stream.write_all(body).await.expect("the body writes");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("the response reads");
        response
    }

    /// N concurrent slow-drip bodies over the aggregate in-flight-body budget
    /// are refused 429 + `Retry-After` at the transport edge, and the runtime
    /// stays alive for the surfaces that never buffer a body.
    #[tokio::test]
    async fn concurrent_slow_drip_bodies_over_the_aggregate_are_refused_429() {
        let runtime = CoreRuntime::build(RuntimeConfig {
            inflight_body_ceiling_bytes: 2000,
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // Two bodies each declare 1000 bytes and never deliver them: each
        // holds its charge, filling the 2 KiB aggregate exactly. The bodies
        // run as spawned tasks so the middleware's acquire (which precedes the
        // body read) happens before the third request arrives.
        let drip_request = |body: Body| {
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/x-protobuf")
                .header("content-length", "1000")
                .body(body)
                .expect("a static request builds")
        };
        let mut held = Vec::new();
        for _ in 0..2 {
            let router = router.clone();
            held.push(tokio::spawn(async move {
                router.oneshot(drip_request(endless())).await
            }));
        }

        // Wait for the aggregate to fill: both acquires must land before the
        // third request, so the third is refused for *this* test's reason.
        for _ in 0..5_000 {
            if runtime.inflight_body_budget().in_flight() == 2000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            runtime.inflight_body_budget().in_flight(),
            2000,
            "both drip bodies held their charges"
        );

        // The third body, also a drip, is refused before a body byte is
        // buffered: 429 + Retry-After, naming the budget.
        let refusal = router
            .clone()
            .oneshot(drip_request(endless()))
            .await
            .expect("the router answers every request");
        assert_eq!(refusal.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            refusal.headers()["retry-after"],
            "1",
            "the aggregate refusal is the retryable transient-overload answer"
        );
        let message = String::from_utf8_lossy(&body_bytes(refusal).await).to_string();
        assert!(
            message.contains("in-flight-body budget"),
            "the refused request names the budget: {message}"
        );

        // The runtime stays alive: a surface that never buffers a request
        // body still answers, even with the transport edge exhausted.
        let health = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("a static request builds"),
            )
            .await
            .expect("the router answers every request");
        assert_eq!(health.status(), StatusCode::OK);

        // Let the held bodies go: aborting the tasks cancels them at the next
        // scheduler tick, dropping the middleware's guard and returning the
        // charges — so the drain is polled, not asserted synchronously.
        for handle in held {
            handle.abort();
        }
        for _ in 0..5_000 {
            if runtime.inflight_body_budget().in_flight() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            runtime.inflight_body_budget().in_flight(),
            0,
            "every held charge is returned when the body future goes away"
        );
        runtime.shutdown();
    }

    /// A body within its declared length passes the gate unchanged — the
    /// aggregate budget never touches a healthy small export.
    #[tokio::test]
    async fn a_within_bound_body_still_passes_through_the_gate() {
        let runtime = CoreRuntime::build(RuntimeConfig {
            inflight_body_ceiling_bytes: 2000,
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // A declared length is the honest charge: a real client declares its
        // body, and the gate must admit it without holding the whole budget.
        let payload = one_span_payload(fx::T1, fx::S1);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .header("content-length", payload.len().to_string())
                    .body(Body::from(payload))
                    .expect("a static request builds"),
            )
            .await
            .expect("the router answers every request");
        assert_eq!(response.status(), StatusCode::OK);
        let answer = ExportTraceServiceResponse::decode(&body_bytes(response).await[..])
            .expect("the admitted answer decodes");
        assert!(
            answer.partial_success.is_some(),
            "the export is admitted through the gate carrying partial_success"
        );
        wait_for_resident(&runtime, 1);
        assert_eq!(
            runtime.inflight_body_budget().in_flight(),
            0,
            "the healthy body's charge was returned"
        );
        runtime.shutdown();
    }

    /// A body that never arrives in full within the per-request read deadline
    /// is refused 408 — and the runtime stays alive.
    #[tokio::test]
    async fn a_body_beyond_the_read_deadline_answers_408() {
        let runtime = CoreRuntime::build(RuntimeConfig {
            body_read_timeout: Duration::from_millis(50),
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // A body that never yields a byte: without the deadline the handler
        // would wait forever; with it, the bounded read answers 408.
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .body(endless())
                    .expect("a static request builds"),
            )
            .await
            .expect("the router answers every request");
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        let message = String::from_utf8_lossy(&body_bytes(response).await).to_string();
        assert!(
            message.contains("read deadline"),
            "the deadline is named: {message}"
        );
        assert_eq!(
            runtime.inflight_body_budget().in_flight(),
            0,
            "the timed-out body returned its charge"
        );

        // The runtime stays alive and still admits a healthy export.
        let healthy = post(router, "/v1/traces", one_span_payload(fx::T2, fx::S2)).await;
        assert_eq!(healthy.status(), StatusCode::OK);
        wait_for_resident(&runtime, 1);
        runtime.shutdown();
    }

    /// The real socket proves the served wire refuses a slow-drip body with
    /// 408 when it passes the read deadline, and that the process keeps
    /// serving after — the whole accept loop, not just the in-process router.
    #[tokio::test]
    async fn a_real_socket_drip_is_refused_408_and_the_server_keeps_serving() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let runtime = CoreRuntime::build(RuntimeConfig {
            body_read_timeout: Duration::from_millis(50),
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral bind");
        let addr = listener.local_addr().expect("an ephemeral address");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());
        let (trigger, gate) = tokio::sync::oneshot::channel::<()>();
        let drain_runtime = Arc::clone(&runtime);
        let serve = axum::serve(listener, router).with_graceful_shutdown(async move {
            let _ = gate.await;
            drain_runtime.begin_drain();
        });
        let server = tokio::spawn(async move { serve.await });

        // A connection that declares a large body and sends a trickle, then
        // stalls: the read deadline fires and the server answers 408.
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("a client connects");
        let head = "POST /v1/traces HTTP/1.1\r\nHost: localhost\r\n\
                    Content-Type: application/x-protobuf\r\n\
                    Content-Length: 4000000\r\n\r\n";
        stream
            .write_all(head.as_bytes())
            .await
            .expect("the head writes");
        stream
            .write_all(b"trickle")
            .await
            .expect("the trickle writes");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("the server answers in time")
            .expect("the response reads");
        let answer = String::from_utf8_lossy(&response);
        assert!(
            answer.starts_with("HTTP/1.1 408"),
            "the served wire refuses the drip with 408: {answer}"
        );

        // The server is still serving: a healthy export succeeds next.
        let export = httpexchange(
            addr,
            "POST /v1/traces HTTP/1.1",
            &one_span_payload(fx::T1, fx::S1),
        )
        .await;
        let export = String::from_utf8_lossy(&export);
        assert!(
            export.starts_with("HTTP/1.1 200 OK"),
            "the serve loop lives on: {export}"
        );
        wait_for_resident(&runtime, 1);

        trigger.send(()).expect("the server is still running");
        server
            .await
            .expect("the server task joins")
            .expect("the server serves cleanly");
        runtime.shutdown();
    }
}
