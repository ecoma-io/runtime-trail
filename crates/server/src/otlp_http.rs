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
//! The transport-edge body bound is the same number the pipeline gates
//! with ([`CoreRuntime::payload_ceiling_bytes`]): the body is read only up
//! to the ceiling, so an over-ceiling export is refused before it is ever
//! decoded — and before its bytes are buffered past the bound.

use std::fmt::Write as _;

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use prost::Message;
use runtime_trail_telemetry_ingestion::{
    AdmissionSignal, ExportLogsPartialSuccess, ExportLogsServiceResponse,
    ExportMetricsPartialSuccess, ExportMetricsServiceResponse, ExportOutcome,
    ExportTracePartialSuccess, ExportTraceServiceResponse, RecordOutcome,
};

use crate::runtime::CoreRuntime;
use std::sync::Arc;

/// The `Retry-After` hint a saturated queue answers with, in seconds.
///
/// The contract names the header, not a value; a local collector's retry
/// is cheap, so one second. It is a hint, not a rate-limit contract.
const RETRY_AFTER_SECS: &str = "1";

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

/// The one OTLP/HTTP path: drain check, bounded body read, admission, wire
/// mapping.
async fn export(runtime: Arc<CoreRuntime>, request: Request, signal: Signal) -> Response {
    // The closing gate answers before the body is read: a draining runtime
    // refuses new telemetry outright and owes no buffering for it.
    if runtime.is_draining() {
        return draining_response();
    }
    let Ok(payload) = to_bytes(request.into_body(), runtime.payload_ceiling_bytes()).await else {
        // The reader only fails past the ceiling bound — the
        // transport-edge refusal, before parsing.
        return payload_over_cap_response(None, runtime.payload_ceiling_bytes());
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
        ExportLogsServiceResponse, ExportMetricsServiceResponse, ExportTraceServiceResponse,
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

        // Freeze the pump so nothing frees queue space mid-test.
        let _frozen = runtime.lock_store_for_test();
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
            },
            payload_ceiling_bytes: 64 * 1024,
            queue_ceiling_bytes: 4096,
            clock: Box::new(crate::runtime::SystemWallClock),
        }
    }
}
