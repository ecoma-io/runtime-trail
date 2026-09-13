//! The OTLP/gRPC surface: the three collector services, hand-written over
//! the vendored collector protos.
//!
//! The vendored codegen in the ingestion crate is prost-only — messages,
//! no service stubs — because build-time tonic codegen would need `protoc`
//! on every machine (ADR 0001). So the glue is written here, on tonic's
//! public server API, in the same shape its generated code takes: per
//! service, a server struct implementing `tower::Service` over the HTTP
//! request, dispatching the one RPC path (`/Export`) to
//! `tonic::server::Grpc::unary` with a `UnaryService` impl that calls the
//! shared pipeline.
//!
//! The codec is a passthrough: the gRPC frame's payload is taken as raw
//! `Bytes` — zero-copy — straight into `pipeline.ingest_*`, which does the
//! one authoritative prost decode. Requests are decoded exactly once;
//! responses are prost-encoded collector messages. An empty frame payload
//! is a legal empty request: the default OTLP export decodes from zero
//! bytes, so an empty export is `OK` with an empty `partial_success` —
//! never an internal error.
//!
//! Two answers are protocol gates, not admission signals: a request whose
//! content-type does not begin with `application/grpc` is refused with
//! HTTP 415 before the body is read (the gRPC-over-HTTP2 spec's rule,
//! quoted at [`is_grpc_content_type`]), and a draining runtime refuses an
//! export before its frame is buffered — still `UNAVAILABLE`.
//!
//! The wire behaviour of every refusal is the backpressure architecture's
//! contract (runtime-constraints.md; the signal table in the ingestion
//! crate):
//!
//! | Signal              | gRPC answer                              |
//! | ------------------- | ---------------------------------------- |
//! | `QueueSaturated`    | `RESOURCE_EXHAUSTED` — the one retryable |
//! | `Draining`          | `UNAVAILABLE` — the closing signal        |
//! | `PayloadOverCap`    | `INVALID_ARGUMENT` naming the ceiling     |
//! | `MalformedRequest`  | `INVALID_ARGUMENT` naming the failure     |
//! | `ExportOverCap`     | `OK` + `partial_success` naming the budget |
//! | per-record refusals | `OK` + `partial_success` naming positions |

use std::convert::Infallible;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes};
use prost::Message;
use runtime_trail_telemetry_ingestion::{
    AdmissionSignal, ExportLogsPartialSuccess, ExportLogsServiceResponse,
    ExportMetricsPartialSuccess, ExportMetricsServiceResponse, ExportOutcome,
    ExportTracePartialSuccess, ExportTraceServiceResponse,
};
use tonic::body::Body as GrpcBody;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::{Service, http};
use tonic::server::{Grpc, UnaryService};
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};

use crate::otlp_http::{rejected_count, rejected_summary};
use crate::runtime::CoreRuntime;

/// The gRPC answer when the transport-edge aggregate in-flight-body budget is
/// exhausted ([ADR 0010]): `RESOURCE_EXHAUSTED`, the retryable
/// transient-overload shape — the same family as queue saturation. The honest
/// message names the budget, not the ingestion queue.
///
/// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
fn inflight_over_budget_status(budget_bytes: usize) -> Status {
    tracing::warn!(
        budget_bytes,
        "transport-edge in-flight body budget exhausted: refusing the producer retryably"
    );
    Status::resource_exhausted(format!(
        "the transport edge's in-flight-body budget of {budget_bytes} bytes is \
         exhausted; buffering another body now would breach the runtime's \
         bounded-memory law. Retry when a buffered body completes"
    ))
}

/// The gRPC service prefix the OTLP/HTTP router nests the trace service
/// under (a gRPC method's path is `/<service>/<method>`).
pub(crate) const TRACE_SERVICE_PREFIX: &str =
    "/opentelemetry.proto.collector.trace.v1.TraceService";

/// The gRPC service prefix the metrics service is nested under.
pub(crate) const METRICS_SERVICE_PREFIX: &str =
    "/opentelemetry.proto.collector.metrics.v1.MetricsService";

/// The gRPC service prefix the logs service is nested under.
pub(crate) const LOGS_SERVICE_PREFIX: &str = "/opentelemetry.proto.collector.logs.v1.LogsService";

/// The method suffix, as the nested service sees it once the router has
/// stripped the service prefix.
const EXPORT_METHOD: &str = "/Export";

// ------------------------------------------------------------------
// The passthrough codec
// ------------------------------------------------------------------

/// A gRPC codec that decodes nothing and encodes prost: the request frame
/// arrives as raw `Bytes` (the transport's own buffer — no copy, no second
/// decode; the pipeline owns the one prost decode), and the response
/// message is prost-encoded for the frame writer.
struct PassthroughCodec<Res> {
    _response: PhantomData<fn() -> Res>,
}

impl<Res> PassthroughCodec<Res> {
    fn new() -> Self {
        Self {
            _response: PhantomData,
        }
    }
}

impl<Res> Codec for PassthroughCodec<Res>
where
    // `Message` already implies `Debug + Send + Sync`, which is everything
    // the `Codec` associated types demand beyond `'static`.
    Res: Message + 'static,
{
    type Encode = Res;
    type Decode = Bytes;
    type Encoder = ProtobufEncoder<Res>;
    type Decoder = PassthroughDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        ProtobufEncoder::new()
    }

    fn decoder(&mut self) -> Self::Decoder {
        PassthroughDecoder
    }
}

/// The decode half: the gRPC frame's payload, verbatim.
struct PassthroughDecoder;

impl Decoder for PassthroughDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        // Zero-copy: the frame's bytes leave the transport buffer as the
        // payload the pipeline will decode. An empty frame payload is
        // handed through too, as an empty item — it is a legal export, and
        // returning `None` here would report the message as *missing*
        // (an internal error) instead of empty.
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}

/// The encode half: a prost message into the gRPC frame buffer.
struct ProtobufEncoder<Res> {
    _response: PhantomData<fn() -> Res>,
}

impl<Res> ProtobufEncoder<Res> {
    fn new() -> Self {
        Self {
            _response: PhantomData,
        }
    }
}

impl<Res> Encoder for ProtobufEncoder<Res>
where
    Res: Message,
{
    type Item = Res;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item.encode(dst).map_err(|error| {
            Status::internal(format!("encoding the OTLP response failed: {error}"))
        })
    }
}

// ------------------------------------------------------------------
// The shared ingest step
// ------------------------------------------------------------------

/// One export admitted, or the signal that refused it — mapped per
/// transport by the service below.
fn ingest(
    runtime: &CoreRuntime,
    payload: &[u8],
    family: Family,
) -> Result<ExportOutcome, AdmissionSignal> {
    let now = runtime.now();
    match family {
        Family::Traces => runtime.pipeline().ingest_spans(now, payload),
        Family::Metrics => runtime.pipeline().ingest_metrics(now, payload),
        Family::Logs => runtime.pipeline().ingest_logs(now, payload),
    }
}

/// Which export endpoint a gRPC method admits for.
#[derive(Clone, Copy, Debug)]
enum Family {
    Traces,
    Metrics,
    Logs,
}

/// The three response messages differ in one field name each; the reply is
/// built per family from the shared outcome mapping.
fn trace_reply(
    outcome: Result<ExportOutcome, AdmissionSignal>,
) -> Result<ExportTraceServiceResponse, Status> {
    outcome.map_or_else(
        |signal| match signal {
            // The transport can carry partial_success, so the whole-export
            // budget refusal rides the response: nothing was admitted, and
            // the budget names itself.
            AdmissionSignal::ExportOverCap { rejection } => Ok(ExportTraceServiceResponse {
                partial_success: Some(ExportTracePartialSuccess {
                    rejected_spans: rejected_count(rejection.observed),
                    error_message: rejection.to_string(),
                }),
            }),
            signal => Err(signal_to_status(signal)),
        },
        |outcome| {
            let (rejected, message) = rejected_summary(&outcome);
            Ok(ExportTraceServiceResponse {
                partial_success: Some(ExportTracePartialSuccess {
                    rejected_spans: rejected_count(rejected),
                    error_message: message,
                }),
            })
        },
    )
}

fn metrics_reply(
    outcome: Result<ExportOutcome, AdmissionSignal>,
) -> Result<ExportMetricsServiceResponse, Status> {
    outcome.map_or_else(
        |signal| match signal {
            AdmissionSignal::ExportOverCap { rejection } => Ok(ExportMetricsServiceResponse {
                partial_success: Some(ExportMetricsPartialSuccess {
                    rejected_data_points: rejected_count(rejection.observed),
                    error_message: rejection.to_string(),
                }),
            }),
            signal => Err(signal_to_status(signal)),
        },
        |outcome| {
            let (rejected, message) = rejected_summary(&outcome);
            Ok(ExportMetricsServiceResponse {
                partial_success: Some(ExportMetricsPartialSuccess {
                    rejected_data_points: rejected_count(rejected),
                    error_message: message,
                }),
            })
        },
    )
}

fn logs_reply(
    outcome: Result<ExportOutcome, AdmissionSignal>,
) -> Result<ExportLogsServiceResponse, Status> {
    outcome.map_or_else(
        |signal| match signal {
            AdmissionSignal::ExportOverCap { rejection } => Ok(ExportLogsServiceResponse {
                partial_success: Some(ExportLogsPartialSuccess {
                    rejected_log_records: rejected_count(rejection.observed),
                    error_message: rejection.to_string(),
                }),
            }),
            signal => Err(signal_to_status(signal)),
        },
        |outcome| {
            let (rejected, message) = rejected_summary(&outcome);
            Ok(ExportLogsServiceResponse {
                partial_success: Some(ExportLogsPartialSuccess {
                    rejected_log_records: rejected_count(rejected),
                    error_message: message,
                }),
            })
        },
    )
}

/// The wire mapping of the admission signals that refuse an export at the
/// transport. `ExportOverCap` never reaches here: both transports carry
/// `partial_success`, so the budget refusal rides the response.
fn signal_to_status(signal: AdmissionSignal) -> Status {
    match signal {
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
            Status::resource_exhausted(format!(
                "the {queue} ingestion queue is at its {ceiling_bytes}-byte in-flight \
                 ceiling and a record of {attempted_bytes} bytes did not fit; the only \
                 retryable admission signal — retry"
            ))
        }
        AdmissionSignal::PayloadOverCap {
            bytes,
            ceiling_bytes,
        } => Status::invalid_argument(format!(
            "the OTLP payload of {bytes} bytes exceeds the {ceiling_bytes}-byte \
             ceiling; refused before parsing, non-retryable"
        )),
        AdmissionSignal::MalformedRequest { detail } => Status::invalid_argument(format!(
            "the payload did not parse as an OTLP export request and cannot be \
             retried: {detail}"
        )),
        AdmissionSignal::Draining => Status::unavailable(
            "the runtime is draining and admits no new telemetry; re-delivery \
             after restart will be admitted fresh",
        ),
        AdmissionSignal::ExportOverCap { rejection } => Status::internal(format!(
            "an export-budget refusal must ride partial_success, never a status: \
             {rejection}"
        )),
    }
}

/// The read-timeout answer for a gRPC request: `DEADLINE_EXCEEDED`, naming
/// the transport-edge bound. The body never arrived complete, so nothing was
/// parsed or admitted ([ADR 0010]).
///
/// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
fn body_read_timeout_status() -> Status {
    Status::deadline_exceeded(
        "the request body did not arrive in full within the read deadline; \
         refused at the transport edge",
    )
}

/// Runs one gRPC unary under the transport-edge guard ([ADR 0010]).
///
/// The frame reader buffers the whole request body before the unary service
/// runs, so the aggregate in-flight-body charge and the per-request read
/// deadline wrap the *entire* unary future. A charge refused answers
/// `RESOURCE_EXHAUSTED` — the retryable transient-overload shape, sibling to
/// queue saturation. A body that never arrives in full within the deadline
/// answers `DEADLINE_EXCEEDED`.
///
/// `unary` is not a future-producing closure but the future itself, built
/// outside — the caller owns the frame reader, so the future the timeout wraps
/// can borrow it without leaving the closure's scope.
///
/// [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md
async fn guarded_unary(
    runtime: &CoreRuntime,
    deadline: Duration,
    charge: usize,
    unary: impl std::future::Future<Output = http::Response<GrpcBody>>,
) -> http::Response<GrpcBody> {
    let Some(_guard) = runtime.inflight_body_budget().try_acquire(charge) else {
        return inflight_over_budget_status(runtime.inflight_body_budget().ceiling_bytes())
            .into_http();
    };
    match tokio::time::timeout(deadline, unary).await {
        Ok(response) => response,
        Err(_elapsed) => body_read_timeout_status().into_http(),
    }
}

// ------------------------------------------------------------------
// The per-service unary handlers
// ------------------------------------------------------------------

/// The trace export's unary service: raw payload in, collector response
/// out. Admission is synchronous and non-blocking, so the future is ready.
struct ExportSpans {
    runtime: Arc<CoreRuntime>,
}

impl UnaryService<bytes::Bytes> for ExportSpans {
    type Response = ExportTraceServiceResponse;
    type Future = std::future::Ready<Result<GrpcResponse<Self::Response>, Status>>;

    fn call(&mut self, request: GrpcRequest<bytes::Bytes>) -> Self::Future {
        let outcome = ingest(&self.runtime, &request.into_inner(), Family::Traces);
        std::future::ready(trace_reply(outcome).map(GrpcResponse::new))
    }
}

/// The metrics export's unary service.
struct ExportMetrics {
    runtime: Arc<CoreRuntime>,
}

impl UnaryService<bytes::Bytes> for ExportMetrics {
    type Response = ExportMetricsServiceResponse;
    type Future = std::future::Ready<Result<GrpcResponse<Self::Response>, Status>>;

    fn call(&mut self, request: GrpcRequest<bytes::Bytes>) -> Self::Future {
        let outcome = ingest(&self.runtime, &request.into_inner(), Family::Metrics);
        std::future::ready(metrics_reply(outcome).map(GrpcResponse::new))
    }
}

/// The logs export's unary service.
struct ExportLogs {
    runtime: Arc<CoreRuntime>,
}

impl UnaryService<bytes::Bytes> for ExportLogs {
    type Response = ExportLogsServiceResponse;
    type Future = std::future::Ready<Result<GrpcResponse<Self::Response>, Status>>;

    fn call(&mut self, request: GrpcRequest<bytes::Bytes>) -> Self::Future {
        let outcome = ingest(&self.runtime, &request.into_inner(), Family::Logs);
        std::future::ready(logs_reply(outcome).map(GrpcResponse::new))
    }
}

// ------------------------------------------------------------------
// The server services
// ------------------------------------------------------------------

/// The OTLP/gRPC trace service, as the router mounts it.
///
/// Mounted under [`TRACE_SERVICE_PREFIX`]: the router strips the service
/// prefix, so the one RPC this service dispatches is `"/Export"`. Any
/// other path is answered `UNIMPLEMENTED`, as the gRPC protocol requires.
#[derive(Clone)]
pub(crate) struct TraceServiceServer {
    runtime: Arc<CoreRuntime>,
}

impl TraceServiceServer {
    pub(crate) fn new(runtime: Arc<CoreRuntime>) -> Self {
        Self { runtime }
    }
}

impl Service<http::Request<axum::body::Body>> for TraceServiceServer {
    type Response = http::Response<GrpcBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<axum::body::Body>) -> Self::Future {
        if !is_grpc_content_type(req.headers()) {
            return Box::pin(std::future::ready(Ok(unsupported_media_type(
                req.uri().path(),
            ))));
        }
        let runtime = Arc::clone(&self.runtime);
        Box::pin(async move {
            match req.uri().path() {
                EXPORT_METHOD => {
                    if runtime.is_draining() {
                        return Ok(signal_to_status(AdmissionSignal::Draining).into_http());
                    }
                    let mut grpc = Grpc::new(PassthroughCodec::<ExportTraceServiceResponse>::new())
                        .max_decoding_message_size(runtime.grpc_decoding_ceiling_bytes());
                    let charge = runtime.grpc_decoding_ceiling_bytes();
                    let deadline = runtime.body_read_timeout();
                    Ok(guarded_unary(
                        &runtime,
                        deadline,
                        charge,
                        grpc.unary(
                            ExportSpans {
                                runtime: Arc::clone(&runtime),
                            },
                            req,
                        ),
                    )
                    .await)
                }
                path => Ok(unimplemented_response(path)),
            }
        })
    }
}

/// The OTLP/gRPC metrics service, mounted under [`METRICS_SERVICE_PREFIX`].
#[derive(Clone)]
pub(crate) struct MetricsServiceServer {
    runtime: Arc<CoreRuntime>,
}

impl MetricsServiceServer {
    pub(crate) fn new(runtime: Arc<CoreRuntime>) -> Self {
        Self { runtime }
    }
}

impl Service<http::Request<axum::body::Body>> for MetricsServiceServer {
    type Response = http::Response<GrpcBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<axum::body::Body>) -> Self::Future {
        if !is_grpc_content_type(req.headers()) {
            return Box::pin(std::future::ready(Ok(unsupported_media_type(
                req.uri().path(),
            ))));
        }
        let runtime = Arc::clone(&self.runtime);
        Box::pin(async move {
            match req.uri().path() {
                EXPORT_METHOD => {
                    if runtime.is_draining() {
                        return Ok(signal_to_status(AdmissionSignal::Draining).into_http());
                    }
                    let mut grpc =
                        Grpc::new(PassthroughCodec::<ExportMetricsServiceResponse>::new())
                            .max_decoding_message_size(runtime.grpc_decoding_ceiling_bytes());
                    let charge = runtime.grpc_decoding_ceiling_bytes();
                    let deadline = runtime.body_read_timeout();
                    Ok(guarded_unary(
                        &runtime,
                        deadline,
                        charge,
                        grpc.unary(
                            ExportMetrics {
                                runtime: Arc::clone(&runtime),
                            },
                            req,
                        ),
                    )
                    .await)
                }
                path => Ok(unimplemented_response(path)),
            }
        })
    }
}

/// The OTLP/gRPC logs service, mounted under [`LOGS_SERVICE_PREFIX`].
#[derive(Clone)]
pub(crate) struct LogsServiceServer {
    runtime: Arc<CoreRuntime>,
}

impl LogsServiceServer {
    pub(crate) fn new(runtime: Arc<CoreRuntime>) -> Self {
        Self { runtime }
    }
}

impl Service<http::Request<axum::body::Body>> for LogsServiceServer {
    type Response = http::Response<GrpcBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<axum::body::Body>) -> Self::Future {
        if !is_grpc_content_type(req.headers()) {
            return Box::pin(std::future::ready(Ok(unsupported_media_type(
                req.uri().path(),
            ))));
        }
        let runtime = Arc::clone(&self.runtime);
        Box::pin(async move {
            match req.uri().path() {
                EXPORT_METHOD => {
                    if runtime.is_draining() {
                        return Ok(signal_to_status(AdmissionSignal::Draining).into_http());
                    }
                    let mut grpc = Grpc::new(PassthroughCodec::<ExportLogsServiceResponse>::new())
                        .max_decoding_message_size(runtime.grpc_decoding_ceiling_bytes());
                    let charge = runtime.grpc_decoding_ceiling_bytes();
                    let deadline = runtime.body_read_timeout();
                    Ok(guarded_unary(
                        &runtime,
                        deadline,
                        charge,
                        grpc.unary(
                            ExportLogs {
                                runtime: Arc::clone(&runtime),
                            },
                            req,
                        ),
                    )
                    .await)
                }
                path => Ok(unimplemented_response(path)),
            }
        })
    }
}

/// The `UNIMPLEMENTED` answer for a path no RPC of this service matches —
/// the trailers-only shape the gRPC protocol puts on the wire.
fn unimplemented_response(path: &str) -> http::Response<GrpcBody> {
    tracing::debug!(path, "unimplemented gRPC method");
    Status::unimplemented(format!("unknown method {path}")).into_http()
}

/// Whether the request speaks THIS server's gRPC dialect: proto framing on
/// `application/grpc` — bare (proto by default), with the proto message
/// format (`application/grpc+proto`, optionally with parameters), or bare
/// with parameters. Everything else is not a request this server answers
/// as gRPC: a JSON post, a form, a missing header — but also the
/// gRPC-shaped dialects the spec's letter would admit because they *begin
/// with* `application/grpc` (`application/grpc-web*`,
/// `application/grpc+json`), whose bodies die in the framing layer as a
/// baffling INTERNAL (their first body byte is not a legal compression
/// flag). All of them get the same bare HTTP 415, and the gRPC-over-HTTP2
/// spec ("Content-Type") prescribes it for the non-gRPC cases:
///
/// > If **Content-Type** does not begin with "application/grpc", gRPC
/// > servers SHOULD respond with HTTP status of 415 (Unsupported Media
/// > Type). This will prevent other HTTP/2 clients from interpreting a
/// > gRPC error response, which uses status 200 (OK), as successful.
///
/// Refusing the gRPC-shaped dialects too is a deliberate, documented
/// deviation from that letter — on the side of naming what is not spoken
/// instead of answering a foreign dialect with an INTERNAL.
///
/// Two pieces of legal HTTP syntax the prefix check must survive (RFC 9110
/// §8.3): the type and subtype are case-insensitive, and optional whitespace
/// may separate the media type from the `;` that opens its parameters
/// (`application/grpc ; charset=utf-8`). Both are accepted; a foreign
/// dialect stays foreign however it is spelled.
fn is_grpc_content_type(headers: &http::HeaderMap) -> bool {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            // The type and subtype are case-insensitive (RFC 9110 §8.3.2),
            // so the dialect is matched on the lowercased value; parameters
            // are never inspected, only the spelling in front of them.
            let lower = value.to_ascii_lowercase();
            if !lower.starts_with("application/grpc") {
                return false;
            }
            let rest = &lower["application/grpc".len()..];
            let after_dialect = rest.strip_prefix("+proto").unwrap_or(rest);
            // Optional whitespace (OWS) may precede the `;` that opens the
            // parameter list.
            let after_dialect = after_dialect.trim_start_matches([' ', '\t']);
            after_dialect.is_empty() || after_dialect.starts_with(';')
        })
}

/// The 415 answer for a request that is not gRPC at all: bare HTTP — no
/// `grpc-status`, no trailers, no body — because there is no gRPC exchange
/// to answer. (The spec prescribes only the status; see
/// [`is_grpc_content_type`] for why a gRPC-shaped answer must not ride it.)
fn unsupported_media_type(path: &str) -> http::Response<GrpcBody> {
    tracing::debug!(path, "non-gRPC content-type refused at the HTTP layer");
    http::Response::builder()
        .status(http::StatusCode::UNSUPPORTED_MEDIA_TYPE)
        .body(GrpcBody::empty())
        .expect("a static 415 response always builds")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use http_body_util::BodyExt;
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

    const TRACE_EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
    const METRICS_EXPORT: &str = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";
    const LOGS_EXPORT: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";
    const TRACE_UNKNOWN: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Gather";

    /// One gRPC message frame: uncompressed flag, big-endian length, bytes.
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut framed = Vec::with_capacity(5 + payload.len());
        framed.push(0_u8);
        framed.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("a test payload fits")
                .to_be_bytes(),
        );
        framed.extend_from_slice(payload);
        framed
    }

    /// The percent-decoding `grpc-message` travels with (the gRPC wire
    /// spec's message encoding, applied by the status writer).
    fn percent_decode(raw: &str) -> String {
        let bytes = raw.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] == b'%' && index + 2 < bytes.len() {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    out.push(byte);
                    index += 3;
                    continue;
                }
            }
            out.push(bytes[index]);
            index += 1;
        }
        String::from_utf8_lossy(&out).to_string()
    }

    /// Everything the wire says about one gRPC answer: the HTTP status, the
    /// `grpc-status` code (headers for the trailers-only answers, trailers
    /// otherwise), the decoded `grpc-message`, and the message bytes.
    struct Answer {
        http: axum::http::StatusCode,
        code: Option<u32>,
        message: String,
        body: bytes::Bytes,
    }

    async fn answer(response: axum::response::Response) -> Answer {
        let (parts, body) = response.into_parts();
        let read = |map: &axum::http::HeaderMap, name: &str| {
            map.get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let header_code = read(&parts.headers, "grpc-status");
        let header_message = read(&parts.headers, "grpc-message");
        let collected = body.collect().await.expect("the body collects");
        let trailer_code = collected
            .trailers()
            .and_then(|trailers| read(trailers, "grpc-status"));
        let trailer_message = collected
            .trailers()
            .and_then(|trailers| read(trailers, "grpc-message"));
        let code = header_code.or(trailer_code);
        let message = header_message.or(trailer_message).unwrap_or_default();
        Answer {
            http: parts.status,
            code: code.and_then(|code| code.parse().ok()),
            message: percent_decode(&message),
            body: collected.to_bytes(),
        }
    }

    async fn call(router: Router, path: &str, payload: Vec<u8>) -> Answer {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Body::from(payload))
            .expect("a static request builds");
        let response = router
            .oneshot(request)
            .await
            .expect("the router answers every request");
        answer(response).await
    }

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

    /// The happy path on all three services: framed OTLP in, an `OK`
    /// response with the collector message out, and records resident
    /// behind the pump.
    #[tokio::test]
    async fn export_keeps_the_records_and_answers_ok() {
        let runtime = test_support::runtime();

        let answer = call(
            build_router(Arc::clone(&runtime), ServerConfig::default()),
            TRACE_EXPORT,
            frame(&one_span_payload(fx::T1, fx::S1)),
        )
        .await;
        assert_eq!(answer.http, axum::http::StatusCode::OK);
        assert_eq!(answer.code, Some(0), "the export is admitted");
        let reply = ExportTraceServiceResponse::decode(&answer.body[5..])
            .expect("the framed reply decodes");
        let partial = reply.partial_success.expect("partial_success is present");
        assert_eq!(partial.rejected_spans, 0);
        assert_eq!(partial.error_message, "");

        let answer = call(
            build_router(Arc::clone(&runtime), ServerConfig::default()),
            METRICS_EXPORT,
            frame(&one_point_payload()),
        )
        .await;
        assert_eq!(answer.code, Some(0));
        let reply = ExportMetricsServiceResponse::decode(&answer.body[5..])
            .expect("the framed reply decodes");
        assert_eq!(
            reply.partial_success.expect("present").rejected_data_points,
            0
        );

        let answer = call(
            build_router(Arc::clone(&runtime), ServerConfig::default()),
            LOGS_EXPORT,
            frame(&one_log_payload()),
        )
        .await;
        assert_eq!(answer.code, Some(0));
        let reply =
            ExportLogsServiceResponse::decode(&answer.body[5..]).expect("the framed reply decodes");
        assert_eq!(
            reply.partial_success.expect("present").rejected_log_records,
            0
        );

        wait_for_resident(&runtime, 3);
        runtime.shutdown();
    }

    /// Saturation over gRPC is `RESOURCE_EXHAUSTED` — the one retryable
    /// status — and it names the saturated queue.
    #[tokio::test]
    /// The freeze holds a std mutex across the `post`/`call` awaits on
    /// purpose: the frozen store is the test's determinism device, and the
    /// awaited handler runs on the router, not the store.
    #[allow(clippy::await_holding_lock)]
    async fn saturated_queue_answers_resource_exhausted() {
        let runtime = CoreRuntime::build(saturation_config()).expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // The pump is parked on the freeze before the fill: a freeze alone
        // leaves the pump one pop of headroom, and a pop after the fill
        // frees exactly the slot this export then fits (the filed flake).
        let _frozen = test_support::freeze_and_park_pump(&runtime);
        assert!(
            saturate_until_full(&runtime),
            "a 4-KiB queue saturates within 2 Ki spans"
        );

        let answer = call(
            router,
            TRACE_EXPORT,
            frame(&one_span_payload(fx::T2, fx::S2)),
        )
        .await;
        assert_eq!(answer.code, Some(8), "RESOURCE_EXHAUSTED");
        assert!(
            answer.message.contains("ingestion"),
            "the saturated queue is named: {:?}",
            answer.message
        );
    }

    /// Draining over gRPC is `UNAVAILABLE` — the closing signal.
    #[tokio::test]
    async fn draining_runtime_answers_unavailable() {
        let runtime = test_support::runtime();
        runtime.begin_drain();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let answer = call(
            router,
            TRACE_EXPORT,
            frame(&one_span_payload(fx::T1, fx::S1)),
        )
        .await;
        assert_eq!(answer.code, Some(14), "UNAVAILABLE");
        assert!(
            answer.message.contains("draining"),
            "the closing signal says so: {:?}",
            answer.message
        );
        runtime.shutdown();
    }

    /// An over-ceiling payload reaches the pipeline's own gate — whose
    /// answer names the contract ceiling — because the gRPC reader's limit
    /// is the ceiling plus framing slack, not a bare generic limit.
    #[tokio::test]
    async fn over_ceiling_payload_answers_invalid_argument_naming_the_ceiling() {
        let runtime = CoreRuntime::build(RuntimeConfig {
            payload_ceiling_bytes: 1024,
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // 4 KiB of zeros: past the 1-KiB ceiling, but far under the gRPC
        // reader's ceiling-plus-slack, so the pipeline's gate is what
        // refuses — as INVALID_ARGUMENT naming 1024.
        let answer = call(router, TRACE_EXPORT, frame(&vec![0_u8; 4096])).await;
        assert_eq!(answer.code, Some(3), "INVALID_ARGUMENT");
        assert!(
            answer.message.contains("1024"),
            "the ceiling is named: {:?}",
            answer.message
        );
        runtime.shutdown();
    }

    /// Bytes that never parse are `INVALID_ARGUMENT` naming the failure —
    /// non-retryable, as the payload is the problem.
    #[tokio::test]
    async fn malformed_payload_answers_invalid_argument() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let answer = call(router, LOGS_EXPORT, frame(&[0xFF, 0xFF, 0xFF, 0xFF, 0x01])).await;
        assert_eq!(answer.code, Some(3), "INVALID_ARGUMENT");
        assert!(
            answer.message.contains("did not parse"),
            "the failure is named: {:?}",
            answer.message
        );
        runtime.shutdown();
    }

    /// A path no RPC of the service matches is `UNIMPLEMENTED`, as the gRPC
    /// protocol requires.
    #[tokio::test]
    async fn unknown_method_answers_unimplemented() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let answer = call(
            router,
            TRACE_UNKNOWN,
            frame(&one_span_payload(fx::T1, fx::S1)),
        )
        .await;
        assert_eq!(answer.code, Some(12), "UNIMPLEMENTED");
        runtime.shutdown();
    }

    /// A legal empty export — a gRPC frame whose payload is zero bytes —
    /// decodes as the default request: nothing is admitted, and the answer
    /// is `OK` with an empty `partial_success`. Not an internal error: the
    /// message is *empty*, not *missing*.
    #[tokio::test]
    async fn empty_payload_frame_answers_ok() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let answer = call(router, TRACE_EXPORT, frame(&[])).await;
        assert_eq!(answer.http, axum::http::StatusCode::OK);
        assert_eq!(answer.code, Some(0), "the empty export is admitted");
        let reply = ExportTraceServiceResponse::decode(&answer.body[5..])
            .expect("the framed reply decodes");
        let partial = reply.partial_success.expect("partial_success is present");
        assert_eq!(partial.rejected_spans, 0);
        assert_eq!(partial.error_message, "");
        assert_eq!(
            runtime.store_stats().resident_records,
            0,
            "an empty export admits nothing"
        );
        runtime.shutdown();
    }

    /// A request whose content-type does not begin with `application/grpc`
    /// is refused at the HTTP layer with 415 — bare, with no `grpc-status`
    /// on it — so a plain HTTP/2 client can never read a gRPC answer's
    /// status-200 envelope as success.
    #[tokio::test]
    async fn non_grpc_content_type_answers_unsupported_media_type() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        let request = axum::http::Request::builder()
            .method("POST")
            .uri(TRACE_EXPORT)
            .header("content-type", "application/json")
            .body(Body::from(br#"{"resource_spans": []}"#.to_vec()))
            .expect("a static request builds");
        let response = router
            .oneshot(request)
            .await
            .expect("the router answers every request");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert!(
            response.headers().get("grpc-status").is_none(),
            "the refusal is bare HTTP, not a gRPC answer: {:?}",
            response.headers()
        );
        runtime.shutdown();
    }

    /// The gRPC-shaped dialects the spec's letter would wave through —
    /// they *begin with* `application/grpc` — are refused at the HTTP
    /// layer all the same (the documented deviation): this server speaks
    /// proto framing only, and `grpc-web`/`grpc+json` bodies would
    /// otherwise die in framing as a baffling INTERNAL. Bare HTTP 415,
    /// like any other non-gRPC request.
    #[tokio::test]
    async fn foreign_grpc_dialects_answer_unsupported_media_type() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        for dialect in [
            "application/grpc-web+proto",
            "application/grpc+json",
            "application/grpc+json ; charset=utf-8",
        ] {
            let request = axum::http::Request::builder()
                .method("POST")
                .uri(TRACE_EXPORT)
                .header("content-type", dialect)
                .body(Body::from(vec![0, 0, 0, 0, 0]))
                .expect("a static request builds");
            let response = router
                .clone()
                .oneshot(request)
                .await
                .expect("the router answers every request");
            assert_eq!(
                response.status(),
                axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{dialect} is refused at the HTTP layer, not answered as gRPC"
            );
            assert!(
                response.headers().get("grpc-status").is_none(),
                "the refusal is bare HTTP, not a gRPC answer: {:?}",
                response.headers()
            );
        }
        runtime.shutdown();
    }

    /// The legal HTTP spellings a prefix check must survive —
    /// case-insensitive type/subtype and optional whitespace before the
    /// parameter list (RFC 9110 §8.3) — reach the gRPC layer and get real
    /// gRPC answers, not the bare-HTTP 415 the foreign dialects get.
    #[tokio::test]
    async fn legal_media_type_spellings_are_answered_as_grpc() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        for (trace, span, spelling) in [
            ([0x01_u8; 16], [0x11_u8; 8], "Application/Grpc"),
            ([0x02_u8; 16], [0x12_u8; 8], "application/grpc+PROTO"),
            (
                [0x03_u8; 16],
                [0x13_u8; 8],
                "application/grpc ; charset=utf-8",
            ),
            (
                [0x04_u8; 16],
                [0x14_u8; 8],
                "application/grpc+proto ; charset=utf-8",
            ),
        ] {
            let request = axum::http::Request::builder()
                .method("POST")
                .uri(TRACE_EXPORT)
                .header("content-type", spelling)
                .header("te", "trailers")
                .body(Body::from(frame(&one_span_payload(trace, span))))
                .expect("a static request builds");
            let answer = answer(
                router
                    .clone()
                    .oneshot(request)
                    .await
                    .expect("the router answers every request"),
            )
            .await;
            assert_eq!(
                answer.code,
                Some(0),
                "{spelling} is answered as gRPC, not refused at the HTTP \
                 layer: {}",
                answer.message
            );
        }
        runtime.shutdown();
    }

    #[tokio::test]
    async fn draining_runtime_refuses_before_buffering_the_frame() {
        let runtime = test_support::runtime();
        runtime.begin_drain();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // A body that never yields a byte: buffering a frame from it would
        // never finish, so the answer proves the gate fires first.
        let endless = Body::from_stream(tonic::codegen::tokio_stream::pending::<
            Result<bytes::Bytes, std::convert::Infallible>,
        >());
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(TRACE_EXPORT)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(endless)
            .expect("a static request builds");
        let response = tokio::time::timeout(Duration::from_secs(5), router.oneshot(request))
            .await
            .expect("the drain gate answers without reading the body")
            .expect("the router answers every request");
        let answer = answer(response).await;
        assert_eq!(answer.code, Some(14), "UNAVAILABLE");
        assert!(
            answer.message.contains("draining"),
            "the closing signal says so: {:?}",
            answer.message
        );
        runtime.shutdown();
    }

    /// The whole-export budget refusal rides `partial_success` on the wire
    /// exactly as the HTTP transport rides it: `OK`, the whole export
    /// counted as rejected, and the budget naming itself.
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
        let answer = call(router, METRICS_EXPORT, frame(&fx::encode(&request))).await;
        assert_eq!(answer.http, axum::http::StatusCode::OK);
        assert_eq!(answer.code, Some(0), "a budget refusal still answers OK");
        let reply = ExportMetricsServiceResponse::decode(&answer.body[5..])
            .expect("the framed reply decodes");
        let partial = reply.partial_success.expect("partial_success is present");
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

    /// The poison-position contract over the wire: exactly the refused
    /// record is named, by its flat document-order position, and the
    /// healthy rest of the export still keeps.
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

        let answer = call(router, TRACE_EXPORT, frame(&fx::encode(&request))).await;
        assert_eq!(answer.code, Some(0), "the export still answers OK");
        let reply = ExportTraceServiceResponse::decode(&answer.body[5..])
            .expect("the framed reply decodes");
        let partial = reply.partial_success.expect("partial_success is present");
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

    /// The naming bound over the wire: an export with more rejections than
    /// the message will name carries the truncation note and the complete
    /// rejected count — the count is never truncated, only the naming.
    #[tokio::test]
    async fn more_rejections_than_named_carry_the_truncation_note_and_full_count() {
        let runtime = test_support::runtime();
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // Seventy unrepresentable spans: every one refused, 6 past the
        // 64-position naming bound.
        let poison: Vec<_> = (1_u64..=70)
            .map(|index| {
                let mut span = fx::trace_span("poison", fx::T1, fx::S1);
                span.trace_id = vec![0x01, 0x02, 0x03]; // a wrong-width id
                span.span_id = index.to_be_bytes().to_vec();
                span
            })
            .collect();
        let request = fx::traces_request(vec![fx::resource_spans(
            None,
            vec![fx::scope_spans(None, poison)],
        )]);

        let answer = call(router, TRACE_EXPORT, frame(&fx::encode(&request))).await;
        assert_eq!(answer.code, Some(0));
        let reply = ExportTraceServiceResponse::decode(&answer.body[5..])
            .expect("the framed reply decodes");
        let partial = reply.partial_success.expect("partial_success is present");
        assert_eq!(partial.rejected_spans, 70, "the count is complete");
        assert!(
            partial.error_message.contains("position 63"),
            "the last named position is the 64th: {:?}",
            partial.error_message
        );
        assert!(
            !partial.error_message.contains("position 64"),
            "the 65th position is summarized, not named: {:?}",
            partial.error_message
        );
        assert!(
            partial
                .error_message
                .contains("6 further rejected records not named"),
            "the truncation is stated: {:?}",
            partial.error_message
        );
        runtime.shutdown();
    }

    use bytes::{Buf, BufMut, Bytes};
    use std::marker::PhantomData;
    use tonic::Status;
    use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

    /// The client half of the passthrough pair: the request message is the
    /// raw payload bytes (tonic frames them; the server's passthrough codec
    /// receives them verbatim), the response message is prost-decoded. The
    /// mirror of [`PassthroughCodec`], for the real-socket test below.
    struct WireClientCodec<Res> {
        _response: PhantomData<fn() -> Res>,
    }

    impl<Res> WireClientCodec<Res> {
        fn new() -> Self {
            Self {
                _response: PhantomData,
            }
        }
    }

    impl<Res> Codec for WireClientCodec<Res>
    where
        Res: prost::Message + Default + 'static,
    {
        type Encode = Bytes;
        type Decode = Res;
        type Encoder = RawBytesEncoder;
        type Decoder = ProstDecoder<Res>;

        fn encoder(&mut self) -> Self::Encoder {
            RawBytesEncoder
        }

        fn decoder(&mut self) -> Self::Decoder {
            ProstDecoder {
                _response: PhantomData,
            }
        }
    }

    /// Writes the payload bytes into the gRPC frame verbatim.
    struct RawBytesEncoder;

    impl Encoder for RawBytesEncoder {
        type Item = Bytes;
        type Error = Status;

        fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
            dst.reserve(item.len());
            dst.put_slice(&item);
            Ok(())
        }
    }

    /// Prost-decodes the response frame.
    struct ProstDecoder<Res> {
        _response: PhantomData<fn() -> Res>,
    }

    impl<Res> Decoder for ProstDecoder<Res>
    where
        Res: prost::Message + Default,
    {
        type Item = Res;
        type Error = Status;

        fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
            Res::decode(&mut src.copy_to_bytes(src.remaining()))
                .map(Some)
                .map_err(|error| {
                    Status::internal(format!("decoding the OTLP response failed: {error}"))
                })
        }
    }

    /// The wire proof, on a real h2c socket: tonic's own client stack — an
    /// `Endpoint` and `Channel` over TCP, a hand-written passthrough client
    /// codec — against the served router. The in-process tower calls above
    /// prove the answers' content; this proves the served wire speaks them.
    ///
    /// A unary export succeeds end to end, and the one retryable refusal —
    /// a saturated queue — arrives as `RESOURCE_EXHAUSTED` **from the
    /// response headers alone**: `Grpc::streaming` returns the `Err`
    /// before a message could be read only when `grpc-status` rode the
    /// initial HEADERS — the trailers-only shape — never when it rode
    /// trailers after a body.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn a_real_h2c_client_exports_and_meets_the_retryable_refusal() {
        use tonic::client::Grpc;
        use tonic::codegen::http::uri::PathAndQuery;
        use tonic::transport::Endpoint;

        // The served socket: the real accept loop, the real router, one
        // saturable runtime behind it.
        let runtime = CoreRuntime::build(saturation_config()).expect("the config is buildable");
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

        let channel = tokio::time::timeout(
            Duration::from_secs(10),
            Endpoint::from_shared(format!("http://{addr}"))
                .expect("a loopback endpoint")
                .connect(),
        )
        .await
        .expect("the client connects in time")
        .expect("the client connects");
        let mut grpc = Grpc::new(channel);
        grpc.ready()
            .await
            .expect("the client channel becomes ready");
        let path = PathAndQuery::from_static(TRACE_EXPORT);

        // (a) The unary export succeeds on the wire, and the record is
        // resident behind the pump.
        let answer = tokio::time::timeout(
            Duration::from_secs(10),
            grpc.unary(
                tonic::Request::new(Bytes::copy_from_slice(&one_span_payload(fx::T1, fx::S1))),
                path.clone(),
                WireClientCodec::<ExportTraceServiceResponse>::new(),
            ),
        )
        .await
        .expect("the unary export answers in time")
        .expect("the export is admitted");
        let reply = answer.into_inner();
        assert_eq!(
            reply
                .partial_success
                .expect("partial_success is present")
                .rejected_spans,
            0,
            "the wire export is fully admitted"
        );
        wait_for_resident(&runtime, 1);

        // (b) Saturate the queue — the pump parked on the frozen store so
        // nothing frees queue space — and meet the refusal over the same
        // socket.
        let frozen = test_support::freeze_and_park_pump(&runtime);
        assert!(
            saturate_until_full(&runtime),
            "a 4-KiB queue saturates within 2 Ki spans"
        );

        grpc.ready()
            .await
            .expect("the client channel is ready again");
        let refusal = tokio::time::timeout(
            Duration::from_secs(10),
            grpc.streaming(
                tonic::Request::new(tonic::codegen::tokio_stream::once(Bytes::copy_from_slice(
                    &one_span_payload(fx::T2, fx::S2),
                ))),
                path,
                WireClientCodec::<ExportTraceServiceResponse>::new(),
            ),
        )
        .await
        .expect("the refusal answers in time");
        let Err(status) = refusal else {
            panic!("a saturated queue must refuse the wire export");
        };
        assert_eq!(
            status.code(),
            tonic::Code::ResourceExhausted,
            "the one retryable status: {status:?}"
        );
        assert!(
            status.message().contains("ingestion"),
            "the saturated queue is named across the wire: {:?}",
            status.message()
        );

        // Release the freeze before shutdown: the drain path must be able
        // to finish the queue.
        drop(frozen);
        trigger.send(()).expect("the server is still running");
        server
            .await
            .expect("the server task joins")
            .expect("the server serves cleanly");
        runtime.shutdown();
    }

    /// A slow-drip frame that never completes: the timeout the read deadline
    /// bounds, on the gRPC side, is the whole unary future.
    #[tokio::test]
    async fn a_frame_beyond_the_read_deadline_answers_deadline_exceeded() {
        use tonic::codegen::tokio_stream::StreamExt;

        let runtime = CoreRuntime::build(RuntimeConfig {
            body_read_timeout: Duration::from_millis(50),
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // A body that yields a partial frame (the opening 5 framing bytes,
        // then nothing): the unary cannot complete, so the read deadline
        // answers DEADLINE_EXCEEDED — never an INTERNAL, never a hang.
        let endless = Body::from_stream(
            tonic::codegen::tokio_stream::iter(vec![Ok::<_, std::convert::Infallible>(
                bytes::Bytes::from_static(&[0u8, 0, 0, 0, 4]),
            )])
            .chain(tonic::codegen::tokio_stream::pending()),
        );
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(TRACE_EXPORT)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(endless)
            .expect("a static request builds");
        let response = router
            .oneshot(request)
            .await
            .expect("the router answers every request");
        let answer = answer(response).await;
        assert_eq!(answer.code, Some(4), "DEADLINE_EXCEEDED");
        assert!(
            answer.message.contains("read deadline"),
            "the deadline is named: {:?}",
            answer.message
        );
        assert_eq!(
            runtime.inflight_body_budget().in_flight(),
            0,
            "the timed-out frame returned its charge"
        );
        runtime.shutdown();
    }

    /// Two taken-in-flight charges exhaust the aggregate, and the next
    /// export is refused `RESOURCE_EXHAUSTED` before its frame is buffered.
    #[tokio::test]
    async fn concurrent_frames_over_the_aggregate_are_refused_resource_exhausted() {
        let runtime = CoreRuntime::build(RuntimeConfig {
            // A gRPC charge is the decoding ceiling — payload ceiling plus the
            // 8 KiB framing slack. With a 1 KiB payload ceiling each frame
            // charges 9 KiB, so a 18 KiB aggregate holds exactly two frames.
            payload_ceiling_bytes: 1024,
            inflight_body_ceiling_bytes: 2 * (1024 + 8 * 1024),
            ..RuntimeConfig::default()
        })
        .expect("the config is buildable");
        let router = build_router(Arc::clone(&runtime), ServerConfig::default());

        // Frames that never complete, each charging the decoding ceiling. The
        // gRPC frame reader and the aggregate budget both hold the charge.
        let framed_request = || {
            axum::http::Request::builder()
                .method("POST")
                .uri(TRACE_EXPORT)
                .header("content-type", "application/grpc")
                .header("te", "trailers")
                .body(Body::from_stream(tonic::codegen::tokio_stream::pending::<
                    Result<bytes::Bytes, std::convert::Infallible>,
                >()))
                .expect("a static request builds")
        };
        let mut held = Vec::new();
        for _ in 0..2 {
            let router = router.clone();
            held.push(tokio::spawn(async move {
                router.oneshot(framed_request()).await
            }));
        }

        let ceiling = runtime.inflight_body_budget().ceiling_bytes();
        for _ in 0..5_000 {
            if runtime.inflight_body_budget().in_flight() >= ceiling {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            runtime.inflight_body_budget().in_flight() >= ceiling,
            "both frames held their charges"
        );

        let refusal = router
            .clone()
            .oneshot(framed_request())
            .await
            .expect("the router answers every request");
        let answer = answer(refusal).await;
        assert_eq!(answer.code, Some(8), "RESOURCE_EXHAUSTED");
        assert!(
            answer.message.contains("in-flight-body budget"),
            "the refused frame names the budget: {:?}",
            answer.message
        );

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
            "every held charge is returned when the frame future goes away"
        );
        runtime.shutdown();
    }

    /// Fills the runtime's queue to saturation through the pipeline, the
    /// pump blocked by a frozen store, one fresh span per export. Returns
    /// whether saturation was reached — the shared fixture of the
    /// saturation tests.
    fn saturate_until_full(runtime: &CoreRuntime) -> bool {
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
                return true;
            }
        }
        false
    }
}
