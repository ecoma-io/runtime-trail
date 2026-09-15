//! The composition root and HTTP server of `runtime-trail`'s native core.
//!
//! This crate is [`layer-app`]: the one place allowed to name concrete
//! storage drivers and wire the Investigation API to a network. It serves
//! the health and version surfaces, the two OTLP transports, and (when a
//! UI build is supplied) the static Loom app. tokio + axum + tonic live
//! here and nowhere else; see `docs/decisions/0001-runtime-language.md`
//! for why the serving stack is confined to the composition root, and
//! `docs/architecture/boundaries.md` for the dependency law.
//!
//! [`layer-app`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! The core now *serves telemetry and investigations*: OTLP ingestion over
//! both transports — `POST /v1/traces|metrics|logs` (protobuf) and the
//! three OTLP/gRPC services — admitted through one pipeline into a real
//! (memory) store under bounded retention, and the Investigation API's
//! trace endpoint (`POST /v1/investigations/traces`, JSON) composing the
//! query engine into one envelope (ADR 0011). It does *not* yet serve the
//! UI or an MCP surface: no correlation strategies, no agent protocol.
//! A client that asks for telemetry in gets it; a client that asks to
//! investigate a resident trace gets one envelope — `docs/roadmap/phases.md`
//! owns what lands next.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, State};
use axum::middleware;
use axum::response::Html;
use axum::routing::{get, post};
use axum::serve::Listener;
use axum::{Json, Router};
use serde::Serialize;
use tower_http::services::ServeDir;

use crate::investigation_http::INVESTIGATION_TRACES_PATH;
use crate::listener::BoundedListener;
use crate::otlp_grpc::{LOGS_SERVICE_PREFIX, METRICS_SERVICE_PREFIX, TRACE_SERVICE_PREFIX};
use crate::runtime::{CoreRuntime, RunSummary, RuntimeConfig};

mod investigation_http;
mod listener;
mod otlp_grpc;
mod otlp_http;
pub mod runtime;
mod transport_guard;

/// The product name reported by the version surface.
pub const PRODUCT: &str = "runtime-trail";

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The drain deadline: how long the runtime keeps consuming its queue
/// after shutdown is requested before dropping what is left — observably.
///
/// `docs/architecture/runtime-constraints.md`, "Drain deadline": at most
/// 5 seconds, so a Ctrl-C is a prompt stop, not a hang.
pub const DRAIN_DEADLINE: Duration = Duration::from_secs(5);

/// The usage text the CLI prints for `--help`.
pub const USAGE: &str = "\
runtime-trail — local developer observability and investigation runtime

USAGE:
    runtime-trail-server [OPTIONS]

OPTIONS:
    --bind <addr>       Address to listen on [default: 127.0.0.1:8599]
    --web-dist <path>   Serve the built UI from this directory
    -h, --help          Print this help
    -V, --version       Print version information
";

/// Runtime configuration for the core server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Address the HTTP server binds.
    pub bind: SocketAddr,
    /// Directory of a built UI to serve at `/`. Absent at bootstrap unless
    /// supplied; the built-in index page is served instead.
    pub web_dist: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 8599)),
            web_dist: None,
        }
    }
}

/// Server state handed to handlers.
#[derive(Serialize)]
struct Health {
    status: &'static str,
    version: &'static str,
}

#[derive(Serialize)]
struct VersionInfo {
    name: &'static str,
    version: &'static str,
    storage_mode: &'static str,
}

/// Builds the application router over one shared runtime graph.
///
/// Every route — the health and version surfaces, the three OTLP/HTTP
/// endpoints and the three OTLP/gRPC services — is bound to the same
/// [`CoreRuntime`]: one admission pipeline, one bounded queue, one store.
/// The request-body ceiling is the runtime's payload ceiling, applied at
/// the transport edge for every route; each endpoint additionally bounds
/// its own body read with the same number.
pub fn build_router(runtime: Arc<CoreRuntime>, config: ServerConfig) -> Router {
    let payload_ceiling = runtime.payload_ceiling_bytes();
    // The OTLP/HTTP endpoints are their own router so the transport-edge
    // aggregate gate can wrap exactly them ([ADR 0010]). The body-budgetting
    // middleware must not wrap the gRPC services (they acquire on the same
    // budget at their own seam) or the health/version/UI surfaces (they never
    // buffer a request body).
    let otlp_http: Router<Arc<CoreRuntime>> = Router::new()
        .route("/v1/traces", post(otlp_http::export_traces))
        .route("/v1/metrics", post(otlp_http::export_metrics))
        .route("/v1/logs", post(otlp_http::export_logs))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&runtime),
            otlp_http::inflight_body_guard,
        ))
        .layer(DefaultBodyLimit::max(payload_ceiling));
    let investigation: Router<Arc<CoreRuntime>> = Router::new()
        .route(
            INVESTIGATION_TRACES_PATH,
            post(investigation_http::investigate_trace_http),
        )
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&runtime),
            otlp_http::inflight_body_guard,
        ))
        .layer(DefaultBodyLimit::max(payload_ceiling));
    let router: Router<Arc<CoreRuntime>> = Router::new()
        .route("/healthz", get(health))
        .route("/version", get(version))
        .merge(otlp_http)
        .merge(investigation)
        .nest_service(
            TRACE_SERVICE_PREFIX,
            otlp_grpc::TraceServiceServer::new(Arc::clone(&runtime)),
        )
        .nest_service(
            METRICS_SERVICE_PREFIX,
            otlp_grpc::MetricsServiceServer::new(Arc::clone(&runtime)),
        )
        .nest_service(
            LOGS_SERVICE_PREFIX,
            otlp_grpc::LogsServiceServer::new(Arc::clone(&runtime)),
        );
    let router = router.with_state(runtime);
    match config.web_dist {
        Some(dir) => router.fallback_service(ServeDir::new(dir)),
        None => router.fallback(builtin_index),
    }
}

async fn health() -> Json<Health> {
    Json(Health {
        status: "ok",
        version: VERSION,
    })
}

async fn version(State(runtime): State<Arc<CoreRuntime>>) -> Json<VersionInfo> {
    Json(VersionInfo {
        name: PRODUCT,
        version: VERSION,
        storage_mode: runtime.storage_mode(),
    })
}

/// The page served when no UI build was supplied — an honest smoke surface,
/// not a placeholder pretending to be the product.
async fn builtin_index() -> Html<String> {
    Html(format!(
        "<!doctype html><title>{PRODUCT}</title>\
         <p>Native core is running. Telemetry is ingested over OTLP \
         (HTTP and gRPC); the Investigation API is served over \
         <code>POST /v1/investigations/traces</code>. Start \
         the server with <code>--web-dist &lt;path&gt;</code> to serve a \
         UI build.</p>"
    ))
}

/// Binds and serves until shutdown is requested (Ctrl-C or SIGTERM).
///
/// # Errors
///
/// Returns the underlying `std::io::Error` when the bind address cannot be
/// opened, when the accept loop itself fails, or when the startup
/// configuration is not buildable.
pub async fn serve(config: ServerConfig) -> std::io::Result<()> {
    serve_with_shutdown(config, shutdown_signal())
        .await
        .map(|_| ())
}

/// Serves until `shutdown` resolves, then closes the session down the
/// drain path and reports what the pump did.
///
/// The shutdown sequence is the resource contract, in order: `shutdown`
/// resolves → the runtime begins draining (new exports are refused with
/// the closing answer, the pump spends at most [`DRAIN_DEADLINE`]
/// finishing what admission already queued) → in-flight requests finish →
/// the workers are joined and whatever the deadline left queued is dropped
/// observably. The summary is what a clean stop vs. a dropped stop looks
/// like, in numbers.
///
/// # Errors
///
/// Returns the underlying `std::io::Error` when the bind address cannot be
/// opened, when the accept loop itself fails, or when the startup
/// configuration is not buildable.
pub async fn serve_with_shutdown(
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<RunSummary> {
    let runtime = CoreRuntime::build(RuntimeConfig::default()).map_err(|error| {
        std::io::Error::other(format!(
            "the startup configuration was refused by the ingestion pipeline: {error}"
        ))
    })?;
    serve_runtime(runtime, config, shutdown).await
}

/// Runs the core on `config.bind` until `shutdown` resolves, then drains it
/// and returns its [`RunSummary`]. Also the seam the tests drive directly,
/// so a test can hand the server a runtime built from a custom
/// [`RuntimeConfig`] — e.g. a tightened `max_connections`.
async fn serve_runtime(
    runtime: Arc<CoreRuntime>,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<RunSummary> {
    let listener = BoundedListener::new(
        tokio::net::TcpListener::bind(config.bind).await?,
        runtime.max_connections(),
    );
    let router = build_router(Arc::clone(&runtime), config);
    tracing::info!(bind = %listener.local_addr()?, "runtime-trail core listening");
    let drain_runtime = Arc::clone(&runtime);
    // The drain deadline is anchored at the stop's ARRIVAL, not at serve
    // start: an in-flight connection cannot extend the shutdown past the
    // contract (runtime-constraints.md, "Drain deadline" ≤ 5 s) — when the
    // deadline expires the serve loop is dropped and whatever is still in
    // flight is cut, observably.
    let (drained_tx, drained_rx) = tokio::sync::oneshot::channel::<()>();
    let graceful = async {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                shutdown.await;
                // Draining starts the moment the stop is requested, not
                // when the last socket closes: new telemetry must meet the
                // closing answer immediately, and the pump gets its full
                // deadline.
                drain_runtime.begin_drain();
                let _ = drained_tx.send(());
            })
            .await
    };
    tokio::pin!(graceful);
    tokio::select! {
        biased;
        result = &mut graceful => result?,
        () = async {
            let _ = drained_rx.await;
            tokio::time::sleep(DRAIN_DEADLINE).await;
        } => {
            tracing::warn!(
                deadline_secs = DRAIN_DEADLINE.as_secs(),
                "drain deadline reached: in-flight connections are cut at process exit"
            );
        }
    }
    Ok(runtime.shutdown())
}

/// Resolves when the process is asked to stop.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install a Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::terminate())
            .expect("install a SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

/// What the command line asked the server to do.
#[derive(Debug, PartialEq, Eq)]
pub enum Cli {
    /// Run the core with this configuration.
    Run(ServerConfig),
    /// Print usage and exit successfully.
    Help,
    /// Print version information and exit successfully.
    Version,
}

/// Parses the server's command line. Kept in the library (not the binary) so
/// the CLI contract is testable like the rest of the core.
///
/// # Errors
///
/// Returns the message to print when an argument is unknown or malformed.
pub fn parse_args<I>(args: I) -> Result<Cli, String>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut args = args.into_iter().map(Into::into);
    let mut config = ServerConfig::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(Cli::Help),
            "--version" | "-V" => return Ok(Cli::Version),
            "--bind" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--bind requires an address".to_string())?;
                config.bind = value
                    .parse()
                    .map_err(|error| format!("invalid --bind address {value:?}: {error}"))?;
            }
            "--web-dist" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--web-dist requires a directory".to_string())?;
                config.web_dist = Some(PathBuf::from(value));
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(Cli::Run(config))
}

#[cfg(test)]
pub(crate) mod test_support {
    //! A runtime for the crate's tests: contract defaults, but no server.

    use std::sync::{Arc, MutexGuard};
    use std::time::Duration;

    use runtime_trail_storage::TelemetryStore;
    use runtime_trail_telemetry_ingestion::{RecordOutcome, fixtures as fx};

    use crate::runtime::{CoreRuntime, RuntimeConfig};

    /// Builds a fresh runtime graph — one pipeline, one store, its workers
    /// running. Tests that mutate retention state must drive it themselves
    /// (the tick period is longer than any test).
    pub(crate) fn runtime() -> Arc<CoreRuntime> {
        CoreRuntime::build(RuntimeConfig::default()).expect("the default config is buildable")
    }

    /// Freezes the store and parks the pump on the freeze, returning the
    /// store guard — the determinism device for the saturation tests, which
    /// fill the queue after this and must not lose a slot before their
    /// probe export is offered.
    ///
    /// The freeze alone is not enough: the pump pops a record *before* its
    /// keep takes the store lock, so a freeze taken while the pump is idle
    /// bounds it to one further pop without bounding when that pop lands —
    /// a pop after the fill finished frees exactly one record's slot, and
    /// the probe export (a smaller record than the fill's) fits it. That
    /// race is the filed flake (#9), reproducible under machine load.
    ///
    /// So the park comes first: one sacrificial record (an identity no
    /// other fixture uses) is offered, and the wait below is for its *pop*
    /// — the queue emptying — not its keep. Once popped, the pump is
    /// blocked on the store lock this call's caller holds and can pop no
    /// more: past that point the queue only ever grows. The bounded poll
    /// cannot pass early — the queue reached zero only through that pop —
    /// so it fails loudly rather than ever leaving the race in place.
    ///
    /// The park record stays popped-but-unkept until the caller drops the
    /// guard; the pump then keeps it like any other queued record.
    ///
    /// # Panics
    ///
    /// If the pump does not pop within 5 seconds — an honest failure: a
    /// pump that cannot take a notified record in 5 seconds would leave
    /// the saturation below racy, which this fixture refuses to paper
    /// over.
    pub(crate) fn freeze_and_park_pump(
        runtime: &CoreRuntime,
    ) -> MutexGuard<'_, Box<dyn TelemetryStore>> {
        let frozen = runtime.lock_store_for_test();
        let park = fx::traces_request(vec![fx::resource_spans(
            None,
            vec![fx::scope_spans(
                None,
                vec![fx::trace_span("park", fx::T1, [0xAA_u8; 8])],
            )],
        )]);
        let park_outcome = runtime
            .pipeline()
            .ingest_spans(fx::now(), &fx::encode(&park))
            .expect("the park record is admitted");
        // The park is only sound if the sacrificial record actually entered the queue.
        assert!(matches!(
            park_outcome.records[0],
            RecordOutcome::Admitted { .. }
        ));
        for _ in 0..5_000 {
            if runtime.queue_len_for_test() == 0 {
                return frozen;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!(
            "the pump never popped the park record: the queue still holds {} \
             records, so the saturation below would race it",
            runtime.queue_len_for_test()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use tower::ServiceExt;

    #[test]
    fn exposes_a_version() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn composition_root_names_the_investigation_api_and_the_memory_driver() {
        assert!(!runtime_trail_investigation::VERSION.is_empty());
        assert_eq!(
            test_support::runtime().storage_mode(),
            "memory",
            "the composition root's only driver selection at this phase"
        );
    }

    #[test]
    fn parses_the_documented_flags() {
        let cli = parse_args(["--bind", "0.0.0.0:9000", "--web-dist", "/srv/ui"])
            .expect("documented flags parse");
        let Cli::Run(config) = cli else {
            panic!("flags should produce a Run command");
        };
        assert_eq!(config.bind, SocketAddr::from(([0, 0, 0, 0], 9000)));
        assert_eq!(config.web_dist.as_deref(), Some(Path::new("/srv/ui")));
    }

    #[test]
    fn rejects_unknown_arguments() {
        let error = parse_args(["--no-such-flag"]).expect_err("unknown flags are refused");
        assert!(error.contains("--no-such-flag"));
    }

    #[test]
    fn defaults_bind_to_localhost() {
        let Cli::Run(config) = parse_args(Vec::<String>::new()).expect("empty args parse") else {
            panic!("empty args should produce a Run command");
        };
        assert!(config.bind.is_ipv4());
        assert_eq!(config.bind, SocketAddr::from(([127, 0, 0, 1], 8599)));
    }

    #[tokio::test]
    async fn healthz_reports_ok() {
        let response = build_router(test_support::runtime(), ServerConfig::default())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/healthz")
                    .body(axum::body::Body::empty())
                    .expect("static request builds"),
            )
            .await
            .expect("in-memory service answers");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        let health: serde_json::Value = serde_json::from_slice(&body).expect("health is JSON");
        assert_eq!(health["status"], "ok");
    }

    #[tokio::test]
    async fn version_reports_the_selected_storage_mode() {
        let runtime = test_support::runtime();
        let response = build_router(Arc::clone(&runtime), ServerConfig::default())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/version")
                    .body(axum::body::Body::empty())
                    .expect("static request builds"),
            )
            .await
            .expect("in-memory service answers");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        let info: serde_json::Value = serde_json::from_slice(&body).expect("version is JSON");
        assert_eq!(info["name"], PRODUCT);
        assert_eq!(
            info["storage_mode"], "memory",
            "the version surface reports the real driver's own mode name"
        );
        runtime.shutdown();
    }

    /// A free local port for a real-socket server.
    fn free_bind() -> SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("an ephemeral bind")
            .local_addr()
            .expect("an ephemeral address")
    }

    /// One HTTP/1.1 request over a real TCP connection; the whole response
    /// as bytes (headers plus body).
    async fn http_exchange(
        bind: SocketAddr,
        request: &str,
        body: &[u8],
    ) -> std::io::Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(bind).await?;
        let head = format!(
            "{request}\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(body).await?;
        // No write-side half-close here: hyper answers `Connection: close`
        // by closing first, but a client EOF mid-exchange reads as the
        // connection ending and the answer is dropped unsent.
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok(response)
    }

    fn utf8(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(bytes)
    }

    /// The whole lifecycle on real sockets: the server answers health and
    /// version, keeps a real OTLP export, and on the shutdown trigger
    /// drains and reports — one graph end to end.
    #[tokio::test]
    async fn serves_otlp_and_drains_on_shutdown() {
        let bind = free_bind();
        let (trigger, gate) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_with_shutdown(
            ServerConfig {
                bind,
                web_dist: None,
            },
            async move {
                let _ = gate.await;
            },
        ));

        // Wait for readiness by polling the health surface on the socket.
        let mut health = None;
        for _ in 0..500 {
            match http_exchange(bind, "GET /healthz HTTP/1.1", b"").await {
                Ok(answer) if utf8(&answer).contains("200 OK") => {
                    health = Some(answer);
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
        let health = health.expect("the server answers /healthz on a real socket");
        assert!(
            utf8(&health).contains("\"status\":\"ok\""),
            "health is honest JSON: {}",
            utf8(&health)
        );

        // The version surface names the real driver.
        let version = http_exchange(bind, "GET /version HTTP/1.1", b"")
            .await
            .expect("version answers");
        assert!(utf8(&version).contains("\"storage_mode\":\"memory\""));

        // A real OTLP export is admitted and kept behind the pump.
        let request = runtime_trail_telemetry_ingestion::fixtures::encode(
            &runtime_trail_telemetry_ingestion::fixtures::traces_request(vec![
                runtime_trail_telemetry_ingestion::fixtures::resource_spans(
                    None,
                    vec![runtime_trail_telemetry_ingestion::fixtures::scope_spans(
                        None,
                        vec![runtime_trail_telemetry_ingestion::fixtures::trace_span(
                            "op", [0x01; 16], [0x11; 8],
                        )],
                    )],
                ),
            ]),
        );
        let export = http_exchange(
            bind,
            "POST /v1/traces HTTP/1.1\r\nContent-Type: application/x-protobuf",
            &request,
        )
        .await
        .expect("the export answers");
        let export = utf8(&export);
        assert!(
            export.starts_with("HTTP/1.1 200 OK"),
            "the export is admitted: {export}"
        );

        // The shutdown trigger: drain, join, report.
        trigger.send(()).expect("the server is still running");
        let summary = server
            .await
            .expect("the server task joins")
            .expect("the server serves cleanly");
        assert_eq!(summary.kept, 1, "the export reached storage");
        assert_eq!(summary.dropped_on_drain, 0, "nothing was dropped");
    }

    /// Opens a raw connection to `bind`, retrying until the server accepts.
    /// Sends no request: the socket is an in-flight connection to the
    /// server — precisely what the accept cap accounts.
    async fn open_connection(bind: SocketAddr) -> tokio::net::TcpStream {
        let mut last_error = None;
        for _ in 0..100 {
            match tokio::net::TcpStream::connect(bind).await {
                Ok(stream) => return stream,
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!("the server never accepted a connection: {last_error:?}");
    }

    /// Reads from `stream` until the answer contains "200 OK" or EOF.
    async fn read_until_ok(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut response = Vec::new();
        let mut buf = [0u8; 256];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => return response,
                Ok(read) => {
                    response.extend_from_slice(&buf[..read]);
                    if String::from_utf8_lossy(&response).contains("200 OK") {
                        return response;
                    }
                }
                Err(error) => panic!("the read failed: {error}"),
            }
        }
    }

    /// The accept seam admits at most `max_connections` concurrent
    /// connections (issue #13): with the cap at two, a third connection
    /// must stay in the kernel backlog — unanswered — until one of the
    /// two holders ends, and is then admitted and served.
    #[tokio::test]
    async fn connection_cap_bounds_concurrent_connections() {
        use tokio::io::AsyncWriteExt;

        let bind = free_bind();
        let (trigger, gate) = tokio::sync::oneshot::channel::<()>();
        let runtime = CoreRuntime::build(RuntimeConfig {
            max_connections: 2,
            ..RuntimeConfig::default()
        })
        .expect("the capped configuration builds");
        let server_task = tokio::spawn(serve_runtime(
            runtime,
            ServerConfig {
                bind,
                web_dist: None,
            },
            async move {
                let _ = gate.await;
            },
        ));

        // Two holders fill the cap; the third connection is backlogged.
        let holder_a = open_connection(bind).await;
        let holder_b = open_connection(bind).await;
        let mut capped = open_connection(bind).await;

        capped
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("the request writes");
        let not_served =
            tokio::time::timeout(Duration::from_millis(500), read_until_ok(&mut capped)).await;
        assert!(
            not_served.is_err(),
            "a connection beyond the cap must not be served while the cap is held; it answered: {}",
            not_served
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default()
        );

        // Free one holder: the backlogged connection is admitted, and its
        // queued request answered.
        drop(holder_a);
        let served = tokio::time::timeout(Duration::from_secs(5), read_until_ok(&mut capped)).await;
        let served = served.expect("the backlogged connection is admitted once a holder ends");
        assert!(
            utf8(&served).contains("200 OK"),
            "the backlogged connection is served: {}",
            utf8(&served)
        );
        drop(holder_b);
        drop(capped);

        // Drain and join the server cleanly.
        trigger.send(()).expect("the server is still running");
        let summary = server_task
            .await
            .expect("the server task joins")
            .expect("the server serves cleanly");
        assert_eq!(summary.kept, 0, "no telemetry was exported");
    }

    /// Shutdown is bounded by the drain deadline (issue #14): an in-flight
    /// request whose body still drips in when the stop arrives cannot
    /// extend the shutdown past the contract's 5 s — the server task joins
    /// within the deadline plus slack, then cuts what is still in flight.
    #[tokio::test]
    async fn shutdown_is_bounded_by_the_drain_deadline() {
        use tokio::io::AsyncWriteExt;

        let bind = free_bind();
        let (trigger, gate) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_with_shutdown(
            ServerConfig {
                bind,
                web_dist: None,
            },
            async move {
                let _ = gate.await;
            },
        ));

        let mut stream = open_connection(bind).await;
        // A body under the payload ceiling, drip-fed one byte at a time: a
        // slow-drip client that would eventually hit the 10 s body read
        // timeout (408), but is still in flight when the drain deadline —
        // anchored at the stop's arrival — expires first.
        let request = "POST /v1/traces HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-protobuf\r\nContent-Length: 1024\r\n\r\n";
        stream
            .write_all(request.as_bytes())
            .await
            .expect("the request head writes");
        for _ in 0..2 {
            stream.write_all(b"x").await.expect("the drip writes");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        trigger.send(()).expect("the server is still running");

        // Keep dripping while the server drains; writes after the deadline
        // cuts the connection fail and are ignored — the drip only exists
        // to keep the request in flight past the deadline.
        let joined = tokio::time::timeout(Duration::from_secs(8), async {
            let drip = async {
                for _ in 0..14 {
                    let _ = stream.write_all(b"x").await;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            };
            tokio::pin!(drip);
            tokio::select! {
                biased;
                joined = server => joined,
                () = drip => unreachable!("the drip outlives the join race"),
            }
        })
        .await;
        let _summary = joined
            .expect(
                "shutdown is not bounded by the drain deadline: the server still held the in-flight body after 8 s",
            )
            .expect("the server task joins")
            .expect("the server serves cleanly");
    }

    /// The index page is honest about what is and is not served: ingestion
    /// yes, and the Investigation API is served over its trace endpoint.
    #[tokio::test]
    async fn the_index_claims_the_investigation_api() {
        let runtime = test_support::runtime();
        let response = build_router(Arc::clone(&runtime), ServerConfig::default())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())
                    .expect("static request builds"),
            )
            .await
            .expect("in-memory service answers");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        let page = String::from_utf8_lossy(&body);
        assert!(page.contains(PRODUCT));
        assert!(
            page.contains("OTLP"),
            "ingestion is served and the page says so: {page}"
        );
        assert!(
            page.to_lowercase().contains("investigation api is served"),
            "the page claims the Investigation API: {page}"
        );
        assert!(
            page.contains("/v1/investigations/traces"),
            "the page names the trace endpoint: {page}"
        );
        runtime.shutdown();
    }
}
