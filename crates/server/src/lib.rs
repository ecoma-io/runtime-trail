//! The composition root and HTTP server of `runtime-trail`'s native core.
//!
//! This crate is [`layer-app`]: the one place allowed to name concrete
//! storage drivers and wire the Investigation API to a network. It serves
//! exactly three things at bootstrap — `/healthz`, `/version`, and (when a
//! UI build is supplied) the static Loom app. tokio + axum live here and
//! nowhere else; see `docs/decisions/0001-runtime-language.md` for why the
//! HTTP stack is confined to the composition root, and
//! `docs/architecture/boundaries.md` for the dependency law.
//!
//! [`layer-app`]: ../../docs/architecture/boundaries.md
//!
//! # Bootstrap status
//!
//! Smoke surfaces only: no Investigation API, no UI routes beyond static
//! serving, no OTLP ingestion. Those land with their phases —
//! `docs/roadmap/phases.md`.

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use runtime_trail_storage_memory::MemoryStore;
use serde::Serialize;
use tower_http::services::ServeDir;

/// The product name reported by the version surface.
pub const PRODUCT: &str = "runtime-trail";

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

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

/// What the composition root selected for this run.
///
/// The store is held, not just named: the composition root really constructs
/// the concrete driver it selected — memory mode is the only selection that
/// exists at bootstrap, and file-backed mode arrives with Phase 3
/// (`docs/roadmap/phases.md`).
#[derive(Debug, Clone, Copy)]
pub struct CoreState {
    store: MemoryStore,
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

/// Builds the application router: health/version surfaces plus static UI
/// serving when a build directory is configured.
pub fn build_router(config: ServerConfig) -> Router {
    let state = CoreState { store: MemoryStore };
    let router = Router::new()
        .route("/healthz", get(health))
        .route("/version", get(version).with_state(state));
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

async fn version(State(state): State<CoreState>) -> Json<VersionInfo> {
    Json(VersionInfo {
        name: PRODUCT,
        version: VERSION,
        storage_mode: state.store.mode_name(),
    })
}

/// The page served when no UI build was supplied — an honest smoke surface,
/// not a placeholder pretending to be the product.
async fn builtin_index() -> Html<String> {
    Html(format!(
        "<!doctype html><title>{PRODUCT}</title>\
         <h1>{PRODUCT} {VERSION}</h1>\
         <p>Native core is running. No UI build was supplied; start the \
         server with <code>--web-dist &lt;path&gt;</code> to serve the \
         Loom app.</p>"
    ))
}

/// Binds and serves until shutdown is requested (Ctrl-C or SIGTERM).
///
/// # Errors
///
/// Returns the underlying `std::io::Error` when the bind address cannot be
/// opened, and when the accept loop itself fails.
pub async fn serve(config: ServerConfig) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    let router = build_router(config);
    tracing::info!(bind = %listener.local_addr()?, "runtime-trail core listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
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
mod tests {
    use super::*;
    use std::path::Path;

    use tower::ServiceExt;

    #[test]
    fn exposes_a_version() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn depends_on_the_investigation_api_and_the_memory_driver() {
        assert!(!runtime_trail_investigation::VERSION.is_empty());
        assert_eq!(runtime_trail_storage_memory::MemoryStore::NAME, "memory");
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
        let response = build_router(ServerConfig::default())
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
        let response = build_router(ServerConfig::default())
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
        assert_eq!(info["storage_mode"], MemoryStore::NAME);
    }
}
