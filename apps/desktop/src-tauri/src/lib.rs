//! The desktop shell of `runtime-trail`.
//!
//! A shell, nothing more ([`layer-app`]): it starts the **same native core**
//! the CLI and the Docker image run — `runtime_trail_server::serve`,
//! in-process — and presents the UI in a webview. Any telemetry, query or
//! storage logic appearing here would be a second core implementation, which
//! `docs/architecture/system.md` forbids.
//!
//! [`layer-app`]: ../../../docs/architecture/boundaries.md
//!
//! The shell starts the core on the default bind address; running the CLI
//! server alongside the desktop app therefore fails the second bind with
//! "address already in use" — one core per machine session, by design.
//!
//! # Lifecycle
//!
//! The shell closes the core down the core's own drain path: when the last
//! window closes, the close hook fires the core's shutdown trigger and the
//! app stays alive — for at most the core's [`DRAIN_DEADLINE`] — while the
//! closing is observable: emitters meet the draining wire signal (HTTP 503 /
//! gRPC `UNAVAILABLE`), in-flight work is cut at the deadline, and the
//! session's `RunSummary` is surfaced before the process exits. This is the
//! same drain the CLI drives on Ctrl-C/SIGTERM
//! (`docs/architecture/system.md`, "Lifecycle"); the shell adds no second
//! shutdown implementation.

use std::sync::Mutex;

use tauri::Manager;
use tauri::async_runtime::{Sender, channel};

pub use runtime_trail_server::{DRAIN_DEADLINE, ServerConfig};

/// The shell's hand on the in-process core: the shutdown trigger and the
/// serve task's join handle.
///
/// Created by [`start_core`] and owned by the app until close; the close
/// hook takes it exactly once, fires the trigger and joins the task on the
/// async runtime.
pub struct CoreHandle {
    /// The drain trigger: resolving it — or dropping it — closes the core
    /// down `serve_with_shutdown`'s drain path: the runtime begins draining
    /// (emitters meet the closing wire answer), the pump spends at most
    /// [`DRAIN_DEADLINE`] finishing what admission already queued, and the
    /// session's `RunSummary` is produced.
    pub shutdown: Sender<()>,
    /// The core's serve task: resolves — bounded by the core's drain
    /// deadline — once the session is closed, yielding the session's
    /// `runtime_trail_server::runtime::RunSummary`.
    pub serve: tauri::async_runtime::JoinHandle<
        std::io::Result<runtime_trail_server::runtime::RunSummary>,
    >,
}

/// Starts the same native core the CLI runs, in-process, on the async
/// runtime the shell shares with it — the one spawn every shell entry (the
/// windowed app and the lifecycle test) goes through.
#[must_use]
pub fn start_core(config: ServerConfig) -> CoreHandle {
    // The shutdown trigger is the serve session's own: `serve_with_shutdown`
    // resolves on it, then the core performs its observable closing — the
    // draining wire signal, the bounded drain, the summary. No second
    // shutdown implementation lives in the shell.
    let (shutdown, mut gate) = channel::<()>(1);
    let serve = tauri::async_runtime::spawn(async move {
        runtime_trail_server::serve_with_shutdown(config, async move {
            let _ = gate.recv().await;
        })
        .await
    });
    CoreHandle { shutdown, serve }
}

/// The core under the app, owned until the close hook takes it. The inner
/// `Option` makes the take exactly-once: the second close request is the
/// exit that follows the completed drain, and must pass through.
struct ManagedCore(Mutex<Option<CoreHandle>>);

/// Boots the desktop shell: one webview window over one in-process core.
///
/// # Panics
///
/// Panics if the webview event loop cannot start — most commonly because the
/// embedded context is invalid (`tauri::generate_context!` panics at compile
/// time on a missing or unreadable bundle icon, so a bad icon surfaces as a
/// build error first) or because no windowing backend is reachable. A shell
/// that cannot open its window has nothing to fall back to.
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            app.manage(ManagedCore(Mutex::new(Some(start_core(
                ServerConfig::default(),
            )))));
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building the runtime-trail desktop shell")
        .run(drain_on_close);
}

/// The close hook: the last window closing asks the core to stop, then
/// keeps the app alive — for at most the core's drain deadline — while the
/// closing is observable, and exits once the drained summary is in.
///
/// The hook never blocks the event loop: the shutdown trigger is a
/// non-blocking send, and the join is awaited on the async runtime the core
/// runs on, so the deadline cut stays inside the core. Exiting before the
/// join would kill the process mid-drain and the closing would never be
/// observable; the exit is deferred to after it.
fn drain_on_close(app: &tauri::AppHandle, event: tauri::RunEvent) {
    let tauri::RunEvent::ExitRequested { api, .. } = event else {
        return;
    };
    // The core is taken here, once. The ExitRequested this hook itself
    // triggers via `exit(0)` — after the join — finds the slot empty and
    // passes through; the drain can never loop.
    let state = app.state::<ManagedCore>();
    let Some(core) = state.0.lock().expect("managed core state").take() else {
        return;
    };
    api.prevent_exit();
    let _ = core.shutdown.try_send(());
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        match core.serve.await {
            Ok(Ok(summary)) => eprintln!("runtime-trail core drained: {summary:?}"),
            Ok(Err(error)) => eprintln!("runtime-trail core failed while closing: {error}"),
            Err(error) => eprintln!("runtime-trail core task was aborted: {error}"),
        }
        app.exit(0);
    });
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::time::{Duration, Instant};

    use super::{DRAIN_DEADLINE, ServerConfig, start_core};
    #[test]
    fn shells_the_same_core() {
        // The shell's only core dependency is the server crate; this pins
        // that edge in code, where the compiler — not a comment — holds it.
        assert!(!runtime_trail_server::VERSION.is_empty());
    }

    /// The desktop shell's drain lifecycle over the real core (#61): the
    /// same spawn `run()` uses serves a real core on a real loopback port;
    /// the same channel the close hook fires shuts it down; an emitting
    /// client observes the closing; an uncompleted in-flight export is cut
    /// at the ≤5 second drain deadline; and the session's `RunSummary`
    /// comes out of the join. Nothing is mocked.
    #[test]
    fn shells_drain_the_core_on_close() {
        // The lifecycle runs headless (no windowing system needed): the
        // global runtime the shell's spawn uses is created lazily. The
        // watchdog turns a missing drain — the awaited join never resolving
        // — into a fast failure instead of a hung test suite.
        let worker = std::thread::spawn(|| {
            tauri::async_runtime::block_on(async { drain_lifecycle().await });
        });
        let hard_limit = DRAIN_DEADLINE + Duration::from_secs(10);
        let started = Instant::now();
        loop {
            if worker.is_finished() {
                break;
            }
            assert!(
                started.elapsed() <= hard_limit,
                "the drained join did not resolve within {hard_limit:?}"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        worker.join().expect("the lifecycle assertions pass");
    }

    /// Drives one full shell lifecycle over real sockets.
    async fn drain_lifecycle() {
        // A loopback port that is free right now, handed to the core
        // (probe-then-release, the same idiom the server's wire tests use).
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("an ephemeral bind");
        let addr = probe.local_addr().expect("an ephemeral address");
        drop(probe);

        let core = start_core(ServerConfig {
            bind: addr,
            web_dist: None,
        });

        // The same core the CLI serves is live through the shell's spawn —
        // polled, because the spawn binds asynchronously.
        let mut health = None;
        for _ in 0..500 {
            match exchange(addr, &health_head(), b"") {
                Ok(answer) if answer.starts_with("HTTP/1.1 200") => {
                    health = Some(answer);
                    break;
                }
                _ => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        let health = health.expect("the core answers /healthz on a real socket");
        assert!(
            health.contains("\"status\":\"ok\""),
            "health is honest JSON: {health}"
        );

        // An emitting client is admitted while the core serves: the empty
        // ExportTraceServiceRequest answers 200 — nothing admitted, nothing
        // rejected, the same wire contract the server's own tests pin.
        let export = exchange(
            addr,
            &export_head(addr, 2),
            &[0x0A, 0x00], // field 1 (resource_spans): an empty ResourceSpans
        )
        .expect("the emitting client is admitted");
        assert!(
            export.starts_with("HTTP/1.1 200"),
            "the emitting client is admitted while serving: {export}"
        );

        // An emitting client keeps a request in flight: a 4,000,000-byte
        // body announced — under the 4 MiB payload ceiling — with a trickle
        // sent and then silence. The request's read deadline (10 s) outlives
        // the drain deadline (5 s), so the in-flight request can only end
        // when the drain cuts it; the join's elapsed time proves the cut.
        let mut drip = TcpStream::connect(addr).expect("an emitting client connects");
        drip.write_all(export_head(addr, 4_000_000).as_bytes())
            .expect("the export head writes");
        drip.write_all(b"trickle").expect("the trickle writes");

        // The close hook's trigger — the same send `drain_on_close` makes
        // when the last window closes.
        core.shutdown
            .try_send(())
            .expect("the core is still serving");
        // Let the trigger reach the serve loop, then observe the closing
        // from an emitter's side.
        std::thread::sleep(Duration::from_millis(50));

        // The closing is observable: the serve loop admits nothing new, so
        // a fresh export never still answers 200 — it meets the refused or
        // closed listener, or (in the shrinking race where it still reached
        // the router) the draining gate's 503.
        let closing = exchange(addr, &export_head(addr, 0), b"");
        let still_admitting = matches!(
            closing.as_deref(),
            Ok(answer) if answer.starts_with("HTTP/1.1 200")
        );
        assert!(
            !still_admitting,
            "a drained core never still admits: {closing:?}"
        );

        // The drained join: the in-flight request was cut at the ≤5 s
        // deadline — not waited out — and the session's summary came out of
        // the join, the same report the shell prints at exit.
        let started = Instant::now();
        let joined = core.serve.await;
        let elapsed = started.elapsed();
        let summary = match joined {
            Ok(Ok(summary)) => summary,
            Ok(Err(error)) => panic!("the core failed while closing: {error}"),
            Err(error) => panic!("the core task was aborted: {error}"),
        };
        assert!(
            elapsed <= DRAIN_DEADLINE + Duration::from_millis(1500),
            "the drain is bounded by the deadline: {elapsed:?}"
        );
        assert!(
            elapsed
                >= DRAIN_DEADLINE
                    .checked_sub(Duration::from_secs(1))
                    .expect("the drain deadline exceeds one second"),
            "the drain actually ran — the in-flight request held the window \
             until the deadline cut it: {elapsed:?}"
        );
        eprintln!("the core surfaced its run summary: {summary:?}");
        drop(drip);
    }

    /// One HTTP/1.1 exchange over a fresh socket: request head and body in,
    /// the raw response out (or the connect/read error, which is itself an
    /// observable draining signature).
    fn exchange(addr: SocketAddr, head: &str, body: &[u8]) -> std::io::Result<String> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.write_all(head.as_bytes())?;
        stream.write_all(body)?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        Ok(String::from_utf8_lossy(&response).into_owned())
    }

    /// The `/healthz` request head; `Connection: close` is honored by
    /// hyper, so the read ends at the answer.
    fn health_head() -> String {
        "GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_owned()
    }

    /// The head of an OTLP/HTTP traces export announcing a body of `length`
    /// bytes — the same surface the CLI serves.
    fn export_head(addr: SocketAddr, length: usize) -> String {
        format!(
            "POST /v1/traces HTTP/1.1\r\nHost: {addr}\r\n\
             Content-Type: application/x-protobuf\r\n\
             Content-Length: {length}\r\nConnection: close\r\n\r\n"
        )
    }
}
