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
        .setup(|_app| {
            let config = runtime_trail_server::ServerConfig::default();
            tauri::async_runtime::spawn(async move {
                if let Err(error) = runtime_trail_server::serve(config).await {
                    eprintln!("runtime-trail core stopped: {error}");
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running the runtime-trail desktop shell");
}

#[cfg(test)]
mod tests {
    #[test]
    fn shells_the_same_core() {
        // The shell's only core dependency is the server crate; this pins
        // that edge in code, where the compiler — not a comment — holds it.
        assert!(!runtime_trail_server::VERSION.is_empty());
    }
}
