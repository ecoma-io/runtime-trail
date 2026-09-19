//! The MCP server: the AI-agent surface of `runtime-trail` (issue #36).
//!
//! A peer of the UI, never a back door ([`layer-agent`]): it depends on the
//! Investigation API and nothing else in the core, exposes no agent-only
//! data path, and inherits the same budgets as UI traffic. The boundary is
//! `docs/architecture/mcp-model.md` and
//! `docs/decisions/0005-mcp-boundary.md`; the dependency law is
//! `docs/architecture/boundaries.md`.
//!
//! [`layer-agent`]: ../../docs/architecture/boundaries.md
//!
//! # Status
//!
//! Implemented for the 1.0 wave (issue #36): four committed tools —
//! [`tools::INVESTIGATE_TRACE`], [`tools::INVESTIGATE_LOG`],
//! [`tools::INVESTIGATE_METRIC`], [`tools::CONTINUE_INVESTIGATION`] — over
//! a synchronous JSON-RPC 2.0 stdio transport ([`protocol::serve`]), with
//! the Investigation API's one committed flow presented field-for-field as
//! the HTTP surface (`crates/server/src/investigation_http.rs`) presents
//! it: the same subject parsing, the same admitted budget and chain
//! ceilings, the same envelope (`render::render_investigation`), the same
//! error vocabulary. See [`tools`] for exactly what is committed and what
//! is deliberately not fabricated.
//!
//! The transport is hand-rolled sync stdio framing on purpose: the
//! maintained MCP SDKs are tokio-based, and tokio is confined to
//! `crates/server` by AGENTS.md — this crate couples to no async runtime.
//!
//! # Modules
//!
//! - [`tools`] — the four tools: argument parsing and the committed flow.
//! - [`render`] — the envelope renderer, mirroring the HTTP surface.
//! - [`protocol`] — the JSON-RPC 2.0 stdio transport.

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod protocol;
pub mod render;
pub mod tools;

pub use protocol::{serve, serve_stdio};
pub use tools::{TOOLS, Tool, ToolError};

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_a_version() {
        assert!(!super::VERSION.is_empty());
    }

    #[test]
    fn depends_on_the_investigation_api() {
        assert!(!runtime_trail_investigation::VERSION.is_empty());
    }
}
