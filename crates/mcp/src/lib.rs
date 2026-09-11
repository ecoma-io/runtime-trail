//! The MCP server: the AI-agent surface of `runtime-trail`.
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
//! # Bootstrap status
//!
//! Declared boundary only — no MCP protocol implementation and no tools.
//! Tools land in Phase 4, designed against the then-existing Investigation
//! API; see `docs/roadmap/phases.md`.

/// This crate's version, as declared in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

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
