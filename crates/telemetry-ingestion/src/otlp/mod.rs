//! The OTLP protobuf message types, as generated once by `prost-build`.
//!
//! These are **checked-in generated artifacts**, not hand-written types: the
//! five `opentelemetry.proto.*.rs` files beside this module are unmodified
//! `prost-build` output, byte-identical to what
//! `crates/telemetry-ingestion/regenerate.sh` produces from the vendored
//! upstream sources in `crates/telemetry-ingestion/protos/` (pinned, with
//! hashes, in `protos/PROVENANCE.md`). Nothing here may be edited by hand;
//! a change to the wire shapes is a change to the pinned upstream version,
//! regenerated and re-pinned in the open.
//!
//! Why generated-and-committed rather than a third-party proto crate: the
//! available pre-generated crate (the OpenTelemetry Rust SDK's
//! `opentelemetry-proto`) couples its versioning to the SDK release train,
//! compiles transport-code siblings (`gen-tonic`) this crate must never see
//! (ADR 0001: no transport dependency outside `layer-app`), and pins its own
//! `prost` major — a second protobuf runtime in a resource-budgeted core.
//! Building from `.proto` sources at every build (`build.rs` + `prost-build`)
//! would require `protoc` on every contributor machine and in CI, against
//! the ambient-toolchain rule (ADR 0001). Committed output keeps the build
//! graph at exactly `prost` — one runtime, no transport, no build-time
//! tool — while the vendored sources and the pinned regenerator keep the
//! artifacts reproducible and reviewable.
//!
//! The module tree mirrors the proto package nesting (`common.v1`,
//! `resource.v1`, `trace.v1`, `logs.v1`, `metrics.v1`) so the generated
//! `super::super::…` cross-references resolve unchanged.
//!
//! Lint and formatting exemptions below are scoped to these generated
//! modules only — generated code is judged by its generator, never by the
//! hand-written lint bar, and must stay byte-identical to `prost-build`
//! output (which is why they ride `include!`, invisible to rustfmt). No
//! product code in this crate is exempt from anything. `dead_code` is
//! silenced at this module level (never inside the generated files) because
//! a complete wire contract necessarily contains message types one
//! direction of the transport does not construct — the response messages
//! are the server's, wave 3.

#[rustfmt::skip]
#[allow(clippy::all)]
#[allow(clippy::pedantic)]
#[allow(dead_code)]
pub mod opentelemetry {
    /// `opentelemetry.proto.common.v1` — attribute values, key-value lists,
    /// the instrumentation scope.
    #[rustfmt::skip]
    #[allow(clippy::all)]
    pub mod common {
        #[rustfmt::skip]
        #[allow(clippy::all)]
        pub mod v1 {
            include!("opentelemetry.proto.common.v1.rs");
        }
    }

    /// `opentelemetry.proto.resource.v1` — the resource envelope.
    #[rustfmt::skip]
    #[allow(clippy::all)]
    pub mod resource {
        #[rustfmt::skip]
        #[allow(clippy::all)]
        pub mod v1 {
            include!("opentelemetry.proto.resource.v1.rs");
        }
    }

    /// `opentelemetry.proto.trace.v1` — the span export request.
    #[rustfmt::skip]
    #[allow(clippy::all)]
    pub mod trace {
        #[rustfmt::skip]
        #[allow(clippy::all)]
        pub mod v1 {
            include!("opentelemetry.proto.trace.v1.rs");
        }
    }

    /// `opentelemetry.proto.logs.v1` — the log export request.
    #[rustfmt::skip]
    #[allow(clippy::all)]
    pub mod logs {
        #[rustfmt::skip]
        #[allow(clippy::all)]
        pub mod v1 {
            include!("opentelemetry.proto.logs.v1.rs");
        }
    }

    /// `opentelemetry.proto.metrics.v1` — the metric export request.
    #[rustfmt::skip]
    #[allow(clippy::all)]
    pub mod metrics {
        #[rustfmt::skip]
        #[allow(clippy::all)]
        pub mod v1 {
            include!("opentelemetry.proto.metrics.v1.rs");
        }
    }

    /// `opentelemetry.proto.collector.*.v1` — the export request/response
    /// envelopes the OTLP endpoints carry. Only the request messages are
    /// admission's input; the responses are the transport's (wave 3), kept
    /// here so one codegen run covers the whole wire contract.
    #[rustfmt::skip]
    #[allow(clippy::all)]
    pub mod collector {
        #[rustfmt::skip]
        #[allow(clippy::all)]
        pub mod trace {
            #[rustfmt::skip]
            #[allow(clippy::all)]
            pub mod v1 {
                include!("opentelemetry.proto.collector.trace.v1.rs");
            }
        }

        #[rustfmt::skip]
        #[allow(clippy::all)]
        pub mod logs {
            #[rustfmt::skip]
            #[allow(clippy::all)]
            pub mod v1 {
                include!("opentelemetry.proto.collector.logs.v1.rs");
            }
        }

        #[rustfmt::skip]
        #[allow(clippy::all)]
        pub mod metrics {
            #[rustfmt::skip]
            #[allow(clippy::all)]
            pub mod v1 {
                include!("opentelemetry.proto.collector.metrics.v1.rs");
            }
        }
    }
}
