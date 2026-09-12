//! Admission signals and per-record outcomes: what ingestion says when it
//! says no.
//!
//! `docs/architecture/runtime-constraints.md`, "The backpressure
//! architecture", contracts the wire behaviour of every admission signal —
//! which is retryable and which is not, and what each maps to on the
//! transport (the transport itself lives in `crates/server`, Phase 1 wave 3;
//! ADR 0001 keeps it out of this crate). This module is the typed contract
//! those mappings will consume: exactly one retryable signal, and per-record
//! refusals that name their reason.

use runtime_trail_telemetry_model::{
    BudgetRejection, DuplicateKey, EntityId, MixedKindArray, SeverityOutOfRange, StreamShapeError,
};

/// The export-level signal a whole ingest call can fail with.
///
/// One of these is **retryable** and none of the others are, exactly as
/// `runtime-constraints.md` contracts it:
///
/// | Signal            | Retryable | Transport mapping (server, wave 3)                  |
/// | ----------------- | --------- | --------------------------------------------------- |
/// | [`AdmissionSignal::QueueSaturated`] | **yes** | HTTP 429 + `Retry-After`; gRPC `RESOURCE_EXHAUSTED` |
/// | [`AdmissionSignal::PayloadOverCap`] | no      | non-retryable reject at the transport edge          |
/// | [`AdmissionSignal::ExportOverCap`]  | no      | `partial_success` naming the budget, or export reject where the transport cannot do partial |
/// | [`AdmissionSignal::MalformedRequest`] | no    | non-retryable export reject (the bytes cannot become a request) |
/// | [`AdmissionSignal::Draining`]       | no¹     | HTTP 503 / gRPC `UNAVAILABLE`                       |
///
/// ¹ `Draining` is the *closing* signal: a spec-conformant emitter may retry
/// it, but the runtime keeps answering until shutdown completes and admits
/// nothing new — the signal exists so an emitter does not hang on a silent
/// connection, not to invite a retry that will succeed.
///
/// Per-record refusals are **not** signals: they ride
/// [`ExportOutcome`](crate::ExportOutcome) as
/// [`RecordOutcome::Rejected`](crate::RecordOutcome::Rejected) so a mixed
/// export can come back as `partial_success` naming which records were
/// rejected and why. Every per-record refusal is non-retryable — the payload
/// is the problem, and retrying cannot shrink it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionSignal {
    /// A bounded hand-off queue is at its accounted-byte ceiling. The one
    /// transient condition in admission, and therefore the one retryable
    /// signal: space may free, so a retry can succeed. Overflow rejects the
    /// producer — every record admitted *before* saturation was already
    /// handed off, and its queue entry stays in flight whatever the
    /// producer does next. A retry's re-deliveries of those records
    /// collapse (spans, metric points) or re-admit (log records, which
    /// have no natural identity) — a collapse never queues, so the retry
    /// adds no second copy of what is already in flight.
    ///
    /// The record whose own offer was refused is ended by the pipeline
    /// before this signal returns: the ledger entry admission just created
    /// is forgotten (and its freshly interned stream released), by the
    /// same lifecycle ADR 0008 gives every other way a record fails to
    /// stay resident. No entry stands behind a delivery that never
    /// happened — so the retry this signal invites can deliver, and no
    /// collapse onto a stranded identity silently swallows the record.
    QueueSaturated {
        /// The queue that refused, named for logs and metrics.
        queue: &'static str,
        /// The queue's accounted-byte ceiling.
        ceiling_bytes: usize,
        /// The accounted size of the record that did not fit.
        attempted_bytes: usize,
    },
    /// The export payload exceeded the OTLP payload ceiling, refused before
    /// parsing. A property of the payload: retrying cannot shrink it.
    PayloadOverCap {
        /// The payload's size in bytes.
        bytes: usize,
        /// The ceiling it exceeded (default 4 MiB).
        ceiling_bytes: usize,
    },
    /// The export carried more data points than the per-export cap. Refused
    /// whole — nothing from the export is admitted. A property of the
    /// payload: retrying cannot shrink it.
    ExportOverCap {
        /// The named budget: [`runtime_trail_telemetry_model::BudgetName::DataPointsPerExport`],
        /// its limit and the observed count.
        rejection: BudgetRejection,
    },
    /// The payload did not parse as an OTLP export request at all (including
    /// wire recursion past the protobuf decoder's limit). The bytes cannot
    /// become a request, so there is nothing to retry.
    MalformedRequest {
        /// What the decoder said, for logs — never parsed back.
        detail: String,
    },
    /// The session is draining (shutdown begun): nothing new is admitted
    /// from this moment. The closing signal — see the table above for its
    /// transport mapping.
    Draining,
}

impl AdmissionSignal {
    /// True only for [`AdmissionSignal::QueueSaturated`] — the one
    /// retryable admission signal, by contract.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::QueueSaturated { .. })
    }
}

/// What admission did with one record of an export, position by position.
///
/// The positions of an [`ExportOutcome`](crate::ExportOutcome) are flat
/// indexes over the export's records in document order (spans within their
/// `ResourceSpans`/`ScopeSpans` nesting, log records likewise, metric data
/// points likewise) — the same numbering `partial_success` will name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    /// A new record entered the runtime; the id names it for this session.
    Admitted {
        /// The record's entity id.
        entity: EntityId,
    },
    /// A re-delivery of a record already admitted under a natural identity:
    /// it collapsed onto the record already admitted, which is what the id
    /// names. Nothing is handed to the queue — the record is already there.
    Collapsed {
        /// The surviving record's entity id.
        entity: EntityId,
    },
    /// A delivery conflicted with an already-admitted record under the same
    /// natural identity: the first stands (the id names it), the conflict is
    /// recorded as an admission anomaly, and nothing is handed to the queue.
    Conflict {
        /// The standing record's entity id.
        entity: EntityId,
    },
    /// The record was refused. Non-retryable; the reason names the budget
    /// where a budget fired.
    Rejected {
        /// Why the record was refused.
        reason: RecordRejection,
    },
}

/// Why one record was refused. Every variant is non-retryable: the payload
/// is the problem, and retrying the same bytes reproduces the refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordRejection {
    /// A model budget was exceeded — the refusal names the budget, its
    /// limit and the observed spend (as `partial_success` will).
    Budget(BudgetRejection),
    /// The record's shape is one the contract cannot represent — for a
    /// metric point, an identity whose kind disagrees with the point's
    /// shape (the shape law, enforced through the admission ledger).
    Shape(StreamShapeError),
    /// The wire carried something no model record can carry — a
    /// wrong-length id, an unknown enum value, a value-shaped field with no
    /// value, a resource feature the model has no slot for. See
    /// [`Unrepresentable`].
    Unrepresentable(Unrepresentable),
    /// A keyed container carried a duplicate key; keeping one occurrence
    /// would silently drop a value the emitter sent.
    DuplicateKey(DuplicateKey),
    /// An array carried elements of more than one value kind.
    MixedKindArray(MixedKindArray),
    /// A severity number outside the model's domain (1–24).
    Severity(SeverityOutOfRange),
}

impl RecordRejection {
    /// Always `false`: a per-record refusal is never retryable.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        false
    }
}

impl std::fmt::Display for RecordRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Budget(rejection) => rejection.fmt(f),
            Self::Shape(error) => error.fmt(f),
            Self::Unrepresentable(error) => error.fmt(f),
            Self::DuplicateKey(error) => error.fmt(f),
            Self::MixedKindArray(error) => error.fmt(f),
            Self::Severity(error) => error.fmt(f),
        }
    }
}

/// Wire bytes no model record can carry.
///
/// The model refuses what it cannot represent faithfully — it never
/// truncates, coerces or invents ([`telemetry-model.md`], "Conformance
/// language"; ADR 0006).
///
/// [`telemetry-model.md`]: ../../docs/architecture/telemetry-model.md
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Unrepresentable {
    /// An id field carried the wrong number of bytes for its width (trace
    /// ids are 16 bytes, span ids 8). Trimming or padding would invent ids
    /// the emitter did not send.
    IdLength {
        /// Which field, for the refusal message.
        field: &'static str,
        /// The width the model carries.
        expected: usize,
        /// The width the wire carried.
        found: usize,
    },
    /// An open protobuf enum carried a value the model has no variant for
    /// (a span kind above `CONSUMER`, a status code above `ERROR`, an
    /// aggregation temporality the model does not know).
    UnknownEnumValue {
        /// Which field, for the refusal message.
        field: &'static str,
        /// The value as sent.
        value: i32,
    },
    /// A field carried nothing the model can represent: a value-shaped
    /// field with no value at all (an attribute key with an unset
    /// `AnyValue`, a number point without its `as_int`/`as_double` oneof),
    /// or an attribute key present only as a Profiling string-table
    /// reference — absent by the proto's own receiver contract, which
    /// leaves the attribute keyless. The model has no "half-empty
    /// attribute" state to coerce to.
    MissingValue {
        /// Which field, for the refusal message.
        field: &'static str,
    },
    /// A `Metric` carried no data at all (the `data` oneof unset) — there
    /// is no stream kind to admit points under.
    EmptyMetric,
    /// The resource carried `entity_refs` (added to the OTLP resource in
    /// v1.11, status Alpha). The telemetry model has no slot for it, and
    /// refusing is the honest alternative to silently dropping what the
    /// emitter sent.
    UnsupportedEntityRefs,
    /// A `trace_state` string carried a member the model's ordered
    /// (vendor, value) entry list cannot represent — a member with no `=`.
    TraceState {
        /// The raw string as sent, for the refusal message.
        raw: String,
    },
}

impl std::fmt::Display for Unrepresentable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdLength {
                field,
                expected,
                found,
            } => write!(
                f,
                "{field} carried {found} bytes where the model carries {expected}"
            ),
            Self::UnknownEnumValue { field, value } => {
                write!(f, "{field} carried the unknown enum value {value}")
            }
            Self::MissingValue { field } => {
                write!(
                    f,
                    "{field} carried no value; the model cannot coerce an absent value"
                )
            }
            Self::EmptyMetric => {
                write!(
                    f,
                    "metric carried no data: there is no stream kind to admit points under"
                )
            }
            Self::UnsupportedEntityRefs => {
                write!(
                    f,
                    "resource carried entity_refs (OTLP v1.11 Alpha), which the model has no slot for"
                )
            }
            Self::TraceState { raw } => {
                write!(
                    f,
                    "trace_state {raw:?} is not a list of vendor=value entries"
                )
            }
        }
    }
}

impl std::error::Error for Unrepresentable {}
