//! The admission pipeline: decode → model gates → ledger semantics →
//! bounded hand-off, per record, in that order.
//!
//! This is the component `docs/architecture/system.md` draws as
//! "telemetry-ingestion": the one door bytes come in through, bounded
//! before anything else. Each `ingest_*` call is one OTLP export request:
//!
//! 1. **Gate** — a draining session admits nothing new
//!    ([`AdmissionSignal::Draining`]); a payload over the OTLP payload
//!    ceiling is refused before parsing ([`AdmissionSignal::PayloadOverCap`],
//!    4 MiB by contract).
//! 2. **Decode** — prost turns the bytes into OTLP types; anything the
//!    decoder cannot read is [`AdmissionSignal::MalformedRequest`],
//!    including payloads nested past the protobuf decoder's recursion
//!    limit (which is why an arbitrarily deep payload cannot overflow this
//!    process's stack).
//! 3. **Export budgets** — before anything is admitted, the metrics path
//!    counts the export's data points and refuses the whole export over
//!    the per-export cap ([`AdmissionSignal::ExportOverCap`], 10,000 by
//!    contract). Nothing from a refused export is admitted.
//! 4. **Per record** — translate wire → model (refusing what no model
//!    record can carry), then hand the record to the
//!    [`AdmissionLedger`](runtime_trail_telemetry_model::AdmissionLedger):
//!    the shape law, the information budgets, and the duplicate-delivery
//!    semantics (collapse, or conflict-recorded). Only a record that
//!    **admitted** is offered to the bounded hand-off queue. A **collapse**
//!    is a re-delivery of the record already admitted: its original queue
//!    entry — still resident, or already consumed by the consumer — is the
//!    whole story, and offering it again would store a second copy of one
//!    natural identity and stamp the retry's admission time onto a record
//!    admitted earlier. A **conflict** is not offered either: the
//!    conflicting payload is refused, and the record that stands travels
//!    by its own delivery.
//! 5. **Outcome** — every record's fate, position by position, as
//!    [`ExportOutcome`]: the numbering `partial_success` will name.
//!
//! # The memory path
//!
//! Peak coexistence of one export is: the decoded OTLP request **or** the
//! model records translated from it — translation moves (strings, vectors
//! and values are taken out of the wire message, not copied) — plus one
//! `Arc` per envelope resource and scope for all of the export's records,
//! plus the queue's reference counts. Nothing copies a payload to queue
//! it; [`StoredRecord`](crate::StoredRecord) holds the ledger's own `Arc`s.
//! After `ingest_*` returns, the decoded wire message is dropped whole; the
//! model records live on shared, and the queue accounts them at the model's
//! accounted size in full.
//!
//! # Saturation is per export, and honest
//!
//! When the hand-off queue saturates mid-export, the call fails with
//! [`AdmissionSignal::QueueSaturated`] — the one retryable signal — and
//! every record admitted **before** saturation was already offered: its
//! queue entry is resident, and the consumer will receive it whatever the
//! producer does next. A retry re-delivers the whole export; spans and
//! metric points collapse onto their standing records and so queue
//! nothing (the entries already in flight are the delivery), while log
//! records — no natural identity — are admitted and queued again. The one
//! record whose own offer the queue refused stands in the ledger with no
//! queue entry: the signal named that refusal loudly and retryably, and
//! no later collapse silently resurrects a delivery that never happened.

use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicBool, Ordering},
};

use prost::Message;
use runtime_trail_telemetry_model::{
    ATTRIBUTE_MAP_NODE_BYTES, AdmissionAnomalies, AdmissionLedger, AdmissionOutcome, AdmissionTime,
    BudgetLimits, LogRecord, MetricPoint, Resource, Span, StreamIdentity,
    budgets::OTLP_PAYLOAD_BYTES, budgets::check_export_point_count,
};

use crate::decode::{self, Envelope};
use crate::otlp::opentelemetry::{
    collector::{logs::v1 as wire_logs, metrics::v1 as wire_metrics, trace::v1 as wire_trace},
    common::v1 as wire,
};
use crate::queue::{QueuedRecord, RecordSink, StoredRecord};
use crate::signal::{AdmissionSignal, RecordOutcome, RecordRejection};

/// What one ingest call did with every record of its export.
///
/// `records[i]` is record *i* of the export in document order — flat over
/// the `ResourceSpans`/`ScopeSpans` (or logs/metrics equivalents) nesting —
/// the same numbering `partial_success` names. The outcome of a failed
/// call is not returned: the call's [`AdmissionSignal`] replaces it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExportOutcome {
    /// One outcome per record, in document order.
    pub records: Vec<RecordOutcome>,
}

impl ExportOutcome {
    /// The number of records the export carried.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the export carried no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// How many records were admitted (and queued).
    #[must_use]
    pub fn admitted(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, RecordOutcome::Admitted { .. }))
            .count()
    }

    /// How many records were refused.
    #[must_use]
    pub fn rejected(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record, RecordOutcome::Rejected { .. }))
            .count()
    }
}

/// Why a pipeline refused to start: its hand-off sink cannot hold the
/// largest record its own budgets admit.
///
/// A misconfiguration must fail at startup, never as an unadmittable
/// record at runtime: a queue ceiling below one legal record would refuse
/// that record forever behind [`AdmissionSignal::QueueSaturated`] — the
/// one retryable signal — and a retry can never shrink a legal record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipelineConfigError {
    /// The accounted-byte ceiling the sink declared.
    pub ceiling_bytes: usize,
    /// The accounted-byte bound no legal record can exceed —
    /// [`Pipeline::legal_record_bound_bytes`] over this pipeline's budgets.
    pub legal_record_bound_bytes: usize,
}

impl std::fmt::Display for PipelineConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hand-off sink ceiling {} bytes is below the worst-case legal record \
             bound of {} bytes accounted: a record the budgets admit could never \
             be queued, and a retry can never shrink it — raise the sink's ceiling \
             or lower the budgets",
            self.ceiling_bytes, self.legal_record_bound_bytes
        )
    }
}

impl std::error::Error for PipelineConfigError {}

/// The admission pipeline. One per session.
pub struct Pipeline {
    limits: BudgetLimits,
    payload_ceiling_bytes: usize,
    ledger: Mutex<AdmissionLedger>,
    sink: Arc<dyn RecordSink>,
    draining: AtomicBool,
}

impl Pipeline {
    /// A pipeline over `sink`, with the model's default budgets and the
    /// contract payload ceiling ([`OTLP_PAYLOAD_BYTES`]).
    ///
    /// # Panics
    ///
    /// If `sink`'s ceiling is below the default budgets' legal-record
    /// bound. The contract numbers guarantee headroom there (a worst-case
    /// budget-shaped spend of ≈43 MiB against the 64 MiB contract queue
    /// ceiling), so this fires only on a sink deliberately built below the
    /// contract — a startup misconfiguration, which is exactly where it
    /// must be loud.
    pub fn new(sink: Arc<dyn RecordSink>) -> Self {
        Self::with_config(sink, BudgetLimits::default(), OTLP_PAYLOAD_BYTES)
            .expect("the contract budgets fit the contract queue ceiling")
    }

    /// A pipeline with explicit startup configuration: budget limits and
    /// the payload ceiling. Like every number in the resource table, both
    /// are fixed when the session starts and never tuned mid-session.
    ///
    /// Construction refuses a sink whose accounted-byte ceiling is below
    /// the worst-case legal record bound: such a sink could never admit a
    /// record its own budgets make legal, and the refusal must surface
    /// here — at startup, with the bound named — rather than at runtime as
    /// a permanently unadmittable record behind the retryable saturation
    /// signal.
    ///
    /// # Errors
    ///
    /// [`PipelineConfigError`] when the sink's ceiling is below the
    /// worst-case legal record bound computed from `limits`.
    pub fn with_config(
        sink: Arc<dyn RecordSink>,
        limits: BudgetLimits,
        payload_ceiling_bytes: usize,
    ) -> Result<Self, PipelineConfigError> {
        let legal_record_bound_bytes = Self::legal_record_bound_bytes(&limits);
        let ceiling_bytes = sink.ceiling_bytes();
        if ceiling_bytes < legal_record_bound_bytes {
            return Err(PipelineConfigError {
                ceiling_bytes,
                legal_record_bound_bytes,
            });
        }
        Ok(Self {
            limits,
            payload_ceiling_bytes,
            ledger: Mutex::new(AdmissionLedger::new(limits)),
            sink,
            draining: AtomicBool::new(false),
        })
    }

    /// The accounted-byte bound no legal record can exceed — the
    /// worst-case spend the admission budgets shape.
    ///
    /// Every attribute set the gates admit is checked entry by entry (an
    /// entry is keyed-container overhead + key bytes + value accounted
    /// size, capped by `attribute_value_bytes`) and by count, so one set's
    /// accounted size is at most its map's one node charge plus
    /// `count × attribute_value_bytes`. A record's budget-shaped spend is
    /// its own attribute set plus its resource's and scope's, and — per
    /// span — every event's and link's set; per point — every exemplar's.
    /// Taking the three record kinds' worst case gives the bound: a sink
    /// below it can refuse a legal record forever, so
    /// [`Pipeline::with_config`] refuses the sink first.
    ///
    /// Everything a record carries *outside* those sets — names, ids,
    /// timestamps, fixed-width fields, and value payloads the budgets do
    /// not shape — arrives inside the payload, which the payload ceiling
    /// bounds; this bound is the budget-shaped part a queue ceiling must
    /// clear.
    #[must_use]
    pub fn legal_record_bound_bytes(limits: &BudgetLimits) -> usize {
        let attribute_set =
            |count: usize| ATTRIBUTE_MAP_NODE_BYTES + count * limits.attribute_value_bytes;
        // Own attributes, the resource's, and the scope's: three per-signal
        // sets charged to every record kind alike.
        let signal_sets = 3 * attribute_set(limits.attributes_per_signal);
        let span = signal_sets
            + limits.span_events_per_span * attribute_set(limits.attributes_per_nested_set)
            + limits.span_links_per_span * attribute_set(limits.attributes_per_nested_set);
        let point = signal_sets
            + limits.exemplars_per_data_point * attribute_set(limits.attributes_per_nested_set);
        span.max(point)
    }

    /// Begins draining: nothing new is admitted from this moment. The
    /// closing gate of shutdown — records already in flight still drain
    /// through the queue to their consumer.
    pub fn begin_draining(&self) {
        self.draining.store(true, Ordering::Release);
    }

    /// Whether the session is draining.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// The hand-off sink admission offers admitted records to. The
    /// consumer side (the server's pump into storage, wave 3) reads the
    /// same object.
    #[must_use]
    pub fn sink(&self) -> &Arc<dyn RecordSink> {
        &self.sink
    }

    /// The admission anomalies recorded so far this session — conflicts,
    /// counted. Whose loss it was must always be visible.
    ///
    /// # Panics
    ///
    /// Never: the ledger's mutex is only poisoned by a panic inside
    /// admission itself.
    #[must_use]
    pub fn anomalies(&self) -> AdmissionAnomalies {
        *self.lock_ledger().anomalies()
    }

    /// The budget limits this pipeline was built with.
    #[must_use]
    pub fn limits(&self) -> &BudgetLimits {
        &self.limits
    }

    /// Ingests one OTLP `ExportTraceServiceRequest`.
    ///
    /// # Errors
    ///
    /// [`AdmissionSignal::Draining`] once draining has begun,
    /// [`AdmissionSignal::PayloadOverCap`] over the payload ceiling,
    /// [`AdmissionSignal::MalformedRequest`] when the bytes do not parse,
    /// and [`AdmissionSignal::QueueSaturated`] — the one retryable signal —
    /// when the hand-off queue saturates mid-export.
    pub fn ingest_spans(
        &self,
        now: AdmissionTime,
        payload: &[u8],
    ) -> Result<ExportOutcome, AdmissionSignal> {
        self.gate(payload)?;
        let request = Self::decode::<wire_trace::ExportTraceServiceRequest>(payload)?;
        let mut ledger = self.lock_ledger();
        let mut outcome = ExportOutcome::default();
        for resource_spans in request.resource_spans {
            let resource = decode::resource(
                resource_spans.resource.unwrap_or_default(),
                &resource_spans.schema_url,
            );
            for scope_spans in resource_spans.scope_spans {
                let envelope = Self::envelope(
                    &resource,
                    scope_spans.scope.unwrap_or_default(),
                    &scope_spans.schema_url,
                );
                for proto in scope_spans.spans {
                    let recorded = match &envelope {
                        Ok(envelope) => {
                            self.admit_span(&mut ledger, now, decode::span(proto, envelope))
                        }
                        Err(reason) => Ok(RecordOutcome::Rejected {
                            reason: reason.clone(),
                        }),
                    };
                    outcome.records.push(recorded?);
                }
            }
        }
        Ok(outcome)
    }

    /// Ingests one OTLP `ExportLogsServiceRequest`.
    ///
    /// # Errors
    ///
    /// As [`Pipeline::ingest_spans`].
    pub fn ingest_logs(
        &self,
        now: AdmissionTime,
        payload: &[u8],
    ) -> Result<ExportOutcome, AdmissionSignal> {
        self.gate(payload)?;
        let request = Self::decode::<wire_logs::ExportLogsServiceRequest>(payload)?;
        let mut ledger = self.lock_ledger();
        let mut outcome = ExportOutcome::default();
        for resource_logs in request.resource_logs {
            let resource = decode::resource(
                resource_logs.resource.unwrap_or_default(),
                &resource_logs.schema_url,
            );
            for scope_logs in resource_logs.scope_logs {
                let envelope = Self::envelope(
                    &resource,
                    scope_logs.scope.unwrap_or_default(),
                    &scope_logs.schema_url,
                );
                for proto in scope_logs.log_records {
                    let recorded = match &envelope {
                        Ok(envelope) => {
                            self.admit_log(&mut ledger, now, decode::log_record(proto, envelope))
                        }
                        Err(reason) => Ok(RecordOutcome::Rejected {
                            reason: reason.clone(),
                        }),
                    };
                    outcome.records.push(recorded?);
                }
            }
        }
        Ok(outcome)
    }

    /// Ingests one OTLP `ExportMetricsServiceRequest`.
    ///
    /// The export's data points are counted **before** anything is
    /// admitted; an export over the per-export cap is refused whole —
    /// no partial admission of a too-big export.
    ///
    /// # Errors
    ///
    /// As [`Pipeline::ingest_spans`], plus
    /// [`AdmissionSignal::ExportOverCap`] when the export carries more
    /// data points than the per-export budget allows.
    pub fn ingest_metrics(
        &self,
        now: AdmissionTime,
        payload: &[u8],
    ) -> Result<ExportOutcome, AdmissionSignal> {
        self.gate(payload)?;
        let request = Self::decode::<wire_metrics::ExportMetricsServiceRequest>(payload)?;

        let mut total_points = 0;
        for resource_metrics in &request.resource_metrics {
            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    total_points += decode::point_count(metric);
                }
            }
        }
        if let Err(rejection) = check_export_point_count(total_points, &self.limits) {
            return Err(AdmissionSignal::ExportOverCap { rejection });
        }

        let mut ledger = self.lock_ledger();
        let mut outcome = ExportOutcome::default();
        for resource_metrics in request.resource_metrics {
            let resource = decode::resource(
                resource_metrics.resource.unwrap_or_default(),
                &resource_metrics.schema_url,
            );
            for scope_metrics in resource_metrics.scope_metrics {
                let scope = decode::scope(
                    scope_metrics.scope.unwrap_or_default(),
                    &scope_metrics.schema_url,
                );
                for metric in scope_metrics.metrics {
                    let identity = match (&resource, &scope) {
                        (Ok(resource), Ok(scope)) => decode::stream_identity(
                            &metric,
                            &Envelope {
                                resource: Arc::clone(resource),
                                scope: Arc::clone(scope),
                            },
                        ),
                        (Err(reason), _) | (_, Err(reason)) => Err(reason.clone()),
                    };
                    for point in decode::into_points(metric) {
                        // A point whose translation failed is refused with
                        // its own reason inside `admit_point`, keeping its
                        // position.
                        let recorded = match &identity {
                            Ok(identity) => self.admit_point(&mut ledger, now, identity, point),
                            Err(reason) => Ok(RecordOutcome::Rejected {
                                reason: reason.clone(),
                            }),
                        };
                        outcome.records.push(recorded?);
                    }
                }
            }
        }
        Ok(outcome)
    }

    /// The two export-level gates, in order: draining, then payload size.
    fn gate(&self, payload: &[u8]) -> Result<(), AdmissionSignal> {
        if self.draining.load(Ordering::Acquire) {
            return Err(AdmissionSignal::Draining);
        }
        if payload.len() > self.payload_ceiling_bytes {
            return Err(AdmissionSignal::PayloadOverCap {
                bytes: payload.len(),
                ceiling_bytes: self.payload_ceiling_bytes,
            });
        }
        Ok(())
    }

    fn decode<R: Message + Default>(payload: &[u8]) -> Result<R, AdmissionSignal> {
        R::decode(payload).map_err(|error| AdmissionSignal::MalformedRequest {
            detail: error.to_string(),
        })
    }

    /// The envelope for one `ScopeSpans`/`ScopeLogs`/`ScopeMetrics` under a
    /// possibly-already-failed resource: a scope failure and a resource
    /// failure alike refuse every record beneath, naming the reason.
    fn envelope(
        resource: &Result<Arc<Resource>, RecordRejection>,
        scope: wire::InstrumentationScope,
        scope_schema_url: &str,
    ) -> Result<Envelope, RecordRejection> {
        let scope = decode::scope(scope, scope_schema_url)?;
        let resource = resource.as_ref().map_err(RecordRejection::clone)?;
        Ok(Envelope {
            resource: Arc::clone(resource),
            scope,
        })
    }

    fn admit_span(
        &self,
        ledger: &mut AdmissionLedger,
        now: AdmissionTime,
        translated: Result<Span, RecordRejection>,
    ) -> Result<RecordOutcome, AdmissionSignal> {
        let span = match translated {
            Ok(span) => span,
            Err(reason) => return Ok(RecordOutcome::Rejected { reason }),
        };
        let admission = ledger.admit_span(span);
        match admission.outcome {
            AdmissionOutcome::Admitted { entity } => {
                let record = admission
                    .record
                    .expect("an admitted span carries its ledger record");
                self.sink
                    .offer(QueuedRecord {
                        entity,
                        admitted_at: now,
                        record: StoredRecord::Span(record),
                    })
                    .map(|()| RecordOutcome::Admitted { entity })
            }
            AdmissionOutcome::Collapsed { entity } => {
                // The law the queue states and this pipeline keeps: a
                // collapse is the record already admitted, never a new
                // delivery. Its original queue entry — resident, or
                // already consumed — is the whole story; offering it again
                // would store a second copy of one natural identity and
                // stamp this retry's admission time onto a record admitted
                // earlier.
                Ok(RecordOutcome::Collapsed { entity })
            }
            AdmissionOutcome::Conflict { entity } => Ok(RecordOutcome::Conflict { entity }),
            AdmissionOutcome::Rejected { rejection } => Ok(RecordOutcome::Rejected {
                reason: RecordRejection::Budget(rejection),
            }),
            AdmissionOutcome::Invalid { error } => Ok(RecordOutcome::Rejected {
                reason: RecordRejection::Shape(error),
            }),
        }
    }

    fn admit_log(
        &self,
        ledger: &mut AdmissionLedger,
        now: AdmissionTime,
        translated: Result<LogRecord, RecordRejection>,
    ) -> Result<RecordOutcome, AdmissionSignal> {
        let record = match translated {
            Ok(record) => record,
            Err(reason) => return Ok(RecordOutcome::Rejected { reason }),
        };
        let admission = ledger.admit_log_record(&record);
        match admission {
            AdmissionOutcome::Admitted { entity } => self
                .sink
                .offer(QueuedRecord {
                    entity,
                    admitted_at: now,
                    record: StoredRecord::Log(Arc::new(record)),
                })
                .map(|()| RecordOutcome::Admitted { entity }),
            AdmissionOutcome::Collapsed { entity } => Ok(RecordOutcome::Collapsed { entity }),
            AdmissionOutcome::Conflict { entity } => Ok(RecordOutcome::Conflict { entity }),
            AdmissionOutcome::Rejected { rejection } => Ok(RecordOutcome::Rejected {
                reason: RecordRejection::Budget(rejection),
            }),
            AdmissionOutcome::Invalid { error } => Ok(RecordOutcome::Rejected {
                reason: RecordRejection::Shape(error),
            }),
        }
    }

    fn admit_point(
        &self,
        ledger: &mut AdmissionLedger,
        now: AdmissionTime,
        identity: &StreamIdentity,
        translated: Result<MetricPoint, RecordRejection>,
    ) -> Result<RecordOutcome, AdmissionSignal> {
        let point = match translated {
            Ok(point) => point,
            Err(reason) => return Ok(RecordOutcome::Rejected { reason }),
        };
        let admission = ledger.admit_metric_point(identity, point);
        match admission.outcome {
            AdmissionOutcome::Admitted { entity } => {
                let point = admission
                    .record
                    .expect("an admitted point carries its ledger record");
                let stream = admission
                    .stream
                    .expect("an admitted point carries its interned stream identity");
                self.sink
                    .offer(QueuedRecord {
                        entity,
                        admitted_at: now,
                        record: StoredRecord::Point { stream, point },
                    })
                    .map(|()| RecordOutcome::Admitted { entity })
            }
            AdmissionOutcome::Collapsed { entity } => {
                // As with spans: a collapse is the point already admitted.
                // Its original queue entry is the whole story — offering
                // it again would store a second copy of one point and
                // stamp the retry's admission time onto it.
                Ok(RecordOutcome::Collapsed { entity })
            }
            AdmissionOutcome::Conflict { entity } => Ok(RecordOutcome::Conflict { entity }),
            AdmissionOutcome::Rejected { rejection } => Ok(RecordOutcome::Rejected {
                reason: RecordRejection::Budget(rejection),
            }),
            AdmissionOutcome::Invalid { error } => Ok(RecordOutcome::Rejected {
                reason: RecordRejection::Shape(error),
            }),
        }
    }

    fn lock_ledger(&self) -> std::sync::MutexGuard<'_, AdmissionLedger> {
        self.ledger.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
