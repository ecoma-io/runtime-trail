//! The admission ledger: entity-id assignment, duplicate delivery, and
//! conflict recording.
//!
//! `docs/architecture/telemetry-model.md`, "Record identity and duplicate
//! delivery": admission is idempotent because OTLP delivery is
//! `at-least-once` in practice. Collapse happens only where the `OTel` data
//! model itself defines identity — spans by `(trace_id, span_id)`, metric
//! points by stream identity plus point attributes, interval and flags.
//! Log records are never collapsed: two byte-identical records are two
//! admitted records. A conflicting delivery never overwrites: the first
//! admitted record stands, and the conflict is recorded as an admission
//! anomaly. Nothing is ever silently destroyed or rewritten (ADR 0006).
//!
//! # One payload, shared (ADR 0008)
//!
//! To detect a conflict the ledger must remember the admitted payload
//! under each natural identity — exactly, never by hash. It does so by
//! **sharing** the payload through `Arc` rather than copying it: the
//! ledger's entry, the [`crate::identity::Admitted`] record the caller
//! takes, and later storage all reference one allocation. The per-record
//! marginal cost of admission is therefore the identity key bytes plus one
//! reference-counted pointer pair — not a second copy of the record. The
//! same sharing applies to stream identities: every point key of a stream
//! references one interned `Arc<StreamIdentity>`, so admitting a thousand
//! points of one stream costs one stream payload, not a thousand.
//! Reference counts are an allocation detail, never an accounting or
//! equality event: identity compares payloads byte-exactly through the
//! `Arc`.
//!
//! Interning happens only on successful admission: a refused delivery
//! leaves no entry behind — its identity stays free, and a later in-budget
//! delivery of the same identity admits cleanly.
//!
//! The ledger is the session: its assigned serials start at 1 and are not
//! persisted. Log records and invalid-id spans get **no** ledger entry at
//! all — with no natural identity there is nothing to collapse onto and
//! nothing to conflict with. When the runtime evicts a record, [`AdmissionLedger::forget`]
//! removes its ledger entry; a re-delivery afterwards is admitted fresh.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use crate::budgets::{
    BudgetLimits, check_log_record, check_point, check_resource, check_scope, check_span,
};
use crate::identity::{AdmissionOutcome, AssignedId, EntityId};
use crate::logs::LogRecord;
use crate::metrics::{MetricPoint, MetricStream, PointIdentity, StreamIdentity};
use crate::spans::Span;

/// The result of admitting one span: the outcome, and the admitted record
/// itself, shared.
///
/// `record` is `Some` when a payload entered or already stands under this
/// identity — the ledger's own `Arc`, so taking it costs a reference
/// count, not a copy. It is `None` when the delivery conflicted (the
/// conflicting payload was never stored — that is the point of the
/// anomaly) or was refused.
#[derive(Clone, Debug)]
pub struct SpanAdmission {
    /// What admission did with the delivery.
    pub outcome: AdmissionOutcome,
    /// The payload the identity now names, when one stands.
    pub record: Option<Arc<Span>>,
}

/// The result of admitting one metric point: the outcome, the admitted
/// point, and the point's interned stream identity.
#[derive(Clone, Debug)]
pub struct PointAdmission {
    /// What admission did with the delivery.
    pub outcome: AdmissionOutcome,
    /// The payload the identity now names, when one stands.
    pub record: Option<Arc<MetricPoint>>,
    /// The stream identity the point belongs to, interned — `None` only
    /// when the delivery was refused before admission.
    pub stream: Option<Arc<StreamIdentity>>,
}

/// The observable runtime counter for admission anomalies.
///
/// Surfaced in investigation coverage; whose loss it was must always be
/// visible. A conflict recorded here is a *recorded* fact, never a
/// rewrite: the first admitted record stands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdmissionAnomalies {
    span_identity_conflicts: u64,
    point_identity_conflicts: u64,
}

impl AdmissionAnomalies {
    /// Conflicting span re-deliveries under an already-admitted
    /// `(trace_id, span_id)`.
    #[must_use]
    pub const fn span_identity_conflicts(self) -> u64 {
        self.span_identity_conflicts
    }

    /// Conflicting metric-point re-deliveries under an already-admitted
    /// point identity.
    #[must_use]
    pub const fn point_identity_conflicts(self) -> u64 {
        self.point_identity_conflicts
    }

    /// Every anomaly recorded this session.
    #[must_use]
    pub const fn total(self) -> u64 {
        self.span_identity_conflicts + self.point_identity_conflicts
    }
}

/// The session-scoped admission ledger.
///
/// One per runtime session, built with the [`BudgetLimits`] every gate
/// here checks against — limits are startup configuration, never
/// mid-session state. The ledger assigns entity ids, collapses duplicates
/// exactly where `OTel` defines identity, records conflicts, and refuses
/// over-budget records before they are known at all.
#[derive(Debug)]
pub struct AdmissionLedger {
    next_serial: u64,
    /// The admitted span payload per natural identity, shared (see the
    /// module docs). Admission time is not kept: it is the caller's
    /// [`crate::identity::Admitted`] wrapper metadata.
    spans: HashMap<(crate::context::TraceId, crate::context::SpanId), (EntityId, Arc<Span>)>,
    /// The admitted point payload per point identity, shared. Keyed by
    /// `Arc<PointIdentity>` so [`AdmissionLedger::forget`] can drop an
    /// entry by its assigned id without duplicating key bytes: the
    /// assigned-id index below holds the same `Arc`.
    points: HashMap<Arc<PointIdentity>, (EntityId, Arc<MetricPoint>)>,
    /// Assigned id → the point identity it was assigned to, for
    /// [`AdmissionLedger::forget`]. One entry per admitted point; the
    /// `Arc` is shared with the `points` key.
    assigned_points: HashMap<AssignedId, Arc<PointIdentity>>,
    /// The interned stream identities this session has admitted points
    /// under. One payload per distinct stream, however many points.
    streams: HashMap<StreamIdentity, Arc<StreamIdentity>>,
    /// The limits every gate here checks against. Startup configuration.
    limits: BudgetLimits,
    anomalies: AdmissionAnomalies,
}

impl Default for AdmissionLedger {
    fn default() -> Self {
        Self::new(BudgetLimits::default())
    }
}

impl AdmissionLedger {
    /// An empty session ledger, gating against `limits`. Serials start
    /// at 1.
    #[must_use]
    pub fn new(limits: BudgetLimits) -> Self {
        Self {
            next_serial: 0,
            spans: HashMap::new(),
            points: HashMap::new(),
            assigned_points: HashMap::new(),
            streams: HashMap::new(),
            limits,
            anomalies: AdmissionAnomalies::default(),
        }
    }

    /// The anomalies recorded so far this session.
    #[must_use]
    pub const fn anomalies(&self) -> &AdmissionAnomalies {
        &self.anomalies
    }

    /// The limits this ledger gates against.
    #[must_use]
    pub const fn limits(&self) -> &BudgetLimits {
        &self.limits
    }

    fn next_assigned(&mut self) -> AssignedId {
        self.next_serial += 1;
        AssignedId::from_serial(
            NonZeroU64::new(self.next_serial).expect("a session admits fewer than 2^64 records"),
        )
    }

    fn assigned_entity(&mut self) -> EntityId {
        EntityId::Assigned(self.next_assigned())
    }

    /// Forgets one admitted record: the ledger entry naming `entity` is
    /// removed, on eviction. After forgetting, a re-delivery of the same
    /// natural identity is admitted fresh — for an assigned id that means
    /// a new serial; for a span it means the natural identity is free
    /// again (and a re-delivery re-admits under the same natural id).
    /// Forgetting an id that names no remembered entry is a no-op.
    pub fn forget(&mut self, entity: EntityId) {
        match entity {
            EntityId::Span {
                trace_id,
                span_id: span,
            } => {
                self.spans.remove(&(trace_id, span));
            }
            EntityId::Assigned(id) => {
                if let Some(identity) = self.assigned_points.remove(&id) {
                    self.points.remove(&identity);
                }
            }
        }
    }
}

impl AdmissionLedger {
    /// Admits one span.
    ///
    /// A span whose trace id and span id are both valid keeps its natural
    /// identity as its entity id: a re-delivery of the same span collapses
    /// onto the record already admitted, and a differing re-delivery is
    /// recorded as a conflict while the first record stands. A span with
    /// an invalid (all-zero) id receives an admission-assigned id and is
    /// never collapsed — nothing in its wire data makes it identical to
    /// anything, so the ledger keeps no entry for it at all.
    #[must_use]
    pub fn admit_span(&mut self, span: Span) -> SpanAdmission {
        if let Err(rejection) = check_span(&span, &self.limits) {
            return SpanAdmission {
                outcome: AdmissionOutcome::Rejected { rejection },
                record: None,
            };
        }
        let Some(key) = span.natural_identity() else {
            // Nothing in the wire data distinguishes this span, so it can
            // never collapse: assigned id, admitted, no ledger entry.
            let entity = self.assigned_entity();
            return SpanAdmission {
                outcome: AdmissionOutcome::Admitted { entity },
                record: Some(Arc::new(span)),
            };
        };
        let entity = EntityId::Span {
            trace_id: key.0,
            span_id: key.1,
        };
        let span = Arc::new(span);
        debug_assert!(
            check_span(&span, &self.limits).is_ok(),
            "admission stored a record its own gate refuses — the admitted-data \
             invariant behind the bounded derived glue is broken"
        );
        match self.spans.get(&key) {
            Some((standing, admitted)) if **admitted == *span => SpanAdmission {
                outcome: AdmissionOutcome::Collapsed { entity: *standing },
                record: Some(Arc::clone(admitted)),
            },
            Some((standing, _)) => {
                self.anomalies.span_identity_conflicts += 1;
                SpanAdmission {
                    outcome: AdmissionOutcome::Conflict { entity: *standing },
                    record: None,
                }
            }
            None => {
                self.spans.insert(key, (entity, Arc::clone(&span)));
                SpanAdmission {
                    outcome: AdmissionOutcome::Admitted { entity },
                    record: Some(span),
                }
            }
        }
    }

    /// Admits one log record.
    ///
    /// Log records are never collapsed: OTLP defines no log-record
    /// identity, and this model refuses to invent a destructive one. Two
    /// byte-identical records are two admitted records — the emitter sent
    /// two. Duplicate suppression at the source is the emitter SDK's job,
    /// not admission's. Every record receives a fresh admission-assigned
    /// id, and the ledger keeps no entry for any of them.
    #[must_use]
    pub fn admit_log_record(&mut self, record: &LogRecord) -> AdmissionOutcome {
        if let Err(rejection) = check_log_record(record, &self.limits) {
            return AdmissionOutcome::Rejected { rejection };
        }
        // No ledger state is kept for a log record: with no natural
        // identity there is nothing to collapse onto and nothing to
        // conflict with.
        let entity = self.assigned_entity();
        AdmissionOutcome::Admitted { entity }
    }

    /// Admits one metric data point of a stream.
    ///
    /// Point identity is the stream identity (resource, scope, name, kind,
    /// temporality) plus the point's attribute set, interval (`start_time`
    /// and `time`) and flags. A re-delivered point with the same identity
    /// collapses onto the record already admitted; a differing one is a
    /// recorded conflict, first record standing. Points always receive
    /// admission-assigned entity ids — collapse identity and cursor
    /// identity are different things.
    ///
    /// Before any of that, the delivery must satisfy the shape law the
    /// stream's kind imposes on the point (the same law
    /// [`MetricStream::new`] enforces) — an incoherent pair is
    /// [`AdmissionOutcome::Invalid`], not a conflict — and then the
    /// budgets: the stream identity's resource and scope, and the point
    /// itself.
    #[must_use]
    pub fn admit_metric_point(
        &mut self,
        stream: &StreamIdentity,
        point: MetricPoint,
    ) -> PointAdmission {
        if let Err(error) = MetricStream::check_identity_coherence(stream) {
            return PointAdmission {
                outcome: AdmissionOutcome::Invalid { error },
                record: None,
                stream: None,
            };
        }
        if let Err(error) = MetricStream::check_point_coherence(stream, &point, 0) {
            return PointAdmission {
                outcome: AdmissionOutcome::Invalid { error },
                record: None,
                stream: None,
            };
        }
        if let Err(rejection) = check_resource(&stream.resource, &self.limits) {
            return PointAdmission {
                outcome: AdmissionOutcome::Rejected { rejection },
                record: None,
                stream: None,
            };
        }
        if let Err(rejection) = check_scope(&stream.scope, &self.limits) {
            return PointAdmission {
                outcome: AdmissionOutcome::Rejected { rejection },
                record: None,
                stream: None,
            };
        }
        if let Err(rejection) = check_point(&point, &self.limits) {
            return PointAdmission {
                outcome: AdmissionOutcome::Rejected { rejection },
                record: None,
                stream: None,
            };
        }
        // Every gate passed: now — and only now — intern the stream
        // identity, so a refused delivery leaves no entry behind.
        let interned = self
            .streams
            .entry(stream.clone())
            .or_insert_with(|| Arc::new(stream.clone()))
            .clone();
        let key = Arc::new(PointIdentity::of_interned(&interned, &point));
        let point = Arc::new(point);
        debug_assert!(
            check_point(&point, &self.limits).is_ok(),
            "admission stored a record its own gate refuses — the admitted-data \
             invariant behind the bounded derived glue is broken"
        );
        match self.points.get(&key) {
            // Collapse comparison runs through the kind-aware payload law:
            // for a gauge, two deliveries differing only in a start_time
            // the emitter was told not to send are the same point, not a
            // conflict.
            Some((standing, admitted)) if admitted.identity_payload_eq(&point, stream.kind) => {
                PointAdmission {
                    outcome: AdmissionOutcome::Collapsed { entity: *standing },
                    record: Some(Arc::clone(admitted)),
                    stream: Some(interned),
                }
            }
            Some((standing, _)) => {
                self.anomalies.point_identity_conflicts += 1;
                PointAdmission {
                    outcome: AdmissionOutcome::Conflict { entity: *standing },
                    record: None,
                    stream: Some(interned),
                }
            }
            None => {
                let entity = self.assigned_entity();
                let EntityId::Assigned(assigned) = entity else {
                    unreachable!("points always receive assigned ids");
                };
                self.points
                    .insert(Arc::clone(&key), (entity, Arc::clone(&point)));
                self.assigned_points.insert(assigned, key);
                PointAdmission {
                    outcome: AdmissionOutcome::Admitted { entity },
                    record: Some(point),
                    stream: Some(interned),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState};
    use crate::resources::{InstrumentationScope, Resource};
    use crate::spans::{EmitterDroppedCounts, SpanKind, SpanStatus, SpanStatusCode};
    use crate::values::{Attributes, Value};

    fn ledger() -> AdmissionLedger {
        AdmissionLedger::new(BudgetLimits::default())
    }

    fn context(trace: [u8; 16], span: [u8; 8]) -> TraceContext {
        TraceContext {
            trace_id: TraceId::from_bytes(trace),
            span_id: SpanId::from_bytes(span),
            flags: TraceFlags::new(1),
            tracestate: TraceState::default(),
        }
    }

    fn empty_resource() -> Resource {
        Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    fn empty_scope() -> InstrumentationScope {
        InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    fn span(trace: [u8; 16], span: [u8; 8], name: &str) -> Span {
        Span {
            context: context(trace, span),
            parent_span_id: None,
            name: name.to_owned(),
            kind: SpanKind::Server,
            start_time_unix_nano: 10,
            end_time_unix_nano: Some(20),
            resource: empty_resource().into(),
            scope: empty_scope().into(),
            attributes: Attributes::default(),
            emitter_dropped: EmitterDroppedCounts::default(),
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: SpanStatusCode::Unset,
                message: String::new(),
            },
        }
    }

    fn log_record(body: &str) -> LogRecord {
        LogRecord {
            timestamp_unix_nano: Some(1),
            observed_timestamp_unix_nano: Some(2),
            severity_number: None,
            severity_text: None,
            body: Some(Value::String(body.to_owned())),
            resource: empty_resource().into(),
            scope: empty_scope().into(),
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_id: None,
            span_id: None,
            trace_flags: None,
            event_name: None,
        }
    }

    #[test]
    fn a_valid_span_keeps_its_natural_identity_and_a_redelivery_collapses() {
        let mut ledger = ledger();
        let first = ledger.admit_span(span([1; 16], [2; 8], "op"));
        let entity = first.outcome.entity().expect("admitted");
        assert_eq!(
            entity,
            EntityId::Span {
                trace_id: TraceId::from_bytes([1; 16]),
                span_id: SpanId::from_bytes([2; 8]),
            },
            "the natural identity is the entity id"
        );
        let record = first.record.expect("the admitted payload is shared out");
        let second = ledger.admit_span(span([1; 16], [2; 8], "op"));
        assert_eq!(
            second.outcome,
            AdmissionOutcome::Collapsed { entity },
            "at-least-once delivery collapses onto the admitted span"
        );
        let collapsed_record = second
            .record
            .expect("collapse hands out the standing record");
        assert!(
            Arc::ptr_eq(&record, &collapsed_record),
            "the duplicate delivery shares the standing payload — one allocation"
        );
        assert_eq!(
            Arc::strong_count(&collapsed_record),
            3,
            "ledger + two handed-out handles, and never a second copy of the bytes"
        );
        assert_eq!(ledger.anomalies().total(), 0);
    }

    #[test]
    fn a_conflicting_redelivery_leaves_the_first_standing_and_records_an_anomaly() {
        let mut ledger = ledger();
        let first = ledger.admit_span(span([1; 16], [2; 8], "op"));
        let standing = first.outcome.entity().expect("admitted");
        let conflicting = ledger.admit_span(span([1; 16], [2; 8], "renamed"));
        assert_eq!(
            conflicting.outcome,
            AdmissionOutcome::Conflict { entity: standing },
            "the first admitted record stands; the conflict is named"
        );
        assert!(
            conflicting.record.is_none(),
            "the conflicting payload was never stored — that is the point"
        );
        assert_eq!(ledger.anomalies().span_identity_conflicts(), 1);
        assert_eq!(ledger.anomalies().total(), 1);
        // Re-sending the original still collapses: the standing record was
        // never rewritten.
        let again = ledger.admit_span(span([1; 16], [2; 8], "op"));
        assert_eq!(
            again.outcome,
            AdmissionOutcome::Collapsed { entity: standing }
        );
        assert!(Arc::ptr_eq(
            &first.record.expect("standing"),
            &again.record.expect("standing")
        ));
    }

    #[test]
    fn byte_identical_log_records_are_two_admitted_records_never_collapsed() {
        let mut ledger = ledger();
        let first = ledger.admit_log_record(&log_record("same"));
        let second = ledger.admit_log_record(&log_record("same"));
        let (
            AdmissionOutcome::Admitted { entity: first_id },
            AdmissionOutcome::Admitted { entity: second_id },
        ) = (first, second)
        else {
            panic!("both records admit");
        };
        assert_ne!(first_id, second_id, "the emitter sent two records");
        assert_eq!(first_id.assigned_serial().map(NonZeroU64::get), Some(1));
        assert_eq!(second_id.assigned_serial().map(NonZeroU64::get), Some(2));
        assert_eq!(ledger.anomalies().total(), 0);
        assert!(
            ledger.spans.is_empty() && ledger.points.is_empty() && ledger.streams.is_empty(),
            "log records leave no ledger entry at all"
        );
    }

    #[test]
    fn invalid_id_spans_get_assigned_ids_and_never_collapse() {
        let mut ledger = ledger();
        let first = ledger.admit_span(span([0; 16], [0; 8], "op"));
        let second = ledger.admit_span(span([0; 16], [0; 8], "op"));
        let (
            AdmissionOutcome::Admitted { entity: first_id },
            AdmissionOutcome::Admitted { entity: second_id },
        ) = (first.outcome, second.outcome)
        else {
            panic!("both admit");
        };
        assert_ne!(first_id, second_id);
        assert!(first_id.assigned_serial().is_some());
        assert!(second_id.assigned_serial().is_some());
        assert!(
            first.record.is_some() && second.record.is_some(),
            "the caller still takes the admitted payload"
        );
        let half_invalid = ledger.admit_span(span([1; 16], [0; 8], "op"));
        assert!(half_invalid.outcome.entity().is_some());
        assert!(
            ledger.spans.is_empty(),
            "no natural identity, no ledger entry"
        );
    }

    #[test]
    fn assigned_serials_are_session_scoped_and_monotonic() {
        let mut ledger = ledger();
        for expected in 1..=4_u64 {
            let outcome = ledger.admit_log_record(&log_record("x"));
            let AdmissionOutcome::Admitted { entity } = outcome else {
                panic!("admitted");
            };
            assert_eq!(
                entity.assigned_serial().map(NonZeroU64::get),
                Some(expected)
            );
        }
    }

    #[test]
    fn entity_ids_are_added_metadata_that_rewrites_nothing() {
        let mut ledger = ledger();
        let original = span([7; 16], [8; 8], "op");
        let snapshot = original.clone();
        let outcome = ledger.admit_span(original);
        assert!(outcome.outcome.entity().is_some());
        assert_eq!(
            snapshot.context.trace_id.as_bytes(),
            [7; 16],
            "emitter data untouched by admission"
        );
        assert_eq!(snapshot.name, "op");
        assert_eq!(snapshot.emitter_dropped, EmitterDroppedCounts::default());
    }
}

#[cfg(test)]
mod metric_points {
    use super::*;
    use crate::context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState};
    use crate::metrics::{
        DATA_POINT_FLAG_NO_RECORDED_VALUE, Exemplar, ExponentialBuckets, ExponentialHistogramPoint,
        HistogramPoint, MetricNumber, MetricStream, NumberPoint, PointShape, QuantileValue,
        StreamKind, SummaryPoint, Temporality,
    };
    use crate::resources::{InstrumentationScope, Resource};
    use crate::spans::{EmitterDroppedCounts, SpanKind, SpanStatus, SpanStatusCode};
    use crate::values::{Attributes, Float, Value};

    fn ledger() -> AdmissionLedger {
        AdmissionLedger::new(BudgetLimits::default())
    }

    fn span(trace: [u8; 16], span: [u8; 8], name: &str) -> Span {
        Span {
            context: TraceContext {
                trace_id: TraceId::from_bytes(trace),
                span_id: SpanId::from_bytes(span),
                flags: TraceFlags::new(1),
                tracestate: TraceState::default(),
            },
            parent_span_id: None,
            name: name.to_owned(),
            kind: SpanKind::Server,
            start_time_unix_nano: 10,
            end_time_unix_nano: Some(20),
            resource: Resource {
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            }
            .into(),
            scope: InstrumentationScope {
                name: String::new(),
                version: None,
                attributes: Attributes::default(),
                schema_url: None,
                dropped_attributes_count: 0,
            }
            .into(),
            attributes: Attributes::default(),
            emitter_dropped: EmitterDroppedCounts::default(),
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: SpanStatusCode::Unset,
                message: String::new(),
            },
        }
    }

    fn resource() -> Resource {
        Resource {
            attributes: Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String("checkout".to_owned()),
            )])
            .expect("unique keys"),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    fn scope() -> InstrumentationScope {
        InstrumentationScope {
            name: "scope".to_owned(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }
    }

    fn stream_identity(kind: StreamKind, temporality: Option<Temporality>) -> StreamIdentity {
        StreamIdentity {
            resource: resource(),
            scope: scope(),
            name: "requests".to_owned(),
            kind,
            temporality,
        }
    }

    fn sum_point(start: u64, time: u64, value: i64) -> MetricPoint {
        MetricPoint::Number(NumberPoint::interval(
            start,
            time,
            MetricNumber::int(value),
            Attributes::default(),
            Vec::new(),
        ))
    }

    fn gauge_point(time: u64, value: i64) -> MetricPoint {
        MetricPoint::Number(NumberPoint::measurement(
            time,
            MetricNumber::int(value),
            Attributes::default(),
            Vec::new(),
        ))
    }

    fn histogram_point(count: u64) -> MetricPoint {
        MetricPoint::Histogram(HistogramPoint {
            attributes: Attributes::default(),
            start_time_unix_nano: 100,
            time_unix_nano: 200,
            count,
            sum: None,
            bucket_counts: vec![0, count, 0],
            explicit_bounds: vec![Float::new(1.0)],
            min: None,
            max: None,
            flags: 0,
            exemplars: Vec::new(),
        })
    }

    fn exponential_point(count: u64) -> MetricPoint {
        MetricPoint::ExponentialHistogram(ExponentialHistogramPoint {
            attributes: Attributes::default(),
            start_time_unix_nano: 100,
            time_unix_nano: 200,
            count,
            sum: None,
            scale: 0,
            zero_count: 0,
            zero_threshold: Float::new(0.0),
            positive: ExponentialBuckets {
                offset: 0,
                bucket_counts: vec![count],
            },
            negative: ExponentialBuckets {
                offset: 0,
                bucket_counts: Vec::new(),
            },
            min: None,
            max: None,
            flags: 0,
            exemplars: Vec::new(),
        })
    }

    fn summary_point(count: u64) -> MetricPoint {
        MetricPoint::Summary(SummaryPoint {
            attributes: Attributes::default(),
            start_time_unix_nano: 100,
            time_unix_nano: 200,
            count,
            sum: None,
            quantiles: vec![QuantileValue {
                quantile: Float::new(0.5),
                value: Float::new(3.0),
            }],
            flags: 0,
            exemplars: Vec::new(),
        })
    }

    fn assert_admitted(admission: &PointAdmission) -> EntityId {
        let entity = admission.outcome.entity().expect("admitted");
        assert!(admission.record.is_some(), "the payload is shared out");
        assert!(admission.stream.is_some(), "the stream is interned");
        entity
    }

    #[test]
    fn duplicate_points_collapse_and_conflicting_points_are_recorded() {
        let mut ledger = ledger();
        let identity = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let first = ledger.admit_metric_point(&identity, sum_point(100, 200, 5));
        let standing = assert_admitted(&first);
        assert!(
            standing.assigned_serial().is_some(),
            "points get assigned ids"
        );
        let second = ledger.admit_metric_point(&identity, sum_point(100, 200, 5));
        assert_eq!(
            second.outcome,
            AdmissionOutcome::Collapsed { entity: standing }
        );
        assert!(Arc::ptr_eq(
            &first.record.expect("standing"),
            &second.record.expect("standing")
        ));
        let conflicting = ledger.admit_metric_point(&identity, sum_point(100, 200, 6));
        assert_eq!(
            conflicting.outcome,
            AdmissionOutcome::Conflict { entity: standing }
        );
        assert!(conflicting.record.is_none());
        assert_eq!(ledger.anomalies().point_identity_conflicts(), 1);
    }

    #[test]
    fn delta_and_cumulative_points_of_one_name_never_collapse_together() {
        let mut ledger = ledger();
        let delta = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Delta),
        );
        let cumulative = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let first = ledger.admit_metric_point(&delta, sum_point(100, 200, 5));
        let second = ledger.admit_metric_point(&cumulative, sum_point(100, 200, 5));
        assert_ne!(
            first.outcome.entity(),
            second.outcome.entity(),
            "a delta stream and a cumulative stream are different series"
        );
        assert_eq!(ledger.anomalies().total(), 0);
    }

    #[test]
    fn gauge_points_identify_by_stream_and_time() {
        let mut ledger = ledger();
        let identity = stream_identity(StreamKind::Gauge, None);
        let first = ledger.admit_metric_point(&identity, gauge_point(50, 1));
        let second = ledger.admit_metric_point(&identity, gauge_point(50, 1));
        let third = ledger.admit_metric_point(&identity, gauge_point(51, 1));
        assert_eq!(second.outcome.entity(), first.outcome.entity());
        assert_ne!(third.outcome.entity(), first.outcome.entity());
    }

    #[test]
    fn a_gauge_start_time_difference_collapses_rather_than_conflicting() {
        // Gauge start_time is preserved when sent but lives outside gauge
        // identity AND outside the payload comparison: two deliveries
        // differing only there are re-deliveries of one point, not a
        // conflict to record.
        let mut ledger = ledger();
        let identity = stream_identity(StreamKind::Gauge, None);
        let first = ledger.admit_metric_point(
            &identity,
            MetricPoint::Number(NumberPoint::measurement_with_start(
                1,
                50,
                MetricNumber::int(1),
                Attributes::default(),
                Vec::new(),
            )),
        );
        let standing = assert_admitted(&first);
        let second = ledger.admit_metric_point(
            &identity,
            MetricPoint::Number(NumberPoint::measurement_with_start(
                2,
                50,
                MetricNumber::int(1),
                Attributes::default(),
                Vec::new(),
            )),
        );
        assert_eq!(
            second.outcome,
            AdmissionOutcome::Collapsed { entity: standing },
            "the start_time difference is not a conflict for a gauge"
        );
        assert_eq!(ledger.anomalies().total(), 0);
    }

    #[test]
    fn differing_point_attributes_make_different_points() {
        let mut ledger = ledger();
        let identity = stream_identity(
            StreamKind::Sum { monotonic: false },
            Some(Temporality::Cumulative),
        );
        let http = MetricPoint::Number(NumberPoint::interval(
            100,
            200,
            MetricNumber::int(1),
            Attributes::from_pairs(vec![("route".to_owned(), Value::String("/a".to_owned()))])
                .expect("unique keys"),
            Vec::new(),
        ));
        let grpc = MetricPoint::Number(NumberPoint::interval(
            100,
            200,
            MetricNumber::int(1),
            Attributes::from_pairs(vec![("route".to_owned(), Value::String("/b".to_owned()))])
                .expect("unique keys"),
            Vec::new(),
        ));
        let first = ledger.admit_metric_point(&identity, http);
        let second = ledger.admit_metric_point(&identity, grpc);
        assert_ne!(
            first.outcome.entity(),
            second.outcome.entity(),
            "the point's own attributes are part of its identity"
        );
        assert_eq!(ledger.anomalies().total(), 0);
    }

    #[test]
    fn a_staleness_marker_point_is_its_own_point() {
        // The staleness flag rides in the point identity: a no-recorded-
        // value marker and a real zero at the same (stream, interval,
        // attributes) are wire-byte-different, so they are different
        // points — and neither conflicts with the other.
        let mut ledger = ledger();
        let identity = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let real_zero = MetricPoint::Number(NumberPoint::interval(
            100,
            200,
            MetricNumber::int(0),
            Attributes::default(),
            Vec::new(),
        ));
        let mut marker_number = NumberPoint::interval(
            100,
            200,
            MetricNumber::int(0),
            Attributes::default(),
            Vec::new(),
        );
        marker_number.flags = DATA_POINT_FLAG_NO_RECORDED_VALUE;
        let marker = MetricPoint::Number(marker_number);
        let first = ledger.admit_metric_point(&identity, real_zero);
        let standing = assert_admitted(&first);
        let delivered = ledger.admit_metric_point(&identity, marker);
        assert_ne!(
            delivered.outcome.entity(),
            Some(standing),
            "the staleness marker is a second entity, not a conflict"
        );
        assert_eq!(ledger.anomalies().total(), 0);
        let again = ledger.admit_metric_point(&identity, {
            let mut number = NumberPoint::interval(
                100,
                200,
                MetricNumber::int(0),
                Attributes::default(),
                Vec::new(),
            );
            number.flags = DATA_POINT_FLAG_NO_RECORDED_VALUE;
            MetricPoint::Number(number)
        });
        assert_eq!(
            again.outcome,
            AdmissionOutcome::Collapsed {
                entity: delivered.outcome.entity().expect("admitted"),
            }
        );
    }

    #[test]
    fn schema_url_never_participates_in_stream_identity() {
        // Resource identity is its attribute map; a stream whose
        // schema_url differs is the same series.
        let mut ledger = ledger();
        let mut other = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        other.resource.schema_url = Some("https://example.com/other-schema".to_owned());
        let first = ledger.admit_metric_point(
            &stream_identity(
                StreamKind::Sum { monotonic: true },
                Some(Temporality::Cumulative),
            ),
            sum_point(100, 200, 5),
        );
        let standing = assert_admitted(&first);
        let second = ledger.admit_metric_point(&other, sum_point(100, 200, 5));
        assert_eq!(
            second.outcome,
            AdmissionOutcome::Collapsed { entity: standing },
            "a schema_url difference is not a different stream"
        );
        assert_eq!(ledger.streams.len(), 1, "one interned stream payload");
    }

    #[test]
    fn an_incoherent_identity_point_pair_is_invalid_not_a_conflict() {
        let mut ledger = ledger();
        let identity = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        // A gauge-shaped point (no start time) under a Sum identity breaks
        // the shape law the stream kind imposes.
        let incoherent = ledger.admit_metric_point(&identity, gauge_point(200, 5));
        let AdmissionOutcome::Invalid { error } = incoherent.outcome else {
            panic!("an incoherent pair is refused as invalid");
        };
        assert_eq!(
            error.to_string(),
            "interval point 0 is missing its start_time"
        );
        assert!(incoherent.record.is_none());
        assert!(incoherent.stream.is_none());
        // A histogram-shaped point under the same Sum identity breaks it
        // by shape.
        let wrong_shape = ledger.admit_metric_point(&identity, histogram_point(4));
        let AdmissionOutcome::Invalid { error } = wrong_shape.outcome else {
            panic!("a shape mismatch is refused as invalid");
        };
        assert!(matches!(
            error,
            crate::metrics::StreamShapeError::KindShapeMismatch {
                expected: PointShape::Number,
                found: PointShape::Histogram,
                ..
            }
        ));
        assert_eq!(ledger.anomalies().total(), 0);
        assert!(ledger.points.is_empty() && ledger.streams.is_empty());
    }

    #[test]
    fn over_budget_points_are_refused_and_leave_the_identity_free() {
        let mut ledger = ledger();
        let identity = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let crowded = MetricPoint::Number(NumberPoint::interval(
            100,
            200,
            MetricNumber::int(5),
            Attributes::from_pairs(
                (0..=i64::try_from(ledger.limits().attributes_per_signal).expect("fits"))
                    .map(|index| (format!("k{index}"), Value::Int(index)))
                    .collect::<Vec<_>>(),
            )
            .expect("unique keys"),
            Vec::new(),
        ));
        let refused = ledger.admit_metric_point(&identity, crowded);
        let AdmissionOutcome::Rejected { rejection } = refused.outcome else {
            panic!("an over-cap point refuses");
        };
        assert_eq!(rejection.budget.slug(), "attributes_per_signal");
        assert!(refused.record.is_none() && refused.stream.is_none());
        assert!(
            ledger.streams.is_empty() && ledger.points.is_empty(),
            "a refused delivery interns nothing — the identity stays free"
        );
        // The same identity, in budget, admits cleanly afterwards.
        let clean = ledger.admit_metric_point(&identity, sum_point(100, 200, 5));
        assert_admitted(&clean);
        assert_eq!(ledger.streams.len(), 1);
    }

    #[test]
    fn every_shape_collapses_duplicates_and_records_conflicts() {
        let mut ledger = ledger();
        let histogram = stream_identity(StreamKind::Histogram, Some(Temporality::Cumulative));
        let exponential = stream_identity(
            StreamKind::ExponentialHistogram,
            Some(Temporality::Cumulative),
        );
        let summary = stream_identity(StreamKind::Summary, None);
        let cases: [(StreamIdentity, MetricPoint, MetricPoint); 3] = [
            (histogram, histogram_point(4), histogram_point(5)),
            (exponential, exponential_point(4), exponential_point(5)),
            (summary, summary_point(4), summary_point(5)),
        ];
        for (identity, first_point, differing) in cases {
            let first = ledger.admit_metric_point(&identity, first_point);
            let standing = assert_admitted(&first);
            let standing_record = first.record.expect("standing");
            // A byte-identical re-delivery of the first point.
            let duplicate = ledger.admit_metric_point(&identity, (*standing_record).clone());
            assert_eq!(
                duplicate.outcome,
                AdmissionOutcome::Collapsed { entity: standing },
                "a {} re-delivery collapses",
                identity.name
            );
            let conflict = ledger.admit_metric_point(&identity, differing);
            assert_eq!(
                conflict.outcome,
                AdmissionOutcome::Conflict { entity: standing },
                "a differing {} re-delivery is recorded",
                identity.name
            );
        }
        assert_eq!(ledger.anomalies().point_identity_conflicts(), 3);
    }

    #[test]
    fn a_thousand_points_of_one_stream_interned_once() {
        let mut ledger = ledger();
        let identity = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let first = ledger.admit_metric_point(&identity, sum_point(1, 1, 0));
        let first_stream = first.stream.expect("interned");
        for time in 2..=1_000_u64 {
            let admission = ledger.admit_metric_point(&identity, sum_point(1, time, 0));
            let stream = admission.stream.expect("interned");
            assert!(
                Arc::ptr_eq(&first_stream, &stream),
                "every point of the stream references one interned payload"
            );
        }
        assert_eq!(ledger.streams.len(), 1, "one stream, one payload");
        assert_eq!(ledger.points.len(), 1_000);
        assert_eq!(ledger.assigned_points.len(), 1_000);
        // Every point key shares the same stream Arc: one payload total.
        let referenced: Vec<&Arc<PointIdentity>> = ledger.assigned_points.values().collect();
        assert!(
            referenced
                .iter()
                .all(|key| Arc::ptr_eq(&key.stream, &first_stream)),
            "the interning is real sharing, not a per-point copy"
        );
        assert_eq!(
            Arc::strong_count(&first_stream),
            1_002,
            "the table, the 1,000 point keys and the handle — nothing else"
        );
    }

    #[test]
    fn forgetting_a_point_admits_its_redelivery_fresh() {
        let mut ledger = ledger();
        let identity = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let first = ledger.admit_metric_point(&identity, sum_point(100, 200, 5));
        let entity = assert_admitted(&first);
        ledger.forget(entity);
        assert!(
            ledger.points.is_empty() && ledger.assigned_points.is_empty(),
            "the eviction removed the ledger entry"
        );
        let redelivered = ledger.admit_metric_point(&identity, sum_point(100, 200, 5));
        let fresh = assert_admitted(&redelivered);
        assert_ne!(
            fresh, entity,
            "after a forget, a re-delivery is a new entity with a new serial"
        );
        assert!(fresh.assigned_serial().is_some());
        assert_eq!(ledger.points.len(), 1);
    }

    #[test]
    fn forgetting_a_span_frees_its_natural_identity() {
        let mut ledger = ledger();
        let trace = [1; 16];
        let span_id = [2; 8];
        let first = ledger.admit_span(span(trace, span_id, "op"));
        let entity = first.outcome.entity().expect("admitted");
        ledger.forget(entity);
        assert!(ledger.spans.is_empty(), "the eviction removed the entry");
        let redelivered = ledger.admit_span(span(trace, span_id, "op"));
        let fresh = redelivered.outcome.entity().expect("re-admitted");
        assert_eq!(
            fresh, entity,
            "a span re-admits under the same natural identity"
        );
        assert_eq!(ledger.spans.len(), 1);
        // Forgetting an id that names nothing is a no-op.
        ledger.forget(EntityId::Assigned(AssignedId::from_serial(
            NonZeroU64::new(9_999).expect("nonzero"),
        )));
    }

    #[test]
    fn a_stream_end_to_end_admits_its_points_under_one_identity() {
        let mut ledger = ledger();
        let stream = MetricStream::new(
            stream_identity(StreamKind::Gauge, None),
            None,
            None,
            Attributes::default(),
            vec![gauge_point(1, 1), gauge_point(2, 2)],
        )
        .expect("a coherent stream");
        let identity = stream.identity();
        let outcomes: Vec<PointAdmission> = stream
            .points
            .iter()
            .map(|point| ledger.admit_metric_point(identity, point.clone()))
            .collect();
        assert_eq!(outcomes.len(), 2);
        assert_ne!(outcomes[0].outcome.entity(), outcomes[1].outcome.entity());
        assert!(Arc::ptr_eq(
            outcomes[0].stream.as_ref().expect("interned"),
            outcomes[1].stream.as_ref().expect("interned")
        ));
    }

    #[test]
    fn the_ledger_gates_exemplars_and_carries_its_limits() {
        let limits = BudgetLimits {
            exemplars_per_data_point: 1,
            ..BudgetLimits::default()
        };
        let mut ledger = AdmissionLedger::new(limits);
        assert_eq!(ledger.limits().exemplars_per_data_point, 1);
        let identity = stream_identity(StreamKind::Gauge, None);
        let exemplar = Exemplar {
            value: MetricNumber::int(1),
            time_unix_nano: 1,
            filtered_attributes: Attributes::default(),
            trace_id: None,
            span_id: None,
        };
        let crowded = MetricPoint::Number(NumberPoint::measurement(
            1,
            MetricNumber::int(1),
            Attributes::default(),
            vec![exemplar.clone(), exemplar],
        ));
        let refused = ledger.admit_metric_point(&identity, crowded);
        assert!(matches!(refused.outcome, AdmissionOutcome::Rejected { .. }));
    }
}
