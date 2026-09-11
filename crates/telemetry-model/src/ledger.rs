//! The admission ledger: entity-id assignment, duplicate delivery, and
//! conflict recording.
//!
//! `docs/architecture/telemetry-model.md`, "Record identity and duplicate
//! delivery": admission is idempotent because OTLP delivery is
//! `at-least-once` in practice. Collapse happens only where the `OTel` data
//! model itself defines identity — spans by `(trace_id, span_id)`, metric
//! points by stream identity plus start time and time. Log records are
//! never collapsed: two byte-identical records are two admitted records.
//! A conflicting delivery never overwrites: the first admitted record
//! stands, and the conflict is recorded as an admission anomaly. Nothing
//! is ever silently destroyed or rewritten (ADR 0006).
//!
//! The ledger is the session: its assigned serials start at 1 and are not
//! persisted. To detect a conflict it must remember the admitted payload
//! under each natural identity, so it holds one copy per natural identity
//! alongside storage's — the honest price of exact (never hash-based)
//! conflict detection.

use std::collections::HashMap;
use std::num::NonZeroU64;

use crate::budgets::{check_log_record, check_point, check_resource, check_span};
use crate::identity::{AdmissionOutcome, AssignedId, EntityId};
use crate::logs::LogRecord;
use crate::metrics::{MetricPoint, PointIdentity, StreamIdentity};
use crate::spans::Span;

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
/// One per runtime session. It assigns entity ids, collapses duplicates
/// exactly where `OTel` defines identity, records conflicts, and refuses
/// over-budget records before they are known at all.
#[derive(Debug, Default)]
pub struct AdmissionLedger {
    next_serial: u64,
    /// The admitted payload per natural span identity, kept to detect
    /// conflicts exactly (never by hash). Admission time is not kept: it
    /// is the caller's [`crate::identity::Admitted`] wrapper metadata.
    spans: HashMap<(crate::context::TraceId, crate::context::SpanId), (EntityId, Span)>,
    /// The admitted payload per point identity, for the same reason.
    points: HashMap<PointIdentity, (EntityId, MetricPoint)>,
    anomalies: AdmissionAnomalies,
}

impl AdmissionLedger {
    /// An empty session ledger. Serials start at 1.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_serial: 0,
            spans: HashMap::new(),
            points: HashMap::new(),
            anomalies: AdmissionAnomalies::default(),
        }
    }

    /// The anomalies recorded so far this session.
    #[must_use]
    pub const fn anomalies(&self) -> &AdmissionAnomalies {
        &self.anomalies
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

    /// Admits one span.
    ///
    /// A span whose trace id and span id are both valid keeps its natural
    /// identity as its entity id: a re-delivery of the same span collapses
    /// onto the record already admitted, and a differing re-delivery is
    /// recorded as a conflict while the first record stands. A span with
    /// an invalid (all-zero) id receives an admission-assigned id and is
    /// never collapsed — nothing in its wire data makes it identical to
    /// anything.
    pub fn admit_span(&mut self, span: Span) -> AdmissionOutcome {
        if let Err(rejection) = check_span(&span) {
            return AdmissionOutcome::Rejected { rejection };
        }
        let Some(key) = span.natural_identity() else {
            // Nothing in the wire data distinguishes this span, so it can
            // never collapse: assigned id, admitted, done.
            let entity = self.assigned_entity();
            return AdmissionOutcome::Admitted { entity };
        };
        let entity = EntityId::Span {
            trace_id: key.0,
            span_id: key.1,
        };
        match self.spans.get(&key) {
            Some((standing, admitted)) if *admitted == span => {
                AdmissionOutcome::Collapsed { entity: *standing }
            }
            Some((standing, _)) => {
                self.anomalies.span_identity_conflicts += 1;
                AdmissionOutcome::Conflict { entity: *standing }
            }
            None => {
                self.spans.insert(key, (entity, span));
                AdmissionOutcome::Admitted { entity }
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
    /// id.
    pub fn admit_log_record(&mut self, record: &LogRecord) -> AdmissionOutcome {
        if let Err(rejection) = check_log_record(record) {
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
    /// temporality) plus the point's attribute set, `start_time` (absent
    /// for gauges) and `time`. A re-delivered point with the same identity
    /// collapses onto the record already admitted; a differing one is a
    /// recorded conflict, first record standing. Points always receive
    /// admission-assigned entity ids — collapse identity and cursor
    /// identity are different things.
    pub fn admit_metric_point(
        &mut self,
        stream: &StreamIdentity,
        point: MetricPoint,
    ) -> AdmissionOutcome {
        if let Err(rejection) = check_resource(&stream.resource) {
            return AdmissionOutcome::Rejected { rejection };
        }
        if let Err(rejection) = check_point(&point) {
            return AdmissionOutcome::Rejected { rejection };
        }
        let key = PointIdentity {
            stream: stream.clone(),
            point_attributes: point.attributes().clone(),
            start_time_unix_nano: point.start_time_unix_nano(),
            time_unix_nano: point.time_unix_nano(),
        };
        match self.points.get(&key) {
            Some((standing, admitted)) if *admitted == point => {
                AdmissionOutcome::Collapsed { entity: *standing }
            }
            Some((standing, _)) => {
                self.anomalies.point_identity_conflicts += 1;
                AdmissionOutcome::Conflict { entity: *standing }
            }
            None => {
                let entity = self.assigned_entity();
                self.points.insert(key, (entity, point));
                AdmissionOutcome::Admitted { entity }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budgets::limits;
    use crate::context::{SpanId, TraceContext, TraceFlags, TraceId, TraceState};
    use crate::logs::LogRecord;
    use crate::metrics::{MetricNumber, MetricStream, NumberPoint, StreamKind, Temporality};
    use crate::resources::{InstrumentationScope, Resource};
    use crate::spans::{EmitterDroppedCounts, Span, SpanKind, SpanStatus, SpanStatusCode};
    use crate::values::{Attributes, Value};

    fn context(trace: [u8; 16], span: [u8; 8]) -> TraceContext {
        TraceContext {
            trace_id: TraceId::from_bytes(trace),
            span_id: SpanId::from_bytes(span),
            flags: TraceFlags::new(1),
            tracestate: TraceState::default(),
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
            attributes: Attributes::default(),
            dropped_attribute_count: 0,
            trace_context: None,
        }
    }

    fn resource() -> Resource {
        Resource {
            attributes: Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String("checkout".to_owned()),
            )]),
            schema_url: None,
        }
    }

    fn scope() -> InstrumentationScope {
        InstrumentationScope {
            name: "scope".to_owned(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
        }
    }

    fn gauge_point(time: u64, value: i64) -> MetricPoint {
        MetricPoint::Number(NumberPoint::measurement(
            time,
            MetricNumber::int(value),
            Attributes::default(),
            Vec::new(),
        ))
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

    #[test]
    fn a_valid_span_keeps_its_natural_identity_and_a_redelivery_collapses() {
        let mut ledger = AdmissionLedger::new();
        let first = ledger.admit_span(span([1; 16], [2; 8], "op"));
        let entity = first.entity().expect("admitted");
        assert_eq!(
            entity,
            EntityId::Span {
                trace_id: TraceId::from_bytes([1; 16]),
                span_id: SpanId::from_bytes([2; 8]),
            },
            "the natural identity is the entity id"
        );
        let second = ledger.admit_span(span([1; 16], [2; 8], "op"));
        assert_eq!(
            second,
            AdmissionOutcome::Collapsed { entity },
            "at-least-once delivery collapses onto the admitted span"
        );
        assert_eq!(ledger.anomalies().total(), 0);
    }

    #[test]
    fn a_conflicting_redelivery_leaves_the_first_standing_and_records_an_anomaly() {
        let mut ledger = AdmissionLedger::new();
        let first = ledger.admit_span(span([1; 16], [2; 8], "op"));
        let standing = first.entity().expect("admitted");
        let conflicting = ledger.admit_span(span([1; 16], [2; 8], "renamed"));
        assert_eq!(
            conflicting,
            AdmissionOutcome::Conflict { entity: standing },
            "the first admitted record stands; the conflict is named"
        );
        assert_eq!(ledger.anomalies().span_identity_conflicts(), 1);
        assert_eq!(ledger.anomalies().total(), 1);
        // Re-sending the original still collapses: the standing record was
        // never rewritten.
        assert_eq!(
            ledger.admit_span(span([1; 16], [2; 8], "op")),
            AdmissionOutcome::Collapsed { entity: standing }
        );
    }

    #[test]
    fn byte_identical_log_records_are_two_admitted_records_never_collapsed() {
        let mut ledger = AdmissionLedger::new();
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
    }

    #[test]
    fn invalid_id_spans_get_assigned_ids_and_never_collapse() {
        let mut ledger = AdmissionLedger::new();
        let first = ledger.admit_span(span([0; 16], [0; 8], "op"));
        let second = ledger.admit_span(span([0; 16], [0; 8], "op"));
        let (
            AdmissionOutcome::Admitted { entity: first_id },
            AdmissionOutcome::Admitted { entity: second_id },
        ) = (first, second)
        else {
            panic!("both admit");
        };
        assert_ne!(first_id, second_id);
        assert!(first_id.assigned_serial().is_some());
        assert!(second_id.assigned_serial().is_some());
        let half_invalid = ledger.admit_span(span([1; 16], [0; 8], "op"));
        assert!(half_invalid.entity().is_some());
    }

    #[test]
    fn assigned_serials_are_session_scoped_and_monotonic() {
        let mut ledger = AdmissionLedger::new();
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
    fn duplicate_points_collapse_and_conflicting_points_are_recorded() {
        let mut ledger = AdmissionLedger::new();
        let identity = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let point = MetricPoint::Number(NumberPoint::interval(
            100,
            200,
            MetricNumber::int(5),
            Attributes::default(),
            Vec::new(),
        ));
        let first = ledger.admit_metric_point(&identity, point.clone());
        let standing = first.entity().expect("admitted");
        assert!(
            standing.assigned_serial().is_some(),
            "points get assigned ids"
        );
        assert_eq!(
            ledger.admit_metric_point(&identity, point),
            AdmissionOutcome::Collapsed { entity: standing }
        );
        let conflicting = ledger.admit_metric_point(
            &identity,
            MetricPoint::Number(NumberPoint::interval(
                100,
                200,
                MetricNumber::int(6),
                Attributes::default(),
                Vec::new(),
            )),
        );
        assert_eq!(conflicting, AdmissionOutcome::Conflict { entity: standing });
        assert_eq!(ledger.anomalies().point_identity_conflicts(), 1);
    }

    #[test]
    fn delta_and_cumulative_points_of_one_name_never_collapse_together() {
        let mut ledger = AdmissionLedger::new();
        let delta = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Delta),
        );
        let cumulative = stream_identity(
            StreamKind::Sum { monotonic: true },
            Some(Temporality::Cumulative),
        );
        let point = gauge_point(1, 1);
        let first = ledger.admit_metric_point(&delta, point.clone());
        let second = ledger.admit_metric_point(&cumulative, point);
        assert_ne!(
            first.entity(),
            second.entity(),
            "a delta stream and a cumulative stream are different series"
        );
        assert_eq!(ledger.anomalies().total(), 0);
    }

    #[test]
    fn gauge_points_identify_by_stream_and_time() {
        let mut ledger = AdmissionLedger::new();
        let identity = stream_identity(StreamKind::Gauge, None);
        let first = ledger.admit_metric_point(&identity, gauge_point(50, 1));
        let second = ledger.admit_metric_point(&identity, gauge_point(50, 1));
        let third = ledger.admit_metric_point(&identity, gauge_point(51, 1));
        assert_eq!(
            second.entity(),
            first.entity(),
            "same (stream, time) collapses"
        );
        assert_ne!(
            third.entity(),
            first.entity(),
            "another time is another point"
        );
    }

    #[test]
    fn over_budget_records_are_refused_by_name_before_admission() {
        let mut ledger = AdmissionLedger::new();
        let mut crowded = span([1; 16], [2; 8], "op");
        crowded.attributes = Attributes::from_pairs(
            (0..=i64::try_from(limits::ATTRIBUTES_PER_SIGNAL).expect("the limit fits an i64"))
                .map(|index| (format!("k{index}"), Value::Int(index)))
                .collect(),
        );
        let outcome = ledger.admit_span(crowded);
        let AdmissionOutcome::Rejected { rejection } = outcome else {
            panic!("over-cap span refuses");
        };
        assert_eq!(rejection.budget.slug(), "attributes_per_signal");
        assert!(!rejection.is_retryable());
        assert_eq!(ledger.anomalies().total(), 0);
        // The refusal was not a partial admission: the identity is free.
        let outcome = ledger.admit_span(span([1; 16], [2; 8], "op"));
        assert!(outcome.entity().is_some());
    }

    #[test]
    fn entity_ids_are_added_metadata_that_rewrites_nothing() {
        let mut ledger = AdmissionLedger::new();
        let original = span([7; 16], [8; 8], "op");
        let snapshot = original.clone();
        let outcome = ledger.admit_span(original);
        assert!(outcome.entity().is_some());
        assert_eq!(
            snapshot.context.trace_id.as_bytes(),
            [7; 16],
            "emitter data untouched by admission"
        );
        assert_eq!(snapshot.name, "op");
        assert_eq!(snapshot.emitter_dropped, EmitterDroppedCounts::default());
    }

    #[test]
    fn a_stream_end_to_end_admits_its_points_under_one_identity() {
        let mut ledger = AdmissionLedger::new();
        let stream = MetricStream::new(
            StreamIdentity {
                resource: resource(),
                scope: scope(),
                name: "requests".to_owned(),
                kind: StreamKind::Gauge,
                temporality: None,
            },
            None,
            None,
            vec![gauge_point(1, 1), gauge_point(2, 2)],
        )
        .expect("a coherent stream");
        let identity = stream.identity();
        let outcomes: Vec<AdmissionOutcome> = stream
            .points
            .iter()
            .map(|point| ledger.admit_metric_point(identity, point.clone()))
            .collect();
        assert_eq!(outcomes.len(), 2);
        assert_ne!(outcomes[0].entity(), outcomes[1].entity());
    }
}
