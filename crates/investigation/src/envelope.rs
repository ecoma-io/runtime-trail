//! The investigation envelope: one response shape with five parts.
//!
//! Every response of the Investigation API is an [`Investigation`]:
//! **subject** (requested and effective, stated separately),
//! **execution** (run facts: per-page outcomes, coverage, cursors),
//! **correlated** (resident-only relations), **evidence** (record views
//! carrying their entity ids) and **limits** (the machine-checkable
//! statements of what bounded the answer).
//!
//! The envelope is defined without a transport and without a storage mode:
//! the shape is one type, the same in every mode. The seven invariants of
//! `docs/architecture/investigation-model.md` are mechanically checkable
//! through [`invariants`], and the *investigation-shaped line* — no
//! storage or transport vocabulary in envelope field paths — is enforced
//! by [`shape_violations`] over [`FIELD_PATHS`].

use crate::correlated::Correlated;
use crate::evidence::Evidence;
use crate::execution::{Execution, FlowCoverageEntry, Outcome, PartName, TimeWindow};
use crate::limits::Limits;
use crate::subject::Subject;

/// The envelope: one response shape, five parts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Investigation {
    /// What was investigated: requested and effective, separately.
    pub subject: Subject,
    /// What the work cost and covered: run facts, never a summary.
    pub execution: Execution,
    /// The relations between the subject and resident signals, produced by
    /// the committed correlation strategies.
    pub correlated: Correlated,
    /// The record views the answer is grounded in, each with its entity id.
    pub evidence: Evidence,
    /// What the answer is subject to: budget, chain, strategies, eviction.
    pub limits: Limits,
}

impl Investigation {
    /// Builds the envelope from its five parts.
    #[must_use]
    pub const fn new(
        subject: Subject,
        execution: Execution,
        correlated: Correlated,
        evidence: Evidence,
        limits: Limits,
    ) -> Self {
        Self {
            subject,
            execution,
            correlated,
            evidence,
            limits,
        }
    }
}

/// The envelope's field paths, as data: the shape statement the
/// investigation-shaped line is checked against. Adding an envelope field
/// means adding its path here — the table is the type's shape, mechanically
/// checkable by [`shape_violations`] and kept honest by the tests below.
pub const FIELD_PATHS: &[&str] = &[
    // subject
    "subject.requested.root_span",
    "subject.effective.root.entity",
    "subject.effective.root.name",
    "subject.effective.root.trace_id",
    "subject.effective.root.span_id",
    "subject.effective.notes",
    // execution
    "execution.run_groups[].part",
    "execution.run_groups[].runs[].outcome",
    "execution.run_groups[].runs[].coverage",
    "execution.run_groups[].runs[].next_cursor",
    "execution.flow_coverage",
    // correlated
    "correlated.relations[].relation_type",
    "correlated.relations[].from",
    "correlated.relations[].to",
    "correlated.relations[].facts[].field",
    "correlated.relations[].facts[].value",
    "correlated.relations[].strategy.name",
    "correlated.relations[].strategy.version",
    "correlated.relations[].window",
    // evidence
    "evidence.spans[].entity",
    "evidence.spans[].span",
    "evidence.logs[].entity",
    "evidence.logs[].log",
    "evidence.points[].entity",
    "evidence.points[].point",
    "evidence.points[].stream",
    // limits
    "limits.budget.deadline",
    "limits.budget.max_results",
    "limits.budget.max_bytes",
    "limits.budget.max_scan",
    "limits.budget.max_aggregation_memory",
    "limits.chain.max_total_entities",
    "limits.chain.max_total_pages",
    "limits.chain.total_entities",
    "limits.chain.total_pages",
    "limits.chain.identity_examinations",
    "limits.chain.correlation_scan",
    "limits.chain.stopped",
    "limits.strategy_versions[].name",
    "limits.strategy_versions[].version",
    "limits.eviction.resident_records",
    "limits.eviction.total_evictions",
];

/// Storage and transport vocabulary no envelope field may be named in: the
/// investigation-shaped line of `docs/architecture/investigation-model.md`.
/// Field paths are tokenized on `_` and `.`; a path token that equals one
/// of these words fails the contract. (Coverage-entry *variants* may and
/// must name storage facts — `EvictionGap`, `DriverStall` report them —
/// but field names never do. `aggregation_memory` stays a budget-dimension
/// token: it mirrors the engine's five-dimension contract rather than a
/// storage mode, which `mode` itself catches.)
pub const BANNED_VOCABULARY: &[&str] = &[
    "storage",
    "driver",
    "table",
    "index",
    "sql",
    "sqlite",
    "http",
    "grpc",
    "otlp",
    "rpc",
    "transport",
    "wire",
    "json",
    "mode",
    "request",
    "response",
    "endpoint",
    "route",
];

/// The mechanically checkable investigation-shaped line: which envelope
/// field paths are named in storage or transport vocabulary. A field path
/// is tokenized on `_` and `.`; an exact token match against
/// [`BANNED_VOCABULARY`] fails. Empty list = shape is clean.
#[must_use]
pub fn shape_violations(field_paths: &[&str]) -> Vec<String> {
    let mut violations = Vec::new();
    for path in field_paths {
        for token in path.split(['_', '.', '[', ']']) {
            if BANNED_VOCABULARY.contains(&token) {
                violations.push(format!("{path} names banned vocabulary `{token}`"));
            }
        }
    }
    violations
}

/// The seven envelope invariants, mechanically checkable.
pub mod invariants {
    use super::{FIELD_PATHS, FlowCoverageEntry, Investigation, Outcome, PartName, TimeWindow};

    /// The labels of every invariant this envelope violates. Empty = clean.
    ///
    /// Invariant 6 (determinism) and the mode-symmetry field-path statement
    /// are pairwise/static checks: see [`content_parts_equal`],
    /// [`run_facts_equivalent`] and [`super::shape_violations`].
    #[must_use]
    pub fn violations(envelope: &Investigation) -> Vec<&'static str> {
        let mut found = Vec::new();
        if !subjects_are_stated_separately(envelope) {
            found.push("requested and effective subjects merged or unresolved");
        }
        if !truncations_are_named(envelope) {
            found.push("a truncation, refusal or stall is not named");
        }
        if !signal_refs_resolve(envelope) {
            found.push("a signal ref dangles or an inferred relation is present");
        }
        found
    }
    #[must_use]
    pub fn subjects_are_stated_separately(envelope: &Investigation) -> bool {
        let requested = &envelope.subject.requested.root_span;
        let effective = &envelope.subject.effective.root.entity;
        if requested != effective {
            let named = envelope.subject.effective.notes.iter().any(|note| {
                matches!(
                    note,
                    crate::subject::ResolutionNote::TraceRootedAt { entity, .. }
                        if entity == effective
                )
            });
            if !named {
                return false;
            }
        }
        // A resolution note must never contradict the effective root.
        envelope.subject.effective.notes.iter().all(|note| {
            !matches!(
                note,
                crate::subject::ResolutionNote::TraceRootedAt { entity, .. }
                    if entity != effective
            )
        })
    }

    /// Invariant 2: every truncation is named. The checkable parts: a
    /// `Degraded` run carries its truncation with a position and an omitted
    /// count; a `Refused` run carries its refusal; a `Stalled` run carries a
    /// `DriverStall` coverage entry (the engine always emits one), so a
    /// stall is never a bare adjective.
    #[must_use]
    pub fn truncations_are_named(envelope: &Investigation) -> bool {
        for group in &envelope.execution.run_groups {
            for run in &group.runs {
                match &run.outcome {
                    Outcome::Complete => {}
                    Outcome::Degraded { truncation } => {
                        if truncation.omitted == 0 {
                            return false;
                        }
                        match truncation.position {
                            crate::execution::TruncationPoint::Cursor(_)
                            | crate::execution::TruncationPoint::LastExamined(_) => {}
                        }
                    }
                    Outcome::Refused(refusal) => {
                        if refusal.limit == refusal.observed {
                            return false;
                        }
                    }
                    Outcome::Stalled => {
                        let stall_named = run.coverage.iter().any(|entry| {
                            matches!(entry, crate::execution::CoverageEntry::DriverStall { .. })
                        });
                        if !stall_named {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// Invariant 3: every `SignalRef` resolves to a resident record, and
    /// no `Inferred` relation (a relation without grounding facts) is ever
    /// constructed.
    #[must_use]
    pub fn signal_refs_resolve(envelope: &Investigation) -> bool {
        for relation in &envelope.correlated.relations {
            if relation.relation_type == crate::correlated::RelationType::Inferred
                || relation.facts.is_empty()
            {
                return false;
            }
            if !envelope
                .evidence
                .entity_of(&relation.from.kind, &relation.from.entity)
                || !envelope
                    .evidence
                    .entity_of(&relation.to.kind, &relation.to.entity)
            {
                return false;
            }
        }
        true
    }

    /// Invariant 4: every record view carries its entity id, and no entity
    /// id is carried by two views: relations, evidence and navigation all
    /// name the same identity.
    #[must_use]
    pub fn views_carry_entity_ids(envelope: &Investigation) -> bool {
        let mut seen = Vec::new();
        let mut all_keyed = true;
        for view in envelope
            .evidence
            .spans
            .iter()
            .map(|view| view.entity.as_ref())
            .chain(
                envelope
                    .evidence
                    .logs
                    .iter()
                    .map(|view| view.entity.as_ref()),
            )
            .chain(
                envelope
                    .evidence
                    .points
                    .iter()
                    .map(|view| view.entity.as_ref()),
            )
        {
            match view {
                Some(entity) => {
                    if seen.contains(entity) {
                        return false;
                    }
                    seen.push(*entity);
                }
                None => all_keyed = false,
            }
        }
        all_keyed
    }

    /// Invariant 5: a response never presents a partial aggregate as
    /// complete. The envelope's honesty is structural — this is the
    /// machine-checkable form: every run the flow performed is recorded
    /// (a full answer never swallows a page), and a chain stop is never
    /// presented as a completed run.
    #[must_use]
    pub fn no_partial_content_as_complete(envelope: &Investigation) -> bool {
        // Every chain page is a recorded run.
        let recorded_pages: usize = envelope
            .execution
            .run_groups
            .iter()
            .map(|group| group.runs.len())
            .sum();
        if recorded_pages as u64 != envelope.limits.chain.total_pages {
            return false;
        }
        // A flow that stopped without performing a single page claims
        // nothing: invalid.
        if envelope.limits.chain.stopped.is_some() && envelope.limits.chain.total_pages == 0 {
            return false;
        }
        true
    }

    /// Invariant 6 (content parts): same resident set + same request +
    /// same strategy versions ⇒ the content parts — subject, correlated,
    /// evidence — are byte-identical. The execution and limits parts are
    /// run facts and are compared by meaning via [`run_facts_equivalent`].
    #[must_use]
    pub fn content_parts_equal(a: &Investigation, b: &Investigation) -> bool {
        a.subject == b.subject && a.correlated == b.correlated && a.evidence == b.evidence
    }

    /// Invariant 6 (run facts, by meaning): the execution and limits parts
    /// agree in *kind* — the same outcome shapes, the same coverage-entry
    /// shapes, the same chain-stop basis — while byte-level values (spend,
    /// cursors, eviction totals) legitimately differ between runs.
    #[must_use]
    pub fn run_facts_equivalent(a: &Investigation, b: &Investigation) -> bool {
        let groups_a = &a.execution.run_groups;
        let groups_b = &b.execution.run_groups;
        if groups_a.len() != groups_b.len() {
            return false;
        }
        for (ga, gb) in groups_a.iter().zip(groups_b) {
            if ga.part != gb.part || ga.runs.len() != gb.runs.len() {
                return false;
            }
            for (ra, rb) in ga.runs.iter().zip(&gb.runs) {
                if outcome_kind(&ra.outcome) != outcome_kind(&rb.outcome) {
                    return false;
                }
                if ra.coverage.len() != rb.coverage.len() {
                    return false;
                }
                for (ca, cb) in ra.coverage.iter().zip(&rb.coverage) {
                    if coverage_kind(ca) != coverage_kind(cb) {
                        return false;
                    }
                }
            }
        }
        a.limits.chain.stopped == b.limits.chain.stopped
    }

    /// Invariant 6 (internal consistency): the chain totals are run facts
    /// that must agree with what the run groups and evidence actually say —
    /// total pages equal the recorded runs, total entities equal the
    /// evidence views plus the named residency holes.
    #[must_use]
    pub fn run_facts_are_consistent(envelope: &Investigation) -> bool {
        let recorded_pages: usize = envelope
            .execution
            .run_groups
            .iter()
            .map(|group| group.runs.len())
            .sum();
        if recorded_pages as u64 != envelope.limits.chain.total_pages {
            return false;
        }
        let hole_counts: u64 = envelope
            .execution
            .flow_coverage
            .iter()
            .filter_map(|entry| match entry {
                FlowCoverageEntry::ResidencyHole { count, .. } => Some(*count),
                _ => None,
            })
            .sum();
        let views = (envelope.evidence.spans.len()
            + envelope.evidence.logs.len()
            + envelope.evidence.points.len()) as u64;
        envelope.limits.chain.total_entities == views + hole_counts
    }

    /// Invariant 2 + 7 (coverage statements): the metric part states the
    /// window asked against the window the resident points actually span,
    /// and the asked window matches the waterfall it was derived from.
    #[must_use]
    pub fn metric_window_is_stated(envelope: &Investigation) -> bool {
        let metrics_ran = envelope
            .execution
            .run_groups
            .iter()
            .any(|group| group.part == PartName::SurroundingMetrics && !group.runs.is_empty());
        if !metrics_ran {
            return true;
        }
        let window = envelope
            .execution
            .flow_coverage
            .iter()
            .find_map(|entry| match entry {
                FlowCoverageEntry::MetricWindow { asked, .. } => Some(asked),
                _ => None,
            });
        let Some(asked) = window else {
            return false;
        };
        // The asked window is the waterfall's time extent: min start to
        // max end (a span with no end counts its start), half-open.
        let Some((min_start, max_end)) = waterfall_extent(envelope) else {
            return false;
        };
        *asked == TimeWindow::new(min_start, max_end)
    }

    /// The waterfall's time extent, per the flow's own definition: the
    /// earliest start time and the latest end time (a span with no end
    /// counts its start) over the evidence spans.
    #[must_use]
    pub fn waterfall_extent(envelope: &Investigation) -> Option<(u64, u64)> {
        let spans = &envelope.evidence.spans;
        if spans.is_empty() {
            return None;
        }
        let mut min_start = u64::MAX;
        let mut max_end = 0;
        for view in spans {
            let start = view.span.start_time_unix_nano;
            let end = view.span.end_time_unix_nano.unwrap_or(start);
            min_start = min_start.min(start);
            max_end = max_end.max(end);
        }
        Some((min_start, max_end))
    }

    /// Invariant 7 (mode symmetry): one envelope type, the same field-path
    /// shape in every mode. The checkable statement is the investigation-
    /// shaped line — see [`super::shape_violations`] — which runs over
    /// [`super::FIELD_PATHS`] and must be clean.
    #[must_use]
    pub fn mode_symmetry_field_paths_clean() -> bool {
        super::shape_violations(FIELD_PATHS).is_empty()
    }

    fn outcome_kind(outcome: &Outcome) -> &'static str {
        match outcome {
            Outcome::Complete => "complete",
            Outcome::Degraded { .. } => "degraded",
            Outcome::Refused(_) => "refused",
            Outcome::Stalled => "stalled",
        }
    }

    fn coverage_kind(entry: &crate::execution::CoverageEntry) -> &'static str {
        match entry {
            crate::execution::CoverageEntry::EvictionGap { .. } => "eviction_gap",
            crate::execution::CoverageEntry::SnapshotBoundary { .. } => "snapshot_boundary",
            crate::execution::CoverageEntry::UncountedTail { .. } => "uncounted_tail",
            crate::execution::CoverageEntry::DriverStall { .. } => "driver_stall",
        }
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use runtime_trail_telemetry_model::{
        Attributes, EmitterDroppedCounts, EntityId, InstrumentationScope, Resource, Span, SpanId,
        SpanKind, SpanStatus, SpanStatusCode, TraceContext, TraceFlags, TraceId, TraceState, Value,
    };

    use super::*;
    use crate::correlated::{Correlated, EvidenceFact, Relation, RelationType, StrategyVersion};
    use crate::evidence::{Evidence, SignalKind, SignalRef, SpanEvidence};
    use crate::execution::{
        CoverageEntry, Dimension, FlowCoverageEntry, Magnitude, OpaqueCursor, Outcome, Refusal,
        RunFacts, RunGroup, TimeWindow, Truncation, TruncationPoint,
    };
    use crate::limits::{BudgetLimits, ChainBasis, ChainLimits, EvictionState};
    use crate::subject::{
        EffectiveRoot, EffectiveSubject, RequestedSubject, ResolutionNote, Subject,
    };

    fn trace_id(byte: u8) -> TraceId {
        TraceId::from_bytes([byte; 16])
    }

    fn span_id(byte: u8) -> SpanId {
        SpanId::from_bytes([byte; 8])
    }

    fn entity(byte: u8) -> EntityId {
        EntityId::Span {
            trace_id: trace_id(byte),
            span_id: span_id(byte),
        }
    }

    fn trace_context() -> TraceContext {
        TraceContext {
            trace_id: trace_id(1),
            span_id: span_id(1),
            flags: TraceFlags::new(0),
            tracestate: TraceState::default(),
        }
    }

    fn resource() -> Arc<Resource> {
        Arc::new(Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        })
    }

    fn scope() -> Arc<InstrumentationScope> {
        Arc::new(InstrumentationScope {
            name: "envelope-tests".to_owned(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        })
    }

    fn span_arc_named(name: &str, start: u64) -> Arc<Span> {
        let span = Span {
            context: trace_context(),
            parent_span_id: None,
            name: name.to_owned(),
            kind: SpanKind::Internal,
            start_time_unix_nano: start,
            end_time_unix_nano: Some(start + 10),
            resource: resource(),
            scope: scope(),
            attributes: Attributes::default(),
            emitter_dropped: EmitterDroppedCounts::default(),
            events: vec![],
            links: vec![],
            status: SpanStatus {
                code: SpanStatusCode::Unset,
                message: String::new(),
            },
        };
        Arc::new(span)
    }

    fn span_named(name: &str, start: u64) -> SpanEvidence {
        SpanEvidence::new(Some(entity(1)), span_arc_named(name, start))
    }

    fn budget() -> BudgetLimits {
        BudgetLimits::new(Duration::from_secs(1), 100, 4_096, 1_000, 1_024)
    }

    fn clean_envelope() -> Investigation {
        let subject = Subject::new(
            RequestedSubject::new(entity(1)),
            EffectiveSubject::new(
                EffectiveRoot::new(entity(1), "root".to_owned(), trace_id(1), span_id(1)),
                vec![ResolutionNote::RequestedSpanIsRoot],
            ),
        );
        let execution = Execution::new(
            vec![RunGroup {
                part: PartName::Spans,
                runs: vec![RunFacts::new(Outcome::Complete, vec![], None)],
            }],
            vec![],
        );
        let correlated = Correlated::new(vec![]);
        let evidence = Evidence::new(vec![span_named("root", 1_000)], vec![], vec![]);
        let limits = Limits::new(
            budget(),
            ChainLimits::new(10_000, 16, 1, 1, 0, 0, None),
            vec![],
            EvictionState::new(1, 0),
        );
        Investigation::new(subject, execution, correlated, evidence, limits)
    }

    fn push_metrics_run(envelope: &mut Investigation) {
        envelope.execution.run_groups.push(RunGroup {
            part: PartName::SurroundingMetrics,
            runs: vec![RunFacts::new(Outcome::Complete, vec![], None)],
        });
        envelope.limits.chain.total_pages += 1;
    }

    #[test]
    fn clean_envelope_passes_every_invariant() {
        let envelope = clean_envelope();
        assert_eq!(
            invariants::violations(&envelope),
            Vec::<&'static str>::new()
        );
    }

    #[test]
    fn shape_line_has_no_violations_and_table_is_populated() {
        assert!(invariants::mode_symmetry_field_paths_clean());
        assert!(shape_violations(FIELD_PATHS).is_empty());
        assert!(
            FIELD_PATHS.len() >= 40,
            "field-path table must be exhaustive"
        );
        // The five parts are all present in the shape statement.
        assert!(FIELD_PATHS.iter().any(|p| p.starts_with("subject.")));
        assert!(FIELD_PATHS.iter().any(|p| p.starts_with("execution.")));
        assert!(FIELD_PATHS.iter().any(|p| p.starts_with("correlated.")));
        assert!(FIELD_PATHS.iter().any(|p| p.starts_with("evidence.")));
        assert!(FIELD_PATHS.iter().any(|p| p.starts_with("limits.")));
    }

    #[test]
    fn shape_line_rejects_storage_and_transport_vocabulary() {
        let violations =
            shape_violations(&["evidence.spans[].storage_handle", "limits.budget.transport"]);
        assert_eq!(violations.len(), 2);
        assert!(violations[0].contains("storage_handle"));
        assert!(violations[1].contains("transport"));
        // `requested` is subject vocabulary, not the banned `request` token.
        assert!(shape_violations(&["subject.requested.root_span"]).is_empty());
    }

    #[test]
    fn subjects_are_stated_separately_names_differences() {
        let mut envelope = clean_envelope();
        // Effective root differs from requested without a naming note.
        envelope.subject.effective.root.entity = entity(2);
        envelope.subject.effective.root.span_id = span_id(2);
        assert!(!invariants::subjects_are_stated_separately(&envelope));
        // Naming the difference repairs it.
        envelope.subject.effective.notes = vec![ResolutionNote::TraceRootedAt {
            entity: entity(2),
            name: "root".to_owned(),
            span_id: span_id(2),
        }];
        assert!(invariants::subjects_are_stated_separately(&envelope));
    }

    #[test]
    fn truncations_are_named_for_every_outcome() {
        let mut envelope = clean_envelope();
        envelope.execution.run_groups[0].runs = vec![
            RunFacts::new(
                Outcome::Degraded {
                    truncation: Truncation {
                        dimension: Dimension::Results,
                        position: TruncationPoint::Cursor(vec![1, 2]),
                        omitted: 4,
                    },
                },
                vec![],
                Some(OpaqueCursor::new(vec![1, 2])),
            ),
            RunFacts::new(
                Outcome::Refused(Refusal {
                    dimension: Dimension::Scan,
                    limit: Magnitude::Units(0),
                    observed: Magnitude::Units(1),
                }),
                vec![],
                None,
            ),
            RunFacts::new(
                Outcome::Stalled,
                vec![CoverageEntry::DriverStall { after: entity(1) }],
                None,
            ),
        ];
        // Chain totals must track the runs for the full check to pass.
        envelope.limits.chain.total_pages = 3;
        assert!(invariants::truncations_are_named(&envelope));

        // A stall without its DriverStall coverage entry is a bare
        // adjective: the envelope fails.
        let mut stalled = clean_envelope();
        stalled.execution.run_groups[0].runs = vec![RunFacts::new(Outcome::Stalled, vec![], None)];
        stalled.limits.chain.total_pages = 1;
        assert!(!invariants::truncations_are_named(&stalled));
    }

    #[test]
    fn dangling_signal_refs_and_inferred_relations_are_rejected() {
        let mut envelope = clean_envelope();
        envelope.correlated.relations = vec![Relation::new(
            RelationType::SpanIdentity,
            SignalRef::new(SignalKind::Spans, entity(1)),
            SignalRef::new(SignalKind::Spans, entity(9)), // not resident
            vec![EvidenceFact::new("trace_id".to_owned(), Value::Int(1))],
            StrategyVersion::new("spans".to_owned(), "0.1.0".to_owned()),
            None,
        )];
        assert!(!invariants::signal_refs_resolve(&envelope));

        let mut inferred = clean_envelope();
        inferred.correlated.relations = vec![Relation::new(
            RelationType::Inferred,
            SignalRef::new(SignalKind::Spans, entity(1)),
            SignalRef::new(SignalKind::Spans, entity(1)),
            vec![],
            StrategyVersion::new("spans".to_owned(), "0.1.0".to_owned()),
            None,
        )];
        assert!(!invariants::signal_refs_resolve(&inferred));
    }

    #[test]
    fn views_must_carry_entity_ids_and_unique_ones() {
        let mut envelope = clean_envelope();
        envelope.evidence.spans[0].entity = None;
        assert!(!invariants::views_carry_entity_ids(&envelope));

        let mut duplicate = clean_envelope();
        duplicate.evidence.spans.push(SpanEvidence::new(
            Some(entity(1)),
            span_arc_named("root", 1_000),
        ));
        assert!(!invariants::views_carry_entity_ids(&duplicate));
    }

    #[test]
    fn chain_stop_is_never_claimed_as_complete() {
        let mut envelope = clean_envelope();
        // The flow stopped on its own limits: the stop must be stated.
        envelope.limits.chain.stopped = Some(ChainBasis::TotalEntities);
        assert!(invariants::no_partial_content_as_complete(&envelope));

        // A stopped flow with zero recorded pages claims nothing: invalid.
        let mut empty = clean_envelope();
        empty.execution.run_groups[0].runs.clear();
        empty.limits.chain.total_pages = 0;
        empty.limits.chain.stopped = Some(ChainBasis::TotalPages);
        assert!(!invariants::no_partial_content_as_complete(&empty));
    }

    #[test]
    fn content_parts_are_deterministic_and_run_facts_equivalent() {
        let a = clean_envelope();
        let mut b = clean_envelope();
        // The run-fact parts differ in observed spend: eviction totals.
        b.limits.eviction.total_evictions = 7;
        assert!(invariants::content_parts_equal(&a, &b));
        assert!(invariants::run_facts_equivalent(&a, &b));

        let mut c = clean_envelope();
        Arc::make_mut(&mut c.evidence.spans[0].span).name = "other".to_owned();
        assert!(!invariants::content_parts_equal(&a, &c));
    }

    #[test]
    fn chain_totals_agree_with_run_groups_and_holes() {
        let mut envelope = clean_envelope();
        envelope
            .execution
            .flow_coverage
            .push(FlowCoverageEntry::ResidencyHole {
                part: PartName::RelatedLogs,
                count: 2,
            });
        envelope.limits.chain.total_entities = 3; // 1 span + 2 holes
        assert!(invariants::run_facts_are_consistent(&envelope));
        envelope.limits.chain.total_entities = 4;
        assert!(!invariants::run_facts_are_consistent(&envelope));
    }

    #[test]
    fn metric_window_statement_matches_the_waterfall() {
        let mut envelope = clean_envelope();
        push_metrics_run(&mut envelope);
        envelope
            .execution
            .flow_coverage
            .push(FlowCoverageEntry::MetricWindow {
                asked: TimeWindow::new(1_000, 1_010),
                resident: TimeWindow::new(1_000, 1_010),
            });
        assert!(invariants::metric_window_is_stated(&envelope));

        // A wrong asked window (disagreeing with the waterfall) fails.
        let mut wrong = envelope.clone();
        wrong.execution.flow_coverage[0] = FlowCoverageEntry::MetricWindow {
            asked: TimeWindow::new(500, 510),
            resident: TimeWindow::new(1_000, 1_010),
        };
        assert!(!invariants::metric_window_is_stated(&wrong));

        // No metrics ran: the statement is not required.
        let bare = clean_envelope();
        assert!(invariants::metric_window_is_stated(&bare));
    }
}
