<script setup lang="ts">
import { ref, computed } from "vue";
import { Button, Card, Stack, Spinner, InlineError, Kbd } from "@ecoma-io/loom";
import { investigateTrace, nanoToMs } from "../api/investigation";
import type {
  CoverageEntry,
  InvestigationEnvelope,
  RunFacts,
} from "../api/investigation";
import TraceWaterfall from "./TraceWaterfall.vue";
import RelatedLogs from "./RelatedLogs.vue";
import MetricsWindow from "./MetricsWindow.vue";
import CorrelatedRelations from "./CorrelatedRelations.vue";

const traceId = ref("00000000000000000000000000000000");
const spanId = ref("0000000000000000");
const envelope = ref<InvestigationEnvelope | null>(null);
const error = ref<string | null>(null);
const loading = ref(false);

/** The span currently selected in the waterfall or logs (span_id string). */
const selectedSpanId = ref<string | null>(null);
/** A log row navigation target: click a log → scroll to its span in the waterfall. */
const navigatedFromLog = ref<string | null>(null);

async function investigate(): Promise<void> {
  loading.value = true;
  error.value = null;
  envelope.value = null;
  selectedSpanId.value = null;
  navigatedFromLog.value = null;
  try {
    envelope.value = await investigateTrace({
      root_span: { span: { trace_id: traceId.value, span_id: spanId.value } },
    });
  } catch (err: unknown) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

function onSelectSpan(spanId: string): void {
  selectedSpanId.value = spanId;
}

function onSelectLog(logSpanId: string | null): void {
  if (logSpanId === null) return;
  selectedSpanId.value = logSpanId;
  navigatedFromLog.value = logSpanId;
}

const subjectRoot = computed(() => envelope.value?.subject.effective.root);
const evidence = computed(() => envelope.value?.evidence);
const correlated = computed(() => envelope.value?.correlated);
const execution = computed(() => envelope.value?.execution);

const hasSomeOutput = computed(
  () =>
    (evidence.value?.spans.length ?? 0) > 0 ||
    (evidence.value?.logs.length ?? 0) > 0 ||
    (evidence.value?.points.length ?? 0) > 0 ||
    (correlated.value?.relations.length ?? 0) > 0,
);

// --- Truthfulness surface ------------------------------------------------
//
// The envelope reports run facts per evidence part (outcomes, coverage) and
// the limits that bounded the answer. When any of it says the answer is not
// complete — a part degraded or refused, coverage named a gap, the chain
// stopped, records were evicted — the UI states it instead of letting a
// truncated answer present as complete. A clean answer (every part Complete,
// no gaps, no stop, no evictions) renders exactly as before.

const PART_LABELS: Record<string, string> = {
  spans: "spans",
  related_logs: "related logs",
  surrounding_metrics: "surrounding metrics",
};

function partLabel(part: string): string {
  return PART_LABELS[part] ?? part;
}

/** Render a wire value as text without stringifying an object as "[object Object]". */
function textOf(value: unknown): string {
  if (typeof value === "string") return value;
  if (
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  ) {
    return String(value);
  }
  if (value === null || value === undefined) return "?";
  return JSON.stringify(value);
}

function entityText(entity: unknown): string {
  if (entity === null || typeof entity !== "object") return "?";
  const named = entity as {
    span?: { trace_id?: string; span_id?: string };
    assigned?: number;
  };
  if (named.span !== undefined) {
    return `span ${named.span.trace_id?.slice(0, 8) ?? "…"}/${
      named.span.span_id?.slice(0, 8) ?? "…"
    }`;
  }
  if (named.assigned !== undefined) return `record #${named.assigned}`;
  return "?";
}

function magnitudeText(magnitude: unknown): string {
  if (magnitude === null || typeof magnitude !== "object") return "?";
  const value = magnitude as {
    duration_ms?: number;
    units?: number;
    bytes?: number;
  };
  if (value.duration_ms !== undefined) return `${value.duration_ms} ms`;
  if (value.units !== undefined) return `${value.units} records`;
  if (value.bytes !== undefined) return `${value.bytes} bytes`;
  return "?";
}

function truncationPointText(position: unknown): string {
  if (position === null || typeof position !== "object") return "";
  const point = position as {
    cursor?: { hex?: string };
    last_examined?: unknown;
  };
  if (point.cursor?.hex !== undefined) {
    return `at cursor ${point.cursor.hex.slice(0, 16)}…`;
  }
  if (point.last_examined !== undefined) {
    return `at ${entityText(point.last_examined)}`;
  }
  return "";
}

/** Why a run stopped short, or null when the run is complete. */
function outcomeReason(outcome: RunFacts["outcome"]): string | null {
  switch (outcome.kind) {
    case "complete":
      return null;
    case "degraded": {
      const truncation = outcome.truncation as
        | { dimension?: unknown; position?: unknown; omitted?: unknown }
        | undefined;
      if (truncation === undefined) return "degraded by a budget truncation";
      const at = truncationPointText(truncation.position);
      return (
        `degraded: the ${textOf(truncation.dimension)} budget ran out, ` +
        `${textOf(truncation.omitted)} record(s) omitted` +
        (at !== "" ? ` ${at}` : "")
      );
    }
    case "refused": {
      const refusal = outcome.refusal as
        | { dimension?: unknown; limit?: unknown; observed?: unknown }
        | undefined;
      if (refusal === undefined) return "refused by the engine";
      return (
        `refused on ${textOf(refusal.dimension)} ` +
        `(limit ${magnitudeText(refusal.limit)}, observed ${magnitudeText(
          refusal.observed,
        )})`
      );
    }
    case "stalled":
      return "stalled on consecutive empty store pages";
    default:
      return `ended with outcome ${outcome.kind}`;
  }
}

function timeText(unixNano: unknown): string {
  if (typeof unixNano !== "string") return textOf(unixNano);
  return new Date(nanoToMs(unixNano)).toISOString();
}

const COVERAGE_LABELS: Record<string, string> = {
  eviction_gap: "Eviction gap",
  snapshot_boundary: "Snapshot boundary",
  uncounted_tail: "Uncounted tail",
  driver_stall: "Driver stall",
  admission_anomalies: "Admission anomalies",
  residency_hole: "Residency hole",
  suppressed_evidence: "Suppressed evidence",
  absent_trace_spans: "Absent trace spans",
  relation_shrinkage: "Relation shrinkage",
  correlation_degradation: "Correlation degradation",
  temporal_strategy_skipped: "Temporal strategy skipped",
};

function correlationStopText(at: unknown): string {
  if (at === null || typeof at !== "object") {
    return "correlation stopped before finishing the resident read";
  }
  const stop = at as { kind?: string; value?: number };
  switch (stop.kind) {
    case "max_depth":
      return `correlation bounded to hop depth ${String(stop.value)}`;
    case "scan_exhausted":
      return `correlation scan allowance exhausted after ${String(
        stop.value,
      )} positions`;
    case "max_relations":
      return `correlated relations cut at ${String(stop.value)}`;
    default:
      return "correlation stopped before finishing the resident read";
  }
}

/** One coverage entry as a labelled statement: type + what it names. */
function describeCoverage(entry: CoverageEntry): {
  label: string;
  text: string;
} {
  const label = COVERAGE_LABELS[entry.kind] ?? entry.kind;
  switch (entry.kind) {
    case "eviction_gap":
      return {
        label,
        text: `records between ${entityText(entry.after)} and ${entityText(
          entry.before,
        )} were evicted before examination`,
      };
    case "snapshot_boundary":
      return {
        label,
        text: `records admitted before ${timeText(
          entry.admission_time_unix_nano,
        )} are outside the walk's snapshot`,
      };
    case "uncounted_tail":
      return {
        label,
        text: `records past ${entityText(entry.after)} were not counted on ${textOf(
          entry.dimension,
        )}`,
      };
    case "driver_stall":
      return {
        label,
        text: `the store stalled after ${entityText(entry.after)}`,
      };
    case "admission_anomalies":
      return {
        label,
        text: `${textOf(entry.total)} identity conflict(s) reported at admission`,
      };
    case "residency_hole":
      return {
        label,
        text: `${textOf(entry.count)} ${partLabel(
          textOf(entry.part),
        )} record(s) left residency before identity naming`,
      };
    case "suppressed_evidence":
      return {
        label,
        text: `${textOf(entry.suppressed)} relation(s) suppressed for log ${entityText(
          entry.log,
        )} (attached to span ${entityText(entry.span)})`,
      };
    case "absent_trace_spans":
      return {
        label,
        text: `log ${entityText(entry.log)} has no resident span to correlate`,
      };
    case "relation_shrinkage":
      return {
        label,
        text: `${textOf(entry.count)} relation(s) dropped because an endpoint left the evidence`,
      };
    case "correlation_degradation":
      return { label, text: correlationStopText(entry.at) };
    case "temporal_strategy_skipped":
      return {
        label,
        text: `temporal co-activity did not run (${textOf(entry.reason)})`,
      };
    default:
      return { label, text: "" };
  }
}

// Routine statements, not gaps: the metric-window statement renders with the
// metrics table, and the temporal-strategy skip is a named policy, never a
// truncation. Neither makes an answer unclean.
const STATEMENT_KINDS = new Set(["metric_window", "temporal_strategy_skipped"]);

const runGroups = computed(() => envelope.value?.execution.run_groups ?? []);

/** Every part run that did not complete, with the reason it stopped. */
const partProblems = computed(() => {
  const problems: { part: string; reason: string }[] = [];
  for (const group of runGroups.value) {
    for (const run of group.runs) {
      const reason = outcomeReason(run.outcome);
      if (reason !== null) {
        problems.push({ part: partLabel(group.part), reason });
      }
    }
  }
  return problems;
});

/** Every coverage entry the answer reports, minus the metrics-window one. */
const rawCoverageEntries = computed(() => {
  const entries: CoverageEntry[] = [];
  for (const group of runGroups.value) {
    for (const run of group.runs) {
      entries.push(...(run.coverage ?? []));
    }
  }
  for (const entry of envelope.value?.execution.flow_coverage ?? []) {
    if (entry.kind !== "metric_window") entries.push(entry);
  }
  return entries;
});

const coverageEntries = computed(() =>
  rawCoverageEntries.value.map(describeCoverage),
);

const limitsStatements = computed(() => {
  const limits = envelope.value?.limits;
  if (limits === undefined) return [];
  const statements: string[] = [
    `chain spent ${limits.chain.total_entities} entities over ${limits.chain.total_pages} pages`,
  ];
  if (limits.chain.stopped !== null && limits.chain.stopped !== undefined) {
    statements.push(`chain stopped at the ${limits.chain.stopped} ceiling`);
  }
  if (limits.eviction.total_evictions > 0) {
    statements.push(
      `${limits.eviction.total_evictions} record(s) evicted during residency (${limits.eviction.resident_records} resident)`,
    );
  }
  return statements;
});

/** False when the run facts say the answer is not complete. */
const isClean = computed(
  () =>
    partProblems.value.length === 0 &&
    rawCoverageEntries.value.every((entry) =>
      STATEMENT_KINDS.has(entry.kind),
    ) &&
    (envelope.value?.limits.chain.stopped ?? null) === null &&
    (envelope.value?.limits.eviction.total_evictions ?? 0) === 0,
);
</script>

<template>
  <Card title="Investigate a trace">
    <template #default>
      <Stack gap="lg">
        <p class="text-sm text-muted-foreground">
          Enter a root span identity to investigate. The investigation API
          returns the trace waterfall, related logs, the surrounding metrics
          window, and correlated relations for exactly that subject.
        </p>

        <div class="flex flex-wrap items-end gap-3">
          <label for="trace-id" class="flex flex-col gap-1">
            <span class="text-xs font-medium"
              >Trace ID <Kbd class="ml-1">32 hex</Kbd></span
            >
            <input
              id="trace-id"
              v-model="traceId"
              class="w-[36ch] rounded border bg-background px-2 py-1 font-mono text-sm"
              placeholder="0000…0000 (32 hex)"
              maxlength="32"
              spellcheck="false"
            />
          </label>
          <label for="span-id" class="flex flex-col gap-1">
            <span class="text-xs font-medium"
              >Span ID <Kbd class="ml-1">16 hex</Kbd></span
            >
            <input
              id="span-id"
              v-model="spanId"
              class="w-[20ch] rounded border bg-background px-2 py-1 font-mono text-sm"
              placeholder="0000…0000 (16 hex)"
              maxlength="16"
              spellcheck="false"
            />
          </label>
          <Button variant="primary" :disabled="loading" @click="investigate">
            <Spinner v-if="loading" class="mr-1" />
            {{ loading ? "Investigating…" : "Investigate" }}
          </Button>
        </div>

        <!-- Error -->
        <InlineError v-if="error !== null" :message="error" />

        <!-- Subject -->
        <div
          v-if="subjectRoot !== undefined"
          class="rounded bg-muted/50 p-3 text-xs"
        >
          <div>
            <span class="font-medium">Resolved root:</span>
            {{ subjectRoot.name }}
            <span class="font-mono opacity-70">
              (trace {{ subjectRoot.trace_id.slice(0, 12) }}… span
              {{ subjectRoot.span_id.slice(0, 8) }}…)
            </span>
          </div>
          <div
            v-if="envelope?.subject.effective.notes.length"
            class="mt-1 text-muted-foreground"
          >
            Notes:
            {{
              envelope.subject.effective.notes
                .map((n) => `${n.kind}${n.name ? ` (${n.name})` : ""}`)
                .join("; ")
            }}
          </div>
        </div>

        <!-- Truthfulness: when the run facts say the answer is not complete,
             state the degradation, coverage gaps and bounding limits instead
             of letting a truncated answer present as complete. -->
        <div
          v-if="envelope !== null && !isClean"
          data-testid="truthfulness-surface"
          role="status"
          class="rounded border border-warning/40 bg-warning-muted p-3 text-xs text-warning-text"
        >
          <div class="font-medium">
            This investigation is not complete — the run facts below name what
            was degraded, truncated or left uncovered.
          </div>
          <ul v-if="partProblems.length > 0" class="mt-2 list-disc pl-4">
            <li v-for="(problem, index) in partProblems" :key="index">
              <span class="font-medium">{{ problem.part }}</span
              >:
              {{ problem.reason }}
            </li>
          </ul>
          <ul v-if="coverageEntries.length > 0" class="mt-2 list-disc pl-4">
            <li v-for="(entry, index) in coverageEntries" :key="index">
              <span class="font-medium">{{ entry.label }}</span
              >:
              {{ entry.text }}
            </li>
          </ul>
          <div v-if="limitsStatements.length > 0" class="mt-2">
            <span class="font-medium">Limits:</span>
            {{ limitsStatements.join("; ") }}
          </div>
        </div>

        <!-- Outputs -->
        <template v-if="hasSomeOutput">
          <!-- Trace Waterfall -->
          <Card title="Trace waterfall">
            <template #default>
              <TraceWaterfall
                :spans="evidence?.spans ?? []"
                :selected-span-id="selectedSpanId"
                @select-span="onSelectSpan"
              />
            </template>
          </Card>

          <!-- Related Logs -->
          <Card title="Related logs">
            <template #default>
              <div class="mb-1 text-xs text-muted-foreground">
                Click a log to navigate to its span in the waterfall.
              </div>
              <RelatedLogs
                :logs="evidence?.logs ?? []"
                :selected-log-span-id="selectedSpanId"
                @select-log="onSelectLog"
              />
            </template>
          </Card>

          <!-- Surrounding Metrics Window -->
          <Card title="Surrounding metrics window">
            <template #default>
              <MetricsWindow
                :points="evidence?.points ?? []"
                :flow-coverage="execution?.flow_coverage ?? []"
              />
            </template>
          </Card>

          <!-- Correlated Relations -->
          <Card title="Correlated relations">
            <template #default>
              <CorrelatedRelations :relations="correlated?.relations ?? []" />
            </template>
          </Card>
        </template>

        <!-- Honest empty state after fetch with no data -->
        <div
          v-if="envelope !== null && !hasSomeOutput"
          class="rounded bg-muted/50 p-4 text-sm text-muted-foreground"
        >
          The investigation returned no evidence or correlated relations for
          this root span.
        </div>
      </Stack>
    </template>
  </Card>
</template>
