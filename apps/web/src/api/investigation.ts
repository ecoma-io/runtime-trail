// The Investigation API client for the Loom web app.
//
// One HTTP door (ADR 0011, docs/architecture/investigation-model.md): the
// UI asks `POST /v1/investigations/traces` for one trace investigation and
// renders the envelope exactly as the runtime returns it — this module owns
// no data path of its own, so the boundary law (apps/web imports only
// `@ecoma-io/loom` and itself) holds by construction.
//
// The wire shapes here mirror `crates/server/src/investigation_http.rs`
// field for field. Timestamps travel as unsigned-64-bit unix nanoseconds —
// too large for a JS number to carry exactly — so the client parses the
// response with a reviver that keeps any non-safe-integer number as its
// canonical decimal string, and duration math goes through `BigInt`.

// Vite exposes env vars as `any`; validate before trusting one.
const configuredUrl: unknown = import.meta.env.VITE_INVESTIGATION_URL;
export const INVESTIGATION_BASE_URL =
  typeof configuredUrl === "string" && configuredUrl.length > 0
    ? configuredUrl
    : "http://127.0.0.1:8599";

export const INVESTIGATION_TRACES_PATH = "/v1/investigations/traces";

/** One entity id: a span named by its trace context, or an assigned serial. */
export type EntityId =
  { span: { trace_id: string; span_id: string } } | { assigned: number };

/** The request body: the subject names one root span by entity id. */
export interface InvestigateTraceRequest {
  root_span: EntityId;
}

/** Un-namespaced seconds/duration field; wall-clock nanoseconds as strings. */
export type UnixNanoString = string;

export interface SpanView {
  entity?: EntityId | null;
  span: {
    trace_id: string;
    span_id: string;
    parent_span_id?: string | null;
    name: string;
    start_time_unix_nano: UnixNanoString;
    end_time_unix_nano: UnixNanoString | null;
  };
}

/** A model `Value` as the HTTP surface renders it (arrays/maps become null). */
export type ModelScalar = string | number | boolean | null;

export interface LogView {
  entity?: EntityId | null;
  log: {
    timestamp_unix_nano: UnixNanoString | null;
    observed_timestamp_unix_nano: UnixNanoString | null;
    body?: ModelScalar;
    trace_id?: string | null;
    span_id?: string | null;
  };
}

export interface PointView {
  entity?: EntityId | null;
  point: {
    shape: string;
    time_unix_nano?: UnixNanoString;
    value?: { int: number } | { double: number };
  };
  stream: { name: string };
}

export interface Evidence {
  spans: SpanView[];
  logs: LogView[];
  points: PointView[];
}

export type SignalKind = "spans" | "log_records" | "metric_points";

export interface SignalRef {
  kind: SignalKind;
  entity: EntityId;
}

export interface EvidenceFact {
  field: string;
  value: ModelScalar;
}

export interface StrategyVersion {
  name: string;
  version: string;
}

export interface TimeWindow {
  from: UnixNanoString;
  to: UnixNanoString;
}

export type RelationType =
  | "span_identity"
  | "trace_identity"
  | "parent_child"
  | "resource_context"
  | "temporal_co_activity"
  | "exemplar_attachment"
  | "inferred";

export interface Relation {
  type: RelationType;
  from: SignalRef;
  to: SignalRef;
  facts: EvidenceFact[];
  strategy: StrategyVersion;
  window?: TimeWindow | null;
}

export interface Correlated {
  relations: Relation[];
}

export interface EffectiveRoot {
  entity: EntityId;
  name: string;
  trace_id: string;
  span_id: string;
}

export interface ResolutionNote {
  kind: string;
  entity?: EntityId;
  name?: string;
  span_id?: string;
}

export interface Subject {
  requested: { root_span: EntityId };
  effective: { root: EffectiveRoot; notes: ResolutionNote[] };
}

export interface CoverageEntry {
  kind: string;
  [field: string]: unknown;
}

export interface FlowCoverageEntry {
  kind: string;
  asked?: TimeWindow;
  resident?: TimeWindow;
  [field: string]: unknown;
}

export interface RunFacts {
  outcome: { kind: string; [field: string]: unknown };
  coverage?: CoverageEntry[];
  next_cursor?: { hex: string } | null;
}

export interface RunGroup {
  part: "spans" | "related_logs" | "surrounding_metrics";
  runs: RunFacts[];
}

export interface Execution {
  run_groups: RunGroup[];
  flow_coverage: FlowCoverageEntry[];
}

export interface Limits {
  budget: {
    deadline_ms: number;
    max_results: number;
    max_bytes: number;
    max_scan: number;
    max_aggregation_memory: number;
  };
  chain: {
    max_total_entities: number;
    max_total_pages: number;
    total_entities: number;
    total_pages: number;
    identity_examinations: number;
    stopped?: string | null;
  };
  strategy_versions: StrategyVersion[];
  eviction: { resident_records: number; total_evictions: number };
}

/** The investigation envelope: the runtime's answer, rendered verbatim. */
export interface InvestigationEnvelope {
  subject: Subject;
  execution: Execution;
  correlated: Correlated;
  evidence: Evidence;
  limits: Limits;
}

/** The HTTP surface answers refusals as JSON errors naming the reason. */
export interface InvestigationError {
  error: string;
}

/**
 * Parses the envelope body so u64 timestamps survive: any number beyond a
 * safe integer becomes its canonical decimal string (the wire renders them
 * as JSON numbers; the client keeps them exact).
 *
 * Unchecked cast: JSON.parse reviver normalises all large u64s to decimal
 * strings; the wire shape mirrors the TypeScript types exactly.
 */
function parseEnvelope(text: string): InvestigationEnvelope {
  const parsed: unknown = JSON.parse(text, (_key, candidate: unknown) => {
    if (typeof candidate === "number" && !Number.isSafeInteger(candidate)) {
      return candidate.toString();
    }
    return candidate;
  });
  return parsed as InvestigationEnvelope;
}

/** One trace investigation, straight from the running runtime. */
export async function investigateTrace(
  request: InvestigateTraceRequest,
  baseUrl: string = INVESTIGATION_BASE_URL,
): Promise<InvestigationEnvelope> {
  const response = await fetch(`${baseUrl}${INVESTIGATION_TRACES_PATH}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });

  const text = await response.text();
  if (!response.ok) {
    let reason = `HTTP ${response.status}`;
    try {
      const errorBody: unknown = JSON.parse(text);
      if (
        typeof errorBody === "object" &&
        errorBody !== null &&
        "error" in errorBody &&
        typeof errorBody.error === "string" &&
        errorBody.error.length > 0
      ) {
        reason = errorBody.error;
      }
    } catch {
      // Non-JSON refusal body: keep the status-only reason.
    }
    throw new Error(reason);
  }

  return parseEnvelope(text);
}

/** Duration in milliseconds between two unix-nano timestamps (BigInt-safe). */
export function nanoSpanMs(
  from: UnixNanoString,
  to: UnixNanoString | null,
): number {
  // The runtime renders an unfinished span's end_time_unix_nano as null
  // (crates/server/src/investigation_http.rs verbatim Option<u64>); its own
  // waterfall_extent treats null end as start (envelope.rs unwrap_or(start)),
  // so a null end is a still-open span with zero elapsed duration.
  return Number((BigInt(to ?? from) - BigInt(from)) / 1_000_000n);
}

/** A unix-nano timestamp as a millisecond clock reading (BigInt-safe). */
export function nanoToMs(unixNano: UnixNanoString): number {
  return Number(BigInt(unixNano) / 1_000_000n);
}
