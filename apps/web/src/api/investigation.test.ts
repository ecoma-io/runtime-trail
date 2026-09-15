import { describe, expect, it, vi, afterEach } from "vitest";
import { investigateTrace, nanoSpanMs, nanoToMs } from "./investigation";

// The wire renders u64 unix-nano timestamps as JSON numbers — larger than
// Number.MAX_SAFE_INTEGER — and the client must keep them exact. This
// fixture mirrors the shape crates/server/src/investigation_http.rs
// produces, timestamps as the raw JSON numbers the fetch body would carry.
const fixtureEnvelope = JSON.stringify({
  subject: {
    requested: {
      root_span: {
        span: {
          trace_id: "11111111111111111111111111111111",
          span_id: "2222222222222222",
        },
      },
    },
    effective: {
      root: {
        entity: {
          span: {
            trace_id: "11111111111111111111111111111111",
            span_id: "2222222222222222",
          },
        },
        name: "e2e.root",
        trace_id: "11111111111111111111111111111111",
        span_id: "2222222222222222",
      },
      notes: [],
    },
  },
  execution: {
    run_groups: [
      {
        part: "spans",
        runs: [
          {
            outcome: { kind: "complete" },
            coverage: [{ kind: "all_entities_scanned" }],
            next_cursor: null,
          },
        ],
      },
    ],
    flow_coverage: [],
  },
  correlated: {
    relations: [
      {
        type: "parent_child",
        from: {
          kind: "spans",
          entity: {
            span: {
              trace_id: "11111111111111111111111111111111",
              span_id: "2222222222222222",
            },
          },
        },
        to: {
          kind: "spans",
          entity: {
            span: {
              trace_id: "11111111111111111111111111111111",
              span_id: "3333333333333333",
            },
          },
        },
        facts: [{ field: "relation", value: "parent_child" }],
        strategy: { name: "span_identity", version: "1.0.0" },
        window: null,
      },
    ],
  },
  evidence: {
    spans: [
      {
        entity: {
          span: {
            trace_id: "11111111111111111111111111111111",
            span_id: "2222222222222222",
          },
        },
        span: {
          trace_id: "11111111111111111111111111111111",
          span_id: "2222222222222222",
          parent_span_id: null,
          name: "e2e.root",
          start_time_unix_nano: 1755200000000000000,
          end_time_unix_nano: 1755200000150000000,
        },
      },
    ],
    logs: [
      {
        entity: { assigned: 1 },
        log: {
          timestamp_unix_nano: 1755200000100000000,
          observed_timestamp_unix_nano: 1755200000100000000,
          body: "the seeded log body",
          trace_id: "11111111111111111111111111111111",
          span_id: "2222222222222222",
        },
      },
    ],
    points: [
      {
        entity: { assigned: 2 },
        point: {
          shape: "number",
          time_unix_nano: 1755200000100000000,
          value: { int: 7 },
        },
        stream: { name: "e2e.requests" },
      },
    ],
  },
  limits: {
    budget: {
      deadline_ms: 5000,
      max_results: 100,
      max_bytes: 1048576,
      max_scan: 100000,
      max_aggregation_memory: 33554432,
    },
    chain: {
      max_total_entities: 1000,
      max_total_pages: 16,
      total_entities: 4,
      total_pages: 1,
      identity_examinations: 4,
      stopped: null,
    },
    strategy_versions: [{ name: "span_identity", version: "1.0.0" }],
    eviction: { resident_records: 3, total_evictions: 0 },
  },
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("investigateTrace", () => {
  it("parses the envelope with large u64 timestamps kept exact as strings", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        text: () => fixtureEnvelope,
      }),
    );
    const envelope = await investigateTrace({
      root_span: {
        span: {
          trace_id: "11111111111111111111111111111111",
          span_id: "2222222222222222",
        },
      },
    });

    expect(envelope.subject.effective.root.name).toBe("e2e.root");
    // Fixture declares one span; the array is non-empty by construction.
    expect(envelope.evidence.spans[0]!.span.start_time_unix_nano).toBe(
      "1755200000000000000",
    );
    // 150ms between the root span's start and end, computed exactly.
    expect(nanoSpanMs("1755200000000000000", "1755200000150000000")).toBe(150);
    expect(envelope.evidence.logs[0]!.log.body).toBe("the seeded log body");
    expect(envelope.correlated.relations[0]!.strategy.name).toBe(
      "span_identity",
    );
    expect(envelope.limits.budget.deadline_ms).toBe(5000);
  });

  it("surfaces the runtime's named error reason on refusal", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
        text: () => JSON.stringify({ error: "unknown entity" }),
      }),
    );

    await expect(
      investigateTrace({
        root_span: {
          span: {
            trace_id: "00000000000000000000000000000000",
            span_id: "0000000000000000",
          },
        },
      }),
    ).rejects.toThrow("unknown entity");
  });

  it("converts a unix-nano timestamp to milliseconds", () => {
    expect(nanoToMs("1755200000000000000")).toBe(1755200000000);
  });
});
