import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

const TRACE_ID = "22222222222222222222222222222222";

function spanId(index: number): string {
  return index.toString(16).padStart(16, "0");
}

// The envelopes are served by route mock, exactly like virtualization.spec.ts
// does: the UI cannot fabricate a truncation, so the fixture itself carries
// the budget-truncated run facts the runtime would report.
const SUBJECT = {
  requested: {
    root_span: { span: { trace_id: TRACE_ID, span_id: spanId(0) } },
  },
  effective: {
    root: {
      entity: { span: { trace_id: TRACE_ID, span_id: spanId(0) } },
      name: "truth-root",
      trace_id: TRACE_ID,
      span_id: spanId(0),
    },
    notes: [],
  },
};

const LIMITS = {
  budget: {
    deadline_ms: 1000,
    max_results: 1000,
    max_bytes: 1000,
    max_scan: 1000,
    max_aggregation_memory: 1000,
  },
  strategy_versions: [{ name: "span-identity", version: "1.0.0" }],
};

// A budget-truncated answer: the spans part degraded on the results budget
// (5000 records omitted), an eviction gap sits in the run coverage, the
// correlation run was cut at its relation ceiling, and the chain stopped at
// the total_pages ceiling with 3 evictions. Every honesty fact the runtime
// would report is present.
function buildDegradedEnvelope(): string {
  return JSON.stringify({
    subject: SUBJECT,
    execution: {
      run_groups: [
        {
          part: "spans",
          runs: [
            {
              outcome: {
                kind: "degraded",
                truncation: {
                  dimension: "results",
                  position: { cursor: { hex: "abcd1234ef567890" } },
                  omitted: 5000,
                },
              },
              coverage: [
                {
                  kind: "eviction_gap",
                  after: {
                    span: { trace_id: TRACE_ID, span_id: spanId(0) },
                  },
                  before: {
                    span: { trace_id: TRACE_ID, span_id: spanId(1) },
                  },
                },
              ],
              next_cursor: null,
            },
          ],
        },
      ],
      flow_coverage: [
        {
          kind: "correlation_degradation",
          at: { kind: "max_relations", value: 100 },
        },
        { kind: "temporal_strategy_skipped", reason: "no_correlation_window" },
      ],
    },
    correlated: { relations: [] },
    evidence: {
      spans: [
        {
          entity: { span: { trace_id: TRACE_ID, span_id: spanId(0) } },
          span: {
            trace_id: TRACE_ID,
            span_id: spanId(0),
            parent_span_id: null,
            name: "truth-root",
            start_time_unix_nano: "1000",
            end_time_unix_nano: "2000",
          },
        },
      ],
      logs: [],
      points: [],
    },
    limits: {
      ...LIMITS,
      chain: {
        max_total_entities: 10000,
        max_total_pages: 16,
        total_entities: 10000,
        total_pages: 16,
        identity_examinations: 50000,
        stopped: "total_pages",
      },
      eviction: { resident_records: 20, total_evictions: 3 },
    },
  });
}

// A clean answer: every part Complete, no coverage entries beyond the
// routine statements (the metric window + the named temporal skip), no
// chain stop, no evictions.
function buildCleanEnvelope(): string {
  return JSON.stringify({
    subject: SUBJECT,
    execution: {
      run_groups: [
        {
          part: "spans",
          runs: [
            { outcome: { kind: "complete" }, coverage: [], next_cursor: null },
          ],
        },
      ],
      flow_coverage: [
        {
          kind: "metric_window",
          asked: { from: "1000", to: "2000" },
          resident: { from: "1000", to: "2000" },
        },
        { kind: "temporal_strategy_skipped", reason: "no_correlation_window" },
      ],
    },
    correlated: { relations: [] },
    evidence: {
      spans: [
        {
          entity: { span: { trace_id: TRACE_ID, span_id: spanId(0) } },
          span: {
            trace_id: TRACE_ID,
            span_id: spanId(0),
            parent_span_id: null,
            name: "truth-root",
            start_time_unix_nano: "1000",
            end_time_unix_nano: "2000",
          },
        },
      ],
      logs: [],
      points: [
        {
          point: { shape: "number", time_unix_nano: "1500", value: { int: 1 } },
          stream: { name: "clean.metric" },
        },
      ],
    },
    limits: {
      ...LIMITS,
      chain: {
        max_total_entities: 10000,
        max_total_pages: 16,
        total_entities: 2,
        total_pages: 1,
        identity_examinations: 1,
      },
      eviction: { resident_records: 20, total_evictions: 0 },
    },
  });
}

async function investigate(page: Page): Promise<void> {
  await page.goto("/");
  await page.fill("#trace-id", TRACE_ID);
  await page.fill("#span-id", spanId(0));
  await page.getByRole("button", { name: "Investigate", exact: true }).click();
}

test("a budget-truncated answer names the degradation, coverage and limits", async ({
  page,
}) => {
  await page.route("**/v1/investigations/traces", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: buildDegradedEnvelope(),
    }),
  );
  await investigate(page);

  const surface = page.getByTestId("truthfulness-surface");
  await expect(surface).toBeVisible();

  // Which part degraded and why: the spans walk ran out of the results
  // budget with 5000 records omitted.
  await expect(surface).toContainText("not complete");
  await expect(surface).toContainText("spans");
  await expect(surface).toContainText("results budget ran out");
  await expect(surface).toContainText("5000 record(s) omitted");

  // The coverage list: each entry type plus what it names.
  await expect(surface).toContainText("Eviction gap");
  await expect(surface).toContainText("were evicted before examination");
  await expect(surface).toContainText("Correlation degradation");
  await expect(surface).toContainText("correlated relations cut at 100");
  await expect(surface).toContainText("Temporal strategy skipped");

  // The limits block: the chain stop and the eviction state.
  await expect(surface).toContainText(
    "chain stopped at the total_pages ceiling",
  );
  await expect(surface).toContainText("3 record(s) evicted");
});

test("a clean answer renders no degradation surface", async ({ page }) => {
  await page.route("**/v1/investigations/traces", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: buildCleanEnvelope(),
    }),
  );
  await investigate(page);

  // The routine statements (metric window, temporal skip) are not gaps:
  // the answer presents exactly as before, with no banner.
  await expect(page.getByTestId("truthfulness-surface")).toHaveCount(0);
  await expect(page.getByText("Resolved root:")).toBeVisible();
});
