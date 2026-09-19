import { expect, test } from "@playwright/test";

const TRACE_ID = "11111111111111111111111111111111";
const SPAN_COUNT = 50_000;

function spanId(index: number): string {
  return index.toString(16).padStart(16, "0");
}

// One 50 000-span envelope: span 0 is the root; spans 1..100 are its
// direct children (the collapse target); spans 101..49999 are span 100's
// children, so all 49 999 remaining rows render as waterfall peers and the
// list must stay windowed. Spread across the full trace, the waterfall
// rows number exactly SPAN_COUNT.
function buildEnvelope(): string {
  const spans = Array.from({ length: SPAN_COUNT }, (_, i) => {
    const parent = i === 0 ? null : i <= 100 ? spanId(0) : spanId(100);
    return {
      entity: { span: { trace_id: TRACE_ID, span_id: spanId(i) } },
      span: {
        trace_id: TRACE_ID,
        span_id: spanId(i),
        parent_span_id: parent,
        name: `span-${i}`,
        start_time_unix_nano: "1000",
        end_time_unix_nano: "2000",
      },
    };
  });
  return JSON.stringify({
    subject: {
      requested: {
        root_span: { span: { trace_id: TRACE_ID, span_id: spanId(0) } },
      },
      effective: {
        root: {
          entity: { span: { trace_id: TRACE_ID, span_id: spanId(0) } },
          name: "span-0",
          trace_id: TRACE_ID,
          span_id: spanId(0),
        },
        notes: [],
      },
    },
    execution: { run_groups: [], flow_coverage: [] },
    correlated: { relations: [] },
    evidence: { spans, logs: [], points: [] },
    limits: {
      budget: {
        deadline_ms: 1000,
        max_results: 1000,
        max_bytes: 1000,
        max_scan: 1000,
        max_aggregation_memory: 1000,
      },
      chain: {
        max_total_entities: SPAN_COUNT,
        max_total_pages: 1,
        total_entities: SPAN_COUNT,
        total_pages: 1,
        identity_examinations: 0,
      },
      strategy_versions: [{ name: "parent-child", version: "1.0.0" }],
      eviction: { resident_records: SPAN_COUNT, total_evictions: 0 },
    },
  });
}

test("the trace waterfall stays windowed over 50 000 spans", async ({
  page,
}) => {
  await page.route("**/v1/investigations/traces", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: buildEnvelope(),
    }),
  );

  await page.goto("/");
  await page.fill("#span-id", spanId(0));
  await page.getByRole("button", { name: "Investigate", exact: true }).click();

  const waterfall = page.getByRole("list", { name: "Trace waterfall" });
  const rows = waterfall.locator("[data-virtual-index]");
  await expect(rows.first()).toHaveAttribute(
    "aria-setsize",
    String(SPAN_COUNT),
  );

  // The list is capped at 384px (max-h-96) → 12 visible + 8 overscan rows:
  // a fixed window, never the whole 50k. Index 0 is the first row.
  await expect(rows).toHaveCount(20);
  await expect(rows.first()).toHaveAttribute("aria-posinset", "1");
  // End: the active row jumps to the last one, which is revealed and
  // focused — the real Chromium focus move, not a jsdom simulation. The
  // reveal clamps scrollTop to the bottom edge (1 599 616px), where the
  // window is first = 49 988 → the final 20 rows (12 visible + 8 overscan
  // above, edge no longer has room below).
  await waterfall.locator('[data-virtual-index="0"]').focus();
  await page.keyboard.press("End");
  await expect(waterfall.locator('[aria-posinset="50000"]')).toBeFocused();
  await expect(waterfall.locator('[aria-posinset="49981"]')).toBeVisible();
  await expect(rows).toHaveCount(20);

  // Home: back to the first row.
  await page.keyboard.press("Home");
  await expect(waterfall.locator('[aria-posinset="1"]')).toBeFocused();

  // Tree semantics: ArrowLeft collapses the expanded root → its whole
  // subtree (spans 1..49999) leaves the rows, leaving exactly one;
  // ArrowRight expands it again to the full 50 000.
  await page.keyboard.press("ArrowLeft");
  await expect(rows).toHaveCount(1);
  await expect(rows.first()).toHaveAttribute("aria-setsize", "1");
  await expect(rows.first()).toContainText("span-0");
  await page.keyboard.press("ArrowRight");
  await expect(rows.first()).toHaveAttribute(
    "aria-setsize",
    String(SPAN_COUNT),
  );
  await expect(rows.nth(1)).toContainText("span-1");

  // Scrolling to the very bottom (clamped to the same 1 599 616px edge as
  // End, then re-measured) still renders only a window of 20 rows.
  await waterfall.evaluate((el) => {
    el.scrollTop = el.scrollHeight;
  });
  await expect(waterfall.locator('[data-virtual-index="49999"]')).toBeVisible();
  await expect(rows).toHaveCount(20);
});
