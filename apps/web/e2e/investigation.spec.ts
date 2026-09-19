import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";
import path from "node:path";

import { decodeTraces } from "./otlp-ids";

const API = "http://127.0.0.1:8599";
// Playwright runs specs with cwd = the config directory (apps/web).
const FIXTURES = path.resolve("../../crates/server/tests/fixtures");

// The runtime does not replay anything: the spec itself ingests the real
// fixture bytes over the OTLP HTTP surface the server binds by default.
test.beforeAll(async ({ request }) => {
  const ingest: [file: string, route: string][] = [
    ["otlp-traces.bin", "/v1/traces"],
    ["otlp-logs.bin", "/v1/logs"],
    ["otlp-metrics.bin", "/v1/metrics"],
  ];
  for (const [file, route] of ingest) {
    const response = await request.post(`${API}${route}`, {
      data: readFileSync(path.join(FIXTURES, file)),
      headers: { "Content-Type": "application/x-protobuf" },
    });
    expect(response.ok(), `ingest ${file} → ${route}`).toBe(true);
  }
});

test("ingest → investigate → select: the full real flow over the API", async ({
  page,
}) => {
  const fixture = readFileSync(path.join(FIXTURES, "otlp-traces.bin"));
  const spans = decodeTraces(fixture);
  const root = spans.find((span) => span.parentSpanId === null);
  expect(root, "the traces fixture has a root span").toBeDefined();

  await page.goto("/");
  await page.fill("#trace-id", root?.traceId ?? "");
  await page.fill("#span-id", root?.spanId ?? "");
  const navigations = await page.evaluate(
    () => performance.getEntriesByType("navigation").length,
  );

  await page.getByRole("button", { name: "Investigate", exact: true }).click();

  // The subject header resolves the root by name.
  await expect(page.getByText("Resolved root:")).toBeVisible();

  // Waterfall: both fixture spans, rendered as windowed rows.
  const waterfall = page.getByRole("list", { name: "Trace waterfall" });
  await expect(waterfall).toContainText("e2e.root");
  await expect(waterfall).toContainText("e2e.child");

  // Related logs: the fixture body and its span column.
  const related = page.getByRole("list", { name: "Related logs" });
  await expect(related).toContainText("the e2e log body");

  // Metrics: the fixture series and its count.
  const metrics = page.getByRole("table", {
    name: "Surrounding metric points",
  });
  const requestRow = metrics
    .getByRole("row")
    .filter({ hasText: "e2e.requests" });
  await expect(
    requestRow.getByRole("cell", { name: "7", exact: true }),
  ).toBeVisible();

  // Correlated relations: the runtime's own set for this trace.
  const relations = page.getByRole("table", { name: "Correlated relations" });
  await expect(relations.locator("tr").nth(1)).toBeVisible();

  // Pointer path: click the child span's row button (the chevron also
  // matches the name, so `.last()` picks the select button) → selected.
  const childButton = waterfall
    .locator('[data-virtual-index="1"]')
    .getByRole("button", { name: /e2e\.child/ })
    .last();
  await childButton.click();
  await expect(childButton).toHaveAttribute("aria-current", "true");

  // Keyboard path: focus the first row, Enter selects its span.
  await waterfall.locator('[data-virtual-index="0"]').focus();
  await page.keyboard.press("Enter");
  const rootButton = waterfall
    .locator('[data-virtual-index="0"]')
    .getByRole("button", { name: /e2e\.root/ })
    .last();
  await expect(rootButton).toHaveAttribute("aria-current", "true");

  // All of it happened over the API: the page never navigated again.
  const after = await page.evaluate(
    () => performance.getEntriesByType("navigation").length,
  );
  expect(after).toBe(navigations);
});
