import { defineConfig } from "@playwright/test";

// Playwright end-to-end gates for the web surface. One shared binary serves
// both the built app and the Investigation API it calls — same origin, no
// CORS in play. `investigation.spec.ts` POSTs the real OTLP fixture bytes
// into that server and drives the whole flow; `virtualization.spec.ts`
// fulfills the investigation route inside the page with a 50 000-span
// envelope so the windowing contract is exercised against real Chromium.
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  use: {
    baseURL: "http://127.0.0.1:8599",
    trace: "on-first-retry",
  },
  webServer: {
    // Built by `moon run web:build server:build`; the dist dir is what the
    // server falls back to for `/` (Single Page App serving).
    command:
      "../../target/debug/runtime-trail-server --bind 127.0.0.1:8599 --web-dist ../../apps/web/dist",
    url: "http://127.0.0.1:8599/healthz",
    reuseExistingServer: true,
    timeout: 60_000,
  },
  projects: [{ name: "chromium", use: { browserName: "chromium" } }],
});
