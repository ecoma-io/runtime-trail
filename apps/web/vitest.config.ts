import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [vue()],
  test: {
    environment: "jsdom",
    setupFiles: ["src/test-setup.ts"],
    // Unit tests live under src; the e2e specs belong to Playwright.
    include: ["src/**/*.{test,spec}.?(c|m)[jt]s?(x)"],
  },
});
