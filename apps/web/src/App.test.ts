import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import App from "./App.vue";

describe("App", () => {
  it("states the bootstrap status instead of promising product features", () => {
    const wrapper = mount(App);

    expect(wrapper.text()).toContain("Runtime Trail");
    expect(wrapper.text()).toContain("no telemetry is ingested");
  });
});
