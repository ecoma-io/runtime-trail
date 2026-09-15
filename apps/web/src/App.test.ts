import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import App from "./App.vue";

describe("App", () => {
  it("states the investigation surface status truthfully", () => {
    const wrapper = mount(App);

    expect(wrapper.text()).toContain("Runtime Trail");
    expect(wrapper.text()).toContain("telemetry ingestion is live");
    expect(wrapper.text()).toContain("trace waterfall");
    expect(wrapper.text()).toContain("related logs");
    expect(wrapper.text()).toContain("surrounding metrics window");
    expect(wrapper.text()).toContain("correlated relations");
  });
});
