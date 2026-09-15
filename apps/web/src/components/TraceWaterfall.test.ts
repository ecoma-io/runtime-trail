import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import TraceWaterfall from "./TraceWaterfall.vue";
import type { SpanView } from "../api/investigation";

// The runtime renders an unfinished span's end_time_unix_nano as JSON null
// (OTLP end_time_unix_nano == 0 → None → verbatim null); issue #32. The
// waterfall must render it instead of crashing on BigInt(null).
const unfinishedSpan: SpanView = {
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
    name: "still-open",
    start_time_unix_nano: "1755200000000000000",
    end_time_unix_nano: null,
  },
};

describe("TraceWaterfall", () => {
  it("renders a trace containing an unfinished (null-end) span", () => {
    const wrapper = mount(TraceWaterfall, {
      props: {
        spans: [unfinishedSpan],
        selectedSpanId: null,
      },
    });

    expect(wrapper.text()).toContain("still-open");
    // A still-open span shows 0ms elapsed — no TypeError from BigInt(null).
    expect(wrapper.text()).toContain("0");
  });
});
