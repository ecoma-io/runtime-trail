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

/** A completed span with the given lineage; ids keep the OTLP byte widths. */
function spanView(
  spanId: string,
  name: string,
  parentSpanId: string | null,
): SpanView {
  return {
    entity: {
      span: {
        trace_id: "11111111111111111111111111111111",
        span_id: spanId,
      },
    },
    span: {
      trace_id: "11111111111111111111111111111111",
      span_id: spanId,
      parent_span_id: parentSpanId,
      name,
      start_time_unix_nano: "1755200000000000000",
      end_time_unix_nano: "1755200000001000000",
    },
  };
}

const parent = spanView("aaaaaaaaaaaaaaaa", "parent", null);
const child = spanView("cccccccccccccccc", "child", "aaaaaaaaaaaaaaaa");

// A parent with an unfinished (null-end) child, the regression shape from
// issue #32.

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

  it("collapses and expands a subtree with ArrowLeft and ArrowRight", async () => {
    const wrapper = mount(TraceWaterfall, {
      props: { spans: [parent, child], selectedSpanId: null },
    });

    // Collapse (WAI-ARIA tree semantics: ArrowLeft): keydown on the row
    // (the roving tab stop), not on any nested control.
    await wrapper
      .get('[data-virtual-index="0"]')
      .trigger("keydown", { key: "ArrowLeft" });
    expect(wrapper.find('[data-virtual-index="1"]').exists()).toBe(false);
    expect(wrapper.find('button[aria-expanded="false"]').exists()).toBe(true);
    // Expand (ArrowRight): the child row reappears.
    await wrapper
      .get('[data-virtual-index="0"]')
      .trigger("keydown", { key: "ArrowRight" });
    expect(wrapper.get('[data-virtual-index="1"]').text()).toContain("child");
    expect(wrapper.find('button[aria-expanded="true"]').exists()).toBe(true);
  });

  it("emits select-span when the active row is activated", async () => {
    const wrapper = mount(TraceWaterfall, {
      props: { spans: [parent, child], selectedSpanId: null },
    });

    await wrapper
      .get('[data-virtual-index="0"]')
      .trigger("keydown", { key: "Enter" });
    expect(wrapper.emitted("select-span")).toEqual([["aaaaaaaaaaaaaaaa"]]);
  });

  it("emits select-span when a row is clicked", async () => {
    const wrapper = mount(TraceWaterfall, {
      props: { spans: [parent, child], selectedSpanId: "aaaaaaaaaaaaaaaa" },
    });

    // The child row's only button is its (chevron-less) select target.
    await wrapper
      .get('[data-virtual-index="1"]')
      .find("button")
      .trigger("click");
    expect(wrapper.emitted("select-span")).toEqual([["cccccccccccccccc"]]);
  });
});
