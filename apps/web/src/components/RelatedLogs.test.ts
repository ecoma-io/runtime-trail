import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import RelatedLogs from "./RelatedLogs.vue";
import type { LogView, ModelScalar } from "../api/investigation";
// Issue #39: structured model values (arrays, key-value lists) render as
// real JSON arrays/objects over the HTTP surface, and an absent body stays
// distinct from a structured-but-empty one ([] / {}).
function logView(body: ModelScalar, seen: number): LogView {
  return {
    entity: { assigned: seen },
    log: {
      timestamp_unix_nano: String(seen),
      observed_timestamp_unix_nano: String(seen),
      body,
      trace_id: null,
      span_id: null,
    },
  };
}

describe("RelatedLogs", () => {
  it("renders structured bodies as JSON and absent bodies as blank", () => {
    const wrapper = mount(RelatedLogs, {
      props: {
        logs: [
          logView(["alpha", "beta"], 1),
          logView({ name: "e2e", attempts: 3 }, 2),
          logView([], 3),
          logView({}, 4),
          logView(null, 5),
        ],
        selectedLogSpanId: null,
      },
    });

    const text = wrapper.text();
    expect(text).toContain('["alpha","beta"]');
    expect(text).toContain('{"name":"e2e","attempts":3}');
    // Structured-but-empty renders as itself — never blank, never "null".
    expect(text).toContain("[]");
    expect(text).toContain("{}");
    // Absent renders as empty text, distinguishable from "[]" and "{}".
    expect(text).not.toContain("null");
  });

  it("emits select-log when the active row is activated with Enter", async () => {
    const wrapper = mount(RelatedLogs, {
      props: {
        logs: [logView("first", 1), logView("second", 2)],
        selectedLogSpanId: null,
      },
    });

    // Enter on the row (the roving tab stop) activates the first row, whose
    // detached log carries no span — select-log(null), the "—" case.
    await wrapper
      .get('[data-virtual-index="0"]')
      .trigger("keydown", { key: "Enter" });
    expect(wrapper.emitted("select-log")).toEqual([[null]]);
  });
  it("emits select-log with the span id when a row is clicked", async () => {
    const withSpan = logView("attached", 2);
    withSpan.log.span_id = "bbbbbbbbbbbbbbbb";
    const wrapper = mount(RelatedLogs, {
      props: {
        logs: [logView("first", 1), withSpan],
        selectedLogSpanId: "bbbbbbbbbbbbbbbb",
      },
    });

    // Timestamp order: "1" then "2" — the second row is the attached log.
    await wrapper.findAll("button")[1]!.trigger("click");
    expect(wrapper.emitted("select-log")).toEqual([["bbbbbbbbbbbbbbbb"]]);
  });
});
