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
});
