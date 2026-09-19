import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import CorrelatedRelations from "./CorrelatedRelations.vue";
import type { Relation } from "../api/investigation";

// Issue #39: fact values derived from structured model values render as
// JSON arrays/objects on the wire; a fact with no value renders
// `field=null` and stays distinguishable from an empty array/object.
const relation: Relation = {
  type: "span_identity",
  from: {
    kind: "log_records",
    entity: { assigned: 7 },
  },
  to: {
    kind: "spans",
    entity: {
      span: {
        trace_id: "11111111111111111111111111111111",
        span_id: "2222222222222222",
      },
    },
  },
  facts: [
    { field: "array_fact", value: ["a", "b"] },
    { field: "object_fact", value: { attempts: 3 } },
    { field: "absent_fact", value: null },
    { field: "scalar_fact", value: "plain" },
  ],
  strategy: { name: "span_identity", version: "1.0.0" },
  window: null,
};

describe("CorrelatedRelations", () => {
  it("renders structured fact values as JSON, distinct from absent", () => {
    const wrapper = mount(CorrelatedRelations, {
      props: { relations: [relation] },
    });

    const text = wrapper.text();
    expect(text).toContain('array_fact=["a","b"]');
    expect(text).toContain('object_fact={"attempts":3}');
    expect(text).toContain("absent_fact=null");
    expect(text).toContain('scalar_fact="plain"');
  });
});
