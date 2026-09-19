import { describe, expect, it } from "vitest";

import { virtualWindow } from "./virtual-window";

describe("virtualWindow", () => {
  it.each([
    // (scrollTop, viewportHeight, itemHeight, count, overscan) → window
    // The pinned window contract App-local VirtualList is stopgapping for
    // (loom issue #406): the numbers the swap depends on must never drift.
    [0, 400, 32, 500, 8, { start: 0, end: 21 }],
    [3200, 400, 32, 500, 8, { start: 92, end: 121 }],
    // A scroll position past the last row clamps to it (jsdom does not
    // clamp scrollTop itself; a real browser never sends this).
    [1_000_000, 400, 32, 500, 8, { start: 491, end: 500 }],
    // A zero viewport still renders the first row plus overscan, so a list
    // measured inside a display:none parent paints something once shown.
    [0, 0, 32, 500, 8, { start: 0, end: 9 }],
    [3200, 400, 32, 500, 0, { start: 100, end: 113 }],
  ])(
    "window(%i, %i, %i, %i, %i) → %j",
    (scrollTop, viewportHeight, itemHeight, count, overscan, expected) => {
      expect(
        virtualWindow(scrollTop, viewportHeight, itemHeight, count, overscan),
      ).toEqual(expected);
    },
  );

  it("degrades to an empty window for degenerate inputs", () => {
    expect(virtualWindow(0, 400, 0, 500, 8)).toEqual({ start: 0, end: 0 });
    expect(virtualWindow(0, 400, 32, 0, 8)).toEqual({ start: 0, end: 0 });
    expect(virtualWindow(-100, -400, 0, 0, 8)).toEqual({ start: 0, end: 0 });
  });
});
