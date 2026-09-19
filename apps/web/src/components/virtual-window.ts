/**
 * The pure windowing geometry behind the app-local `VirtualList`: which row
 * range a scroll viewport must render. The maths are the load-bearing fact —
 * a trace of ten thousand spans is only cheap if the rendered window is — so
 * they live here, outside the component, where the unit tests can pin them
 * without stubbing a single DOM dimension.
 *
 * This is the stopgap companion to `VirtualList.vue`: the same numeric
 * contract as `@ecoma-io/loom`'s `virtualWindow` so the swap to the real
 * component (loom issue #406, shipped in `@ecoma-io/loom@0.6.0`) is an
 * import change and nothing else.
 */

export interface VirtualWindow {
  /** First rendered row index, inclusive. */
  start: number;
  /** One past the last rendered row index, exclusive. */
  end: number;
}

/**
 * The rows whose top edge falls inside `[scrollTop, scrollTop + viewportHeight)`
 * plus `overscan` rows above and below — the buffer that keeps a fast scroll
 * from painting empty space while the browser catches up.
 *
 * Degenerate inputs degrade to an empty window rather than a nonsense one:
 * no items, a zero viewport (jsdom, a `display: none` parent at measure
 * time) or a non-positive item height render nothing. A scroll position past
 * the last row is clamped to it — the browser clamps `scrollTop` itself, and
 * jsdom does not.
 */
export function virtualWindow(
  scrollTop: number,
  viewportHeight: number,
  itemHeight: number,
  count: number,
  overscan: number,
): VirtualWindow {
  if (count <= 0 || itemHeight <= 0) return { start: 0, end: 0 };
  const first = Math.min(
    Math.max(0, Math.floor(Math.max(0, scrollTop) / itemHeight)),
    count - 1,
  );
  const visible = Math.max(
    1,
    Math.ceil(Math.max(0, viewportHeight) / itemHeight),
  );
  return {
    start: Math.max(0, first - overscan),
    end: Math.min(count, first + visible + overscan),
  };
}
