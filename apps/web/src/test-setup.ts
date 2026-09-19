// Browser APIs Loom relies on that jsdom does not implement. Keep this to
// the smallest stub that answers — each entry must name the component that
// needs it.

// `useTheme` reads `window.matchMedia` for the system color scheme.
function stubMatchMedia(query: string): MediaQueryList {
  return {
    matches: false,
    media: query,
    onchange: null,
    addListener: () => undefined,
    removeListener: () => undefined,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    dispatchEvent: () => false,
  };
}

window.matchMedia = (query) => stubMatchMedia(query);

// Loom's `Table` (used by RelatedLogs and CorrelatedRelations) observes
// its cells' size in a post-flush hook.
class StubResizeObserver implements ResizeObserver {
  // The real observer reports layout size changes the tests never drive.
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  observe(): void {}
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  unobserve(): void {}
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  disconnect(): void {}
}

window.ResizeObserver = StubResizeObserver;

export {};
