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

export {};
