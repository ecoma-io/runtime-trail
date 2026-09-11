// The storage stand-in: the layer the view above must never import. Never
// re-export this from `view` in a legal way — the fixture exists to fail.
export const store = "canary-storage";
