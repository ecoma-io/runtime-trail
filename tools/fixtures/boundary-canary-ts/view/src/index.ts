// The violation, in the web app's own grammar: a `layer-view` module opening
// a compile-time door straight into `layer-storage`. This file and line are
// asserted by tools/check-arch-canary.mjs.
import { store } from "../../storage/src/index";

export const view = `view over ${store}`;
