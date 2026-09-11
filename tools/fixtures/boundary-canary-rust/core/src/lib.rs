// The correlation-engine stand-in: the layer the driver below must never
// reach. Never make this importable from `driver` in a legal way — the
// fixture exists to fail.
pub const CANARY: &str = "canary-core";
