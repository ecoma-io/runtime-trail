// The violation, in cargo's own grammar: a `layer-storage-driver` crate
// reaching a `layer-correlation` crate through a path dependency. This file
// and line are asserted by tools/check-arch-canary.mjs.
pub use canary_core::CANARY;

pub const DRIVER: &str = "canary-driver";
