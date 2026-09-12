//! Opaque, fingerprint-bound cursors.
//!
//! A cursor encodes (position in the total order, last entity id, query
//! fingerprint), is opaque to callers, and is rejected when presented
//! under a different query (invariant 3). A cursor continues within the
//! snapshot its first page evaluated; records admitted after it are
//! outside every later page - a declared boundary in coverage, never a
//! silent skip. Gaps under eviction are named in coverage, never silent.
