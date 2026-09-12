//! Total deterministic ordering over results.
//!
//! Every result is totally ordered, deterministically across identical
//! queries; every tie in the order keys is broken by the record's entity
//! id ([query-model.md](../../docs/architecture/query-model.md)). Without
//! a total order there is no stable pagination.
