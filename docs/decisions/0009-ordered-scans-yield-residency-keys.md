# 0009 — Ordered scans yield each record with its residency key

- **Status:** Accepted
- **Date:** 2026-09-13
- **Part of:** Phase 2 — Investigation runtime (M2, Query Engine)

## Context

The Query Engine's contract
([query-model.md](../architecture/query-model.md)) needs, for every record it
examines: its **admission time** (the records flow's order key and the
cursor's resume anchor) and its **entity id** (the pagination tie-break, the
cursor's last entity, and every coverage name — an eviction gap names entity
ids, the snapshot boundary is an admission key). The M2 engine design
conditions its cursor, ordering and coverage machinery on both facts being
retrievable per record.

The storage contract's retrieval surface did not hand either fact over. A
scan walks the residency order internally — the driver's records live in a
map keyed by `AdmissionKey` — but yielded bare records, discarding the key at
the seam; `ScanPage.cursor` carried only the batch tail's key, and only when
the page was full with a successor behind it. Log records and metric points
carry no identity on the record itself (an admission-assigned id exists only
in the driver's entity index), and admission time lives only on the
write-path `Admitted` hand-off. An engine on that surface cannot anchor a
cursor, break a tie, or name a coverage gap. The honest candidates were:
scan with `limit = 1` so every record happens to be paired with its own
cursor — one driver call per record on the retrieval path — or change the
contract. The wave-2 implementer stopped at the design gate rather than
improvise either; the finding was verified against the code before this
decision: the key exists in the scan iterator and is dropped exactly where
the page is built.

## Decision

**Scans yield keyed items.** `ScanPage<T>.items` becomes
`Vec<ScanItem<T>>` with `ScanItem { key: AdmissionKey, record: T }` — each
record carries the residency-order position it is yielded at.
`ScanPage.cursor` keeps its meaning: the last item's key, set only when a
record follows the page.

Zero new information flows: the key is the same one the walk already orders
by; the seam stops discarding it. The scan stays a location primitive — a
residency key is a location fact, not content filtering — so the contract's
"storage stays storage" rule is untouched, and no driver grows a second door.

## Consequences

- The engine anchors cursors at the last examined record's key, breaks ties
  by `key.entity()`, and names eviction gaps and snapshot boundaries with
  contract-carried facts — the cursor's resume anchor is exact under
  eviction, with no re-walk and no per-record workaround.
- **`limit = 1` lockstep rejected**: one driver call per record is exactly
  the retrieval-path overhead the scan primitive's batching exists to avoid.
- Consumers at the time of the change: the contract crate and its one in-memory
  driver. No server, bench or query code had compiled against the bare-item
  shape, so the shape change breaks nothing outside the two crates.
- The per-item cost is one small `Copy` key beside the record the caller
  already receives, bounded by the page limit the caller chooses. It is a
  scan-path shape, never a retention term: the scan allocates nothing the
  caller does not already walk.
