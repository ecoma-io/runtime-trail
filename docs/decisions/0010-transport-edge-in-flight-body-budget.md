# 0010 — The transport edge bounds aggregate in-flight request bodies

- **Status:** Accepted
- **Date:** 2026-09-13
- **Part of:** Phase 1 — Telemetry ingestion (backpressure hardening)

## Context

The runtime's resource law
([runtime-constraints.md](../architecture/runtime-constraints.md)) commits to
"signal, never buffer-without-bound" and "never unbounded"; the overloaded
steady-state RSS stays _bounded, not fixed_. Those commitments hold
everywhere _after_ admission — the queue ceiling, the retention ceilings, the
per-request payload ceiling — but nothing bounded the transport edge _before_
admission.

The finding (Swept Blocker #1): `crates/server/src/lib.rs` serves with
`axum::serve(listener, router)` and no connection cap, no concurrency limit
and no request timeout. Each in-flight HTTP request fully buffers its body
before admission: `crates/server/src/otlp_http.rs` reads
`to_bytes(request.into_body(), ceiling_bytes)` up to
[`OTLP_PAYLOAD_BYTES`](https://github.com/ecoma-io/runtime-trail/blob/main/crates/telemetry-model/src/budgets.rs)
(4 MiB, `crates/telemetry-model/src/budgets.rs:52`); the gRPC frame reader
decodes up to 4 MiB plus 8 KiB slack
(`crates/server/src/runtime.rs`, `GRPC_DECODING_SLACK_BYTES`). There is no
_aggregate_ in-flight-body budget and no time bound: a slow-drip client holds
~4 MiB indefinitely per connection, N concurrent slow connections grow RSS by
~4 MiB × N, and the process can OOM. The 64 MiB queue ceiling and the storage
retention ceilings engage only at/after admission — the transport-edge
buffering is never counted. This violates
[runtime-constraints.md](../architecture/runtime-constraints.md) ("signal,
never buffer-without-bound" under `Ingestion under overload`; "never
unbounded" under `Overload steady-state RSS`).

A local-trust posture (default bind `127.0.0.1`) reduces — but does not
remove — the exposure: a local misbehaving process, a local agent, or a stray
socket can still drive the same growth, and an operator may bind `0.0.0.0`.
The bound must hold regardless of bind address.

## Existing controls (and their edge)

The runtime already bounds one body in bytes and the queue in accounted bytes:

- `payload_ceiling_bytes` (default `OTLP_PAYLOAD_BYTES` = 4 MiB): the number
  `to_bytes` reads up to. It is a _per-request_ bound; it says nothing about
  how many bodies buffer at once.
- `queue_ceiling_bytes` (default `QUEUE_CEILING_BYTES` = 64 MiB): the bounded
  hand-off queue's accounted ceiling. It engages _after_ the body is read.
- `DefaultBodyLimit::max(payload_ceiling)` and the handler's
  `declared_over_ceiling` / `LengthLimitError` checks: per-request, again.

None of them counts _in-flight request-body bytes being buffered_.

## Decision

**The transport edge must bound aggregate in-flight request-body memory and
refuse admission promptly when that bound is exceeded; a per-request read
timeout bounds a stuck or slow-drip client so a single body cannot hold memory
indefinitely.**

1. **An aggregate in-flight body budget**, shared by both transports, that
   counts request bodies being buffered at the transport edge (before
   admission) and refuses new buffering once the aggregate exceeds a named,
   startup-configurable bound. The default is derived from the queue ceiling:
   **64 MiB aggregate in-flight body bytes** — one queue ceiling's worth, the
   same order as the "In-flight per queue" row in
   [runtime-constraints.md](../architecture/runtime-constraints.md). Naming it
   as a row in that table (added in the same change) keeps the "ownership law"
   — one home for the number, derived, not a magic constant. Per request the
   charge is the _declared_ `Content-Length` when it is honest (present and at
   or under the payload ceiling), else the payload ceiling — the worst case the
   bounded read can buffer.
2. **Refuse with the retryable signal.** Aggregate overflow is transient
   overload (bodies complete and release), so it answers the same way the
   saturated-queue signal does: **HTTP 429 + `Retry-After`** for OTLP/HTTP,
   **gRPC `RESOURCE_EXHAUSTED`** for OTLP/gRPC — each naming the budget. This
   makes the existing "the only retryable admission signal" statement about
   queue saturation a narrowing, not a definition: every transient overload
   (queue saturation _or_ transport-edge body overload) is retryable and
   retryable answers are exactly these two wire shapes. The runtime-constraints
   signal list is amended to say so.
3. **A per-request read timeout** bounds a single body's buffering window. A
   stuck or slow-drip client holds its body (and its charge) for at most the
   timeout. The seam is the body read itself — the handler's `to_bytes` is
   wrapped in `tokio::time::timeout`, and the gRPC unary is wrapped the same
   way — because a route-level `tower::timeout` would change the service error
   type to `BoxError` (axum/hyper then cannot send the honest answer and
   instead drops the connection) and would time the whole handler (admission
   included), not just the read. Default: **10 seconds** — generous next to a
   local SDK's wire time (a localhost batch completes in milliseconds), so a
   healthy client almost never sees it, yet it bounds a drip tightly. On
   expiry: **HTTP 408 Request Timeout** naming the bound; **gRPC
   `DEADLINE_EXCEEDED`**.
4. **Graceful drain is untouched.** The timeout is scoped to a request future,
   never to the drain state. Drain is signalled on the pipeline
   (`begin_drain`) and the pump's work is bounded by `DRAIN_DEADLINE`; a
   timed-out in-flight request completes (and releases its charge) and then
   the graceful-shutdown wait for in-flight requests proceeds. A request that
   arrives _during_ drain is refused 503/`UNAVAILABLE` by the existing closing
   gate before its body is read, so it never enters a body-read timeout.
5. **The bound holds for every bind address.** The budget is on the runtime
   graph, not the listener; `0.0.0.0` shares the same gate as loopback.
6. **A connection cap at the accept seam.** `RuntimeConfig::max_connections`
   (default **256**) bounds concurrently accepted sockets. Each accepted
   connection holds an owned semaphore permit for exactly as long as its
   socket lives, and a connection is accepted only once a permit is
   available — so when the cap is exhausted `accept` pends and further
   sockets queue in the kernel backlog, unserved, until a slot frees. The
   cap therefore bounds every connection-shaped resource (file descriptors,
   per-connection tasks, buffered request heads) no matter how many sockets
   the OS admits, while the in-flight body budget (item 1) bounds the bytes
   those sockets can buffer.

### Mechanism (per transport)

- **OTLP/HTTP:** an axum middleware
  (`middleware::from_fn`, applied in `build_router` over the OTLP routes,
  before the body is read) acquires the charge; if the aggregate is full it
  answers 429 + `Retry-After` naming the budget, without touching the body.
  Non-OTLP surfaces (`/healthz`, `/version`, the UI fallback) never buffer a
  request body and pass through. Requests refused by an earlier honest gate —
  draining (503), content-type (415, the same gate the handler runs), or
  declared-over-ceiling (413) — also pass through so the already-contracted
  answer — never the budget's 429 — is the one on the wire.
- **OTLP/gRPC:** tonic's frame reader bounds one body to
  `payload_ceiling + GRPC_DECODING_SLACK_BYTES` via
  `max_decoding_message_size`, but nothing bounds how many frames buffer
  concurrently. The same aggregate is acquired (charging
  `grpc_decoding_ceiling_bytes`, the honest worst case per frame) around the
  gRPC unary, and the unary is wrapped in the same read timeout. A refused
  charge answers `RESOURCE_EXHAUSTED`.

## Consequences

- **Bounded transport-edge memory by construction:** at most 64 MiB of request
  bodies buffering at once (the named, startup-configurable budget), and each
  body held for at most 10 s. Combined with the queue ceiling and retention
  ceilings, every memory domain of an overloaded session is bounded.
- **Honest refusal ordering preserved:** the existing gates (draining 503 →
  content-type 415 → declared-over-ceiling 413 → bounded read) keep their
  precedence; the aggregate answers only where no earlier gate owns the
  refusal. The middleware runs the same three gates in the same order the
  handler answers them, so the aggregate's 429 is never dressed over a
  request an earlier gate already refused — the wire answer does not depend
  on whether the budget happens to be hot.
- **Behavior change, rarely hit:** a local SDK that sends a full body quickly
  sees no change; the 429/`RESOURCE_EXHAUSTED` and 408/`DEADLINE_EXCEEDED`
  answers are new refuse-shapes for the misbehaving cases the finding names.
- **Numbers are named, not magic:** `inflight_body_ceiling_bytes` defaults to
  one queue ceiling (64 MiB) and `body_read_timeout` to 10 s, both
  startup-configurable and both rows in
  [runtime-constraints.md](../architecture/runtime-constraints.md).
- **The connection cap is an orthogonal sixth bound:** in-flight body bytes
  are capped by charge (item 1); concurrently served sockets are capped by
  `max_connections` — a scale the admission gates never saw. The default 256
  keeps existing behavior for realistic SDK fan-out, and the kernel backlog
  absorbs overflow without dropping the sockets.
- The budget is shared between HTTP and gRPC — one aggregate for the whole
  transport edge, not two independently unbounded pools.
