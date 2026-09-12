//! The bounded hand-off queue: admission's output port, bounded in
//! accounted bytes.
//!
//! `docs/architecture/runtime-constraints.md`, "The backpressure
//! architecture", rule 2: every hand-off runs through a queue bounded in
//! accounted bytes with a defined overflow policy — overflow **rejects the
//! producer**, the one transient, retryable admission signal. Drop-oldest
//! exists only in retention eviction, never in queues; nothing here buffers
//! without a bound.
//!
//! # The ceiling
//!
//! The default ceiling is the contract number — **64 MiB accounted** per
//! queue ("In-flight per queue", runtime-constraints.md) — and like every
//! number in that table it is startup-configurable, never mid-session
//! state: the ceiling is fixed when the queue is built. Accounting is the
//! model's [accounted size](runtime_trail_telemetry_model::Accounted), so
//! the ceiling means the same thing here it will mean in every storage
//! mode. A legal record always fits an empty default queue: the admission
//! budgets bound any one record far below 64 MiB.
//!
//! # Threading
//!
//! [`BoundedQueue::try_push`] never blocks: admission runs on request paths
//! and must not wait on a consumer (rule 4, "persistence never blocks
//! admission" — restated in runtime-constraints.md). [`BoundedQueue::pop`]
//! and [`BoundedQueue::pop_timeout`] block for the consumer — the pump the
//! server (wave 3) runs into storage, under the drain deadline. std
//! synchronisation only: tokio lives in `crates/server` and nowhere else,
//! and a non-blocking `try_push` is exactly the shape an async caller wants.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use runtime_trail_telemetry_model::{
    Accounted, AdmissionTime, EntityId, LogRecord, MetricPoint, Span, StreamIdentity,
};

use crate::signal::AdmissionSignal;

/// The default accounted-byte ceiling of a bounded hand-off queue: the
/// contract number from `docs/architecture/runtime-constraints.md`,
/// "Numeric limits (targets)" — "In-flight per queue: ≤ 64 MiB accounted".
/// Startup-configurable: an operator tunes it when the queue is built,
/// never mid-session. Changing this default is an architecture change.
pub const QUEUE_CEILING_BYTES: usize = 64 * 1024 * 1024;

/// The name the pipeline's hand-off queue is known by — the name
/// [`AdmissionSignal::QueueSaturated`] carries.
pub const PIPELINE_QUEUE_NAME: &str = "ingestion";

/// One admitted record as the queue carries it: the record, the entity id
/// admission assigned it, and the runtime's admission time.
///
/// Only **newly admitted** records are queued. A collapsed re-delivery is
/// the record already admitted — queueing it again would store a second
/// copy of it — and a conflict left nothing to store by definition.
#[derive(Clone, Debug)]
pub struct QueuedRecord {
    /// The entity id admission assigned the record.
    pub entity: EntityId,
    /// When the runtime admitted it — separate from every emitter timestamp.
    pub admitted_at: AdmissionTime,
    /// The record, exactly as admitted, shared — never copied.
    pub record: StoredRecord,
}

/// The model record a queued [`QueuedRecord`] carries.
///
/// Every variant holds the model's own `Arc` handles (the ledger's for
/// spans and points; admission's for log records, which the ledger keeps no
/// entry for). Queueing costs a reference count, never a payload copy.
#[derive(Clone, Debug)]
pub enum StoredRecord {
    /// A span, shared with the admission ledger.
    Span(Arc<Span>),
    /// A log record; log records have no natural identity, so the ledger
    /// holds no second handle.
    Log(Arc<LogRecord>),
    /// A metric data point and the interned stream identity it belongs to,
    /// both shared with the ledger — one stream payload for all of a
    /// stream's points.
    Point {
        /// The stream the point belongs to.
        stream: Arc<StreamIdentity>,
        /// The point itself.
        point: Arc<MetricPoint>,
    },
}

/// The accounted size of a queued record: the model's accounted size of
/// everything the record references, charged **in full wherever it
/// appears** — the model's rule for `Arc`-shared payloads
/// (`telemetry-model.md`, "Information budgets"). Sharing is a heap-cost
/// optimisation, never an accounting event: the same record queued twice
/// (two log records are never collapsed, so this really happens) counts
/// twice, and a point charges its stream identity in full. Over-counting is
/// the honest direction for a byte ceiling.
impl Accounted for StoredRecord {
    fn accounted_size(&self) -> usize {
        match self {
            Self::Span(span) => span.accounted_size(),
            Self::Log(record) => record.accounted_size(),
            Self::Point { stream, point } => {
                // The identity's whole accounted content — resource, scope,
                // name and descriptor (description, unit, metadata) — rides
                // with the point: one formula, the model's own
                // `StreamIdentity::accounted_size`.
                point.accounted_size() + stream.accounted_size()
            }
        }
    }
}

/// The output port of admission: where admitted records are handed off.
///
/// The only implementation today is [`BoundedQueue`] — the accounted-byte
/// bounded hand-off the backpressure architecture requires. Wave 3 wires
/// storage behind the *consumer* side of that queue (a bounded pump under
/// the drain deadline, in `crates/server`); `crates/storage`'s real ingest
/// trait had not landed when this crate did, so admission depends on this
/// port instead of on any storage shape — the dependency law
/// (`layer-ingest → model, storage`) is honoured by construction, and the
/// port stays unchanged when storage arrives.
pub trait RecordSink: Send + Sync + 'static {
    /// The accounted-byte ceiling this sink enforces on what it holds.
    ///
    /// [`Pipeline::with_config`](crate::pipeline::Pipeline::with_config)
    /// checks the worst-case legal record against it at construction, so
    /// a ceiling below one legal record fails at startup instead of
    /// stranding records at runtime behind the retryable saturation
    /// signal. A sink that buffers without a ceiling returns
    /// [`usize::MAX`].
    fn ceiling_bytes(&self) -> usize;

    /// Hands one admitted record to the next stage. Never blocks: a full
    /// sink refuses the producer with
    /// [`AdmissionSignal::QueueSaturated`] instead of buffering without a
    /// bound or dropping.
    ///
    /// # Errors
    ///
    /// `QueueSaturated` when the sink is at its accounted-byte ceiling —
    /// the one retryable admission signal.
    fn offer(&self, record: QueuedRecord) -> Result<(), AdmissionSignal>;
}

struct QueueInner {
    items: VecDeque<QueuedRecord>,
    accounted: usize,
}

/// A bounded hand-off queue, bounded in accounted bytes.
///
/// Overflow rejects the producer ([`BoundedQueue::try_push`] →
/// [`AdmissionSignal::QueueSaturated`]); nothing is ever dropped here, and
/// no record is buffered outside the ceiling. Memory ordering is a plain
/// mutex — the queue is not the hot path's bottleneck, honesty is.
pub struct BoundedQueue {
    name: &'static str,
    ceiling_bytes: usize,
    inner: Mutex<QueueInner>,
    item_added: Condvar,
}

impl BoundedQueue {
    /// A queue with `ceiling_bytes` of accounted-byte headroom.
    ///
    /// The default ceiling is [`QUEUE_CEILING_BYTES`] — the contract
    /// number; pass an operator's startup configuration instead where one
    /// exists.
    #[must_use]
    pub fn new(name: &'static str, ceiling_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            name,
            ceiling_bytes,
            inner: Mutex::new(QueueInner {
                items: VecDeque::new(),
                accounted: 0,
            }),
            item_added: Condvar::new(),
        })
    }

    /// The queue's name, as [`AdmissionSignal::QueueSaturated`] carries it.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The accounted-byte ceiling this queue was built with.
    #[must_use]
    pub const fn ceiling_bytes(&self) -> usize {
        self.ceiling_bytes
    }

    /// The accounted bytes currently in flight in this queue — never above
    /// [`BoundedQueue::ceiling_bytes`].
    ///
    /// # Panics
    ///
    /// Never: the mutex is only poisoned by a panic inside the queue, and
    /// the queue's critical sections are arithmetic and `VecDeque` moves.
    #[must_use]
    pub fn accounted_bytes(&self) -> usize {
        self.lock().accounted
    }

    /// The number of records currently in flight.
    ///
    /// # Panics
    ///
    /// As [`BoundedQueue::accounted_bytes`]: only a poisoned mutex.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().items.len()
    }

    /// Whether the queue holds no records.
    ///
    /// # Panics
    ///
    /// As [`BoundedQueue::accounted_bytes`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().items.is_empty()
    }

    /// The record at the front of the queue, without removing it.
    ///
    /// # Panics
    ///
    /// As [`BoundedQueue::accounted_bytes`].
    #[must_use]
    pub fn front(&self) -> Option<QueuedRecord> {
        self.lock().items.front().cloned()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueueInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Pops the front record, blocking until one is available.
    ///
    /// The consumer's side of the hand-off: the pump the server runs into
    /// storage. Blocks, never drops.
    ///
    /// # Panics
    ///
    /// As [`BoundedQueue::accounted_bytes`].
    pub fn pop(&self) -> QueuedRecord {
        let mut inner = self.lock();
        while inner.items.is_empty() {
            inner = self
                .item_added
                .wait(inner)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        Self::pop_locked(&mut inner)
    }

    /// Pops the front record if one arrives within `deadline`, else `None`.
    ///
    /// The drain-deadline shape: the pump waits at most this long for the
    /// next record before giving up on completeness. The deadline is one
    /// absolute `Instant`, computed once: a wakeup without work — a
    /// spurious `wait_timeout` return, or a notify whose record another
    /// consumer took — waits out the **remaining** time and never re-arms
    /// the full deadline, so a noisy queue cannot stretch the drain bound.
    ///
    /// # Panics
    ///
    /// As [`BoundedQueue::accounted_bytes`].
    pub fn pop_timeout(&self, deadline: Duration) -> Option<QueuedRecord> {
        let mut inner = self.lock();
        let ends_at = Instant::now().checked_add(deadline);
        while inner.items.is_empty() {
            let Some(remaining) =
                ends_at.and_then(|ends| ends.checked_duration_since(Instant::now()))
            else {
                if ends_at.is_none() {
                    // The deadline cannot be represented (further out than
                    // `Instant` measures): it is no deadline at all — wait
                    // without one, exactly as `pop` does.
                    inner = self
                        .item_added
                        .wait(inner)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    continue;
                }
                return None;
            };
            let (guard, _timed_out) = self
                .item_added
                .wait_timeout(inner, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inner = guard;
        }
        Some(Self::pop_locked(&mut inner))
    }

    /// Wakes every waiter without adding a record. Test-only: `offer` is
    /// the sole production notifier, and the drain-deadline law above — a
    /// wakeup without work never re-arms the deadline — is observable
    /// only when wakeups arrive without work.
    #[cfg(test)]
    pub(crate) fn wake_waiters_without_work_for_test(&self) {
        let _inner = self.lock();
        self.item_added.notify_all();
    }

    fn pop_locked(inner: &mut QueueInner) -> QueuedRecord {
        let record = inner
            .items
            .pop_front()
            .expect("caller checked the queue is non-empty");
        inner.accounted -= record.record.accounted_size();
        record
    }
}

impl RecordSink for BoundedQueue {
    fn ceiling_bytes(&self) -> usize {
        self.ceiling_bytes
    }

    fn offer(&self, record: QueuedRecord) -> Result<(), AdmissionSignal> {
        let bytes = record.record.accounted_size();
        let mut inner = self.lock();
        if inner.accounted + bytes > self.ceiling_bytes {
            return Err(AdmissionSignal::QueueSaturated {
                queue: self.name,
                ceiling_bytes: self.ceiling_bytes,
                attempted_bytes: bytes,
            });
        }
        inner.accounted += bytes;
        inner.items.push_back(record);
        drop(inner);
        self.item_added.notify_one();
        Ok(())
    }
}
