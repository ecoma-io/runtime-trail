//! The transport-edge guard: an aggregate in-flight request-body budget shared
//! by both OTLP transports, plus the honest refuse-shapes it answers with.
//!
//! Before admission, each in-flight request body is buffered at the transport
//! edge: OTLP/HTTP's `to_bytes` reads up to the payload ceiling, tonic's gRPC
//! frame reader buffers up to the decoding ceiling. The runtime's queue and
//! retention ceilings engage only *after* admission, so without this gate N
//! concurrent slow-drip bodies grow RSS by ~ceiling × N and can OOM the
//! process — the finding [ADR 0010](../../docs/decisions/0010-transport-edge-in-flight-body-budget.md)
//! names. This module is that gate: it counts the bytes being buffered and
//! refuses new buffering once the aggregate exceeds its budget. The read
//! timeout lives at the read seam in the handlers ([`super::otlp_http`],
//! [`super::otlp_grpc`]); this module owns the budget and the refusal shapes.
//!
//! The budget is a plain atomic counter — no semaphore, no lazy registration:
//! a request acquires its charge by an atomic `fetch_add` that refuses when the
//! running total would pass the ceiling, and releases it by `fetch_sub` when
//! its body is done. The charge is the *declared* `Content-Length` when it is
//! honest (present and at or under the payload ceiling), else the payload
//! ceiling — the worst case the bounded read can buffer; gRPC charges its
//! decoding ceiling, the worst case its frame reader can buffer.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The transport-edge aggregate gate, counted in buffered body bytes.
///
/// One instance per runtime graph, shared by both OTLP transports, so there is
/// a single aggregate for all transport-edge buffering — not two independently
/// unbounded pools ([ADR 0010](../../docs/decisions/0010-transport-edge-in-flight-body-budget.md)).
#[derive(Debug)]
pub struct InflightBodyBudget {
    /// The aggregate ceiling, in bytes: how much request body may be buffered
    /// at the transport edge at once.
    ceiling_bytes: usize,
    /// The bytes currently charged to this gate (bodies being buffered).
    in_flight: AtomicUsize,
}

impl InflightBodyBudget {
    /// A new gate with the given ceiling.
    #[must_use]
    pub fn new(ceiling_bytes: usize) -> Self {
        Self {
            ceiling_bytes,
            in_flight: AtomicUsize::new(0),
        }
    }

    /// The aggregate ceiling, in bytes.
    #[must_use]
    pub fn ceiling_bytes(&self) -> usize {
        self.ceiling_bytes
    }

    /// The bytes currently charged to this gate — the test seam that observes a
    /// hold, and the honest number a refusal response names.
    #[cfg(test)]
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Attempts to charge `charge` bytes to the gate, returning a guard that
    /// releases them on drop — or `None` when charging would exceed the
    /// ceiling. The caller must release exactly what it acquired.
    #[must_use]
    pub fn try_acquire(self: &Arc<Self>, charge: usize) -> Option<Acquired> {
        let mut current = self.in_flight.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(charge)?;
            if next > self.ceiling_bytes {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Acquired {
                        budget: Arc::clone(self),
                        charge,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// A plain release of `charge` bytes from the gate — the same release an
    /// [`Acquired`] guard performs on drop, exposed for the cases where the
    /// guard would outlive the body's buffering window.
    pub fn release(&self, charge: usize) {
        debug_assert!(
            charge <= self.in_flight.load(Ordering::Relaxed),
            "releasing more than is in flight"
        );
        self.in_flight.fetch_sub(charge, Ordering::AcqRel);
    }
}

/// A held charge on an [`InflightBodyBudget`]: the bytes are released when
/// this drops.
#[derive(Debug)]
pub struct Acquired {
    budget: Arc<InflightBodyBudget>,
    charge: usize,
}

impl Drop for Acquired {
    fn drop(&mut self) {
        self.budget.release(self.charge);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gate_refuses_when_the_aggregate_would_exceed_the_ceiling() {
        let budget = Arc::new(InflightBodyBudget::new(1024));

        let first = budget.try_acquire(512).expect("half the budget is fine");
        assert_eq!(budget.in_flight(), 512);
        // 512 + 768 already exceeds 1024, so the second charge must be refused.
        assert!(
            budget.try_acquire(768).is_none(),
            "the aggregate may not pass the ceiling"
        );
        let second = budget
            .try_acquire(512)
            .expect("a second charge can still fit");
        assert_eq!(budget.in_flight(), 512 + 512);
        // The aggregate is now at 1024, exactly the ceiling: no further room.
        assert!(
            budget.try_acquire(1).is_none(),
            "no further buffering once the aggregate is at the ceiling"
        );

        // Letting go returns the headroom: the same charge now fits.
        drop(first);
        assert_eq!(budget.in_flight(), 512);
        let again = budget
            .try_acquire(256)
            .expect("released headroom is reusable");
        assert_eq!(budget.in_flight(), 512 + 256);

        drop(second);
        drop(again);
        assert_eq!(budget.in_flight(), 0, "every acquired byte is returned");
    }

    #[test]
    fn a_charge_over_the_ceiling_cannot_round_trip() {
        let budget = Arc::new(InflightBodyBudget::new(1024));
        assert!(
            budget.try_acquire(1025).is_none(),
            "a single body buffering more than the whole budget is refused"
        );
        assert_eq!(budget.in_flight(), 0);
    }
}
