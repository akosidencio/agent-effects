//! Watching effects for metrics.
//!
//! An [`EffectObserver`] (`RuntimeBuilder::observer`) is told about every
//! effect the runtime records and every transition it stores, after the
//! write succeeds. That covers calls, recovery, compensation, approval and
//! operator decisions alike. That is enough for every metric that matters:
//!
//! - counts by transition or by status reached (committed, failed, unknown,
//!   retries, compensations);
//! - attempt duration ([`Observation::in_previous_status`] when leaving
//!   `Executing`);
//! - time spent unknown (the same, when leaving `Unknown`);
//! - end-to-end duration ([`Observation::since_created`] when settling);
//! - levels such as "awaiting approval", as up/down counts. These are deltas
//!   since the observer started; for exact current numbers, query
//!   [`Runtime::pending`](crate::Runtime::pending).
//!
//! The core crate depends on no metrics library; `agent-effects-otel`
//! implements an observer with OpenTelemetry. An observer that panics is
//! caught and logged: a metrics bug must never break an effect whose
//! transition is already stored.

use std::time::Duration;

use crate::state::{EffectStatus, Transition};
use crate::store::EffectRecord;

/// One stored transition.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Observation<'a> {
    /// The effect after the transition.
    pub record: &'a EffectRecord,
    /// What happened.
    pub transition: Transition,
    /// The status before.
    pub from: EffectStatus,
    /// The status after.
    pub to: EffectStatus,
    /// How long the effect was in `from`, e.g. an attempt's duration when
    /// `from` is `Executing`.
    pub in_previous_status: Duration,
    /// How long since the effect was first recorded.
    pub since_created: Duration,
}

/// Receives effect lifecycle events, for metrics. Both methods default to
/// doing nothing.
///
/// Calls are synchronous and run on the runtime's tasks: keep them cheap
/// (increment a counter, record a histogram), and hand anything slow to
/// another task.
pub trait EffectObserver: Send + Sync + 'static {
    /// A new effect was recorded (`Pending`).
    fn on_created(&self, record: &EffectRecord) {
        let _ = record;
    }

    /// A transition was stored.
    fn on_transition(&self, observation: &Observation<'_>) {
        let _ = observation;
    }
}
