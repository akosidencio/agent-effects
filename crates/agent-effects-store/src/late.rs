//! Supplementary observations that cannot change authoritative effect state.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::{EffectId, EffectRecord, Lease, WorkerId};

/// What a worker observed after it could no longer record an authoritative result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LateOutcome {
    /// The action returned success; this is not remote verification.
    Succeeded,
    /// The action returned an error; this does not prove non-application.
    Failed,
    /// The action panicked or was cancelled; its outcome is uncertain.
    Interrupted,
}

/// Append-only, untrusted evidence for operator reconciliation.
///
/// No input, output, response body, credential, or error message is retained.
/// Neither this observation nor its timestamps authorize a state transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LateObservation {
    /// Unique observation id, used for idempotent append and pagination.
    pub id: EffectId,
    /// The effect this evidence concerns.
    pub effect_id: EffectId,
    /// The reporting worker.
    pub worker: WorkerId,
    /// The original fencing epoch, not the current holder's epoch.
    pub lease_epoch: u64,
    /// The action attempt, starting at 1.
    pub attempt: u32,
    /// Start timestamp originally persisted by the store, if available.
    pub attempt_started_at: Option<SystemTime>,
    /// The reporting worker's observation time; not a lease clock authority.
    pub observed_at: SystemTime,
    /// What the worker saw, without sensitive response data.
    pub outcome: LateOutcome,
}

impl LateObservation {
    /// Builds supplementary evidence about one attempted action.
    pub fn new(
        record: &EffectRecord,
        lease: &Lease,
        outcome: LateOutcome,
        observed_at: SystemTime,
    ) -> Self {
        Self {
            id: EffectId::new(),
            effect_id: record.id,
            worker: lease.owner.clone(),
            lease_epoch: lease.epoch,
            attempt: record.attempt_count,
            attempt_started_at: record.attempt_started_at,
            observed_at,
            outcome,
        }
    }
}
