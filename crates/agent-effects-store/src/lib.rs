//! Storage contract for [`agent-effects`](https://crates.io/crates/agent-effects).
//!
//! This crate holds what a store persists and enforces: effect identity
//! ([`id`]), classification ([`kind`], [`failure`]), the state machine
//! ([`state`]), records, leases and audit events, and the [`EffectStore`]
//! trait. Store backends depend on this crate only, never on the runtime.
//!
//! The rules (lease fencing, version checks, the transition table, attempt
//! bookkeeping) live in [`EffectRecord`]'s pure methods. A store's job is
//! only to run "load, apply, save the record and its audit event"
//! atomically, so every backend behaves the same. The `testkit` feature's
//! `testkit::conformance` suite checks that.

pub mod failure;
pub mod id;
pub mod kind;
pub mod state;
#[cfg(feature = "testkit")]
pub mod testkit;

mod error;
mod record;

use std::future::Future;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use error::StoreError;
pub use failure::{Disposition, FailureClass};
pub use id::{
    EffectId, EffectKey, EffectName, IdempotencyKey, IdentityError, LogicalKey, WorkerId,
};
pub use kind::EffectKind;
pub use record::EffectRecord;
pub use state::{EffectStatus, InvalidTransition, Transition};

/// Persistence for effect records, leases and audit events.
///
/// Times are passed in by the caller rather than read from a clock, so the
/// runtime's clock governs leases. Stores must keep at least millisecond
/// precision.
///
/// Every method that changes a record must apply the change through the
/// matching [`EffectRecord`] method inside one atomic unit (a transaction or
/// a lock), so concurrent callers can never interleave between the check and
/// the write.
pub trait EffectStore: Send + Sync + 'static {
    /// Inserts a new effect, or returns the existing record with the same
    /// [`EffectKey`] untouched.
    ///
    /// Atomic on the key: of any number of concurrent calls for one key,
    /// exactly one reports [`InsertOutcome::inserted`].
    fn insert_or_get(
        &self,
        new: NewEffect,
    ) -> impl Future<Output = Result<InsertOutcome, StoreError>> + Send;

    /// Loads a record by id.
    fn get(
        &self,
        id: EffectId,
    ) -> impl Future<Output = Result<Option<EffectRecord>, StoreError>> + Send;

    /// Loads a record by its logical identity.
    fn get_by_key(
        &self,
        key: &EffectKey,
    ) -> impl Future<Output = Result<Option<EffectRecord>, StoreError>> + Send;

    /// Takes the execution lease, via [`EffectRecord::acquire_lease`].
    fn acquire_lease(
        &self,
        id: EffectId,
        owner: &WorkerId,
        now: SystemTime,
        ttl: Duration,
    ) -> impl Future<Output = Result<Lease, StoreError>> + Send;

    /// Extends a held lease, via [`EffectRecord::renew_lease`].
    fn renew_lease(
        &self,
        lease: &Lease,
        now: SystemTime,
        ttl: Duration,
    ) -> impl Future<Output = Result<Lease, StoreError>> + Send;

    /// Gives up a lease, via [`EffectRecord::release_lease`]. Releasing a
    /// lease that was already lost is not an error.
    fn release_lease(&self, lease: &Lease) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Applies a status transition, via [`EffectRecord::apply`], and appends
    /// its audit event in the same atomic unit. Returns the updated record.
    fn transition(
        &self,
        request: TransitionRequest,
    ) -> impl Future<Output = Result<EffectRecord, StoreError>> + Send;

    /// Lists records matching `query`, ordered by id (creation time).
    fn list(
        &self,
        query: ListQuery,
    ) -> impl Future<Output = Result<Vec<EffectRecord>, StoreError>> + Send;

    /// The audit events of one effect, ordered by sequence.
    fn events(
        &self,
        id: EffectId,
    ) -> impl Future<Output = Result<Vec<EffectEvent>, StoreError>> + Send;
}

/// A new effect to record.
#[derive(Clone, Debug, PartialEq)]
pub struct NewEffect {
    /// The id the record gets if it is inserted.
    pub id: EffectId,
    /// The logical identity; unique per store.
    pub key: EffectKey,
    /// The effect's kind.
    pub kind: EffectKind,
    /// The input, already redacted for storage.
    pub input: Option<Value>,
    /// A stable hash of the unredacted input, used to reject a reused key
    /// with a different input.
    pub input_fingerprint: Option<String>,
    /// Who asked for the effect, e.g. `agent:refund-agent`.
    pub created_by: Option<String>,
    /// The creation time.
    pub now: SystemTime,
}

impl NewEffect {
    /// A new effect with a fresh id and no input or actor.
    pub fn new(key: EffectKey, kind: EffectKind, now: SystemTime) -> Self {
        Self {
            id: EffectId::new(),
            key,
            kind,
            input: None,
            input_fingerprint: None,
            created_by: None,
            now,
        }
    }
}

/// The result of [`EffectStore::insert_or_get`].
#[derive(Clone, Debug, PartialEq)]
pub struct InsertOutcome {
    /// The new or existing record.
    pub record: EffectRecord,
    /// Whether this call created it.
    pub inserted: bool,
}

/// Proof of holding an effect's execution lease.
///
/// Every acquisition increments the record's lease epoch. A store accepts a
/// lease only while the record still carries its owner and epoch and it has
/// not expired. That fences off a worker whose lease was taken over: all its
/// later writes fail with [`StoreError::LeaseLost`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    /// The effect the lease is for.
    pub effect_id: EffectId,
    /// The holder.
    pub owner: WorkerId,
    /// The fencing token.
    pub epoch: u64,
    /// When the lease lapses unless renewed.
    pub expires_at: SystemTime,
}

/// A request to move an effect through one [`Transition`].
#[derive(Clone, Debug, PartialEq)]
pub struct TransitionRequest {
    /// The effect to change.
    pub id: EffectId,
    /// The record version the caller last saw; the change fails with
    /// [`StoreError::VersionConflict`] if it moved on.
    pub expected_version: u64,
    /// The caller's lease. `None` is only accepted while nobody holds a live
    /// lease, e.g. for an operator resolving an effect.
    pub lease: Option<Lease>,
    /// The transition to apply.
    pub transition: Transition,
    /// The time of the change.
    pub now: SystemTime,
    /// The action's result, stored when present.
    pub output: Option<Value>,
    /// A failure to record as the effect's last error.
    pub error: Option<ErrorRecord>,
    /// For [`Transition::ScheduleRetry`] and [`Transition::ResolvedRetry`]:
    /// when the next attempt may start. Defaults to `now`.
    pub next_attempt_at: Option<SystemTime>,
    /// Who made the change, for the audit event.
    pub actor: Option<String>,
    /// Extra audit detail, already redacted.
    pub payload: Option<Value>,
}

impl TransitionRequest {
    /// A transition with no output, error, actor or payload.
    pub fn new(
        record: &EffectRecord,
        lease: Option<&Lease>,
        transition: Transition,
        now: SystemTime,
    ) -> Self {
        Self {
            id: record.id,
            expected_version: record.version,
            lease: lease.cloned(),
            transition,
            now,
            output: None,
            error: None,
            next_attempt_at: None,
            actor: None,
            payload: None,
        }
    }
}

/// A recorded failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorRecord {
    /// How the failure was classified, if it was.
    pub class: Option<FailureClass>,
    /// A description, already redacted.
    pub message: String,
}

/// One entry of an effect's audit trail.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectEvent {
    /// The effect.
    pub effect_id: EffectId,
    /// Position in the trail, starting at 1. Equal to the record's version
    /// after the transition.
    pub sequence: u64,
    /// What happened.
    pub transition: Transition,
    /// The status before.
    pub from: EffectStatus,
    /// The status after.
    pub to: EffectStatus,
    /// The record's attempt count after the transition.
    pub attempt: u32,
    /// Who made the change.
    pub actor: Option<String>,
    /// Extra detail, already redacted.
    pub payload: Option<Value>,
    /// When it happened.
    pub at: SystemTime,
}

/// A filter for [`EffectStore::list`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListQuery {
    /// Only these statuses. Empty means any status.
    pub statuses: Vec<EffectStatus>,
    /// Only records without a live lease at this time.
    pub lease_expired_at: Option<SystemTime>,
    /// Only records with a greater id, for paging.
    pub after: Option<EffectId>,
    /// At most this many records.
    pub limit: usize,
}

impl ListQuery {
    /// The default page size.
    pub const DEFAULT_LIMIT: usize = 100;

    /// Records in any of `statuses`.
    pub fn statuses(statuses: impl IntoIterator<Item = EffectStatus>) -> Self {
        Self {
            statuses: statuses.into_iter().collect(),
            lease_expired_at: None,
            after: None,
            limit: Self::DEFAULT_LIMIT,
        }
    }

    /// Records stuck mid-attempt: executing or verifying with no live lease
    /// at `now`. Recovery moves these to Unknown.
    pub fn expired_leases(now: SystemTime) -> Self {
        Self {
            lease_expired_at: Some(now),
            ..Self::statuses([EffectStatus::Executing, EffectStatus::Verifying])
        }
    }

    /// Continues after `id`.
    #[must_use]
    pub fn after(mut self, id: EffectId) -> Self {
        self.after = Some(id);
        self
    }

    /// Caps the page size.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Whether `record` passes the filter (ignoring `limit`).
    pub fn matches(&self, record: &EffectRecord) -> bool {
        (self.statuses.is_empty() || self.statuses.contains(&record.status))
            && self
                .lease_expired_at
                .is_none_or(|now| record.live_lease_owner(now).is_none())
            && self.after.is_none_or(|after| record.id > after)
    }
}
