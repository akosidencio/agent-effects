use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::id::{EffectId, EffectKey, WorkerId};
use crate::kind::EffectKind;
use crate::state::{EffectStatus, InvalidTransition, Transition};
use crate::{EffectEvent, ErrorRecord, Lease, NewEffect, StoreError, TransitionRequest};

/// The durable state of one effect.
///
/// The methods are pure: they check a change against the record and apply
/// it in memory. Stores call them inside a transaction and persist the
/// result, so the rules are the same for every backend.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectRecord {
    /// Record id.
    pub id: EffectId,
    /// Logical identity, unique per store.
    pub key: EffectKey,
    /// The effect's kind.
    pub kind: EffectKind,
    /// Current status.
    pub status: EffectStatus,
    /// The input, redacted for storage.
    pub input: Option<Value>,
    /// Hash of the unredacted input.
    pub input_fingerprint: Option<String>,
    /// The latest action or verification result.
    pub output: Option<Value>,
    /// The latest recorded failure.
    pub last_error: Option<ErrorRecord>,
    /// Who asked for the effect.
    pub created_by: Option<String>,
    /// Attempts started so far.
    pub attempt_count: u32,
    /// An attempt may have applied the effect, and no evidence has shown
    /// otherwise since. Set when an outcome becomes unknown; cleared only by
    /// a trusted verification or an operator. While it is set, the effect
    /// cannot become `Failed` through a failed attempt (see [`Self::apply`]).
    pub may_have_applied: bool,
    /// Compensation attempts started or scheduled so far.
    pub compensation_attempts: u32,
    /// When a scheduled retry may start.
    pub next_attempt_at: Option<SystemTime>,
    /// When the latest attempt started. Settle delays count from here.
    pub attempt_started_at: Option<SystemTime>,
    /// Current lease holder, if any. The lease may have expired.
    pub lease_owner: Option<WorkerId>,
    /// Fencing token; incremented by every lease acquisition.
    pub lease_epoch: u64,
    /// When the current lease lapses.
    pub lease_expires_at: Option<SystemTime>,
    /// Incremented by every transition. Lease operations do not change it.
    pub version: u64,
    /// Creation time.
    pub created_at: SystemTime,
    /// Time of the latest change.
    pub updated_at: SystemTime,
    /// When the effect committed.
    pub committed_at: Option<SystemTime>,
}

impl EffectRecord {
    /// A fresh `Pending` record.
    pub fn new(new: NewEffect) -> Self {
        Self {
            id: new.id,
            key: new.key,
            kind: new.kind,
            status: EffectStatus::Pending,
            input: new.input,
            input_fingerprint: new.input_fingerprint,
            output: None,
            last_error: None,
            created_by: new.created_by,
            attempt_count: 0,
            may_have_applied: false,
            compensation_attempts: 0,
            next_attempt_at: None,
            attempt_started_at: None,
            lease_owner: None,
            lease_epoch: 0,
            lease_expires_at: None,
            version: 0,
            created_at: new.now,
            updated_at: new.now,
            committed_at: None,
        }
    }

    /// The holder of a lease that is still live at `now`.
    pub fn live_lease_owner(&self, now: SystemTime) -> Option<&WorkerId> {
        match (&self.lease_owner, self.lease_expires_at) {
            (Some(owner), Some(expires_at)) if expires_at > now => Some(owner),
            _ => None,
        }
    }

    /// Takes the lease for `ttl`, provided nobody holds a live one.
    ///
    /// # Errors
    ///
    /// [`StoreError::LeaseHeld`] if a live lease exists, even one held by
    /// `owner`: two tasks of one worker must not run the same effect either.
    pub fn acquire_lease(
        &mut self,
        owner: &WorkerId,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        if let Some(holder) = self.live_lease_owner(now) {
            return Err(StoreError::LeaseHeld {
                owner: holder.clone(),
                expires_at: self.lease_expires_at.unwrap_or(now),
            });
        }
        let expires_at = expiry(now, ttl);
        self.lease_epoch += 1;
        self.lease_owner = Some(owner.clone());
        self.lease_expires_at = Some(expires_at);
        Ok(Lease {
            effect_id: self.id,
            owner: owner.clone(),
            epoch: self.lease_epoch,
            expires_at,
        })
    }

    /// Extends `lease` to `now + ttl`.
    ///
    /// # Errors
    ///
    /// [`StoreError::LeaseLost`] if the lease expired or was taken over.
    /// Renewal is strict: an expired lease cannot be revived, even if nobody
    /// else took it.
    pub fn renew_lease(
        &mut self,
        lease: &Lease,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.check_lease(lease, now)?;
        let expires_at = expiry(now, ttl);
        self.lease_expires_at = Some(expires_at);
        Ok(Lease {
            expires_at,
            ..lease.clone()
        })
    }

    /// Clears `lease` if it is still the current one, expired or not.
    /// Returns whether anything changed.
    pub fn release_lease(&mut self, lease: &Lease) -> bool {
        let current = lease.effect_id == self.id
            && self.lease_epoch == lease.epoch
            && self.lease_owner.as_ref() == Some(&lease.owner);
        if current {
            self.lease_owner = None;
            self.lease_expires_at = None;
        }
        current
    }

    /// Applies `request` and returns the audit event to persist with it.
    /// On error the record is unchanged.
    ///
    /// Checks, in order: the lease (or the absence of a live one), the
    /// version, and the transition table. Then it updates the bookkeeping:
    ///
    /// - [`Transition::StartAttempt`] increments the attempt count, stamps
    ///   `attempt_started_at` and clears `next_attempt_at`;
    /// - [`Transition::StartCompensation`] sets `compensation_attempts` to 1;
    ///   [`Transition::ScheduleCompensationRetry`] increments it and sets
    ///   `next_attempt_at`;
    /// - a retry transition sets `next_attempt_at` (default `now`);
    /// - reaching `Committed` stamps `committed_at`;
    /// - reaching `Unknown` sets `may_have_applied`; evidence that the effect
    ///   did not apply clears it (`VerificationNotApplied`,
    ///   `ResolvedNotApplied`, or a `ScheduleRetry` out of `Verifying`, which
    ///   the runtime issues only after a trusted "not applied");
    /// - `output` and `error`, when given, replace the stored ones.
    ///
    /// `Failed` must mean the effect did not apply. A failed attempt proves
    /// that only for itself, so `FailedDefinitively` is refused while
    /// `may_have_applied` is set (except for `Read` effects, which apply
    /// nothing).
    ///
    /// # Errors
    ///
    /// [`StoreError::LeaseLost`], [`StoreError::LeaseHeld`],
    /// [`StoreError::VersionConflict`] or [`StoreError::InvalidTransition`].
    pub fn apply(&mut self, request: TransitionRequest) -> Result<EffectEvent, StoreError> {
        let now = request.now;
        match &request.lease {
            Some(lease) => self.check_lease(lease, now)?,
            None => {
                if let Some(owner) = self.live_lease_owner(now) {
                    return Err(StoreError::LeaseHeld {
                        owner: owner.clone(),
                        expires_at: self.lease_expires_at.unwrap_or(now),
                    });
                }
            }
        }
        if request.expected_version != self.version {
            return Err(StoreError::VersionConflict {
                expected: request.expected_version,
                actual: self.version,
            });
        }
        let from = self.status;
        let to = from.apply(request.transition)?;
        if request.transition == Transition::FailedDefinitively
            && self.may_have_applied
            && self.kind != EffectKind::Read
        {
            return Err(InvalidTransition {
                from,
                event: request.transition,
            }
            .into());
        }

        match request.transition {
            Transition::StartAttempt => {
                self.attempt_count = self.attempt_count.saturating_add(1);
                self.attempt_started_at = Some(now);
                self.next_attempt_at = None;
            }
            Transition::ScheduleRetry | Transition::ResolvedRetry => {
                self.next_attempt_at = Some(request.next_attempt_at.unwrap_or(now));
            }
            Transition::StartCompensation => {
                self.compensation_attempts = 1;
                self.next_attempt_at = None;
            }
            Transition::ScheduleCompensationRetry => {
                self.compensation_attempts = self.compensation_attempts.saturating_add(1);
                self.next_attempt_at = Some(request.next_attempt_at.unwrap_or(now));
            }
            _ => {}
        }
        if to == EffectStatus::Committed {
            self.committed_at = Some(now);
        }
        if to == EffectStatus::Unknown {
            self.may_have_applied = true;
        }
        let shown_not_applied = matches!(
            request.transition,
            Transition::VerificationNotApplied | Transition::ResolvedNotApplied
        ) || (from == EffectStatus::Verifying
            && request.transition == Transition::ScheduleRetry);
        if shown_not_applied {
            self.may_have_applied = false;
        }
        if let Some(output) = request.output {
            self.output = Some(output);
        }
        if let Some(error) = request.error {
            self.last_error = Some(error);
        }
        self.status = to;
        self.version += 1;
        self.updated_at = now;

        Ok(EffectEvent {
            effect_id: self.id,
            sequence: self.version,
            transition: request.transition,
            from,
            to,
            attempt: self.attempt_count,
            actor: request.actor,
            payload: request.payload,
            at: now,
        })
    }

    fn check_lease(&self, lease: &Lease, now: SystemTime) -> Result<(), StoreError> {
        let valid = lease.effect_id == self.id
            && self.lease_epoch == lease.epoch
            && self.lease_owner.as_ref() == Some(&lease.owner)
            && self
                .lease_expires_at
                .is_some_and(|expires_at| expires_at > now);
        if valid {
            Ok(())
        } else {
            Err(StoreError::LeaseLost)
        }
    }
}

/// `now + ttl`. An overflowing TTL yields an already-expired lease, which
/// fails safe: the holder is fenced off immediately.
fn expiry(now: SystemTime, ttl: Duration) -> SystemTime {
    now.checked_add(ttl).unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{EffectName, LogicalKey};

    const TTL: Duration = Duration::from_secs(30);

    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000 + secs)
    }

    fn record() -> EffectRecord {
        let key = EffectKey::new(
            EffectName::new("payment.charge").unwrap(),
            LogicalKey::new("order_1").unwrap(),
        );
        EffectRecord::new(NewEffect::new(key, EffectKind::IrreversibleWrite, t(0)))
    }

    fn worker(name: &str) -> WorkerId {
        WorkerId::new(name)
    }

    #[test]
    fn failed_apply_leaves_the_record_unchanged() {
        let mut rec = record();
        let lease = rec.acquire_lease(&worker("a"), t(0), TTL).unwrap();
        let before = rec.clone();

        let mut bad = TransitionRequest::new(&rec, Some(&lease), Transition::Succeeded, t(1));
        bad.output = Some(Value::from(1));
        assert!(matches!(
            rec.apply(bad),
            Err(StoreError::InvalidTransition(_))
        ));
        assert_eq!(rec, before);
    }

    #[test]
    fn stale_epoch_is_fenced_off_after_takeover() {
        let mut rec = record();
        let old = rec.acquire_lease(&worker("a"), t(0), TTL).unwrap();
        let new = rec.acquire_lease(&worker("b"), t(30), TTL).unwrap();
        assert_eq!(new.epoch, old.epoch + 1);

        let request = TransitionRequest::new(&rec, Some(&old), Transition::StartAttempt, t(31));
        assert!(matches!(rec.apply(request), Err(StoreError::LeaseLost)));
        assert!(
            !rec.release_lease(&old),
            "stale release must not clear b's lease"
        );
        assert_eq!(rec.live_lease_owner(t(31)), Some(&worker("b")));
    }

    #[test]
    fn same_owner_cannot_double_acquire() {
        let mut rec = record();
        rec.acquire_lease(&worker("a"), t(0), TTL).unwrap();
        assert!(matches!(
            rec.acquire_lease(&worker("a"), t(1), TTL),
            Err(StoreError::LeaseHeld { .. })
        ));
    }

    #[test]
    fn overflowing_ttl_fails_safe() {
        let mut rec = record();
        let lease = rec
            .acquire_lease(&worker("a"), t(0), Duration::MAX)
            .unwrap();
        assert_eq!(lease.expires_at, t(0));
        assert_eq!(rec.live_lease_owner(t(0)), None);
    }

    #[test]
    fn a_failed_attempt_cannot_fail_an_effect_that_may_have_applied() {
        let mut rec = record();
        let lease = rec.acquire_lease(&worker("a"), t(0), TTL).unwrap();
        for transition in [
            Transition::StartAttempt,
            Transition::OutcomeUnknown,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
        ] {
            rec.apply(TransitionRequest::new(&rec, Some(&lease), transition, t(1)))
                .unwrap();
        }
        assert!(
            rec.may_have_applied,
            "the unknown outcome is remembered across retries"
        );
        let before = rec.clone();
        let fail = TransitionRequest::new(&rec, Some(&lease), Transition::FailedDefinitively, t(2));
        assert!(matches!(
            rec.apply(fail),
            Err(StoreError::InvalidTransition(_))
        ));
        assert_eq!(rec, before);

        // A trusted verification clears it; then a failure is a failure.
        for transition in [
            Transition::StartVerification,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
        ] {
            rec.apply(TransitionRequest::new(&rec, Some(&lease), transition, t(3)))
                .unwrap();
        }
        assert!(!rec.may_have_applied);
        rec.apply(TransitionRequest::new(
            &rec,
            Some(&lease),
            Transition::FailedDefinitively,
            t(4),
        ))
        .unwrap();
        assert_eq!(rec.status, EffectStatus::Failed);
    }

    #[test]
    fn bookkeeping_follows_transitions() {
        let mut rec = record();
        let lease = rec.acquire_lease(&worker("a"), t(0), TTL).unwrap();

        let event = rec
            .apply(TransitionRequest::new(
                &rec,
                Some(&lease),
                Transition::StartAttempt,
                t(1),
            ))
            .unwrap();
        assert_eq!((event.sequence, event.attempt), (1, 1));
        assert_eq!(rec.attempt_started_at, Some(t(1)));

        let mut retry = TransitionRequest::new(&rec, Some(&lease), Transition::ScheduleRetry, t(2));
        retry.next_attempt_at = Some(t(10));
        retry.error = Some(ErrorRecord {
            class: Some(crate::FailureClass::Transient),
            message: "connection refused".into(),
        });
        rec.apply(retry).unwrap();
        assert_eq!(rec.next_attempt_at, Some(t(10)));
        assert!(rec.last_error.is_some());

        rec.apply(TransitionRequest::new(
            &rec,
            Some(&lease),
            Transition::StartAttempt,
            t(10),
        ))
        .unwrap();
        assert_eq!(rec.attempt_count, 2);
        assert_eq!(rec.next_attempt_at, None);

        let mut done = TransitionRequest::new(&rec, Some(&lease), Transition::Succeeded, t(11));
        done.output = Some(serde_json::json!({ "payment": "pi_1" }));
        let event = rec.apply(done).unwrap();
        assert_eq!(event.to, EffectStatus::Committed);
        assert_eq!(rec.committed_at, Some(t(11)));
        assert_eq!(rec.version, 4);
    }
}
