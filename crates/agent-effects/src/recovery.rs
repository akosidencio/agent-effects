//! Recovery and the operator API.
//!
//! - [`Runtime::recover`] marks attempts whose worker died as unknown, then
//!   finishes every unsettled effect that has a registered
//!   [handler](crate::handler) from its stored input. Closure effects are
//!   not durable, so they wait for a caller to re-run them;
//! - [`Runtime::pending`] lists the effects waiting for a caller or an
//!   operator;
//! - [`Runtime::resolve`] records an operator's decision.

use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::error::RuntimeError;
use crate::id::EffectId;
use crate::runtime::Runtime;
use crate::state::{EffectStatus, Transition};
use crate::store::{
    EffectRecord, EffectStore, ErrorRecord, ListQuery, StoreError, TransitionRequest,
};

/// Page size for recovery scans.
const PAGE: usize = 100;

/// What one [`Runtime::recover`] pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecoveryReport {
    /// Effects whose worker's lease expired mid-attempt or mid-verification,
    /// now marked unknown.
    pub marked_unknown: Vec<EffectId>,
    /// Effects that matched the scan but were taken by another caller or
    /// worker before this pass reached them.
    pub skipped: Vec<EffectId>,
    /// Effects with a registered handler that this pass moved forward, and
    /// the status each ended in. `Committed`, `Failed` and `Rejected` are
    /// settled; `NeedsIntervention` waits for an operator; anything else is
    /// still in progress or still unknown.
    pub resumed: Vec<(EffectId, EffectStatus)>,
    /// Unsettled effects with no registered handler (closure effects). They
    /// need a caller to run them again with the same key.
    pub unhandled: Vec<EffectId>,
    /// Effects whose handler could not be run, with the reason, e.g. a
    /// stored input that no longer deserializes.
    pub resume_errors: Vec<(EffectId, String)>,
}

/// An operator's decision about an effect the runtime could not resolve.
#[derive(Clone, Debug, PartialEq)]
pub enum Resolution {
    /// The effect applied. `output` becomes its result for every later call,
    /// so it should deserialize into the type those calls expect.
    Applied {
        /// The effect's output, as JSON.
        output: Option<Value>,
    },
    /// The effect did not apply. It ends `Failed`, with the operator's note
    /// as its error.
    NotApplied,
    /// Running the effect again is safe. The next call runs it, even if its
    /// retry budget is spent, since this is an explicit decision. On a
    /// failed compensation: try the compensation again.
    Retry,
    /// For a failed compensation: the operator undid the effect by hand. It
    /// ends `Compensated`.
    Compensated,
}

impl Resolution {
    /// [`Resolution::Applied`] with `output` serialized.
    ///
    /// # Errors
    ///
    /// If `output` cannot be serialized to JSON.
    pub fn applied<T: Serialize + ?Sized>(output: &T) -> Result<Self, serde_json::Error> {
        Ok(Self::Applied {
            output: Some(serde_json::to_value(output)?),
        })
    }
}

impl<S: EffectStore> Runtime<S> {
    /// One recovery pass, in two steps.
    ///
    /// 1. Every effect whose worker's lease expired mid-attempt or
    ///    mid-verification becomes `Unknown`, never `Failed`: it may have
    ///    changed the outside world.
    /// 2. Every unsettled effect nobody holds (see [`Self::pending`]) whose
    ///    name has a registered [handler](crate::handler) is finished from its
    ///    stored input, exactly as if its caller had called again: verified,
    ///    re-run if that is safe, or escalated. This includes effects left
    ///    `Pending` by a crash. Effects waiting for an operator, and retries
    ///    scheduled for later, are left alone. Unsettled closure effects are
    ///    reported as `unhandled`: only a caller can re-run them.
    ///
    /// Effects are resumed one at a time, so a pass lasts as long as their
    /// retries and verifications take. Safe to run from several workers at
    /// once: every change happens under a lease.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Store`] if the store fails. Progress made before the
    /// failure is kept. A handler that cannot run is reported in
    /// `resume_errors` instead.
    pub async fn recover(&self) -> Result<RecoveryReport, RuntimeError> {
        let mut report = RecoveryReport::default();
        self.mark_abandoned(&mut report).await?;
        self.resume_pending(&mut report).await?;
        Ok(report)
    }

    async fn mark_abandoned(&self, report: &mut RecoveryReport) -> Result<(), RuntimeError> {
        let mut after = None;
        loop {
            let mut query = ListQuery::expired_leases(self.now()).limit(PAGE);
            if let Some(id) = after {
                query = query.after(id);
            }
            let page = self.store().list(query).await?;
            let full = page.len() == PAGE;
            after = page.last().map(|record| record.id);
            for record in page {
                if self.mark_unknown(record.id).await? {
                    report.marked_unknown.push(record.id);
                } else {
                    report.skipped.push(record.id);
                }
            }
            if !full {
                return Ok(());
            }
        }
    }

    async fn resume_pending(&self, report: &mut RecoveryReport) -> Result<(), RuntimeError> {
        let mut after = None;
        loop {
            let page = self.pending(after, PAGE).await?;
            let full = page.len() == PAGE;
            after = page.last().map(|record| record.id);
            for record in page {
                let id = record.id;
                let not_due = record.next_attempt_at.is_some_and(|at| at > self.now());
                let operator = matches!(
                    record.status,
                    EffectStatus::NeedsIntervention
                        | EffectStatus::CompensationFailed
                        | EffectStatus::AwaitingApproval
                );
                if operator || not_due {
                    continue;
                }
                let Some(resume) = self.resumer(record.key.name.as_str()) else {
                    report.unhandled.push(id);
                    continue;
                };
                match resume(self.clone(), record).await {
                    Ok(status) => {
                        info!(effect.id = %id, %status, "recovery resumed effect");
                        report.resumed.push((id, status));
                    }
                    Err(e) => {
                        warn!(effect.id = %id, error = %e, "recovery could not resume effect");
                        report.resume_errors.push((id, e.to_string()));
                    }
                }
            }
            if !full {
                return Ok(());
            }
        }
    }

    /// Runs [`Self::recover`] every `interval`, forever. Spawn it:
    ///
    /// ```ignore
    /// tokio::spawn({
    ///     let runtime = runtime.clone();
    ///     async move { runtime.run_recovery(Duration::from_secs(30)).await }
    /// });
    /// ```
    ///
    /// A failed pass is logged and retried at the next tick.
    pub async fn run_recovery(&self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match self.recover().await {
                Ok(report) if !report.marked_unknown.is_empty() || !report.resumed.is_empty() => {
                    info!(
                        marked_unknown = report.marked_unknown.len(),
                        resumed = report.resumed.len(),
                        unhandled = report.unhandled.len(),
                        "recovery pass"
                    );
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "recovery pass failed"),
            }
        }
    }

    /// Effects waiting for a caller to re-run them or an operator to decide,
    /// ordered by creation, `limit` at a time after `after`.
    ///
    /// These are the effects that are unsettled with nobody working on
    /// them: `Pending` (for example a worker died during a backoff wait),
    /// `Executing` or `Verifying` with an expired lease (not yet recovered),
    /// `Unknown`, `NeedsIntervention`, `AwaitingApproval`, an interrupted
    /// `Compensating`, and `CompensationFailed`.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Store`] if the store fails.
    pub async fn pending(
        &self,
        after: Option<EffectId>,
        limit: usize,
    ) -> Result<Vec<EffectRecord>, RuntimeError> {
        let mut query = ListQuery::statuses([
            EffectStatus::Pending,
            EffectStatus::AwaitingApproval,
            EffectStatus::Executing,
            EffectStatus::Verifying,
            EffectStatus::Unknown,
            EffectStatus::NeedsIntervention,
            EffectStatus::Compensating,
            EffectStatus::CompensationFailed,
        ])
        .limit(limit);
        query.lease_expired_at = Some(self.now());
        query.after = after;
        Ok(self.store().list(query).await?)
    }

    /// Records an operator's decision about an effect that is `Unknown` or
    /// `NeedsIntervention`. `actor` identifies the operator (e.g.
    /// `operator:alice`); `note` says why, for the audit trail.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Store`] wrapping:
    ///
    /// - [`StoreError::NotFound`] for an unknown id;
    /// - [`StoreError::InvalidTransition`] if the effect is not unresolved,
    ///   for example already committed;
    /// - [`StoreError::LeaseHeld`] while a caller or worker is working on it;
    /// - [`StoreError::VersionConflict`] if it changed during the call.
    pub async fn resolve(
        &self,
        id: EffectId,
        resolution: Resolution,
        actor: impl Into<String>,
        note: impl Into<String>,
    ) -> Result<EffectRecord, RuntimeError> {
        let record = self
            .store()
            .get(id)
            .await?
            .ok_or(StoreError::NotFound(id))?;
        let note = note.into();
        let transition = match resolution {
            Resolution::Applied { .. } => Transition::ResolvedApplied,
            Resolution::NotApplied => Transition::ResolvedNotApplied,
            Resolution::Retry => Transition::ResolvedRetry,
            Resolution::Compensated => Transition::ResolvedCompensated,
        };
        let mut request = TransitionRequest::new(&record, None, transition, self.now());
        request.actor = Some(actor.into());
        request.payload = Some(json!({ "note": note }));
        match resolution {
            Resolution::Applied { output } => request.output = output,
            Resolution::NotApplied => {
                request.error = Some(ErrorRecord {
                    class: None,
                    message: note,
                });
            }
            Resolution::Retry | Resolution::Compensated => {}
        }
        let record = self.store().transition(request).await?;
        info!(effect.id = %id, %transition, "effect resolved by an operator");
        Ok(record)
    }

    /// Approves an effect waiting in `AwaitingApproval`. It becomes
    /// `Pending`: a caller's next call runs it, and so does recovery for a
    /// registered handler. `actor` is recorded as the approver.
    ///
    /// # Errors
    ///
    /// As for [`Self::resolve`]; `InvalidTransition` if it is not awaiting
    /// approval.
    pub async fn approve(
        &self,
        id: EffectId,
        actor: impl Into<String>,
        note: impl Into<String>,
    ) -> Result<EffectRecord, RuntimeError> {
        self.decide(id, Transition::Approve, actor.into(), note.into())
            .await
    }

    /// Denies an effect waiting in `AwaitingApproval`. It ends `Rejected`,
    /// with `reason` as its error.
    ///
    /// # Errors
    ///
    /// As for [`Self::approve`].
    pub async fn deny(
        &self,
        id: EffectId,
        actor: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<EffectRecord, RuntimeError> {
        self.decide(id, Transition::Deny, actor.into(), reason.into())
            .await
    }

    async fn decide(
        &self,
        id: EffectId,
        transition: Transition,
        actor: String,
        note: String,
    ) -> Result<EffectRecord, RuntimeError> {
        let record = self
            .store()
            .get(id)
            .await?
            .ok_or(StoreError::NotFound(id))?;
        let mut request = TransitionRequest::new(&record, None, transition, self.now());
        request.actor = Some(actor);
        if transition == Transition::Deny {
            request.error = Some(ErrorRecord {
                class: None,
                message: note.clone(),
            });
        }
        request.payload = Some(json!({ "note": note }));
        let record = self.store().transition(request).await?;
        info!(effect.id = %id, %transition, "approval decided by an operator");
        Ok(record)
    }

    /// Moves one effect with an expired lease to `Unknown`. Returns `false`
    /// if someone else got to it first.
    async fn mark_unknown(&self, id: EffectId) -> Result<bool, RuntimeError> {
        let store = self.store();
        let lease = match store
            .acquire_lease(id, self.worker_id(), self.now(), self.lease_ttl())
            .await
        {
            Ok(lease) => lease,
            Err(StoreError::LeaseHeld { .. }) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let marked = async {
            let record = store.get(id).await?.ok_or(StoreError::NotFound(id))?;
            if !matches!(
                record.status,
                EffectStatus::Executing | EffectStatus::Verifying
            ) {
                return Ok(false);
            }
            let mut request =
                TransitionRequest::new(&record, Some(&lease), Transition::LeaseExpired, self.now());
            request.actor = Some(format!("recovery:{}", self.worker_id()));
            store.transition(request).await?;
            info!(
                effect.id = %id,
                effect.name = %record.key.name,
                "worker lost mid-effect; outcome is now unknown"
            );
            Ok::<_, RuntimeError>(true)
        }
        .await;
        if let Err(e) = store.release_lease(&lease).await {
            warn!(error = %e, "could not release recovery lease; it will expire");
        }
        marked
    }
}
