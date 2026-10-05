//! Recovery and the operator API.
//!
//! Closures are not durable, so recovery cannot finish an effect on its own
//! (that needs the v0.2 handler registry). What it can do safely:
//!
//! - [`Runtime::recover`] marks attempts whose worker died as unknown, so
//!   their state is visible and honest;
//! - [`Runtime::pending`] lists the effects waiting for a caller to re-run
//!   them or an operator to decide;
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
pub struct RecoveryReport {
    /// Effects whose worker's lease expired mid-attempt or mid-verification,
    /// now marked unknown.
    pub marked_unknown: Vec<EffectId>,
    /// Effects that matched the scan but were taken by another caller or
    /// worker before this pass reached them.
    pub skipped: Vec<EffectId>,
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
    /// retry budget is spent, since this is an explicit decision.
    Retry,
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
    /// Marks every effect whose worker's lease expired mid-attempt or
    /// mid-verification as `Unknown`.
    ///
    /// Such an effect may have changed the outside world, so it becomes
    /// `Unknown`, never `Failed`. A later call with the same key then
    /// verifies it, re-runs it if that is safe, or escalates it. Effects
    /// that are pending, already unknown or settled are left alone, as is
    /// anything another worker holds.
    ///
    /// Safe to run from several workers at once: each effect is taken under
    /// a lease before it is changed.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Store`] if the store fails. Effects marked before the
    /// failure stay marked.
    pub async fn recover(&self) -> Result<RecoveryReport, RuntimeError> {
        let mut report = RecoveryReport::default();
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
                return Ok(report);
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
                Ok(report) if !report.marked_unknown.is_empty() => {
                    info!(
                        count = report.marked_unknown.len(),
                        "recovery marked effects unknown"
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
    /// `Unknown` and `NeedsIntervention`.
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
            EffectStatus::Executing,
            EffectStatus::Verifying,
            EffectStatus::Unknown,
            EffectStatus::NeedsIntervention,
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
            Resolution::Retry => {}
        }
        let record = self.store().transition(request).await?;
        info!(effect.id = %id, %transition, "effect resolved by an operator");
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
