//! Undoing committed effects.
//!
//! Compensation is a durable lifecycle of its own:
//! `Committed → Compensating → Compensated`, or `CompensationFailed` when it
//! fails for good and an operator must finish it. Each attempt is recorded
//! before it runs, retried with backoff, and resumed after a crash: by the
//! next call, or by recovery for a registered
//! [`CompensableEffect`](crate::handler::CompensableEffect).
//!
//! A compensation **must be idempotent**: an attempt that crashed or failed
//! ambiguously is simply run again. Forward
//! [`CompensationContext::idempotency_key`] to the remote system. It is
//! stable across attempts and distinct from the effect's own key.
//!
//! ```
//! use agent_effects::{CompensationOutcome, EffectFailure, EffectOutcome, Runtime};
//! use agent_effects_memory::MemoryStore;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), agent_effects::RuntimeError> {
//! let runtime = Runtime::new(MemoryStore::new());
//! runtime
//!     .effect("inventory.reserve", "order-7")
//!     .run(|_| async { Ok::<_, EffectFailure>("reservation-1".to_string()) })
//!     .await?;
//!
//! // Later, the order is cancelled.
//! let outcome = runtime
//!     .compensation("inventory.reserve", "order-7")
//!     .reason("order cancelled")
//!     .run(|ctx, reservation: Option<String>| async move {
//!         // Release `reservation`, sending ctx.idempotency_key().
//!         Ok::<_, EffectFailure>(())
//!     })
//!     .await?;
//! assert_eq!(outcome, CompensationOutcome::Compensated);
//! # Ok(())
//! # }
//! ```

use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tracing::{Instrument, debug, field, info_span, warn};

use crate::effect::EffectFailure;
use crate::error::RuntimeError;
use crate::failure::{Disposition, FailureClass};
use crate::fault::FaultPoint;
use crate::id::{EffectId, EffectKey, EffectName, IdempotencyKey, LogicalKey};
use crate::retry::RetryPolicy;
use crate::runtime::{Interrupt, Runtime, jitter_sample, last_error};
use crate::state::{EffectStatus, Transition};
use crate::store::{EffectRecord, EffectStore, ErrorRecord, Lease, StoreError};

/// How many times one call re-reads the record after losing a lease race.
const MAX_ROUNDS: usize = 4;

/// What a compensation attempt knows about the effect it undoes.
#[derive(Clone, Debug)]
pub struct CompensationContext {
    pub(crate) id: EffectId,
    pub(crate) key: EffectKey,
    pub(crate) attempt: u32,
    pub(crate) reason: Option<String>,
}

impl CompensationContext {
    /// The effect's record id.
    pub fn effect_id(&self) -> EffectId {
        self.id
    }

    /// The effect's logical identity.
    pub fn key(&self) -> &EffectKey {
        &self.key
    }

    /// The key to forward to the remote system for the undo. Stable across
    /// compensation attempts and distinct from the effect's own key.
    pub fn idempotency_key(&self) -> IdempotencyKey {
        self.key.compensation_idempotency_key()
    }

    /// The compensation attempt number, starting at 1.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Why the effect is being undone, if the caller said.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// How a compensation ended, or where it stands.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompensationOutcome {
    /// The effect is undone. Calling again changes nothing.
    Compensated,
    /// Undoing failed for good (`CompensationFailed`). An operator must
    /// finish it by hand ([`Resolution::Compensated`](crate::Resolution::Compensated))
    /// or order a retry ([`Resolution::Retry`](crate::Resolution::Retry)).
    Failed(ErrorRecord),
    /// Another caller or worker holds the effect right now.
    InProgress {
        /// The effect.
        id: EffectId,
    },
    /// The effect has not committed, so there is nothing to undo, or it is
    /// not known whether there is (`Unknown`, `NeedsIntervention`): resolve
    /// it first.
    NotCommitted {
        /// The effect.
        id: EffectId,
        /// Its status.
        status: EffectStatus,
    },
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// An erased compensation: context, stored input, stored output.
pub(crate) type Compensator = Arc<
    dyn Fn(
            CompensationContext,
            Option<Value>,
            Option<Value>,
        ) -> BoxFuture<Result<(), EffectFailure>>
        + Send
        + Sync,
>;

/// Everything about a compensation except the code that performs it.
pub(crate) struct CompensationSpec {
    pub(crate) key: EffectKey,
    pub(crate) reason: Option<String>,
    pub(crate) actor: Option<String>,
    pub(crate) retry: Option<RetryPolicy>,
    pub(crate) attempt_timeout: Option<Duration>,
}

/// Undoes a closure effect. Created by [`Runtime::compensation`].
#[must_use = "a compensation does nothing until `run` is awaited"]
pub struct CompensationBuilder<'a, S> {
    runtime: &'a Runtime<S>,
    name: String,
    key: String,
    reason: Option<String>,
    actor: Option<String>,
    retry: Option<RetryPolicy>,
    attempt_timeout: Option<Duration>,
}

impl<'a, S: EffectStore> CompensationBuilder<'a, S> {
    pub(crate) fn new(runtime: &'a Runtime<S>, name: String, key: String) -> Self {
        Self {
            runtime,
            name,
            key,
            reason: None,
            actor: None,
            retry: None,
            attempt_timeout: None,
        }
    }

    /// Why the effect is being undone; recorded in the audit trail.
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Who is undoing it, e.g. `agent:support-bot`.
    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// The retry policy for compensation attempts; defaults to the
    /// runtime's. Its `max_attempts` bounds attempts over the compensation's
    /// life.
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = Some(policy);
        self
    }

    /// Gives up waiting for one attempt after `timeout`, and retries.
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = Some(timeout);
        self
    }

    /// Undoes the effect with `compensate`, which receives the effect's
    /// stored output (`None` if it had none). Calling again after it is
    /// undone returns [`CompensationOutcome::Compensated`] without running
    /// anything.
    ///
    /// # Errors
    ///
    /// Infrastructure failures, an invalid name or key, or no such effect.
    pub async fn run<T, F, Fut>(self, compensate: F) -> Result<CompensationOutcome, RuntimeError>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(CompensationContext, Option<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), EffectFailure>> + Send + 'static,
    {
        let key = EffectKey::new(EffectName::new(self.name)?, LogicalKey::new(self.key)?);
        let compensate: Compensator = Arc::new(move |ctx, _input, output| {
            match output.map(serde_json::from_value::<T>).transpose() {
                Ok(output) => Box::pin(compensate(ctx, output)),
                Err(e) => Box::pin(std::future::ready(Err(EffectFailure::permanent(format!(
                    "stored output does not match the compensation's type: {e}"
                ))))),
            }
        });
        let spec = CompensationSpec {
            key,
            reason: self.reason,
            actor: self.actor,
            retry: self.retry,
            attempt_timeout: self.attempt_timeout,
        };
        self.runtime.compensate_effect(spec, compensate).await
    }
}

impl<S: EffectStore> Runtime<S> {
    /// Starts undoing the closure effect `(name, key)`. See
    /// [`CompensationBuilder::run`].
    pub fn compensation(
        &self,
        name: impl Into<String>,
        key: impl Display,
    ) -> CompensationBuilder<'_, S> {
        CompensationBuilder::new(self, name.into(), key.to_string())
    }

    /// Drives the compensation of `spec.key` on a spawned task, so dropping
    /// the caller does not abandon an attempt midway.
    pub(crate) async fn compensate_effect(
        &self,
        spec: CompensationSpec,
        compensate: Compensator,
    ) -> Result<CompensationOutcome, RuntimeError> {
        let span = info_span!(
            "agent_effect.compensate",
            effect.name = %spec.key.name,
            effect.logical_key = %spec.key.key,
            effect.id = field::Empty,
        );
        let runtime = self.clone();
        tokio::spawn(
            async move { runtime.drive_compensation(spec, compensate).await }.instrument(span),
        )
        .await
        .unwrap_or_else(|e| Err(RuntimeError::Internal(e.to_string())))
    }

    async fn drive_compensation(
        &self,
        spec: CompensationSpec,
        compensate: Compensator,
    ) -> Result<CompensationOutcome, RuntimeError> {
        let store = self.store();
        let mut record =
            store
                .get_by_key(&spec.key)
                .await?
                .ok_or_else(|| RuntimeError::NoSuchEffect {
                    key: spec.key.to_string(),
                })?;
        tracing::Span::current().record("effect.id", field::display(record.id));
        for _ in 0..MAX_ROUNDS {
            if let Some(outcome) = observe_compensation(&record) {
                return Ok(outcome);
            }
            let lease = match store
                .acquire_lease(record.id, self.worker_id(), self.now(), self.lease_ttl())
                .await
            {
                Ok(lease) => lease,
                Err(StoreError::LeaseHeld { .. }) => {
                    return Ok(CompensationOutcome::InProgress { id: record.id });
                }
                Err(e) => return Err(e.into()),
            };
            let current = store
                .get(record.id)
                .await?
                .ok_or(StoreError::NotFound(record.id))?;
            let result = self
                .compensate_leased(current, &lease, &spec, &compensate)
                .await;
            if let Err(e) = store.release_lease(&lease).await {
                warn!(error = %e, "could not release lease; it will expire");
            }
            match result {
                Ok(settled) => {
                    return Ok(observe_compensation(&settled)
                        .unwrap_or(CompensationOutcome::InProgress { id: settled.id }));
                }
                Err(Interrupt::LeaseLost) => {
                    record = store
                        .get(record.id)
                        .await?
                        .ok_or(StoreError::NotFound(record.id))?;
                }
                Err(Interrupt::Error(e)) => return Err(e),
            }
        }
        Ok(CompensationOutcome::InProgress { id: record.id })
    }

    /// Starts the next attempt of a compensation found `Compensating`, or
    /// ends it `CompensationFailed` if that attempt is a retry the budget
    /// does not allow.
    async fn resume_compensation(
        &self,
        record: &EffectRecord,
        lease: &Lease,
        actor: Option<&str>,
        policy: RetryPolicy,
    ) -> Result<EffectRecord, Interrupt> {
        let resumed = match record.next_attempt_at {
            // A retry that was scheduled but has not started: it is not an
            // attempt yet. Wait for it, then start it.
            Some(at) => {
                self.sleep_leased(lease, at).await?;
                None
            }
            // An attempt that a crash or a dead worker cut short. Another
            // one is a retry, so it needs budget left.
            None if policy.allows_another(record.compensation_attempts) => {
                Some(json!({ "resumed": true }))
            }
            None => {
                return self
                    .transition_leased(record, lease, actor, Transition::CompensationFailed, |r| {
                        r.error = Some(ErrorRecord {
                            class: Some(FailureClass::Ambiguous),
                            message: "a compensation attempt was interrupted, and no retries \
                                      are left"
                                .into(),
                        });
                    })
                    .await;
            }
        };
        self.transition_leased(
            record,
            lease,
            actor,
            Transition::StartCompensationRetry,
            |r| r.payload = resumed,
        )
        .await
    }

    /// Starts or resumes the compensation and runs attempts until it
    /// succeeds or fails for good.
    async fn compensate_leased(
        &self,
        record: EffectRecord,
        lease: &Lease,
        spec: &CompensationSpec,
        compensate: &Compensator,
    ) -> Result<EffectRecord, Interrupt> {
        let actor = spec.actor.as_deref();
        let policy = spec.retry.unwrap_or_else(|| self.default_retry());
        let reason = spec.reason.as_ref().map(|r| json!({ "reason": r }));
        let mut record = match record.status {
            EffectStatus::Committed => {
                self.transition_leased(&record, lease, actor, Transition::StartCompensation, |r| {
                    r.payload = reason;
                })
                .await?
            }
            EffectStatus::Compensating => {
                let resumed = self
                    .resume_compensation(&record, lease, actor, policy)
                    .await?;
                if resumed.status != EffectStatus::Compensating {
                    return Ok(resumed);
                }
                resumed
            }
            _ => return Ok(record),
        };
        self.checkpoint(FaultPoint::AfterCompensationStarted);

        loop {
            let failure = match self
                .attempt_compensation(&record, lease, spec, compensate)
                .await?
            {
                Ok(()) => {
                    return self
                        .transition_leased(
                            &record,
                            lease,
                            actor,
                            Transition::CompensationSucceeded,
                            |_| {},
                        )
                        .await;
                }
                Err(failure) => failure,
            };
            debug!(%failure, "compensation attempt failed");
            let class = failure.class();
            let error = failure.to_record();
            // Compensations are idempotent, so an ambiguous failure is
            // simply retried; only a permanent one gives up early.
            let retryable = !matches!(class.disposition(), Disposition::Fail);
            if !(retryable && policy.allows_another(record.compensation_attempts)) {
                return self
                    .transition_leased(&record, lease, actor, Transition::CompensationFailed, |r| {
                        r.error = Some(error);
                    })
                    .await;
            }
            let retry_class = if class == FailureClass::Ambiguous {
                FailureClass::Transient
            } else {
                class
            };
            let delay = policy.delay(
                record.compensation_attempts.saturating_sub(1),
                retry_class,
                jitter_sample(),
            );
            let at = self.now() + delay;
            record = self
                .transition_leased(
                    &record,
                    lease,
                    actor,
                    Transition::ScheduleCompensationRetry,
                    |r| {
                        r.next_attempt_at = Some(at);
                        r.error = Some(error);
                    },
                )
                .await?;
            self.sleep_leased(lease, at).await?;
            record = self
                .transition_leased(
                    &record,
                    lease,
                    actor,
                    Transition::StartCompensationRetry,
                    |_| {},
                )
                .await?;
        }
    }

    /// Runs one compensation attempt on its own task, under the lease and
    /// the attempt timeout. A panic or a timeout is an ambiguous failure.
    async fn attempt_compensation(
        &self,
        record: &EffectRecord,
        lease: &Lease,
        spec: &CompensationSpec,
        compensate: &Compensator,
    ) -> Result<Result<(), EffectFailure>, Interrupt> {
        let ctx = CompensationContext {
            id: record.id,
            key: record.key.clone(),
            attempt: record.compensation_attempts,
            reason: spec.reason.clone(),
        };
        let mut task = tokio::spawn(compensate(ctx, record.input.clone(), record.output.clone()));
        let joined = match spec.attempt_timeout {
            None => self.with_lease(lease, &mut task).await?,
            Some(limit) => {
                match self
                    .with_lease(lease, tokio::time::timeout(limit, &mut task))
                    .await?
                {
                    Ok(joined) => joined,
                    Err(_elapsed) => {
                        task.abort();
                        return Ok(Err(EffectFailure::ambiguous(format!(
                            "compensation attempt timed out after {limit:?}"
                        ))));
                    }
                }
            }
        };
        Ok(joined.unwrap_or_else(|join_error| {
            Err(EffectFailure::ambiguous(format!(
                "compensation did not complete: {join_error}"
            )))
        }))
    }

    async fn sleep_leased(&self, lease: &Lease, until: SystemTime) -> Result<(), Interrupt> {
        let wait = until.duration_since(self.now()).unwrap_or_default();
        if wait.is_zero() {
            return Ok(());
        }
        self.with_lease(lease, tokio::time::sleep(wait)).await
    }
}

/// The outcome to report without acting, or `None` to try to take the lease
/// and work on the compensation. Whether a lease is still live is left to
/// `acquire_lease`, which may judge it by the store's own clock.
fn observe_compensation(record: &EffectRecord) -> Option<CompensationOutcome> {
    let id = record.id;
    match record.status {
        EffectStatus::Compensated => Some(CompensationOutcome::Compensated),
        EffectStatus::CompensationFailed => Some(CompensationOutcome::Failed(last_error(record))),
        EffectStatus::Committed | EffectStatus::Compensating => None,
        status => Some(CompensationOutcome::NotCommitted { id, status }),
    }
}
