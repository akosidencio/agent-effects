//! The effect runtime.
//!
//! [`Runtime::effect`] starts a builder; running it inserts or finds the
//! effect's record, then either reports its settled outcome or takes the
//! execution lease and moves it forward (design §7, "Re-attaching").

use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::task::{JoinError, JoinHandle};
use tracing::{Instrument, Span, debug, field, info_span, warn};

use crate::clock::{Clock, SystemClock};
use crate::effect::{EffectBuilder, EffectContext, EffectFailure, EffectOutcome, EffectSpec};
use crate::error::RuntimeError;
use crate::failure::{Disposition, FailureClass};
use crate::id::{EffectId, WorkerId};
use crate::policy::UnknownPlan;
use crate::state::{EffectStatus, Transition};
use crate::store::{
    EffectRecord, EffectStore, ErrorRecord, Lease, NewEffect, StoreError, TransitionRequest,
};

/// How many times one call re-reads the record after losing a lease race
/// before reporting the effect as in progress.
const MAX_ROUNDS: usize = 4;

/// Executes effects against a store. Cheap to clone; clones share state.
pub struct Runtime<S> {
    inner: Arc<Inner<S>>,
}

struct Inner<S> {
    store: S,
    clock: Arc<dyn Clock>,
    worker: WorkerId,
    lease_ttl: Duration,
}

impl<S> Clone for Runtime<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Configures a [`Runtime`].
#[must_use]
pub struct RuntimeBuilder<S> {
    store: S,
    clock: Arc<dyn Clock>,
    worker: Option<WorkerId>,
    lease_ttl: Duration,
}

impl<S: EffectStore> RuntimeBuilder<S> {
    /// The time source for leases. Defaults to the system clock.
    pub fn clock(mut self, clock: impl Clock) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    /// This runtime's identity as a lease holder. Defaults to a random id.
    /// Must be unique among runtimes sharing a store.
    pub fn worker_id(mut self, worker: WorkerId) -> Self {
        self.worker = Some(worker);
        self
    }

    /// How long a lease lasts without renewal. Running attempts renew it
    /// every third of this. If a worker dies, others wait this long before
    /// treating its in-flight effect as unknown. Defaults to 30 seconds;
    /// values under 3 ms are raised to 3 ms.
    pub fn lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl.max(Duration::from_millis(3));
        self
    }

    /// Builds the runtime.
    pub fn build(self) -> Runtime<S> {
        Runtime {
            inner: Arc::new(Inner {
                store: self.store,
                clock: self.clock,
                worker: self.worker.unwrap_or_else(WorkerId::random),
                lease_ttl: self.lease_ttl,
            }),
        }
    }
}

/// Points where a crash can be simulated. Wired to the fault injector in M7.
#[derive(Clone, Copy, Debug)]
enum FaultPoint {
    Inserted,
    AttemptPersisted,
    ActionReturned,
}

fn checkpoint(_point: FaultPoint) {}

/// Why moving an effect forward stopped early.
enum Interrupt {
    /// Our lease expired or was taken over; re-read the record.
    LeaseLost,
    Error(RuntimeError),
}

impl From<StoreError> for Interrupt {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::LeaseLost => Self::LeaseLost,
            other => Self::Error(other.into()),
        }
    }
}

impl<S: EffectStore> Runtime<S> {
    /// A runtime with default settings.
    pub fn new(store: S) -> Self {
        Self::builder(store).build()
    }

    /// Starts configuring a runtime.
    pub fn builder(store: S) -> RuntimeBuilder<S> {
        RuntimeBuilder {
            store,
            clock: Arc::new(SystemClock),
            worker: None,
            lease_ttl: Duration::from_secs(30),
        }
    }

    /// Describes an effect: `name` is its type (e.g. `payment.charge`),
    /// `key` identifies this occurrence (e.g. the order id). Every call with
    /// the same name and key refers to the same effect.
    pub fn effect(&self, name: impl Into<String>, key: impl Display) -> EffectBuilder<S> {
        EffectBuilder::new(self.clone(), name.into(), key.to_string())
    }

    /// The underlying store.
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    /// This runtime's lease-holder identity.
    pub fn worker_id(&self) -> &WorkerId {
        &self.inner.worker
    }

    fn now(&self) -> SystemTime {
        self.inner.clock.now()
    }

    pub(crate) async fn execute<T, F, Fut>(
        &self,
        spec: EffectSpec,
        action: F,
    ) -> Result<EffectOutcome<T>, RuntimeError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
    {
        let span = info_span!(
            "agent_effect.execute",
            effect.name = %spec.key.name,
            effect.logical_key = %spec.key.key,
            effect.kind = spec.capabilities.kind.as_str(),
            effect.id = field::Empty,
            effect.status = field::Empty,
            effect.attempt = field::Empty,
        );
        if spec.capabilities.unknown_always_escalates() {
            span.in_scope(|| {
                warn!(
                    "effect is neither idempotent nor verifiable: \
                     any unknown outcome will need an operator"
                );
            });
        }
        // Run on a spawned task so that dropping the caller's future cannot
        // abort an attempt between invoking the action and recording it.
        let runtime = self.clone();
        let task = tokio::spawn(async move { runtime.drive(spec, action).await }.instrument(span));
        task.await
            .unwrap_or_else(|e| Err(RuntimeError::Internal(e.to_string())))
    }

    async fn drive<T, F, Fut>(
        &self,
        spec: EffectSpec,
        action: F,
    ) -> Result<EffectOutcome<T>, RuntimeError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
    {
        let store = self.store();
        let inserted = store
            .insert_or_get(NewEffect {
                id: EffectId::new(),
                key: spec.key.clone(),
                kind: spec.capabilities.kind,
                input: spec.input.clone(),
                input_fingerprint: spec.fingerprint.clone(),
                created_by: spec.actor.clone(),
                now: self.now(),
            })
            .await?;
        let mut record = inserted.record;
        Span::current().record("effect.id", field::display(record.id));
        checkpoint(FaultPoint::Inserted);
        if !inserted.inserted {
            check_matches(&record, &spec)?;
        }

        for _ in 0..MAX_ROUNDS {
            if let Some(outcome) = self.observe(&record)? {
                return Ok(outcome);
            }
            let lease = match store
                .acquire_lease(
                    record.id,
                    self.worker_id(),
                    self.now(),
                    self.inner.lease_ttl,
                )
                .await
            {
                Ok(lease) => lease,
                Err(StoreError::LeaseHeld { .. }) => {
                    return Ok(EffectOutcome::InProgress { id: record.id });
                }
                Err(e) => return Err(e.into()),
            };
            // Re-read under the lease: the record may have moved on since.
            let current = store
                .get(record.id)
                .await?
                .ok_or(StoreError::NotFound(record.id))?;
            let advanced = self.advance(current, &lease, &spec, &action).await;
            if let Err(e) = store.release_lease(&lease).await {
                warn!(error = %e, "could not release lease; it will expire");
            }
            match advanced {
                Ok((settled, output)) => {
                    Span::current().record("effect.status", settled.status.as_str());
                    return report(&settled, output);
                }
                Err(Interrupt::LeaseLost) => {
                    warn!("lease lost mid-effect; re-reading the record");
                    record = store
                        .get(record.id)
                        .await?
                        .ok_or(StoreError::NotFound(record.id))?;
                }
                Err(Interrupt::Error(e)) => return Err(e),
            }
        }
        Ok(EffectOutcome::InProgress { id: record.id })
    }

    /// The outcome to report without acting, or `None` if this call should
    /// take the lease and move the effect forward.
    fn observe<T: DeserializeOwned>(
        &self,
        record: &EffectRecord,
    ) -> Result<Option<EffectOutcome<T>>, RuntimeError> {
        match record.status {
            EffectStatus::Committed
            | EffectStatus::Failed
            | EffectStatus::Rejected
            | EffectStatus::NeedsIntervention => report(record, None).map(Some),
            EffectStatus::Pending
            | EffectStatus::Executing
            | EffectStatus::Verifying
            | EffectStatus::Unknown
                if record.live_lease_owner(self.now()).is_none() =>
            {
                Ok(None)
            }
            _ => Ok(Some(EffectOutcome::InProgress { id: record.id })),
        }
    }

    /// Moves a leased effect forward until it settles or needs something
    /// this call cannot do. Returns the record and, if this call ran the
    /// action successfully, its output.
    async fn advance<T, F, Fut>(
        &self,
        mut record: EffectRecord,
        lease: &Lease,
        spec: &EffectSpec,
        action: &F,
    ) -> Result<(EffectRecord, Option<T>), Interrupt>
    where
        T: Serialize + Send + 'static,
        F: Fn(EffectContext) -> Fut,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
    {
        let mut output = None;
        let mut attempted = false;
        loop {
            match record.status {
                EffectStatus::Pending => {
                    record = self
                        .transition(&record, lease, spec, Transition::StartAttempt, |r| {
                            r.payload = Some(json!({ "worker": self.worker_id() }));
                        })
                        .await?;
                    Span::current().record("effect.attempt", record.attempt_count);
                    checkpoint(FaultPoint::AttemptPersisted);
                    attempted = true;
                    (record, output) = self.attempt(record, lease, spec, action).await?;
                }
                // The previous holder's lease expired mid-attempt. Whatever
                // it was doing may have happened.
                EffectStatus::Executing | EffectStatus::Verifying => {
                    record = self
                        .transition(&record, lease, spec, Transition::LeaseExpired, |_| {})
                        .await?;
                }
                EffectStatus::Unknown => match spec.capabilities.unknown_plan() {
                    UnknownPlan::Escalate => {
                        record = self
                            .transition(&record, lease, spec, Transition::Escalate, |_| {})
                            .await?;
                    }
                    // Re-run an unknown outcome from an earlier call. One
                    // that this call produced is left for the next call
                    // until retry budgets exist (M4).
                    UnknownPlan::Reexecute if !attempted => {
                        record = self
                            .transition(&record, lease, spec, Transition::ScheduleRetry, |_| {})
                            .await?;
                    }
                    // Verification arrives in M4.
                    UnknownPlan::Reexecute | UnknownPlan::Verify => break,
                },
                _ => break,
            }
        }
        Ok((record, output))
    }

    /// Runs the action once, renewing the lease meanwhile, and records the
    /// result.
    async fn attempt<T, F, Fut>(
        &self,
        record: EffectRecord,
        lease: &Lease,
        spec: &EffectSpec,
        action: &F,
    ) -> Result<(EffectRecord, Option<T>), Interrupt>
    where
        T: Serialize + Send + 'static,
        F: Fn(EffectContext) -> Fut,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
    {
        let ctx = EffectContext {
            id: record.id,
            key: record.key.clone(),
            attempt: record.attempt_count,
        };
        // A separate task, so a panicking action is caught as a JoinError.
        let result = self.with_heartbeat(tokio::spawn(action(ctx)), lease).await;
        checkpoint(FaultPoint::ActionReturned);

        match result {
            Ok(Ok(value)) => {
                let (output, payload) = match serde_json::to_value(&value) {
                    Ok(output) => (Some(output), None),
                    // The effect applied; failing to store its output must
                    // not make it look failed. The caller still gets the value.
                    Err(e) => (None, Some(json!({ "output_not_stored": e.to_string() }))),
                };
                let record = self
                    .transition(&record, lease, spec, Transition::Succeeded, |r| {
                        r.output = output;
                        r.payload = payload;
                    })
                    .await?;
                Ok((record, Some(value)))
            }
            Ok(Err(failure)) => {
                let transition = match failure.class().disposition() {
                    Disposition::Unknown => Transition::OutcomeUnknown,
                    // No retries before M4: a retryable failure is final.
                    Disposition::Retry | Disposition::Fail => Transition::FailedDefinitively,
                };
                debug!(%failure, "action failed");
                let record = self
                    .transition(&record, lease, spec, transition, |r| {
                        r.error = Some(failure.to_record());
                    })
                    .await?;
                Ok((record, None))
            }
            Err(join_error) => {
                // The action may have sent its request before panicking.
                let record = self
                    .transition(&record, lease, spec, Transition::OutcomeUnknown, |r| {
                        r.error = Some(ErrorRecord {
                            class: Some(FailureClass::Ambiguous),
                            message: format!("action did not complete: {join_error}"),
                        });
                    })
                    .await?;
                Ok((record, None))
            }
        }
    }

    /// Awaits `task`, renewing `lease` every third of its TTL. A lost lease
    /// stops renewal but not the task: the attempt is already in flight, and
    /// the store will refuse our writes afterwards.
    async fn with_heartbeat<T>(
        &self,
        mut task: JoinHandle<T>,
        lease: &Lease,
    ) -> Result<T, JoinError> {
        let ttl = self.inner.lease_ttl;
        let period = ttl / 3;
        let mut renewing = true;
        loop {
            tokio::select! {
                result = &mut task => return result,
                () = tokio::time::sleep(period), if renewing => {
                    match self.store().renew_lease(lease, self.now(), ttl).await {
                        Ok(_) => {}
                        Err(StoreError::LeaseLost) => {
                            warn!("lease lost while the action was running");
                            renewing = false;
                        }
                        Err(e) => warn!(error = %e, "lease renewal failed; will retry"),
                    }
                }
            }
        }
    }

    async fn transition(
        &self,
        record: &EffectRecord,
        lease: &Lease,
        spec: &EffectSpec,
        transition: Transition,
        customize: impl FnOnce(&mut TransitionRequest),
    ) -> Result<EffectRecord, StoreError> {
        let mut request = TransitionRequest::new(record, Some(lease), transition, self.now());
        request.actor.clone_from(&spec.actor);
        customize(&mut request);
        let record = self.store().transition(request).await?;
        debug!(%transition, status = %record.status, "effect transition");
        Ok(record)
    }
}

fn check_matches(record: &EffectRecord, spec: &EffectSpec) -> Result<(), RuntimeError> {
    if record.kind != spec.capabilities.kind {
        return Err(RuntimeError::KindMismatch {
            id: record.id,
            stored: record.kind,
            requested: spec.capabilities.kind,
        });
    }
    if record.input_fingerprint != spec.fingerprint {
        return Err(RuntimeError::InputMismatch { id: record.id });
    }
    Ok(())
}

/// The outcome a record represents. `fresh` is this call's action output,
/// preferred over the stored copy.
fn report<T: DeserializeOwned>(
    record: &EffectRecord,
    fresh: Option<T>,
) -> Result<EffectOutcome<T>, RuntimeError> {
    let id = record.id;
    Ok(match record.status {
        EffectStatus::Committed => match fresh {
            Some(value) => EffectOutcome::Committed(value),
            None => EffectOutcome::Committed(
                serde_json::from_value(record.output.clone().unwrap_or(Value::Null))
                    .map_err(|source| RuntimeError::Output { id, source })?,
            ),
        },
        EffectStatus::Failed => EffectOutcome::Failed(last_error(record)),
        EffectStatus::Rejected => EffectOutcome::Rejected(last_error(record)),
        EffectStatus::NeedsIntervention => EffectOutcome::NeedsIntervention { id },
        EffectStatus::Unknown => EffectOutcome::Unknown { id },
        _ => EffectOutcome::InProgress { id },
    })
}

fn last_error(record: &EffectRecord) -> ErrorRecord {
    record.last_error.clone().unwrap_or_else(|| ErrorRecord {
        class: None,
        message: "no error was recorded".into(),
    })
}
