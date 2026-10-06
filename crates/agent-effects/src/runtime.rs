//! The effect runtime.
//!
//! [`Runtime::effect`] starts a builder; running it inserts or finds the
//! effect's record, then either reports its settled outcome or takes the
//! execution lease and moves it forward (design §7, "Re-attaching").

use std::collections::hash_map::RandomState;
use std::fmt::Display;
use std::future::Future;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tracing::{Instrument, Span, debug, field, info_span, warn};

use crate::clock::{Clock, SystemClock};
use crate::effect::{
    EffectBuilder, EffectContext, EffectFailure, EffectOutcome, EffectSpec, Precondition,
};
use crate::error::RuntimeError;
use crate::failure::{Disposition, FailureClass};
#[cfg(feature = "fault-injection")]
use crate::fault::FaultInjector;
use crate::fault::FaultPoint;
use crate::handler::{EffectHandler, Handler, Registered, Registry, Resume, Submission};
use crate::id::{EffectId, WorkerId};
use crate::kind::EffectKind;
use crate::policy::UnknownPlan;
use crate::retry::RetryPolicy;
use crate::state::{EffectStatus, Transition};
use crate::store::{
    EffectRecord, EffectStore, ErrorRecord, Lease, NewEffect, StoreError, TransitionRequest,
};
use crate::verification::{NotFoundReading, Verification, VerificationMode, Verifier};

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
    retry: RetryPolicy,
    handlers: Registry<S>,
    #[cfg(feature = "fault-injection")]
    faults: Option<Arc<FaultInjector>>,
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
    retry: RetryPolicy,
    handlers: Registry<S>,
    #[cfg(feature = "fault-injection")]
    faults: Option<Arc<FaultInjector>>,
}

impl<S: EffectStore> RuntimeBuilder<S> {
    /// Registers a durable handler under its [`EffectHandler::NAME`], so
    /// [`Runtime::submit`] can run it and [`Runtime::recover`] can finish
    /// its effects without a caller.
    ///
    /// # Panics
    ///
    /// If a handler is already registered under the same name.
    pub fn register<H: EffectHandler>(mut self, handler: Handler<H>) -> Self {
        assert!(
            !self.handlers.contains_key(H::NAME),
            "a handler is already registered for effect `{}`",
            H::NAME
        );
        self.handlers.insert(H::NAME, Registered::new(handler));
        self
    }

    /// The time source for leases and schedules. Defaults to the system
    /// clock. Tests with paused Tokio time want
    /// [`TokioClock`](crate::clock::TokioClock).
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

    /// How long a lease lasts without renewal. The runtime renews it every
    /// third of this while it works on an effect, including while it waits
    /// between retries. If a worker dies, others wait this long before
    /// treating its in-flight effect as unknown. Defaults to 30 seconds;
    /// values under 3 ms are raised to 3 ms.
    pub fn lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl.max(Duration::from_millis(3));
        self
    }

    /// The retry policy for effects that do not set their own. Defaults to
    /// [`RetryPolicy::default`]: 5 attempts, 1 s to 30 s exponential backoff
    /// with jitter.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// Simulates crashes at the injector's armed points. For tests; see
    /// [`fault`](crate::fault).
    #[cfg(feature = "fault-injection")]
    pub fn fault_injector(mut self, injector: Arc<FaultInjector>) -> Self {
        self.faults = Some(injector);
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
                retry: self.retry,
                handlers: self.handlers,
                #[cfg(feature = "fault-injection")]
                faults: self.faults,
            }),
        }
    }
}

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
            retry: RetryPolicy::default(),
            handlers: Registry::new(),
            #[cfg(feature = "fault-injection")]
            faults: None,
        }
    }

    /// Describes an effect: `name` is its type (e.g. `payment.charge`),
    /// `key` identifies this occurrence (e.g. the order id). Every call with
    /// the same name and key refers to the same effect.
    pub fn effect(&self, name: impl Into<String>, key: impl Display) -> EffectBuilder<S> {
        EffectBuilder::new(self.clone(), name.into(), key.to_string())
    }

    /// Runs a registered handler's effect with `input`, or attaches to an
    /// earlier run with the same `key`. Await the returned [`Submission`].
    ///
    /// Behaves like [`EffectBuilder::run`], with the handler's properties.
    /// The input is stored in full, so recovery can finish the effect if
    /// this process dies.
    pub fn submit<H: EffectHandler>(
        &self,
        key: impl Display,
        input: H::Input,
    ) -> Submission<'_, S, H> {
        Submission::new(self, key, input)
    }

    pub(crate) fn handler<H: EffectHandler>(&self) -> Option<Handler<H>> {
        self.inner.handlers.get(H::NAME)?.typed::<H>()
    }

    pub(crate) fn resumer(&self, name: &str) -> Option<Resume<S>> {
        self.inner.handlers.get(name).map(|r| Arc::clone(&r.resume))
    }

    /// The underlying store.
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    /// This runtime's lease-holder identity.
    pub fn worker_id(&self) -> &WorkerId {
        &self.inner.worker
    }

    /// Waits until nobody is working on effect `id`, or `timeout` passes, and
    /// reports where it stands. For a caller that got
    /// [`EffectOutcome::InProgress`].
    ///
    /// Returns `InProgress` if the effect is still being worked on at the
    /// deadline, or if it is unsettled and nobody holds it (for example a
    /// worker crashed while waiting to retry). Running the effect again then
    /// takes it over.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Store`] if the store fails or has no such effect, and
    /// [`RuntimeError::Output`] if a committed output does not deserialize
    /// into `T`.
    pub async fn wait<T: DeserializeOwned>(
        &self,
        id: EffectId,
        timeout: Duration,
    ) -> Result<EffectOutcome<T>, RuntimeError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut pause = Duration::from_millis(10);
        loop {
            let record = self
                .store()
                .get(id)
                .await?
                .ok_or(StoreError::NotFound(id))?;
            let busy = !settled(record.status) && record.live_lease_owner(self.now()).is_some();
            let now = tokio::time::Instant::now();
            if !busy {
                return report(&record, None);
            }
            if now >= deadline {
                return Ok(EffectOutcome::InProgress { id });
            }
            tokio::time::sleep(pause.min(deadline - now)).await;
            pause = (pause * 2).min(Duration::from_millis(250));
        }
    }

    pub(crate) fn default_retry(&self) -> RetryPolicy {
        self.inner.retry
    }

    pub(crate) fn now(&self) -> SystemTime {
        self.inner.clock.now()
    }

    pub(crate) fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl
    }

    /// A point where a crash can be injected; a no-op without the
    /// `fault-injection` feature.
    #[cfg_attr(not(feature = "fault-injection"), allow(clippy::unused_self))]
    fn checkpoint(&self, point: FaultPoint) {
        #[cfg(feature = "fault-injection")]
        if let Some(faults) = &self.inner.faults {
            faults.reach(point);
        }
        #[cfg(not(feature = "fault-injection"))]
        let _ = point;
    }

    pub(crate) async fn execute<T, F, Fut, V>(
        &self,
        spec: EffectSpec,
        action: F,
        verifier: V,
    ) -> Result<EffectOutcome<T>, RuntimeError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
        V: Verifier<T>,
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
        let task = tokio::spawn(
            async move { runtime.drive(spec, action, verifier).await }.instrument(span),
        );
        task.await
            .unwrap_or_else(|e| Err(RuntimeError::Internal(e.to_string())))
    }

    async fn drive<T, F, Fut, V>(
        &self,
        spec: EffectSpec,
        action: F,
        verifier: V,
    ) -> Result<EffectOutcome<T>, RuntimeError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
        V: Verifier<T>,
    {
        self.checkpoint(FaultPoint::BeforeInsert);
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
        self.checkpoint(FaultPoint::AfterInsert);
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
            let driver = Driver {
                rt: self,
                spec: &spec,
                action: &action,
                verifier: &verifier,
                lease: &lease,
            };
            let advanced = driver.advance(current).await;
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
            status if settled(status) => report(record, None).map(Some),
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
}

/// One call's work on one leased effect.
struct Driver<'a, S, F, V> {
    rt: &'a Runtime<S>,
    spec: &'a EffectSpec,
    action: &'a F,
    verifier: &'a V,
    lease: &'a Lease,
}

impl<S: EffectStore, F, V> Driver<'_, S, F, V> {
    /// Moves the effect forward until it settles or needs something this
    /// call cannot do. Returns the record and, if this call produced one, the
    /// output to report.
    async fn advance<T, Fut>(
        &self,
        mut record: EffectRecord,
    ) -> Result<(EffectRecord, Option<T>), Interrupt>
    where
        T: Serialize + Send + 'static,
        F: Fn(EffectContext) -> Fut,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
        V: Verifier<T>,
    {
        let mut output = None;
        let mut verification_exhausted = false;
        loop {
            match record.status {
                EffectStatus::Pending => {
                    if let Some(at) = record.next_attempt_at {
                        self.sleep_until(at).await?;
                    }
                    if record.attempt_count == 0
                        && let Some(reason) = self.check_precondition(&record).await?
                    {
                        record = self
                            .transition(&record, Transition::PreconditionRejected, |r| {
                                r.error = Some(reason);
                            })
                            .await?;
                        continue;
                    }
                    record = self
                        .transition(&record, Transition::StartAttempt, |r| {
                            r.payload = Some(json!({ "worker": self.rt.worker_id() }));
                        })
                        .await?;
                    Span::current().record("effect.attempt", record.attempt_count);
                    self.rt.checkpoint(FaultPoint::AfterAttemptPersisted);
                    let (next, produced) = self.attempt(record).await?;
                    record = next;
                    if produced.is_some() {
                        output = produced;
                    }
                }
                // The previous holder's lease expired mid-attempt or
                // mid-verification. Whatever it was doing may have happened.
                EffectStatus::Executing | EffectStatus::Verifying => {
                    record = self
                        .transition(&record, Transition::LeaseExpired, |_| {})
                        .await?;
                }
                EffectStatus::Unknown => match self.spec.capabilities.unknown_plan() {
                    UnknownPlan::Verify if !verification_exhausted => {
                        let (next, verified, exhausted) = self.verify(record, None).await?;
                        record = next;
                        output = verified.or(output);
                        verification_exhausted = exhausted;
                    }
                    // Still unknown after every check this call may make;
                    // a later call or recovery tries again.
                    UnknownPlan::Verify => break,
                    UnknownPlan::Reexecute if self.retry().allows_another(record.attempt_count) => {
                        record = self
                            .schedule_retry(&record, FailureClass::Ambiguous, None)
                            .await?;
                    }
                    UnknownPlan::Reexecute | UnknownPlan::Escalate => {
                        record = self
                            .transition(&record, Transition::Escalate, |_| {})
                            .await?;
                    }
                },
                _ => break,
            }
        }
        Ok((record, output))
    }

    /// Runs the action once and records what happened.
    async fn attempt<T, Fut>(
        &self,
        record: EffectRecord,
    ) -> Result<(EffectRecord, Option<T>), Interrupt>
    where
        T: Serialize + Send + 'static,
        F: Fn(EffectContext) -> Fut,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
        V: Verifier<T>,
    {
        // A separate task, so a panicking action is caught as a JoinError.
        // If the lease is lost while waiting, the handle is dropped and the
        // task finishes detached: the request is already in flight.
        let mut task = tokio::spawn((self.action)(context(&record)));
        self.rt.checkpoint(FaultPoint::AfterActionStarted);
        let joined = match self.spec.attempt_timeout {
            None => self.leased(&mut task).await?,
            Some(limit) => match self.leased(tokio::time::timeout(limit, &mut task)).await? {
                Ok(joined) => joined,
                Err(_elapsed) => {
                    task.abort();
                    Ok(Err(EffectFailure::ambiguous(format!(
                        "attempt timed out after {limit:?}"
                    ))))
                }
            },
        };
        self.rt.checkpoint(FaultPoint::AfterActionReturned);

        match joined {
            Ok(Ok(value)) if self.spec.capabilities.verification != VerificationMode::None => {
                let (record, verified, _) = self.verify(record, Some(output_json(&value))).await?;
                let output = match record.status {
                    EffectStatus::Committed => verified.or(Some(value)),
                    _ => None,
                };
                Ok((record, output))
            }
            Ok(Ok(value)) => {
                let (output, payload) = output_json(&value);
                let record = self
                    .transition(&record, Transition::Succeeded, |r| {
                        r.output = output;
                        r.payload = payload;
                    })
                    .await?;
                Ok((record, Some(value)))
            }
            Ok(Err(failure)) => {
                debug!(%failure, "action failed");
                let class = failure.class();
                let error = failure.to_record();
                let record = match class.disposition() {
                    Disposition::Retry if self.retry().allows_another(record.attempt_count) => {
                        self.schedule_retry(&record, class, Some(error)).await?
                    }
                    // This attempt definitely failed, but an earlier one may
                    // have applied the effect: that is not "Failed". Look
                    // again if the effect can be verified, else escalate.
                    Disposition::Retry | Disposition::Fail
                        if record.may_have_applied && record.kind != EffectKind::Read =>
                    {
                        let unknown = self
                            .transition(&record, Transition::OutcomeUnknown, |r| {
                                r.error = Some(error);
                                r.payload =
                                    Some(json!({ "earlier_attempt_may_have_applied": true }));
                            })
                            .await?;
                        if self.spec.capabilities.unknown_plan() == UnknownPlan::Verify {
                            unknown
                        } else {
                            self.transition(&unknown, Transition::Escalate, |_| {})
                                .await?
                        }
                    }
                    Disposition::Retry | Disposition::Fail => {
                        self.transition(&record, Transition::FailedDefinitively, |r| {
                            r.error = Some(error);
                        })
                        .await?
                    }
                    Disposition::Unknown => {
                        self.transition(&record, Transition::OutcomeUnknown, |r| {
                            r.error = Some(error);
                        })
                        .await?
                    }
                };
                Ok((record, None))
            }
            Err(join_error) => {
                // The action may have sent its request before panicking.
                let record = self
                    .transition(&record, Transition::OutcomeUnknown, |r| {
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

    /// Asks the remote system what happened, starting from `Executing`
    /// (after a success, whose output JSON and audit note are `succeeded`)
    /// or `Unknown`.
    ///
    /// Ends in `Committed`, `Failed`, `Pending` (re-run scheduled: the effect
    /// verifiably did not apply), `NeedsIntervention` (conflict) or `Unknown`.
    /// The flag reports that the checks ran out while inconclusive.
    async fn verify<T>(
        &self,
        record: EffectRecord,
        succeeded: Option<(Option<Value>, Option<Value>)>,
    ) -> Result<(EffectRecord, Option<T>, bool), Interrupt>
    where
        T: Serialize + Send + 'static,
        V: Verifier<T>,
    {
        let mut record = self
            .transition(&record, Transition::StartVerification, |r| {
                if let Some((output, payload)) = succeeded {
                    (r.output, r.payload) = (output, payload);
                }
            })
            .await?;
        self.rt.checkpoint(FaultPoint::AfterVerificationStarted);
        let mode = self.spec.capabilities.verification;
        let max_checks = self.retry().max_attempts.max(1);
        let mut last_problem = String::from("no check completed");

        for check in 0..max_checks {
            let Some(future) = self.verifier.check(context(&record)) else {
                break;
            };
            let found = match self.leased(tokio::spawn(future)).await? {
                Ok(Ok(found)) => found,
                Ok(Err(failure)) => {
                    last_problem = format!("check failed: {failure}");
                    Verification::Inconclusive
                }
                Err(join_error) => {
                    last_problem = format!("check did not complete: {join_error}");
                    Verification::Inconclusive
                }
            };
            match found {
                Verification::Confirmed(value) => {
                    let (output, payload) = output_json(&value);
                    record = self
                        .transition(&record, Transition::VerificationConfirmed, |r| {
                            r.output = output;
                            r.payload = payload;
                        })
                        .await?;
                    return Ok((record, Some(value), false));
                }
                Verification::Conflict { details } => {
                    record = self
                        .transition(&record, Transition::VerificationConflict, |r| {
                            r.error = Some(ErrorRecord {
                                class: None,
                                message: details,
                            });
                        })
                        .await?;
                    return Ok((record, None, false));
                }
                Verification::NotApplied => {
                    let started = record.attempt_started_at.unwrap_or(self.rt.now());
                    let elapsed = self.rt.now().duration_since(started).unwrap_or_default();
                    match mode.read_not_found(elapsed) {
                        NotFoundReading::NotApplied => {
                            let error = ErrorRecord {
                                class: None,
                                message: "verification found that the effect did not apply".into(),
                            };
                            record = if self.retry().allows_another(record.attempt_count) {
                                self.schedule_retry(&record, FailureClass::Transient, Some(error))
                                    .await?
                            } else {
                                self.transition(&record, Transition::VerificationNotApplied, |r| {
                                    r.error = Some(error);
                                })
                                .await?
                            };
                            return Ok((record, None, false));
                        }
                        NotFoundReading::TooEarly { wait } => {
                            last_problem = "not visible yet within the settle delay".into();
                            self.sleep(wait).await?;
                        }
                    }
                }
                Verification::Inconclusive => {
                    if last_problem == "no check completed" {
                        last_problem = "remote system could not tell".into();
                    }
                    let delay = self
                        .retry()
                        .delay(check, FailureClass::Transient, jitter_sample());
                    self.sleep(delay).await?;
                }
            }
        }

        record = self
            .transition(&record, Transition::OutcomeUnknown, |r| {
                r.error = Some(ErrorRecord {
                    class: Some(FailureClass::Ambiguous),
                    message: format!(
                        "verification inconclusive after {max_checks} checks: {last_problem}"
                    ),
                });
            })
            .await?;
        Ok((record, None, true))
    }

    /// Evaluates the precondition. Returns the reason to reject, if any.
    async fn check_precondition(
        &self,
        record: &EffectRecord,
    ) -> Result<Option<ErrorRecord>, Interrupt> {
        let Some(precondition) = &self.spec.precondition else {
            return Ok(None);
        };
        let max_checks = self.retry().max_attempts.max(1);
        let mut last_reason = String::new();
        for check in 1..=max_checks {
            let rejection = |message: String| {
                Some(ErrorRecord {
                    class: None,
                    message,
                })
            };
            match self
                .leased(tokio::spawn(precondition(context(record))))
                .await?
            {
                Ok(Precondition::Satisfied) => return Ok(None),
                Ok(Precondition::Rejected { reason }) => return Ok(rejection(reason)),
                Ok(Precondition::RetryLater { after, reason }) => {
                    last_reason = reason;
                    if check < max_checks {
                        self.sleep(after).await?;
                    }
                }
                // A broken check must not let the effect through.
                Err(join_error) => {
                    return Ok(rejection(format!(
                        "precondition check did not complete: {join_error}"
                    )));
                }
            }
        }
        Ok(Some(ErrorRecord {
            class: None,
            message: format!("precondition not satisfied after {max_checks} checks: {last_reason}"),
        }))
    }

    /// Records that the next attempt waits for a backoff delay. The wait
    /// itself happens when the loop next sees the record as `Pending`, so a
    /// crash during it leaves a record that is safe to resume.
    async fn schedule_retry(
        &self,
        record: &EffectRecord,
        class: FailureClass,
        error: Option<ErrorRecord>,
    ) -> Result<EffectRecord, Interrupt> {
        let retry = record.attempt_count.saturating_sub(1);
        let delay = self.retry().delay(retry, class, jitter_sample());
        let at = self.rt.now() + delay;
        debug!(?delay, ?class, "retry scheduled");
        self.transition(record, Transition::ScheduleRetry, |r| {
            r.next_attempt_at = Some(at);
            r.error = error;
            r.payload =
                Some(json!({ "delay_ms": u64::try_from(delay.as_millis()).unwrap_or(u64::MAX) }));
        })
        .await
    }

    async fn sleep_until(&self, at: SystemTime) -> Result<(), Interrupt> {
        let wait = at.duration_since(self.rt.now()).unwrap_or_default();
        self.sleep(wait).await
    }

    async fn sleep(&self, duration: Duration) -> Result<(), Interrupt> {
        if duration.is_zero() {
            return Ok(());
        }
        self.leased(tokio::time::sleep(duration)).await
    }

    /// Awaits `future`, renewing the lease every third of its TTL. Stops
    /// with [`Interrupt::LeaseLost`] if the lease is lost meanwhile.
    async fn leased<Fut: Future>(&self, future: Fut) -> Result<Fut::Output, Interrupt> {
        let ttl = self.rt.inner.lease_ttl;
        tokio::pin!(future);
        loop {
            tokio::select! {
                output = &mut future => return Ok(output),
                () = tokio::time::sleep(ttl / 3) => {
                    match self.rt.store().renew_lease(self.lease, self.rt.now(), ttl).await {
                        Ok(_) => {}
                        Err(StoreError::LeaseLost) => return Err(Interrupt::LeaseLost),
                        Err(e) => warn!(error = %e, "lease renewal failed; will retry"),
                    }
                }
            }
        }
    }

    async fn transition(
        &self,
        record: &EffectRecord,
        transition: Transition,
        customize: impl FnOnce(&mut TransitionRequest),
    ) -> Result<EffectRecord, Interrupt> {
        let mut request =
            TransitionRequest::new(record, Some(self.lease), transition, self.rt.now());
        request.actor.clone_from(&self.spec.actor);
        customize(&mut request);
        let record = self.rt.store().transition(request).await?;
        debug!(%transition, status = %record.status, "effect transition");
        Ok(record)
    }

    fn retry(&self) -> &RetryPolicy {
        &self.spec.retry
    }
}

fn context(record: &EffectRecord) -> EffectContext {
    EffectContext {
        id: record.id,
        key: record.key.clone(),
        attempt: record.attempt_count,
    }
}

/// An output as JSON, or a note for the audit trail if it cannot be stored.
/// The effect applied either way: failing to store its output must not make
/// it look failed, and the caller still gets the value.
fn output_json<T: Serialize>(value: &T) -> (Option<Value>, Option<Value>) {
    match serde_json::to_value(value) {
        Ok(output) => (Some(output), None),
        Err(e) => (None, Some(json!({ "output_not_stored": e.to_string() }))),
    }
}

/// A uniform sample in `[0, 1)` for jitter, from the standard library's
/// per-instance random hash keys.
fn jitter_sample() -> f64 {
    let bits = RandomState::new().hash_one(0_u8) >> 11;
    #[allow(clippy::cast_precision_loss)]
    let sample = bits as f64 / (1_u64 << 53) as f64;
    sample
}

/// Statuses a new call reports as they are, without acting.
fn settled(status: EffectStatus) -> bool {
    matches!(
        status,
        EffectStatus::Committed
            | EffectStatus::Failed
            | EffectStatus::Rejected
            | EffectStatus::NeedsIntervention
    )
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

/// The outcome a record represents. `fresh` is this call's output,
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
