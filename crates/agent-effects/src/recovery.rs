//! Recovery and the operator API.
//!
//! - [`Runtime::recover`] marks attempts whose worker died as unknown, then
//!   finishes every unsettled effect that has a registered
//!   [handler](crate::handler) from its stored input. Closure effects are
//!   not durable, so they wait for a caller to re-run them;
//! - [`RecoveryConfig`] bounds how many effects one pass works on at once;
//! - [`Runtime::pending`] lists the effects waiting for a caller or an
//!   operator;
//! - [`Runtime::resolve`] records an operator's decision.
//!
//! # Concurrency
//!
//! By default a pass works on one effect at a time, as v0.2.0 did, so one
//! slow handler holds up every effect behind it.
//! [`RecoveryConfig::max_concurrency`] lets a pass work on several at once:
//!
//! ```
//! use agent_effects::{RecoveryConfig, Runtime};
//! use agent_effects_memory::MemoryStore;
//!
//! let runtime = Runtime::builder(MemoryStore::new())
//!     .recovery(RecoveryConfig::SEQUENTIAL.max_concurrency(8).batch_size(200))
//!     .build();
//! ```
//!
//! Concurrency changes how fast a backlog drains, never what happens to an
//! effect: each one is still worked on only under its lease, so two passes,
//! in one process or many, cannot both act on it. A pass never holds more
//! than `max_concurrency` effects in flight or more than one page of
//! `batch_size` records in memory. Every in-flight effect may use a store
//! connection, and so may its lease renewals: keep `max_concurrency` below
//! the store's connection pool size.
//!
//! # Shutdown
//!
//! [`Runtime::run_recovery_until`] stops at a signal: the pass under way
//! starts nothing new and waits for the effects it already started. Each
//! effect runs on a task of its own, so dropping a recovery future (or
//! aborting its task) never interrupts an effect between its action and
//! the record of its result; it only stops waiting for it. An effect cut
//! short by the process exiting is recovered like any other crash.

use std::collections::HashMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::task::{Context, Waker};
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};
use tokio::task::{JoinError, JoinSet};
use tokio::time::MissedTickBehavior;
use tracing::{Instrument, Span, info, warn};

use crate::error::RuntimeError;
use crate::id::EffectId;
use crate::runtime::Runtime;
use crate::state::{EffectStatus, Transition};
use crate::store::{
    EffectRecord, EffectStore, ErrorRecord, ListQuery, StoreError, TransitionRequest,
};

/// How a recovery pass schedules its work. See [concurrency](self#concurrency).
///
/// Set it with [`RuntimeBuilder::recovery`](crate::RuntimeBuilder::recovery)
/// for [`Runtime::recover`] and [`Runtime::run_recovery`], or pass one to
/// [`Runtime::recover_with`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryConfig {
    max_concurrency: usize,
    batch_size: usize,
}

impl RecoveryConfig {
    /// One effect at a time, scanning 100 records per page: the behavior of
    /// v0.2.0, and the default.
    pub const SEQUENTIAL: Self = Self {
        max_concurrency: 1,
        batch_size: 100,
    };

    /// Works on up to `max` effects at once. `0` is treated as `1`.
    #[must_use]
    pub const fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = if max == 0 { 1 } else { max };
        self
    }

    /// Reads `size` records per store scan. `0` is treated as `1`.
    #[must_use]
    pub const fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = if size == 0 { 1 } else { size };
        self
    }

    /// The most effects a pass works on at once.
    pub const fn concurrency_limit(&self) -> usize {
        self.max_concurrency
    }

    /// Records read per store scan.
    pub const fn page_size(&self) -> usize {
        self.batch_size
    }
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self::SEQUENTIAL
    }
}

/// What one [`Runtime::recover`] pass did.
///
/// With [`RecoveryConfig::max_concurrency`] above 1, effects appear in the
/// order they finished, not the order they were found.
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
    /// Effects whose scheduled retry was not due yet. A later pass resumes
    /// them once it is.
    pub not_due: Vec<EffectId>,
    /// The pass stopped early for a shutdown
    /// ([`Runtime::run_recovery_until`]). Effects it did not reach are left
    /// for a later pass.
    pub stopped: bool,
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
    /// The pass follows the runtime's [`RecoveryConfig`]: by default one
    /// effect at a time, so a pass lasts as long as their retries and
    /// verifications take. Safe to run from several workers at once: every
    /// change happens under a lease.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Store`] if the store fails. Progress made before the
    /// failure is kept, and effects already started are waited for before
    /// the error is returned. A handler that cannot run is reported in
    /// `resume_errors` instead.
    pub async fn recover(&self) -> Result<RecoveryReport, RuntimeError> {
        self.recover_with(self.recovery_config()).await
    }

    /// [`Self::recover`] with `config` instead of the runtime's own.
    ///
    /// # Errors
    ///
    /// As for [`Self::recover`].
    pub async fn recover_with(
        &self,
        config: RecoveryConfig,
    ) -> Result<RecoveryReport, RuntimeError> {
        let never = pin!(std::future::pending::<()>());
        self.recovery_pass(config, &mut Shutdown::new(never)).await
    }

    async fn recovery_pass<F: Future<Output = ()>>(
        &self,
        config: RecoveryConfig,
        shutdown: &mut Shutdown<'_, F>,
    ) -> Result<RecoveryReport, RuntimeError> {
        let mut report = RecoveryReport::default();
        self.mark_abandoned(config, &mut report, shutdown).await?;
        if !shutdown.fired() {
            self.resume_pending(config, &mut report, shutdown).await?;
        }
        report.stopped = shutdown.fired();
        Ok(report)
    }

    async fn mark_abandoned<F: Future<Output = ()>>(
        &self,
        config: RecoveryConfig,
        report: &mut RecoveryReport,
        shutdown: &mut Shutdown<'_, F>,
    ) -> Result<(), RuntimeError> {
        let mut tasks = Tasks::new(config.max_concurrency);
        let mut failure = None;
        let mut after = None;
        'scan: loop {
            let mut query = ListQuery::expired_leases(self.now()).limit(config.batch_size);
            if let Some(id) = after {
                query = query.after(id);
            }
            let page = match self.store().list(query).await {
                Ok(page) => page,
                Err(e) => {
                    failure = Some(e.into());
                    break;
                }
            };
            let full = page.len() == config.batch_size;
            after = page.last().map(|record| record.id);
            for record in page {
                if !tasks
                    .wait_for_slot(shutdown, |done| {
                        note_marked(report, &mut failure, done);
                    })
                    .await
                    || failure.is_some()
                {
                    break 'scan;
                }
                let runtime = self.clone();
                let id = record.id;
                tasks.spawn(id, async move { runtime.mark_unknown(id).await });
            }
            if !full || shutdown.check() {
                break;
            }
        }
        while let Some(done) = tasks.next().await {
            note_marked(report, &mut failure, done);
        }
        failure.map_or(Ok(()), Err)
    }

    async fn resume_pending<F: Future<Output = ()>>(
        &self,
        config: RecoveryConfig,
        report: &mut RecoveryReport,
        shutdown: &mut Shutdown<'_, F>,
    ) -> Result<(), RuntimeError> {
        let mut tasks = Tasks::new(config.max_concurrency);
        let mut failure = None;
        let mut after = None;
        'scan: loop {
            let page = match self.pending(after, config.batch_size).await {
                Ok(page) => page,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            let full = page.len() == config.batch_size;
            after = page.last().map(|record| record.id);
            for record in page {
                let id = record.id;
                let operator = matches!(
                    record.status,
                    EffectStatus::NeedsIntervention
                        | EffectStatus::CompensationFailed
                        | EffectStatus::AwaitingApproval
                );
                if operator {
                    continue;
                }
                if record.next_attempt_at.is_some_and(|at| at > self.now()) {
                    report.not_due.push(id);
                    continue;
                }
                let Some(resume) = self.resumer(record.key.name.as_str()) else {
                    report.unhandled.push(id);
                    continue;
                };
                if !tasks
                    .wait_for_slot(shutdown, |done| note_resumed(report, done))
                    .await
                {
                    break 'scan;
                }
                tasks.spawn(id, resume(self.clone(), record));
            }
            if !full || shutdown.check() {
                break;
            }
        }
        while let Some(done) = tasks.next().await {
            note_resumed(report, done);
        }
        failure.map_or(Ok(()), Err)
    }

    /// Runs [`Self::recover`] every `interval`, forever, followed by
    /// [`Self::prune`] when the runtime has a
    /// [retention policy](crate::RetentionPolicy). Spawn it:
    ///
    /// ```ignore
    /// tokio::spawn({
    ///     let runtime = runtime.clone();
    ///     async move { runtime.run_recovery(Duration::from_secs(30)).await }
    /// });
    /// ```
    ///
    /// A failed pass is logged and retried at the next tick. To stop it
    /// cleanly, use [`Self::run_recovery_until`].
    pub async fn run_recovery(&self, interval: Duration) {
        self.run_recovery_until(interval, std::future::pending())
            .await;
    }

    /// [`Self::run_recovery`] until `shutdown` completes, for a graceful
    /// stop. A pass under way when it does starts no further effects, waits
    /// for those it started, and is the last; no pruning follows it. A
    /// pass in a store's wait between ticks returns at once.
    ///
    /// ```ignore
    /// let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    /// let worker = tokio::spawn({
    ///     let runtime = runtime.clone();
    ///     async move {
    ///         runtime
    ///             .run_recovery_until(Duration::from_secs(30), async {
    ///                 let _ = stopped.await;
    ///             })
    ///             .await
    ///     }
    /// });
    /// // On SIGTERM:
    /// let _ = stop.send(());
    /// worker.await?;
    /// ```
    ///
    /// Waiting for started effects can take as long as their handlers do;
    /// bound it with a timeout if needed. Dropping this future is still
    /// safe: effects in flight finish on their own tasks.
    pub async fn run_recovery_until(&self, interval: Duration, shutdown: impl Future<Output = ()>) {
        let signal = pin!(shutdown);
        let mut shutdown = Shutdown::new(signal);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            if shutdown.wait(ticker.tick()).await.is_none() {
                return;
            }
            match self
                .recovery_pass(self.recovery_config(), &mut shutdown)
                .await
            {
                Ok(report)
                    if !report.marked_unknown.is_empty()
                        || !report.resumed.is_empty()
                        || !report.resume_errors.is_empty() =>
                {
                    info!(
                        marked_unknown = report.marked_unknown.len(),
                        resumed = report.resumed.len(),
                        resume_errors = report.resume_errors.len(),
                        unhandled = report.unhandled.len(),
                        not_due = report.not_due.len(),
                        "recovery pass"
                    );
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "recovery pass failed"),
            }
            if shutdown.fired() {
                return;
            }
            match self.prune().await {
                Ok(report) if report.total() > 0 => {
                    info!(pruned = report.total(), "pruned settled effects");
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "pruning failed"),
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
        self.resolve_with_output_schema(id, resolution, 1, actor, note)
            .await
    }

    /// Like [`Self::resolve`], recording `output_schema_version` when an
    /// operator supplies an applied result. The caller must authorize the
    /// operator and validate the output; actor text is an audit label only.
    ///
    /// # Errors
    ///
    /// As for [`Self::resolve`].
    pub async fn resolve_with_output_schema(
        &self,
        id: EffectId,
        resolution: Resolution,
        output_schema_version: u32,
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
            Resolution::Applied { output } => {
                request.output = output;
                request.output_schema_version = Some(output_schema_version);
            }
            Resolution::NotApplied => {
                request.error = Some(ErrorRecord {
                    class: None,
                    message: note,
                });
            }
            Resolution::Retry | Resolution::Compensated => {}
        }
        let record = self.commit_transition(&record, request).await?;
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
        let record = self.commit_transition(&record, request).await?;
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
            self.commit_transition(&record, request).await?;
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

fn note_marked(
    report: &mut RecoveryReport,
    failure: &mut Option<RuntimeError>,
    (id, done): (EffectId, Result<Result<bool, RuntimeError>, JoinError>),
) {
    match done {
        Ok(Ok(true)) => report.marked_unknown.push(id),
        Ok(Ok(false)) => report.skipped.push(id),
        Ok(Err(e)) => {
            failure.get_or_insert(e);
        }
        Err(e) => {
            failure.get_or_insert(RuntimeError::Internal(e.to_string()));
        }
    }
}

fn note_resumed(
    report: &mut RecoveryReport,
    (id, done): (
        EffectId,
        Result<Result<EffectStatus, RuntimeError>, JoinError>,
    ),
) {
    match done.unwrap_or_else(|e| Err(RuntimeError::Internal(e.to_string()))) {
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

/// The effects a pass is working on, each on a task of its own, at most
/// `limit` at once. Dropping it aborts the tasks, which only stops waiting:
/// an effect's attempt runs on a task the runtime spawned beneath it.
struct Tasks<T> {
    set: JoinSet<T>,
    ids: HashMap<tokio::task::Id, EffectId>,
    limit: usize,
}

impl<T: Send + 'static> Tasks<T> {
    fn new(limit: usize) -> Self {
        Self {
            set: JoinSet::new(),
            ids: HashMap::new(),
            limit,
        }
    }

    fn spawn(&mut self, id: EffectId, task: impl Future<Output = T> + Send + 'static) {
        let handle = self.set.spawn(task.instrument(Span::current()));
        self.ids.insert(handle.id(), id);
    }

    /// The next task to finish, or `None` once none are left.
    async fn next(&mut self) -> Option<(EffectId, Result<T, JoinError>)> {
        let joined = self.set.join_next_with_id().await?;
        let task = match &joined {
            Ok((task, _)) => *task,
            Err(e) => e.id(),
        };
        let id = self.ids.remove(&task)?;
        Some((id, joined.map(|(_, output)| output)))
    }

    /// Waits, handing each finished task to `done`, until there is room for
    /// another. Returns `false` if `shutdown` fired: start nothing more.
    async fn wait_for_slot<F: Future<Output = ()>>(
        &mut self,
        shutdown: &mut Shutdown<'_, F>,
        mut done: impl FnMut((EffectId, Result<T, JoinError>)),
    ) -> bool {
        while self.set.len() >= self.limit {
            match shutdown.wait(self.next()).await {
                Some(Some(finished)) => done(finished),
                Some(None) => break,
                None => return false,
            }
        }
        !shutdown.check()
    }
}

/// A shutdown signal that may be checked any number of times, also after
/// it fired.
struct Shutdown<'a, F> {
    signal: Pin<&'a mut F>,
    fired: bool,
}

impl<'a, F: Future<Output = ()>> Shutdown<'a, F> {
    fn new(signal: Pin<&'a mut F>) -> Self {
        Self {
            signal,
            fired: false,
        }
    }

    fn fired(&self) -> bool {
        self.fired
    }

    /// Whether the signal has fired, without waiting.
    fn check(&mut self) -> bool {
        if !self.fired {
            let mut cx = Context::from_waker(Waker::noop());
            self.fired = self.signal.as_mut().poll(&mut cx).is_ready();
        }
        self.fired
    }

    /// `work`'s output, or `None` if the signal fires first.
    async fn wait<T>(&mut self, work: impl Future<Output = T>) -> Option<T> {
        if self.fired {
            return None;
        }
        let signal = &mut self.signal;
        tokio::select! {
            biased;
            () = signal.as_mut() => {
                self.fired = true;
                None
            }
            output = work => Some(output),
        }
    }
}
