//! Lease-free retry scheduling: a retry's time is stored, the lease is
//! released for the wait, and a fresh lease is taken (and the record re-read)
//! once the retry is due. Releasing the lease must never let a retry start
//! early, run twice, exceed its budget or gain a permission the stored state
//! did not grant.
//!
//! Paused Tokio time with a `TokioClock`: minutes of backoff run instantly.
#[path = "support/fault_store.rs"]
mod fault_store;
#[path = "support/retry_load.rs"]
mod retry_load;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::store::{EffectStore, ErrorRecord, Transition};
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    Clock, CompensableEffect, CompensationContext, CompensationOutcome, EffectContext,
    EffectFailure, EffectHandler, EffectId, EffectKey, EffectKind, EffectName, EffectOutcome,
    EffectRecord, EffectStatus, FailureClass, Handler, LogicalKey, RetryPolicy, Runtime,
    RuntimeBuilder, TokioClock, VerifiableEffect, Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;
use fault_store::FaultStore;
use tokio::time::Instant;

const TTL: Duration = Duration::from_secs(30);
const NAME: &str = "remote.create";
const RES: &str = "res";
/// One minute between attempts: well over the default lease-free
/// threshold (the lease TTL).
const MINUTE: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    initial_delay: Duration::from_secs(60),
    max_delay: Duration::from_secs(60),
    multiplier: 1.0,
    jitter: false,
};

type Store = FaultStore<MemoryStore>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T, EffectFailure>> + Send>>;

fn builder(store: &Store, clock: TokioClock, worker: &str) -> RuntimeBuilder<Store> {
    Runtime::builder(store.clone())
        .clock(clock)
        .lease_ttl(TTL)
        .retry_policy(MINUTE)
        .worker_id(WorkerId::new(worker))
}

fn setup() -> (Store, TokioClock) {
    (FaultStore::new(MemoryStore::new()), TokioClock::new())
}

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(NAME).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

async fn record(store: &Store, logical: &str) -> EffectRecord {
    store.get_by_key(&key(logical)).await.unwrap().unwrap()
}

async fn transitions(store: &Store, logical: &str) -> Vec<Transition> {
    let id = record(store, logical).await.id;
    store
        .events(id)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.transition)
        .collect()
}

fn create(
    remote: &FakeRemote,
) -> impl Fn(EffectContext) -> BoxFuture<String> + Send + Sync + use<> {
    let remote = remote.clone();
    move |_| {
        let remote = remote.clone();
        Box::pin(async move { remote.create(RES, None).await })
    }
}

fn quiet_crashes() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !info.to_string().contains("fault injected") {
                default(info);
            }
        }));
    });
}

/// Lets paused time run up to `at` from the start, with every task idle.
async fn until(start: Instant, at: Duration) {
    tokio::time::sleep_until(start + at).await;
}

#[tokio::test(start_paused = true)]
async fn a_long_backoff_is_waited_out_without_the_lease() {
    let (store, clock) = setup();
    let rt = builder(&store, clock, "w").build();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let (start, t0) = (Instant::now(), clock.now());
    let call = tokio::spawn({
        let (rt, remote) = (rt.clone(), remote.clone());
        async move { rt.effect(NAME, "k").run(create(&remote)).await }
    });

    until(start, Duration::from_secs(1)).await;
    let waiting = record(&store, "k").await;
    assert_eq!(waiting.status, EffectStatus::Pending);
    assert_eq!(
        waiting.next_attempt_at,
        Some(t0 + Duration::from_secs(60)),
        "the schedule is stored before the lease is released"
    );
    assert_eq!(
        waiting.live_lease_owner(clock.now()),
        None,
        "nobody holds it"
    );
    assert_eq!(waiting.lease_owner, None, "released, not left to expire");

    until(start, Duration::from_secs(59)).await;
    assert_eq!(remote.requests(), 1, "nothing runs before the retry is due");

    let outcome = call.await.unwrap().unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert!(start.elapsed() >= Duration::from_secs(60));
    assert_eq!(remote.requests(), 2);
    assert_eq!(
        store.controls.renewals.load(Ordering::SeqCst),
        0,
        "no lease was renewed through a minute of backoff"
    );
    assert_eq!(
        transitions(&store, "k").await,
        [
            Transition::StartAttempt,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
            Transition::Succeeded,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_short_backoff_keeps_the_lease() {
    let (store, clock) = setup();
    let short = RetryPolicy {
        initial_delay: Duration::from_secs(5),
        max_delay: Duration::from_secs(5),
        ..MINUTE
    };
    let rt = builder(&store, clock, "w").retry_policy(short).build();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    let call = tokio::spawn({
        let (rt, remote) = (rt.clone(), remote.clone());
        async move { rt.effect(NAME, "k").run(create(&remote)).await }
    });
    until(start, Duration::from_secs(1)).await;
    let waiting = record(&store, "k").await;
    assert_eq!(waiting.status, EffectStatus::Pending);
    assert_eq!(
        waiting.live_lease_owner(clock.now()),
        Some(&WorkerId::new("w")),
        "below the threshold (the lease TTL) the lease is kept"
    );
    assert_eq!(
        call.await.unwrap().unwrap(),
        EffectOutcome::Committed("res#1".into())
    );
}

#[tokio::test(start_paused = true)]
async fn lease_free_backoff_can_be_turned_off() {
    let (store, clock) = setup();
    let rt = builder(&store, clock, "w")
        .lease_free_backoff(Duration::MAX)
        .build();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    let call = tokio::spawn({
        let (rt, remote) = (rt.clone(), remote.clone());
        async move { rt.effect(NAME, "k").run(create(&remote)).await }
    });
    until(start, Duration::from_secs(30)).await;
    assert_eq!(
        record(&store, "k").await.live_lease_owner(clock.now()),
        Some(&WorkerId::new("w")),
        "held through the backoff, as in 0.2.0"
    );
    assert_eq!(
        call.await.unwrap().unwrap(),
        EffectOutcome::Committed("res#1".into())
    );
    assert!(store.controls.renewals.load(Ordering::SeqCst) >= 5);
}

#[tokio::test(start_paused = true)]
async fn the_attempt_budget_holds_across_lease_free_backoffs() {
    let (store, clock) = setup();
    let five = RetryPolicy {
        max_attempts: 5,
        ..MINUTE
    };
    let rt = builder(&store, clock, "w").retry_policy(five).build();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient); 9]);
    let outcome = rt
        .effect(NAME, "k")
        .retry(five)
        .run(create(&remote))
        .await
        .unwrap();
    // Four lease-free waits in one call: none of them counts as a lost
    // round, so the call reports the real outcome, not "in progress".
    assert!(
        matches!(
            outcome,
            EffectOutcome::Failed(ErrorRecord {
                class: Some(FailureClass::Transient),
                ..
            })
        ),
        "{outcome:?}"
    );
    assert_eq!(remote.requests(), 5, "exactly the lifetime budget");
    assert_eq!(record(&store, "k").await.attempt_count, 5);
}

#[tokio::test(start_paused = true)]
async fn other_callers_and_recovery_never_start_a_retry_early() {
    let (store, clock) = setup();
    let first = builder(&store, clock, "first").build();
    let second = builder(&store, clock, "second").build();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    let a = tokio::spawn({
        let (rt, remote) = (first.clone(), remote.clone());
        async move { rt.effect(NAME, "k").run(create(&remote)).await }
    });
    until(start, Duration::from_secs(1)).await;

    // The lease is free, so another caller can take it, but finds the retry
    // not due and waits too, without the lease.
    let b = tokio::spawn({
        let (rt, remote) = (second.clone(), remote.clone());
        async move { rt.effect(NAME, "k").run(create(&remote)).await }
    });
    until(start, Duration::from_secs(2)).await;
    let report = second.recover().await.unwrap();
    let id = record(&store, "k").await.id;
    assert_eq!(report.not_due, [id], "recovery leaves it for later");
    assert_eq!(report.resumed, [] as [(EffectId, EffectStatus); 0]);
    until(start, Duration::from_secs(59)).await;
    assert_eq!(remote.requests(), 1);

    // Both wake when it is due: one runs it, the other waits for that run.
    let (a, b) = (a.await.unwrap().unwrap(), b.await.unwrap().unwrap());
    assert_eq!(a, EffectOutcome::Committed("res#1".into()));
    assert_eq!(b, a);
    assert_eq!(remote.requests(), 2, "the retry ran once");
    assert_eq!(remote.applications(RES), 1);
}

#[tokio::test(start_paused = true)]
async fn rate_limits_are_honoured_without_the_lease() {
    let (store, clock) = setup();
    let rt = builder(&store, clock, "w").build();
    let calls = Arc::new(AtomicU32::new(0));
    let start = Instant::now();
    let outcome = rt
        .effect(NAME, "k")
        .kind(EffectKind::IdempotentWrite)
        .run({
            let calls = Arc::clone(&calls);
            move |_| {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        Err(EffectFailure::rate_limited(
                            Some(Duration::from_secs(300)),
                            "slow down",
                        ))
                    } else {
                        Ok::<_, EffectFailure>("done".to_string())
                    }
                }
            }
        })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("done".into()));
    assert!(
        start.elapsed() >= Duration::from_secs(300),
        "waited the provider's Retry-After, longer than the policy's backoff"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.controls.renewals.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn retry_after_on_an_ambiguous_outcome_never_authorizes_a_rerun() {
    let (store, clock) = setup();
    let rt = builder(&store, clock, "w").build();
    let calls = Arc::new(AtomicU32::new(0));
    // Neither idempotent nor verifiable: a Retry-After header says when the
    // provider wants to hear from us, not that the request did not apply.
    let outcome = rt
        .effect(NAME, "k")
        .run({
            let calls = Arc::clone(&calls);
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Err::<String, _>(
                        EffectFailure::ambiguous("gateway timeout")
                            .with_retry_after(Duration::from_secs(120)),
                    )
                }
            }
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    tokio::time::sleep(Duration::from_secs(600)).await;
    let report = rt.recover().await.unwrap();
    assert!(report.resumed.is_empty() && report.not_due.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 1, "never re-run");
    assert_eq!(
        record(&store, "k").await.status,
        EffectStatus::NeedsIntervention
    );
}

#[tokio::test(start_paused = true)]
async fn an_ambiguous_idempotent_effect_retries_after_retry_after_without_the_lease() {
    let (store, clock) = setup();
    let rt = builder(&store, clock, "w").build();
    let remote = FakeRemote::new(clock).script([Behavior::LoseRequest]);
    let start = Instant::now();
    // The provider deduplicates on the idempotency key: re-sending is safe.
    let outcome = rt
        .effect(NAME, "k")
        .remote_idempotency(true)
        .run({
            let remote = remote.clone();
            move |ctx: EffectContext| {
                let remote = remote.clone();
                Box::pin(async move {
                    remote
                        .create(RES, Some(ctx.idempotency_key()))
                        .await
                        .map_err(|e| e.with_retry_after(Duration::from_secs(90)))
                }) as BoxFuture<String>
            }
        })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert!(
        start.elapsed() >= Duration::from_secs(90),
        "Retry-After kept"
    );
    assert_eq!(remote.applications(RES), 1);
    assert_eq!(store.controls.renewals.load(Ordering::SeqCst), 0);
}

/// A verifiable handler that creates `RES` on a `FakeRemote`.
#[derive(Clone)]
struct Create {
    remote: FakeRemote,
    executions: Arc<AtomicU32>,
}

impl EffectHandler for Create {
    const NAME: &'static str = NAME;
    type Input = String;
    type Output = String;
    type Error = EffectFailure;

    async fn execute(&self, _: &EffectContext, resource: &String) -> Result<String, EffectFailure> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.remote.create(resource, None).await
    }
}

impl VerifiableEffect for Create {
    async fn verify(
        &self,
        _: &EffectContext,
        resource: &String,
    ) -> Result<Verification<String>, EffectFailure> {
        Ok(match self.remote.find(resource).await? {
            Some(id) => Verification::Confirmed(id),
            None => Verification::NotApplied,
        })
    }
}

impl CompensableEffect for Create {
    async fn compensate(
        &self,
        ctx: &CompensationContext,
        resource: &String,
        _: Option<&String>,
    ) -> Result<(), EffectFailure> {
        self.remote
            .cancel(resource, Some(ctx.idempotency_key()))
            .await
    }
}

fn handler(remote: &FakeRemote) -> (Handler<Create>, Arc<AtomicU32>) {
    let executions = Arc::new(AtomicU32::new(0));
    let create = Create {
        remote: remote.clone(),
        executions: Arc::clone(&executions),
    };
    (Handler::new(create).verifiable().compensable(), executions)
}

/// A worker that submits `RES` and crashes at `point`, after its first
/// attempt failed transiently.
async fn crashed_at(store: &Store, clock: TokioClock, remote: &FakeRemote, point: FaultPoint) {
    let injector = Arc::new(FaultInjector::new().at(point).crash());
    let doomed = builder(store, clock, "doomed")
        .register(handler(remote).0)
        .fault_injector(Arc::clone(&injector))
        .build();
    let crashed = doomed.submit::<Create>("k", RES.into()).await;
    assert!(crashed.is_err(), "{crashed:?}");
    assert!(injector.reached().contains(&point));
}

#[tokio::test(start_paused = true)]
async fn a_worker_lost_during_a_lease_free_backoff_is_finished_by_recovery_when_due() {
    quiet_crashes();
    let (store, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    crashed_at(
        &store,
        clock,
        &remote,
        FaultPoint::AfterBackoffLeaseReleased,
    )
    .await;
    let waiting = record(&store, "k").await;
    assert_eq!(
        (waiting.status, waiting.lease_owner),
        (EffectStatus::Pending, None)
    );

    let (create, executions) = handler(&remote);
    let recovery = builder(&store, clock, "recovery").register(create).build();
    // Nobody holds it, so recovery sees it at once, and leaves it until due.
    assert_eq!(recovery.recover().await.unwrap().not_due, [waiting.id]);
    until(start, Duration::from_secs(60)).await;
    let report = recovery.recover().await.unwrap();
    assert_eq!(report.resumed, [(waiting.id, EffectStatus::Committed)]);
    assert_eq!(executions.load(Ordering::SeqCst), 1, "the retry, once");
    assert_eq!(remote.requests(), 2);
    assert_eq!(remote.applications(RES), 1);
}

#[tokio::test(start_paused = true)]
async fn a_worker_lost_right_after_scheduling_is_finished_by_recovery_when_due() {
    quiet_crashes();
    let (store, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    crashed_at(&store, clock, &remote, FaultPoint::AfterRetryScheduled).await;
    let waiting = record(&store, "k").await;
    assert_eq!(waiting.status, EffectStatus::Pending);
    assert!(
        waiting.live_lease_owner(clock.now()).is_some(),
        "it died holding the lease"
    );

    let (create, executions) = handler(&remote);
    let recovery = builder(&store, clock, "recovery").register(create).build();
    until(start, Duration::from_secs(45)).await;
    // The dead worker's lease has expired, but the retry is not due.
    let report = recovery.recover().await.unwrap();
    assert_eq!(report.not_due, [waiting.id]);
    assert_eq!(remote.requests(), 1);
    until(start, Duration::from_secs(60)).await;
    let report = recovery.recover().await.unwrap();
    assert_eq!(report.resumed, [(waiting.id, EffectStatus::Committed)]);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(remote.applications(RES), 1);
}

#[tokio::test(start_paused = true)]
async fn recovery_leaves_a_long_backoff_for_a_later_pass() {
    let (store, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::LoseRequest]);
    let (create, executions) = handler(&remote);
    let rt = builder(&store, clock, "recovery").register(create).build();
    // An intent recorded by a process that died before running it.
    let submit = tokio::spawn({
        let rt = builder(&store, clock, "doomed")
            .register(handler(&remote).0)
            .fault_injector(Arc::new(
                FaultInjector::new().at(FaultPoint::AfterInsert).crash(),
            ))
            .build();
        async move { rt.submit::<Create>("k", RES.into()).await }
    });
    assert!(submit.await.unwrap().is_err());
    let id = record(&store, "k").await.id;

    // The lost request is verified as not applied; the re-run is a minute
    // away. The pass stores that and moves on instead of holding a slot.
    let start = Instant::now();
    let report = rt.recover().await.unwrap();
    assert!(start.elapsed() < Duration::from_secs(10), "did not wait");
    assert_eq!(report.resumed, [(id, EffectStatus::Pending)]);
    let waiting = record(&store, "k").await;
    assert_eq!(waiting.lease_owner, None);
    assert!(waiting.next_attempt_at.is_some());

    until(start, Duration::from_secs(60)).await;
    let report = rt.recover().await.unwrap();
    assert_eq!(report.resumed, [(id, EffectStatus::Committed)]);
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    assert_eq!(remote.applications(RES), 1);
}

#[tokio::test(start_paused = true)]
async fn several_workers_finding_one_due_retry_run_it_once() {
    quiet_crashes();
    let (store, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    crashed_at(
        &store,
        clock,
        &remote,
        FaultPoint::AfterBackoffLeaseReleased,
    )
    .await;

    until(start, Duration::from_secs(61)).await;
    let mut counts = Vec::new();
    let passes: Vec<_> = (0..3)
        .map(|i| {
            let (create, executions) = handler(&remote);
            counts.push(executions);
            let rt = builder(&store, clock, &format!("r{i}"))
                .register(create)
                .build();
            tokio::spawn(async move { rt.recover().await.unwrap() })
        })
        .collect();
    for pass in passes {
        pass.await.unwrap();
    }
    let total: u32 = counts.iter().map(|c| c.load(Ordering::SeqCst)).sum();
    assert_eq!(total, 1, "exactly one worker ran the retry");
    assert_eq!(record(&store, "k").await.status, EffectStatus::Committed);
    assert_eq!(remote.requests(), 2);
    assert_eq!(remote.applications(RES), 1);
}

#[tokio::test(start_paused = true)]
async fn a_store_outage_during_the_backoff_does_not_disturb_the_retry() {
    let (store, clock) = setup();
    let rt = builder(&store, clock, "w").build();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    let call = tokio::spawn({
        let (rt, remote) = (rt.clone(), remote.clone());
        async move { rt.effect(NAME, "k").run(create(&remote)).await }
    });
    until(start, Duration::from_secs(5)).await;
    // Nothing touches the store during a lease-free wait.
    store.controls.offline();
    until(start, Duration::from_secs(50)).await;
    store.controls.reconnect();
    assert_eq!(
        call.await.unwrap().unwrap(),
        EffectOutcome::Committed("res#1".into())
    );
    assert_eq!(remote.requests(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_store_outage_when_the_retry_is_due_keeps_the_schedule_for_the_next_call() {
    let (store, clock) = setup();
    let rt = builder(&store, clock, "w").build();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    let call = tokio::spawn({
        let (rt, remote) = (rt.clone(), remote.clone());
        async move { rt.effect(NAME, "k").run(create(&remote)).await }
    });
    until(start, Duration::from_secs(5)).await;
    store.controls.offline();
    // The store is down when the retry is due: the call reports it, and the
    // effect stays exactly as stored.
    let error = call.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("outage"), "{error}");
    store.controls.reconnect();
    let waiting = record(&store, "k").await;
    assert_eq!(
        (waiting.status, waiting.attempt_count),
        (EffectStatus::Pending, 1)
    );
    assert_eq!(remote.requests(), 1);

    let outcome = rt.effect(NAME, "k").run(create(&remote)).await.unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!(remote.requests(), 2);
    assert_eq!(record(&store, "k").await.attempt_count, 2);
}

#[tokio::test(start_paused = true)]
async fn compensation_retries_wait_without_the_lease() {
    let (store, clock) = setup();
    let remote = FakeRemote::new(clock);
    let (create, _) = handler(&remote);
    let rt = builder(&store, clock, "w").register(create).build();
    assert_eq!(
        rt.submit::<Create>("k", RES.into()).await.unwrap(),
        EffectOutcome::Committed("res#1".into())
    );
    let remote = remote.script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    let undo = tokio::spawn({
        let rt = rt.clone();
        async move { rt.compensate::<Create>("k").await }
    });
    until(start, Duration::from_secs(1)).await;
    let waiting = record(&store, "k").await;
    assert_eq!(waiting.status, EffectStatus::Compensating);
    assert!(waiting.next_attempt_at.is_some());
    assert_eq!(waiting.lease_owner, None);

    assert_eq!(
        undo.await.unwrap().unwrap(),
        CompensationOutcome::Compensated
    );
    assert!(start.elapsed() >= Duration::from_secs(60));
    assert_eq!(remote.cancellations(RES), 1);
    assert_eq!(store.controls.renewals.load(Ordering::SeqCst), 0);
    assert_eq!(record(&store, "k").await.compensation_attempts, 2);
}

#[tokio::test(start_paused = true)]
async fn recovery_finishes_a_compensation_whose_retry_came_due() {
    quiet_crashes();
    let (store, clock) = setup();
    let remote = FakeRemote::new(clock);
    let (create, _) = handler(&remote);
    let rt = builder(&store, clock, "w").register(create).build();
    rt.submit::<Create>("k", RES.into()).await.unwrap();
    let remote = remote.script([Behavior::Fail(FailureClass::Transient)]);
    let start = Instant::now();
    let injector = Arc::new(
        FaultInjector::new()
            .at(FaultPoint::AfterBackoffLeaseReleased)
            .crash(),
    );
    let doomed = builder(&store, clock, "doomed")
        .register(handler(&remote).0)
        .fault_injector(Arc::clone(&injector))
        .build();
    assert!(doomed.compensate::<Create>("k").await.is_err());
    let waiting = record(&store, "k").await;
    assert_eq!(
        (
            waiting.status,
            waiting.lease_owner,
            waiting.compensation_attempts
        ),
        (EffectStatus::Compensating, None, 1)
    );

    let (create, _) = handler(&remote);
    let recovery = builder(&store, clock, "recovery").register(create).build();
    assert_eq!(recovery.recover().await.unwrap().not_due, [waiting.id]);
    until(start, Duration::from_secs(60)).await;
    let report = recovery.recover().await.unwrap();
    assert_eq!(report.resumed, [(waiting.id, EffectStatus::Compensated)]);
    assert_eq!(remote.cancellations(RES), 1);
    assert_eq!(record(&store, "k").await.compensation_attempts, 2);
}

mod real_time {
    //! The shared real-time scenarios, run here on the in-memory store.
    use super::retry_load;
    use agent_effects::fault::FaultPoint;
    use agent_effects_memory::MemoryStore;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_released_during_backoff() {
        retry_load::lease_released_during_backoff(MemoryStore::new()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn budget_across_backoffs() {
        retry_load::budget_across_backoffs(MemoryStore::new()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_after_scheduling() {
        retry_load::crash_then_recover_when_due(
            MemoryStore::new(),
            FaultPoint::AfterRetryScheduled,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_during_lease_free_backoff() {
        retry_load::crash_then_recover_when_due(
            MemoryStore::new(),
            FaultPoint::AfterBackoffLeaseReleased,
        )
        .await;
    }
}
