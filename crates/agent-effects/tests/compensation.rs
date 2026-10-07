//! Compensation: undoing committed effects durably.

use std::sync::{Arc, Once};
use std::time::Duration;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::handler::{CompensableEffect, EffectHandler, Handler};
use agent_effects::store::{EffectStore, TransitionRequest};
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    Clock, CompensationContext, CompensationOutcome, EffectContext, EffectFailure, EffectKey,
    EffectKind, EffectName, EffectOutcome, EffectStatus, FailureClass, LogicalKey, Resolution,
    RetryPolicy, Runtime, RuntimeError, TokioClock, Transition, WorkerId,
};
use agent_effects_memory::MemoryStore;

const TTL: Duration = Duration::from_secs(30);
const RES: &str = "res";

fn setup() -> (MemoryStore, Runtime<MemoryStore>, TokioClock) {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let rt = Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new("w"))
        .lease_ttl(TTL)
        .build();
    (store, rt, clock)
}

fn key(name: &str, logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(name).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

async fn trail(store: &MemoryStore, key: &EffectKey) -> Vec<Transition> {
    let record = store.get_by_key(key).await.unwrap().unwrap();
    store
        .events(record.id)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.transition)
        .collect()
}

/// Creates `RES` as a committed closure effect `reserve:k`.
async fn reserve(rt: &Runtime<MemoryStore>, remote: &FakeRemote) {
    let remote = remote.clone();
    let outcome = rt
        .effect("reserve", "k")
        .kind(EffectKind::ReversibleWrite)
        .run(move |ctx| {
            let remote = remote.clone();
            async move { remote.create(RES, Some(ctx.idempotency_key())).await }
        })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
}

/// Undoes `reserve:k` by cancelling `RES` under the compensation key.
async fn release(
    rt: &Runtime<MemoryStore>,
    remote: &FakeRemote,
    policy: Option<RetryPolicy>,
) -> Result<CompensationOutcome, RuntimeError> {
    let remote = remote.clone();
    let mut compensation = rt.compensation("reserve", "k").reason("order cancelled");
    if let Some(policy) = policy {
        compensation = compensation.retry(policy);
    }
    compensation
        .run(
            move |ctx: CompensationContext, reservation: Option<String>| {
                let remote = remote.clone();
                async move {
                    assert_eq!(
                        reservation.as_deref(),
                        Some("res#1"),
                        "the stored output is passed"
                    );
                    remote.cancel(RES, Some(ctx.idempotency_key())).await
                }
            },
        )
        .await
}

#[tokio::test(start_paused = true)]
async fn a_committed_effect_is_undone_once() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock);
    reserve(&rt, &remote).await;

    assert_eq!(
        release(&rt, &remote, None).await.unwrap(),
        CompensationOutcome::Compensated
    );
    assert!(!remote.exists(RES));
    let requests = remote.requests();
    assert_eq!(
        release(&rt, &remote, None).await.unwrap(),
        CompensationOutcome::Compensated,
        "calling again changes nothing"
    );
    assert_eq!(remote.requests(), requests, "and sends nothing");
    assert_eq!(remote.cancellations(RES), 1);
    assert_eq!(
        trail(&store, &key("reserve", "k")).await,
        [
            Transition::StartAttempt,
            Transition::Succeeded,
            Transition::StartCompensation,
            Transition::CompensationSucceeded,
        ]
    );
    let events = store
        .events(
            store
                .get_by_key(&key("reserve", "k"))
                .await
                .unwrap()
                .unwrap()
                .id,
        )
        .await
        .unwrap();
    assert_eq!(
        events[2].payload.as_ref().unwrap()["reason"],
        "order cancelled"
    );

    // Running the effect again reports what happened to it.
    let rerun = rt
        .effect("reserve", "k")
        .kind(EffectKind::ReversibleWrite)
        .run(|_| async { Ok::<_, EffectFailure>(String::new()) })
        .await
        .unwrap();
    assert!(
        matches!(rerun, EffectOutcome::Compensated { .. }),
        "{rerun:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn only_committed_effects_can_be_undone() {
    let (_, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Permanent)]);
    let failed = rt
        .effect("reserve", "k")
        .kind(EffectKind::ReversibleWrite)
        .run({
            let remote = remote.clone();
            move |_| {
                let remote = remote.clone();
                async move { remote.create(RES, None).await }
            }
        })
        .await
        .unwrap();
    assert!(matches!(failed, EffectOutcome::Failed(_)));
    let outcome = release(&rt, &remote, None).await.unwrap();
    assert!(
        matches!(
            outcome,
            CompensationOutcome::NotCommitted {
                status: EffectStatus::Failed,
                ..
            }
        ),
        "{outcome:?}"
    );

    let err = rt
        .compensation("reserve", "missing")
        .run(|_, _: Option<String>| async { Ok(()) })
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::NoSuchEffect { .. }), "{err}");
}

#[tokio::test(start_paused = true)]
async fn ambiguous_and_transient_failures_are_retried_and_deduplicated() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock);
    reserve(&rt, &remote).await;
    // Unreachable, then cancelled-but-the-answer-was-lost, then fine.
    let remote = remote.script([Behavior::Unreachable, Behavior::CommitThenDrop]);

    assert_eq!(
        release(&rt, &remote, None).await.unwrap(),
        CompensationOutcome::Compensated
    );
    assert_eq!(
        remote.cancellations(RES),
        1,
        "the idempotency key deduplicated the re-send"
    );
    let record = store
        .get_by_key(&key("reserve", "k"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.compensation_attempts, 3);
    assert_eq!(
        trail(&store, &key("reserve", "k")).await[2..],
        [
            Transition::StartCompensation,
            Transition::ScheduleCompensationRetry,
            Transition::StartCompensationRetry,
            Transition::ScheduleCompensationRetry,
            Transition::StartCompensationRetry,
            Transition::CompensationSucceeded,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_retry_that_fell_due_while_its_worker_was_down_counts_once() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock);
    reserve(&rt, &remote).await;
    // Attempt 1 failed and attempt 2 was scheduled; then the worker died,
    // and nobody resumed the compensation until after attempt 2 was due.
    let record = store
        .get_by_key(&key("reserve", "k"))
        .await
        .unwrap()
        .unwrap();
    let lease = store
        .acquire_lease(record.id, &WorkerId::new("dead"), clock.now(), TTL)
        .await
        .unwrap();
    let record = store
        .transition(TransitionRequest::new(
            &record,
            Some(&lease),
            Transition::StartCompensation,
            clock.now(),
        ))
        .await
        .unwrap();
    let mut retry = TransitionRequest::new(
        &record,
        Some(&lease),
        Transition::ScheduleCompensationRetry,
        clock.now(),
    );
    retry.next_attempt_at = Some(clock.now() + Duration::from_secs(1));
    store.transition(retry).await.unwrap();
    tokio::time::advance(TTL * 2).await;

    // Attempt 2 fails too; attempt 3 is the last the budget allows.
    let remote = remote.script([Behavior::Unreachable]);
    let budget = RetryPolicy {
        max_attempts: 3,
        ..RetryPolicy::NONE
    };
    assert_eq!(
        release(&rt, &remote, Some(budget)).await.unwrap(),
        CompensationOutcome::Compensated
    );
    let record = store
        .get_by_key(&key("reserve", "k"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.compensation_attempts, 3);
}

#[tokio::test(start_paused = true)]
async fn a_permanent_failure_waits_for_an_operator() {
    let (_, rt, clock) = setup();
    let remote = FakeRemote::new(clock);
    reserve(&rt, &remote).await;
    let remote = remote.script([Behavior::Fail(FailureClass::Permanent)]);

    let outcome = release(&rt, &remote, None).await.unwrap();
    assert!(
        matches!(outcome, CompensationOutcome::Failed(_)),
        "{outcome:?}"
    );
    assert!(remote.exists(RES));
    let rerun = rt
        .effect("reserve", "k")
        .kind(EffectKind::ReversibleWrite)
        .run(|_| async { Ok::<_, EffectFailure>(String::new()) })
        .await
        .unwrap();
    let EffectOutcome::NeedsIntervention { id } = rerun else {
        panic!("a failed compensation needs an operator: {rerun:?}");
    };

    // The operator fixes the cause and orders a retry.
    rt.resolve(id, Resolution::Retry, "operator:dennis", "provider fixed")
        .await
        .unwrap();
    assert_eq!(
        release(&rt, &remote, None).await.unwrap(),
        CompensationOutcome::Compensated
    );
    assert!(!remote.exists(RES));
}

#[tokio::test(start_paused = true)]
async fn an_operator_can_record_a_manual_undo() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock);
    reserve(&rt, &remote).await;
    let remote = remote.script([Behavior::Unreachable; 5]);
    let outcome = release(&rt, &remote, None).await.unwrap();
    assert!(
        matches!(outcome, CompensationOutcome::Failed(_)),
        "budget spent: {outcome:?}"
    );

    let id = store
        .get_by_key(&key("reserve", "k"))
        .await
        .unwrap()
        .unwrap()
        .id;
    let record = rt
        .resolve(
            id,
            Resolution::Compensated,
            "operator:dennis",
            "released in the dashboard",
        )
        .await
        .unwrap();
    assert_eq!(record.status, EffectStatus::Compensated);
    assert_eq!(
        release(&rt, &remote, None).await.unwrap(),
        CompensationOutcome::Compensated
    );
}

/// A reservation handler that can be undone.
struct Reserve {
    remote: FakeRemote,
}

impl EffectHandler for Reserve {
    const NAME: &'static str = "reserve";
    type Input = String;
    type Output = String;
    type Error = EffectFailure;

    fn kind(&self) -> EffectKind {
        EffectKind::ReversibleWrite
    }

    fn remote_idempotency(&self) -> bool {
        true
    }

    async fn execute(
        &self,
        ctx: &EffectContext,
        resource: &String,
    ) -> Result<String, EffectFailure> {
        self.remote
            .create(resource, Some(ctx.idempotency_key()))
            .await
    }
}

impl CompensableEffect for Reserve {
    async fn compensate(
        &self,
        ctx: &CompensationContext,
        resource: &String,
        output: Option<&String>,
    ) -> Result<(), EffectFailure> {
        assert_eq!(output.map(String::as_str), Some("res#1"));
        self.remote
            .cancel(resource, Some(ctx.idempotency_key()))
            .await
    }
}

#[tokio::test(start_paused = true)]
async fn a_compensable_handler_is_undone_by_type() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let rt = Runtime::builder(store.clone())
        .clock(clock)
        .register(
            Handler::new(Reserve {
                remote: remote.clone(),
            })
            .compensable(),
        )
        .build();
    rt.submit::<Reserve>("k", RES.into()).await.unwrap();
    let outcome = rt
        .compensate::<Reserve>("k")
        .reason("customer cancelled")
        .actor("agent:support")
        .await
        .unwrap();
    assert_eq!(outcome, CompensationOutcome::Compensated);
    assert!(!remote.exists(RES));

    // Without `.compensable()` the runtime refuses.
    let plain = Runtime::builder(MemoryStore::new())
        .register(Handler::new(Reserve { remote }))
        .build();
    let err = plain.compensate::<Reserve>("k").await.unwrap_err();
    assert!(matches!(err, RuntimeError::NotCompensable { .. }), "{err}");
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

#[tokio::test(start_paused = true)]
async fn recovery_finishes_an_interrupted_compensation() {
    quiet_crashes();
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let handler = || {
        Handler::new(Reserve {
            remote: remote.clone(),
        })
        .compensable()
    };

    let doomed = Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new("doomed"))
        .lease_ttl(TTL)
        .register(handler())
        .fault_injector(Arc::new(
            FaultInjector::new()
                .at(FaultPoint::AfterCompensationStarted)
                .crash(),
        ))
        .build();
    doomed.submit::<Reserve>("k", RES.into()).await.unwrap();
    assert!(
        doomed.compensate::<Reserve>("k").await.is_err(),
        "crashed mid-compensation"
    );
    assert_eq!(
        store
            .get_by_key(&key("reserve", "k"))
            .await
            .unwrap()
            .unwrap()
            .status,
        EffectStatus::Compensating
    );

    // Nobody calls again; recovery finishes it.
    tokio::time::sleep(TTL).await;
    let recovery = Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new("recovery"))
        .register(handler())
        .build();
    let report = recovery.recover().await.unwrap();
    assert_eq!(report.resumed.len(), 1, "{report:?}");
    assert_eq!(report.resumed[0].1, EffectStatus::Compensated);
    assert!(!remote.exists(RES));
    assert_eq!(remote.cancellations(RES), 1);
}

#[tokio::test(start_paused = true)]
async fn a_caller_finishes_an_interrupted_closure_compensation() {
    quiet_crashes();
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let doomed = Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new("doomed"))
        .lease_ttl(TTL)
        .fault_injector(Arc::new(
            FaultInjector::new()
                .at(FaultPoint::AfterCompensationStarted)
                .crash(),
        ))
        .build();
    reserve(&doomed, &remote).await;
    assert!(release(&doomed, &remote, None).await.is_err());

    tokio::time::sleep(TTL).await;
    let restarted = Runtime::builder(store.clone()).clock(clock).build();
    let report = restarted.recover().await.unwrap();
    assert_eq!(
        report.unhandled.len(),
        1,
        "closure compensations need a caller"
    );
    assert_eq!(
        release(&restarted, &remote, None).await.unwrap(),
        CompensationOutcome::Compensated
    );
    assert_eq!(remote.cancellations(RES), 1);
}
