//! Recovery and the operator API: marking dead workers' effects unknown,
//! listing what needs attention, resolving it, and taking over from a
//! stalled worker without duplicating its effect.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use agent_effects::store::{EffectStore, ErrorRecord, NewEffect, StoreError, TransitionRequest};
use agent_effects::testkit::FakeRemote;
use agent_effects::{
    Clock, EffectFailure, EffectId, EffectKey, EffectKind, EffectName, EffectOutcome, EffectStatus,
    LogicalKey, ManualClock, RecoveryReport, Resolution, RetryPolicy, Runtime, RuntimeError,
    TokioClock, Transition, Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;
use tokio::sync::Notify;

const TTL: Duration = Duration::from_secs(30);
const RES: &str = "res";

const NO_WAIT: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_delay: Duration::ZERO,
    max_delay: Duration::ZERO,
    multiplier: 1.0,
    jitter: false,
};

fn runtime(store: &MemoryStore, clock: impl Clock, worker: &str) -> Runtime<MemoryStore> {
    Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new(worker))
        .lease_ttl(TTL)
        .retry_policy(NO_WAIT)
        .build()
}

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new("op").unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

async fn record(store: &MemoryStore, logical: &str) -> agent_effects::EffectRecord {
    store.get_by_key(&key(logical)).await.unwrap().unwrap()
}

/// Leaves an effect as a worker that died would: leased by `crashed` and
/// moved through `transitions`.
async fn abandoned(
    store: &MemoryStore,
    clock: &impl Clock,
    logical: &str,
    transitions: &[Transition],
) -> EffectId {
    let mut record = store
        .insert_or_get(NewEffect::new(
            key(logical),
            EffectKind::IrreversibleWrite,
            clock.now(),
        ))
        .await
        .unwrap()
        .record;
    let lease = store
        .acquire_lease(record.id, &WorkerId::new("crashed"), clock.now(), TTL)
        .await
        .unwrap();
    for &transition in transitions {
        record = store
            .transition(TransitionRequest::new(
                &record,
                Some(&lease),
                transition,
                clock.now(),
            ))
            .await
            .unwrap();
    }
    record.id
}

#[tokio::test]
async fn recovery_marks_only_abandoned_attempts_unknown() {
    let store = MemoryStore::new();
    let clock = Arc::new(ManualClock::default());
    let executing = abandoned(&store, &clock, "executing", &[Transition::StartAttempt]).await;
    let verifying = abandoned(
        &store,
        &clock,
        "verifying",
        &[Transition::StartAttempt, Transition::StartVerification],
    )
    .await;
    let waiting = abandoned(
        &store,
        &clock,
        "waiting",
        &[Transition::StartAttempt, Transition::ScheduleRetry],
    )
    .await;
    clock.advance(TTL);
    // Taken after the others expired: still live.
    let live = abandoned(&store, &clock, "live", &[Transition::StartAttempt]).await;

    let rt = runtime(&store, Arc::clone(&clock), "recovery");
    let report = rt.recover().await.unwrap();
    assert_eq!(
        report,
        RecoveryReport {
            marked_unknown: vec![executing, verifying],
            skipped: vec![],
        }
    );
    for logical in ["executing", "verifying"] {
        let record = record(&store, logical).await;
        assert_eq!(record.status, EffectStatus::Unknown, "{logical}");
        assert_eq!(
            record.lease_owner, None,
            "{logical}: recovery releases its lease"
        );
        let events = store.events(record.id).await.unwrap();
        let last = events.last().unwrap();
        assert_eq!(last.transition, Transition::LeaseExpired);
        assert_eq!(last.actor.as_deref(), Some("recovery:recovery"));
    }
    assert_eq!(
        record(&store, "waiting").await.status,
        EffectStatus::Pending
    );
    assert_eq!(record(&store, "live").await.status, EffectStatus::Executing);

    assert_eq!(
        rt.recover().await.unwrap(),
        RecoveryReport::default(),
        "idempotent"
    );

    let pending: Vec<_> = rt
        .pending(None, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(
        pending,
        [executing, verifying, waiting],
        "everything but the live one"
    );
    assert!(!pending.contains(&live));
    let page: Vec<_> = rt
        .pending(Some(executing), 1)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(page, [verifying]);
}

#[tokio::test]
async fn recovery_pages_through_many_effects() {
    let store = MemoryStore::new();
    let clock = Arc::new(ManualClock::default());
    for n in 0..250 {
        abandoned(
            &store,
            &clock,
            &format!("e{n}"),
            &[Transition::StartAttempt],
        )
        .await;
    }
    clock.advance(TTL);
    let report = runtime(&store, Arc::clone(&clock), "recovery")
        .recover()
        .await
        .unwrap();
    assert_eq!(report.marked_unknown.len(), 250);
}

#[tokio::test(start_paused = true)]
async fn the_recovery_loop_catches_dead_workers() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let id = abandoned(&store, &clock, "dead", &[Transition::StartAttempt]).await;
    let rt = runtime(&store, clock, "recovery");
    let task = tokio::spawn({
        let rt = rt.clone();
        async move { rt.run_recovery(Duration::from_secs(5)).await }
    });
    tokio::time::sleep(TTL + Duration::from_secs(6)).await;
    task.abort();
    assert_eq!(
        store.get(id).await.unwrap().unwrap().status,
        EffectStatus::Unknown
    );
}

/// A worker that runs `before`, signals, stalls until released, then runs
/// `after`. The stall outlasts its lease: the manual clock moves, its
/// heartbeat (every TTL/3 of real time) does not get to run.
struct Stalled {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Stalled {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn taking_over_a_stalled_worker_verifies_instead_of_duplicating() {
    let store = MemoryStore::new();
    let clock = Arc::new(ManualClock::default());
    let remote = FakeRemote::new(Arc::clone(&clock));
    let stall = Stalled::new();

    // Worker a creates the resource, then stalls before recording it.
    let a = runtime(&store, Arc::clone(&clock), "a");
    let stalled = tokio::spawn({
        let (remote, started, release) =
            (remote.clone(), stall.started.clone(), stall.release.clone());
        async move {
            a.effect("op", "stalled")
                .run(move |_| {
                    let (remote, started, release) =
                        (remote.clone(), started.clone(), release.clone());
                    async move {
                        let id = remote.create(RES, None).await?;
                        started.notify_one();
                        release.notified().await;
                        Ok::<_, EffectFailure>(id)
                    }
                })
                .await
        }
    });
    stall.started.notified().await;
    clock.advance(TTL);

    let report = runtime(&store, Arc::clone(&clock), "recovery")
        .recover()
        .await
        .unwrap();
    assert_eq!(report.marked_unknown.len(), 1);

    // A caller re-runs the effect on worker b, with verification.
    let b = runtime(&store, Arc::clone(&clock), "b");
    let lookup = remote.clone();
    let taken_over = b
        .effect("op", "stalled")
        .verify(move |_| {
            let lookup = lookup.clone();
            async move {
                Ok::<_, EffectFailure>(match lookup.find(RES).await? {
                    Some(id) => Verification::Confirmed(id),
                    None => Verification::NotApplied,
                })
            }
        })
        .run({
            let remote = remote.clone();
            move |_| {
                let remote = remote.clone();
                async move { remote.create(RES, None).await }
            }
        })
        .await
        .unwrap();
    assert_eq!(taken_over, EffectOutcome::Committed("res#1".into()));

    stall.release.notify_one();
    let late = stalled.await.unwrap().unwrap();
    assert_eq!(
        late, taken_over,
        "a's late result agrees and changes nothing"
    );
    assert_eq!(remote.applications(RES), 1, "created exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn taking_over_a_stalled_worker_with_remote_idempotency_never_duplicates() {
    // Worst case for verification: a's request is still in flight and lands
    // *after* b has re-run the effect. Only the remote idempotency key keeps
    // this to one application.
    let store = MemoryStore::new();
    let clock = Arc::new(ManualClock::default());
    let remote = FakeRemote::new(Arc::clone(&clock));
    let stall = Stalled::new();
    let create = |remote: FakeRemote| {
        move |ctx: agent_effects::EffectContext| {
            let remote = remote.clone();
            async move { remote.create(RES, Some(ctx.idempotency_key())).await }
        }
    };

    let a = runtime(&store, Arc::clone(&clock), "a");
    let stalled = tokio::spawn({
        let (remote, started, release) =
            (remote.clone(), stall.started.clone(), stall.release.clone());
        async move {
            a.effect("op", "in-flight")
                .remote_idempotency(true)
                .run(move |ctx| {
                    let (remote, started, release) =
                        (remote.clone(), started.clone(), release.clone());
                    async move {
                        started.notify_one();
                        release.notified().await;
                        remote.create(RES, Some(ctx.idempotency_key())).await
                    }
                })
                .await
        }
    });
    stall.started.notified().await;
    clock.advance(TTL);
    runtime(&store, Arc::clone(&clock), "recovery")
        .recover()
        .await
        .unwrap();

    let b = runtime(&store, Arc::clone(&clock), "b");
    let taken_over = b
        .effect("op", "in-flight")
        .remote_idempotency(true)
        .run(create(remote.clone()))
        .await
        .unwrap();
    assert_eq!(taken_over, EffectOutcome::Committed("res#1".into()));

    stall.release.notify_one();
    assert_eq!(stalled.await.unwrap().unwrap(), taken_over);
    assert_eq!(remote.requests(), 2, "both workers sent the request");
    assert_eq!(remote.applications(RES), 1, "the remote applied it once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn taking_over_an_unverifiable_stalled_worker_escalates_then_an_operator_resolves() {
    let store = MemoryStore::new();
    let clock = Arc::new(ManualClock::default());
    let stall = Stalled::new();

    let a = runtime(&store, Arc::clone(&clock), "a");
    let stalled = tokio::spawn({
        let (started, release) = (stall.started.clone(), stall.release.clone());
        async move {
            a.effect("op", "opaque")
                .run(move |_| {
                    let (started, release) = (started.clone(), release.clone());
                    async move {
                        started.notify_one();
                        release.notified().await;
                        Ok::<_, EffectFailure>("sent".to_string())
                    }
                })
                .await
        }
    });
    stall.started.notified().await;
    clock.advance(TTL);
    runtime(&store, Arc::clone(&clock), "recovery")
        .recover()
        .await
        .unwrap();

    let b = runtime(&store, Arc::clone(&clock), "b");
    let calls = Arc::new(AtomicU32::new(0));
    let attempt = || {
        let calls = Arc::clone(&calls);
        b.effect("op", "opaque").run(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, EffectFailure>("duplicate".to_string()) }
        })
    };
    let EffectOutcome::NeedsIntervention { id } = attempt().await.unwrap() else {
        panic!("an unverifiable irreversible effect must go to an operator");
    };
    stall.release.notify_one();
    assert_eq!(
        stalled.await.unwrap().unwrap(),
        EffectOutcome::NeedsIntervention { id },
        "a is fenced off"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0, "b never re-ran it");

    // The operator checks the remote system by hand and confirms it.
    let resolved = b
        .resolve(
            id,
            Resolution::applied("sent").unwrap(),
            "operator:dennis",
            "found the message in the provider's outbox",
        )
        .await
        .unwrap();
    assert_eq!(resolved.status, EffectStatus::Committed);
    assert_eq!(
        attempt().await.unwrap(),
        EffectOutcome::Committed("sent".into())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let last = store.events(id).await.unwrap().pop().unwrap();
    assert_eq!(last.transition, Transition::ResolvedApplied);
    assert_eq!(last.actor.as_deref(), Some("operator:dennis"));
    assert_eq!(
        last.payload.unwrap()["note"],
        "found the message in the provider's outbox"
    );
}

/// An effect that escalated after an ambiguous failure.
async fn escalated(rt: &Runtime<MemoryStore>, logical: &str) -> EffectId {
    let outcome = rt
        .effect("op", logical)
        .run(|_| async { Err::<String, _>(EffectFailure::ambiguous("connection reset")) })
        .await
        .unwrap();
    let EffectOutcome::NeedsIntervention { id } = outcome else {
        panic!("{outcome:?}");
    };
    id
}

#[tokio::test]
async fn resolving_not_applied_fails_the_effect_with_the_note() {
    let store = MemoryStore::new();
    let rt = runtime(&store, Arc::new(ManualClock::default()), "w");
    let id = escalated(&rt, "nope").await;
    rt.resolve(
        id,
        Resolution::NotApplied,
        "operator:dennis",
        "provider has no record",
    )
    .await
    .unwrap();
    let outcome = rt
        .effect("op", "nope")
        .run(|_| async { Ok::<_, EffectFailure>("never".to_string()) })
        .await
        .unwrap();
    assert_eq!(
        outcome,
        EffectOutcome::Failed(ErrorRecord {
            class: None,
            message: "provider has no record".into(),
        })
    );
}

#[tokio::test]
async fn resolving_retry_reruns_even_with_the_budget_spent() {
    let store = MemoryStore::new();
    let rt = Runtime::builder(store.clone())
        .clock(Arc::new(ManualClock::default()))
        .retry_policy(RetryPolicy::NONE)
        .build();
    let id = escalated(&rt, "again").await;
    rt.resolve(
        id,
        Resolution::Retry,
        "operator:dennis",
        "provider confirmed it never arrived",
    )
    .await
    .unwrap();
    let outcome = rt
        .effect("op", "again")
        .run(|ctx| async move { Ok::<_, EffectFailure>(ctx.attempt()) })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_refuses_settled_busy_and_unknown_ids() {
    let store = MemoryStore::new();
    let clock = Arc::new(ManualClock::default());
    let rt = runtime(&store, Arc::clone(&clock), "w");

    rt.effect("op", "done")
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    let done = record(&store, "done").await.id;
    let err = rt
        .resolve(done, Resolution::NotApplied, "operator:dennis", "oops")
        .await
        .unwrap_err();
    assert!(
        matches!(err, RuntimeError::Store(StoreError::InvalidTransition(_))),
        "{err}"
    );

    let busy = abandoned(
        &store,
        &clock,
        "busy",
        &[Transition::StartAttempt, Transition::OutcomeUnknown],
    )
    .await;
    let err = rt
        .resolve(busy, Resolution::Retry, "operator:dennis", "too early")
        .await
        .unwrap_err();
    assert!(
        matches!(err, RuntimeError::Store(StoreError::LeaseHeld { .. })),
        "{err}"
    );

    let err = rt
        .resolve(EffectId::new(), Resolution::Retry, "operator:dennis", "?")
        .await
        .unwrap_err();
    assert!(
        matches!(err, RuntimeError::Store(StoreError::NotFound(_))),
        "{err}"
    );
}
