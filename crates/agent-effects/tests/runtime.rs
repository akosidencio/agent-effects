//! Runtime behaviour: running, replaying and re-attaching to effects.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_effects::store::{EffectStore, ErrorRecord, NewEffect, TransitionRequest};
use agent_effects::{
    Clock, EffectContext, EffectFailure, EffectKey, EffectKind, EffectName, EffectOutcome,
    EffectStatus, FailureClass, LogicalKey, ManualClock, RetryPolicy, Runtime, RuntimeError,
    Transition, WorkerId,
};
use agent_effects_memory::MemoryStore;
use serde_json::json;
use tokio::sync::Notify;

const TTL: Duration = Duration::from_secs(30);

/// Retries without waiting: these tests run on a manual clock and real time.
/// Failure semantics with realistic backoff live in `failure_semantics.rs`.
const NO_WAIT: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_delay: Duration::ZERO,
    max_delay: Duration::ZERO,
    multiplier: 1.0,
    jitter: false,
};

fn runtime(store: &MemoryStore, clock: &Arc<ManualClock>, worker: &str) -> Runtime<MemoryStore> {
    Runtime::builder(store.clone())
        .clock(Arc::clone(clock))
        .worker_id(WorkerId::new(worker))
        .lease_ttl(TTL)
        .retry_policy(NO_WAIT)
        .build()
}

fn setup() -> (MemoryStore, Arc<ManualClock>, Runtime<MemoryStore>) {
    let store = MemoryStore::new();
    let clock = Arc::new(ManualClock::default());
    let rt = runtime(&store, &clock, "worker-a");
    (store, clock, rt)
}

fn key(name: &str, key: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(name).unwrap(),
        LogicalKey::new(key).unwrap(),
    )
}

async fn transitions(store: &MemoryStore, key: &EffectKey) -> Vec<Transition> {
    let record = store.get_by_key(key).await.unwrap().unwrap();
    store
        .events(record.id)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.transition)
        .collect()
}

/// Charges `amount` for `order`, counting how often the action runs.
async fn charge(
    rt: &Runtime<MemoryStore>,
    order: &str,
    amount: u32,
    calls: &Arc<AtomicU32>,
) -> Result<EffectOutcome<String>, RuntimeError> {
    let calls = Arc::clone(calls);
    rt.effect("payment.charge", order)
        .kind(EffectKind::IrreversibleWrite)
        .input(&json!({ "amount": amount }))
        .actor("agent:test")
        .run(move |ctx| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, EffectFailure>(format!("pi_{}", ctx.attempt())) }
        })
        .await
}

#[tokio::test]
async fn commits_once_and_replays_the_result() {
    let (store, _, rt) = setup();
    let calls = Arc::new(AtomicU32::new(0));

    let first = charge(&rt, "order-1", 42, &calls).await.unwrap();
    let second = charge(&rt, "order-1", 42, &calls).await.unwrap();

    assert_eq!(first, EffectOutcome::Committed("pi_1".into()));
    assert_eq!(second, first, "the second call replays the recorded result");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the action ran once");

    let key = key("payment.charge", "order-1");
    assert_eq!(
        transitions(&store, &key).await,
        [Transition::StartAttempt, Transition::Succeeded]
    );
    let record = store.get_by_key(&key).await.unwrap().unwrap();
    assert_eq!(record.output, Some(json!("pi_1")));
    assert_eq!(record.created_by.as_deref(), Some("agent:test"));
    assert_eq!(record.lease_owner, None, "the lease is released");
}

#[tokio::test]
async fn a_reused_key_with_a_different_input_or_kind_is_rejected() {
    let (_, _, rt) = setup();
    let calls = Arc::new(AtomicU32::new(0));
    charge(&rt, "order-1", 42, &calls).await.unwrap();

    let err = charge(&rt, "order-1", 43, &calls).await.unwrap_err();
    assert!(matches!(err, RuntimeError::InputMismatch { .. }), "{err}");

    let err = rt
        .effect("payment.charge", "order-1")
        .kind(EffectKind::Read)
        .input(&json!({ "amount": 42 }))
        .run(|_| async { Ok::<_, EffectFailure>(String::new()) })
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::KindMismatch { .. }), "{err}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_action_sees_the_effect_identity() {
    let (store, _, rt) = setup();
    let seen: Arc<Mutex<Option<EffectContext>>> = Arc::default();
    let sink = Arc::clone(&seen);
    rt.effect("email.send", 7)
        .run(move |ctx| {
            *sink.lock().unwrap() = Some(ctx);
            async { Ok::<_, EffectFailure>(()) }
        })
        .await
        .unwrap();

    let ctx = seen.lock().unwrap().clone().unwrap();
    let key = key("email.send", "7");
    let record = store.get_by_key(&key).await.unwrap().unwrap();
    assert_eq!(ctx.effect_id(), record.id);
    assert_eq!(ctx.key(), &key);
    assert_eq!(ctx.idempotency_key(), key.idempotency_key());
    assert_eq!(ctx.attempt(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_caller_does_not_abort_the_effect() {
    let (store, _, rt) = setup();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicU32::new(0));

    let call = {
        let (started, release, calls) = (started.clone(), release.clone(), calls.clone());
        rt.effect("server.provision", "web-1").run(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            let (started, release) = (started.clone(), release.clone());
            async move {
                started.notify_one();
                release.notified().await;
                Ok::<_, EffectFailure>("srv-123".to_string())
            }
        })
    };
    tokio::select! {
        _ = call => panic!("the action is blocked; the call cannot finish"),
        () = started.notified() => {}
    }
    // The caller's future is dropped here, mid-attempt.
    release.notify_one();

    let key = key("server.provision", "web-1");
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.get_by_key(&key).await.unwrap().unwrap().status != EffectStatus::Committed {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the attempt finishes and is recorded without its caller");

    let replay = rt
        .effect("server.provision", "web-1")
        .run(|_| async { Ok::<_, EffectFailure>("never runs".to_string()) })
        .await
        .unwrap();
    assert_eq!(replay, EffectOutcome::Committed("srv-123".into()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_concurrent_caller_sees_the_effect_in_progress() {
    let (_, _, rt) = setup();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let first = {
        let rt = rt.clone();
        let (started, release) = (started.clone(), release.clone());
        tokio::spawn(async move {
            rt.effect("inventory.reserve", "sku-9")
                .kind(EffectKind::ReversibleWrite)
                .run(move |_| {
                    let (started, release) = (started.clone(), release.clone());
                    async move {
                        started.notify_one();
                        release.notified().await;
                        Ok::<_, EffectFailure>(1u32)
                    }
                })
                .await
        })
    };
    started.notified().await;

    let second = rt
        .effect("inventory.reserve", "sku-9")
        .kind(EffectKind::ReversibleWrite)
        .run(|_| async { Ok::<_, EffectFailure>(2u32) })
        .await
        .unwrap();
    assert!(
        matches!(second, EffectOutcome::InProgress { .. }),
        "{second:?}"
    );

    release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), EffectOutcome::Committed(1));
}

#[tokio::test]
async fn a_definitive_failure_is_recorded_and_replayed() {
    let (store, _, rt) = setup();
    let calls = Arc::new(AtomicU32::new(0));
    let declined = || {
        let calls = Arc::clone(&calls);
        rt.effect("payment.charge", "order-2").run(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<String, _>(EffectFailure::permanent("card declined")) }
        })
    };

    let expected = EffectOutcome::Failed(ErrorRecord {
        class: Some(FailureClass::Permanent),
        message: "card declined".into(),
    });
    assert_eq!(declined().await.unwrap(), expected);
    assert_eq!(declined().await.unwrap(), expected);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        transitions(&store, &key("payment.charge", "order-2")).await,
        [Transition::StartAttempt, Transition::FailedDefinitively]
    );
}

#[tokio::test]
async fn an_ambiguous_irreversible_effect_goes_to_an_operator() {
    let (store, _, rt) = setup();
    let calls = Arc::new(AtomicU32::new(0));
    let timed_out = || {
        let calls = Arc::clone(&calls);
        rt.effect("email.send", "welcome-1").run(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(EffectFailure::ambiguous("timed out after sending")) }
        })
    };

    let first = timed_out().await.unwrap();
    assert!(
        matches!(first, EffectOutcome::NeedsIntervention { .. }),
        "{first:?}"
    );
    let second = timed_out().await.unwrap();
    assert_eq!(second, first, "the effect is never re-sent");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        transitions(&store, &key("email.send", "welcome-1")).await,
        [
            Transition::StartAttempt,
            Transition::OutcomeUnknown,
            Transition::Escalate
        ]
    );
}

#[tokio::test]
async fn an_unsent_request_is_retried_not_escalated() {
    let (store, _, rt) = setup();
    let outcome = rt
        .effect("email.send", "welcome-2")
        .run(|_| async {
            Err::<(), _>(EffectFailure::ambiguous("connection refused").request_sent(false))
        })
        .await
        .unwrap();
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
    let record = store
        .get_by_key(&key("email.send", "welcome-2"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.attempt_count, NO_WAIT.max_attempts,
        "retried to the budget"
    );
}

#[tokio::test]
async fn safely_repeatable_effects_are_rerun_after_an_unknown_outcome() {
    for (kind, remote_idempotency) in [
        (EffectKind::IdempotentWrite, false),
        (EffectKind::IrreversibleWrite, true),
    ] {
        let (store, _, rt) = setup();
        let calls = Arc::new(AtomicU32::new(0));
        let flaky = || {
            let calls = Arc::clone(&calls);
            rt.effect("crm.update", "contact-5")
                .kind(kind)
                .remote_idempotency(remote_idempotency)
                .run(move |ctx| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if ctx.attempt() == 1 {
                            Err(EffectFailure::ambiguous("connection reset"))
                        } else {
                            Ok(format!("updated on attempt {}", ctx.attempt()))
                        }
                    }
                })
        };

        let first = flaky().await.unwrap();
        assert_eq!(
            first,
            EffectOutcome::Committed("updated on attempt 2".into()),
            "{kind:?}: re-run within the same call"
        );
        assert_eq!(flaky().await.unwrap(), first, "and replayed afterwards");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            transitions(&store, &key("crm.update", "contact-5")).await,
            [
                Transition::StartAttempt,
                Transition::OutcomeUnknown,
                Transition::ScheduleRetry,
                Transition::StartAttempt,
                Transition::Succeeded,
            ]
        );
    }
}

fn explode() -> Result<(), EffectFailure> {
    panic!("bug in the action")
}

#[tokio::test]
async fn a_panicking_action_is_an_unknown_outcome() {
    let (store, _, rt) = setup();
    let outcome = rt
        .effect("email.send", "welcome-3")
        .run(|_| async { explode() })
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    let record = store
        .get_by_key(&key("email.send", "welcome-3"))
        .await
        .unwrap()
        .unwrap();
    let error = record.last_error.unwrap();
    assert_eq!(error.class, Some(FailureClass::Ambiguous));
    assert!(error.message.contains("panicked"), "{}", error.message);
}

/// Leaves a record as a worker that crashed mid-attempt would.
async fn crashed_attempt(
    store: &MemoryStore,
    clock: &ManualClock,
    key: &EffectKey,
    kind: EffectKind,
) {
    let record = store
        .insert_or_get(NewEffect::new(key.clone(), kind, clock.now()))
        .await
        .unwrap()
        .record;
    let lease = store
        .acquire_lease(record.id, &WorkerId::new("crashed"), clock.now(), TTL)
        .await
        .unwrap();
    store
        .transition(TransitionRequest::new(
            &record,
            Some(&lease),
            Transition::StartAttempt,
            clock.now(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn reattaching_to_a_crashed_irreversible_effect_never_reruns_it() {
    let (store, clock, rt) = setup();
    let key = key("payment.charge", "order-3");
    crashed_attempt(&store, &clock, &key, EffectKind::IrreversibleWrite).await;
    let calls = Arc::new(AtomicU32::new(0));
    let call = || {
        let calls = Arc::clone(&calls);
        rt.effect("payment.charge", "order-3").run(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, EffectFailure>(()) }
        })
    };

    let while_leased = call().await.unwrap();
    assert!(
        matches!(while_leased, EffectOutcome::InProgress { .. }),
        "{while_leased:?}"
    );

    clock.advance(TTL);
    let after_expiry = call().await.unwrap();
    assert!(
        matches!(after_expiry, EffectOutcome::NeedsIntervention { .. }),
        "{after_expiry:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        transitions(&store, &key).await,
        [
            Transition::StartAttempt,
            Transition::LeaseExpired,
            Transition::Escalate
        ]
    );
}

#[tokio::test]
async fn reattaching_to_a_crashed_idempotent_effect_reruns_it() {
    let (store, clock, rt) = setup();
    let key = key("crm.update", "contact-6");
    crashed_attempt(&store, &clock, &key, EffectKind::IdempotentWrite).await;
    clock.advance(TTL);

    let outcome = rt
        .effect("crm.update", "contact-6")
        .kind(EffectKind::IdempotentWrite)
        .run(|ctx| async move { Ok::<_, EffectFailure>(ctx.attempt()) })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed(2));
    assert_eq!(
        transitions(&store, &key).await,
        [
            Transition::StartAttempt,
            Transition::LeaseExpired,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
            Transition::Succeeded,
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_heartbeat_keeps_a_long_attempt_leased() {
    let store = MemoryStore::new();
    let ttl = Duration::from_millis(300);
    let worker = |name: &str| {
        Runtime::builder(store.clone())
            .worker_id(WorkerId::new(name))
            .lease_ttl(ttl)
            .build()
    };
    let (a, b) = (worker("a"), worker("b"));

    let long = tokio::spawn(async move {
        a.effect("report.generate", "q3")
            .run(|_| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok::<_, EffectFailure>("done".to_string())
            })
            .await
    });
    // Well past one TTL: without renewal, b would take the effect over.
    tokio::time::sleep(ttl * 2).await;
    let contender = b
        .effect("report.generate", "q3")
        .run(|_| async { Ok::<_, EffectFailure>("duplicate".to_string()) })
        .await
        .unwrap();
    assert!(
        matches!(contender, EffectOutcome::InProgress { .. }),
        "{contender:?}"
    );

    assert_eq!(
        long.await.unwrap().unwrap(),
        EffectOutcome::Committed("done".into())
    );
    assert_eq!(
        transitions(&store, &key("report.generate", "q3")).await,
        [Transition::StartAttempt, Transition::Succeeded]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fenced_worker_cannot_record_its_result() {
    let store = MemoryStore::new();
    let clock = Arc::new(ManualClock::default());
    let (a, b) = (runtime(&store, &clock, "a"), runtime(&store, &clock, "b"));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let stalled = {
        let (started, release) = (started.clone(), release.clone());
        tokio::spawn(async move {
            a.effect("payment.refund", "order-4")
                .run(move |_| {
                    let (started, release) = (started.clone(), release.clone());
                    async move {
                        started.notify_one();
                        release.notified().await;
                        Ok::<_, EffectFailure>("re_1".to_string())
                    }
                })
                .await
        })
    };
    started.notified().await;
    // a stalls past its lease; b takes over and cannot rule out a duplicate.
    clock.advance(TTL);
    let taken_over = b
        .effect("payment.refund", "order-4")
        .run(|_| async { Ok::<_, EffectFailure>("re_2".to_string()) })
        .await
        .unwrap();
    assert!(
        matches!(taken_over, EffectOutcome::NeedsIntervention { .. }),
        "{taken_over:?}"
    );

    release.notify_one();
    let late = stalled.await.unwrap().unwrap();
    assert_eq!(
        late, taken_over,
        "a's late success must not overwrite b's decision"
    );
    assert_eq!(
        transitions(&store, &key("payment.refund", "order-4")).await,
        [
            Transition::StartAttempt,
            Transition::LeaseExpired,
            Transition::Escalate
        ]
    );
}

#[tokio::test]
async fn invalid_requests_are_errors() {
    let (_, _, rt) = setup();
    let err = rt
        .effect("", "x")
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::Identity(_)), "{err}");

    let unserializable: BTreeMap<Vec<u8>, u8> = BTreeMap::from([(vec![1], 1)]);
    let err = rt
        .effect("a.b", "x")
        .input(&unserializable)
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::Input(_)), "{err}");
}

#[tokio::test]
async fn a_changed_output_type_is_reported() {
    let (_, _, rt) = setup();
    rt.effect("a.b", "x")
        .run(|_| async { Ok::<_, EffectFailure>("text".to_string()) })
        .await
        .unwrap();
    let err = rt
        .effect("a.b", "x")
        .run(|_| async { Ok::<_, EffectFailure>(1u32) })
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::Output { .. }), "{err}");
}
