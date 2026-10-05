//! Failure semantics: classification, retries, timeouts, preconditions and
//! verification, against a scripted remote system.
//!
//! Every test runs on paused Tokio time with a `TokioClock`, so realistic
//! backoff schedules (seconds to minutes) complete instantly and
//! deterministically.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use agent_effects::store::{EffectStore, ErrorRecord, NewEffect, TransitionRequest};
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    Clock, EffectContext, EffectFailure, EffectKey, EffectKind, EffectName, EffectOutcome,
    FailureClass, LogicalKey, Precondition, RetryPolicy, Runtime, TokioClock, Transition,
    Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;
use tokio::sync::Notify;
use tokio::time::Instant;

const RES: &str = "res";
const NAME: &str = "remote.create";

const KINDS: [EffectKind; 4] = [
    EffectKind::Read,
    EffectKind::IdempotentWrite,
    EffectKind::ReversibleWrite,
    EffectKind::IrreversibleWrite,
];

const CLASSES: [FailureClass; 7] = [
    FailureClass::Transient,
    FailureClass::Permanent,
    FailureClass::Ambiguous,
    FailureClass::RateLimited {
        retry_after: Some(Duration::from_secs(5)),
    },
    FailureClass::Authentication,
    FailureClass::Authorization,
    FailureClass::Validation,
];

type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T, EffectFailure>> + Send>>;

fn setup() -> (MemoryStore, Runtime<MemoryStore>, TokioClock) {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let rt = Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new("worker"))
        .build();
    (store, rt, clock)
}

/// An action that creates `RES`, forwarding the idempotency key if asked.
fn create(
    remote: &FakeRemote,
    send_key: bool,
) -> impl Fn(EffectContext) -> BoxFuture<String> + Send + Sync + 'static {
    let remote = remote.clone();
    move |ctx| {
        let remote = remote.clone();
        let key = send_key.then(|| ctx.idempotency_key());
        Box::pin(async move { remote.create(RES, key).await })
    }
}

/// A verification that looks `RES` up.
fn find(
    remote: &FakeRemote,
) -> impl Fn(EffectContext) -> BoxFuture<Verification<String>> + Send + Sync + 'static {
    let remote = remote.clone();
    move |_| {
        let remote = remote.clone();
        Box::pin(async move {
            Ok(match remote.find(RES).await? {
                Some(id) => Verification::Confirmed(id),
                None => Verification::NotApplied,
            })
        })
    }
}

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(NAME).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

async fn transitions(store: &MemoryStore, logical: &str) -> Vec<Transition> {
    let record = store.get_by_key(&key(logical)).await.unwrap().unwrap();
    store
        .events(record.id)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.transition)
        .collect()
}

async fn last_error(store: &MemoryStore, logical: &str) -> ErrorRecord {
    let record = store.get_by_key(&key(logical)).await.unwrap().unwrap();
    record.last_error.expect("an error was recorded")
}

#[tokio::test(start_paused = true)]
async fn every_failure_class_for_every_kind() {
    for kind in KINDS {
        for class in CLASSES {
            let (_, rt, clock) = setup();
            // An ambiguous failure that did not in fact apply, so a re-run
            // creates the resource exactly once.
            let first = if class == FailureClass::Ambiguous {
                Behavior::LoseRequest
            } else {
                Behavior::Fail(class)
            };
            let remote = FakeRemote::new(clock).script([first]);
            let outcome = rt
                .effect(NAME, "matrix")
                .kind(kind)
                .run(create(&remote, false))
                .await
                .unwrap();

            let case = format!("{kind:?} × {class:?}");
            let repeatable = kind.is_naturally_idempotent();
            match class {
                FailureClass::Transient | FailureClass::RateLimited { .. } => {
                    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()), "{case}");
                    assert_eq!(remote.requests(), 2, "{case}: retried once");
                }
                FailureClass::Ambiguous if repeatable => {
                    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()), "{case}");
                    assert_eq!(remote.requests(), 2, "{case}: re-run, it is repeatable");
                }
                FailureClass::Ambiguous => {
                    assert!(
                        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
                        "{case}: {outcome:?}"
                    );
                    assert_eq!(remote.requests(), 1, "{case}: never re-run blindly");
                }
                _ => {
                    assert_eq!(
                        outcome,
                        EffectOutcome::Failed(ErrorRecord {
                            class: Some(class),
                            message: "remote refused the request".into(),
                        }),
                        "{case}"
                    );
                    assert_eq!(remote.requests(), 1, "{case}: not retried");
                }
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn retries_stop_at_the_budget_with_backoff() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient); 5]);
    let outcome = rt
        .effect(NAME, "budget")
        .run(create(&remote, false))
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
    assert_eq!(remote.requests(), 5);
    let events = store
        .events(store.get_by_key(&key("budget")).await.unwrap().unwrap().id)
        .await
        .unwrap();
    let retries: Vec<u64> = events
        .iter()
        .filter(|e| e.transition == Transition::ScheduleRetry)
        .map(|e| e.payload.as_ref().unwrap()["delay_ms"].as_u64().unwrap())
        .collect();
    assert_eq!(retries.len(), 4);
    // Default policy: 1 s doubling, equal jitter keeps each in [d/2, d].
    for (retry, delay) in retries.iter().enumerate() {
        let full = 1000 << retry;
        assert!(
            (full / 2..=full).contains(delay),
            "retry {retry}: {delay} ms"
        );
    }
    assert_eq!(
        events.last().unwrap().transition,
        Transition::FailedDefinitively
    );
}

#[tokio::test(start_paused = true)]
async fn rate_limits_wait_as_long_as_asked() {
    let (_, rt, clock) = setup();
    let limited = FailureClass::RateLimited {
        retry_after: Some(Duration::from_secs(120)),
    };
    let remote = FakeRemote::new(clock).script([Behavior::Fail(limited)]);
    let start = Instant::now();
    let outcome = rt
        .effect(NAME, "limited")
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert!(
        start.elapsed() >= Duration::from_secs(120),
        "{:?}",
        start.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn remote_idempotency_makes_a_commit_then_drop_safe() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::CommitThenDrop]);
    let outcome = rt
        .effect(NAME, "idem")
        .remote_idempotency(true)
        .run(create(&remote, true))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!(remote.requests(), 2, "re-sent");
    assert_eq!(remote.applications(RES), 1, "but applied once");
    assert_eq!(
        transitions(&store, "idem").await,
        [
            Transition::StartAttempt,
            Transition::OutcomeUnknown,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
            Transition::Succeeded,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn verification_confirms_a_commit_then_drop_without_rerunning() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::CommitThenDrop]);
    let outcome = rt
        .effect(NAME, "verified")
        .verify(find(&remote))
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!((remote.requests(), remote.applications(RES)), (1, 1));
    assert_eq!(
        transitions(&store, "verified").await,
        [
            Transition::StartAttempt,
            Transition::OutcomeUnknown,
            Transition::StartVerification,
            Transition::VerificationConfirmed,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn verification_reruns_a_lost_request_and_confirms_the_retry() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::LoseRequest]);
    let outcome = rt
        .effect(NAME, "lost")
        .verify(find(&remote))
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!((remote.requests(), remote.applications(RES)), (2, 1));
    assert_eq!(
        transitions(&store, "lost").await,
        [
            Transition::StartAttempt,
            Transition::OutcomeUnknown,
            Transition::StartVerification,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
            Transition::StartVerification,
            Transition::VerificationConfirmed,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn an_eventually_consistent_lookup_is_waited_out() {
    let (_, rt, clock) = setup();
    let remote = FakeRemote::new(clock)
        .script([Behavior::CommitThenDrop])
        .lag(Duration::from_secs(10));
    let start = Instant::now();
    let outcome = rt
        .effect(NAME, "lagging")
        .verify_eventually(Duration::from_secs(15), find(&remote))
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!(
        remote.applications(RES),
        1,
        "not re-run while the lookup lagged"
    );
    assert!(start.elapsed() >= Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn treating_a_lagging_lookup_as_authoritative_duplicates() {
    // The hazard `verify_eventually` exists for: "not found" from a lagging
    // index, trusted at once, re-runs an effect that already happened.
    let (_, rt, clock) = setup();
    let remote = FakeRemote::new(clock)
        .script([Behavior::CommitThenDrop])
        .lag(Duration::from_secs(10));
    rt.effect(NAME, "misconfigured")
        .verify(find(&remote))
        .run(create(&remote, false))
        .await
        .unwrap();
    assert!(remote.applications(RES) > 1, "{}", remote.applications(RES));
}

#[tokio::test(start_paused = true)]
async fn a_success_is_verified_as_a_postcondition() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock).lag(Duration::from_secs(10));
    let outcome = rt
        .effect(NAME, "post")
        .verify_eventually(Duration::from_secs(15), find(&remote))
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!(remote.requests(), 1);
    assert_eq!(
        transitions(&store, "post").await,
        [
            Transition::StartAttempt,
            Transition::StartVerification,
            Transition::VerificationConfirmed,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn inconclusive_verification_stays_unknown_until_a_later_call_resolves_it() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::LoseRequest]);
    let checks = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&checks);
    let first = rt
        .effect(NAME, "murky")
        .verify(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, EffectFailure>(Verification::<String>::Inconclusive) }
        })
        .run(create(&remote, false))
        .await
        .unwrap();
    assert!(matches!(first, EffectOutcome::Unknown { .. }), "{first:?}");
    assert_eq!(
        checks.load(Ordering::SeqCst),
        5,
        "one check per allowed attempt"
    );
    assert!(
        last_error(&store, "murky")
            .await
            .message
            .contains("inconclusive")
    );

    let second = rt
        .effect(NAME, "murky")
        .verify(|_| async { Ok::<_, EffectFailure>(Verification::Confirmed("found".to_string())) })
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(second, EffectOutcome::Committed("found".into()));
    assert_eq!(remote.requests(), 1, "never re-run without proof");
}

#[tokio::test(start_paused = true)]
async fn a_verification_conflict_goes_to_an_operator() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::CommitThenDrop]);
    let outcome = rt
        .effect(NAME, "conflict")
        .verify(|_| async {
            Ok::<_, EffectFailure>(Verification::<String>::Conflict {
                details: "a payment exists with a different amount".into(),
            })
        })
        .run(create(&remote, false))
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        last_error(&store, "conflict").await.message,
        "a payment exists with a different amount"
    );
}

#[tokio::test(start_paused = true)]
async fn an_exhausted_budget_escalates_an_unknown_outcome() {
    let (_, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::LoseRequest, Behavior::LoseRequest]);
    let outcome = rt
        .effect(NAME, "spent")
        .kind(EffectKind::IdempotentWrite)
        .retry(RetryPolicy {
            max_attempts: 2,
            ..RetryPolicy::default()
        })
        .run(create(&remote, false))
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    assert_eq!(remote.requests(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_attempt_is_ambiguous() {
    for (kind, expect_committed) in [
        (EffectKind::IrreversibleWrite, false),
        (EffectKind::IdempotentWrite, true),
    ] {
        let (store, rt, clock) = setup();
        let remote = FakeRemote::new(clock).script([Behavior::Hang]);
        let outcome = rt
            .effect(NAME, "slow")
            .kind(kind)
            .attempt_timeout(Duration::from_secs(10))
            .run(create(&remote, false))
            .await
            .unwrap();
        if expect_committed {
            assert_eq!(
                outcome,
                EffectOutcome::Committed("res#1".into()),
                "{kind:?}"
            );
            assert_eq!(remote.requests(), 2);
        } else {
            assert!(
                matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
                "{outcome:?}"
            );
            assert!(
                last_error(&store, "slow")
                    .await
                    .message
                    .contains("timed out")
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_rejecting_precondition_stops_the_effect_before_it_runs() {
    let (store, rt, clock) = setup();
    let remote = FakeRemote::new(clock);
    let outcome = rt
        .effect(NAME, "stale")
        .precondition(|_| async { Precondition::reject("order is no longer pending") })
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        EffectOutcome::Rejected(ErrorRecord {
            class: None,
            message: "order is no longer pending".into(),
        })
    );
    assert_eq!(remote.requests(), 0);
    assert_eq!(
        transitions(&store, "stale").await,
        [Transition::PreconditionRejected]
    );
}

#[tokio::test(start_paused = true)]
async fn a_deferring_precondition_is_checked_again() {
    let (_, rt, clock) = setup();
    let remote = FakeRemote::new(clock);
    let checks = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&checks);
    let start = Instant::now();
    let outcome = rt
        .effect(NAME, "deferred")
        .precondition(move |_| {
            let check = counted.fetch_add(1, Ordering::SeqCst);
            async move {
                if check < 2 {
                    Precondition::retry_later(Duration::from_secs(30), "stock not settled")
                } else {
                    Precondition::Satisfied
                }
            }
        })
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!(checks.load(Ordering::SeqCst), 3);
    assert!(start.elapsed() >= Duration::from_secs(60));
}

#[tokio::test(start_paused = true)]
async fn a_precondition_that_never_clears_rejects() {
    let (_, rt, clock) = setup();
    let remote = FakeRemote::new(clock);
    let outcome = rt
        .effect(NAME, "never")
        .precondition(|_| async {
            Precondition::retry_later(Duration::from_secs(30), "stock not settled")
        })
        .run(create(&remote, false))
        .await
        .unwrap();
    match outcome {
        EffectOutcome::Rejected(error) => {
            assert!(
                error.message.contains("after 5 checks: stock not settled"),
                "{}",
                error.message
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(remote.requests(), 0);
}

#[tokio::test(start_paused = true)]
async fn the_precondition_runs_only_before_the_first_attempt() {
    // "Not created yet" turns false *because* the first attempt applied. If
    // the precondition ran before the re-run, it would reject an effect that
    // happened.
    let (_, rt, clock) = setup();
    let remote = FakeRemote::new(clock).script([Behavior::CommitThenDrop]);
    let checks = Arc::new(AtomicU32::new(0));
    let (counted, observed) = (Arc::clone(&checks), remote.clone());
    let outcome = rt
        .effect(NAME, "once")
        .remote_idempotency(true)
        .precondition(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            let created = observed.applications(RES);
            async move {
                if created == 0 {
                    Precondition::Satisfied
                } else {
                    Precondition::reject("already created")
                }
            }
        })
        .run(create(&remote, true))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!(checks.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_crash_during_backoff_resumes_on_schedule() {
    let (store, rt, clock) = setup();
    // A worker failed its first attempt, scheduled a retry in 60 s, then died.
    let record = store
        .insert_or_get(NewEffect::new(
            key("resume"),
            EffectKind::IrreversibleWrite,
            clock.now(),
        ))
        .await
        .unwrap()
        .record;
    let lease = store
        .acquire_lease(
            record.id,
            &WorkerId::new("crashed"),
            clock.now(),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let record = store
        .transition(TransitionRequest::new(
            &record,
            Some(&lease),
            Transition::StartAttempt,
            clock.now(),
        ))
        .await
        .unwrap();
    let mut retry = TransitionRequest::new(
        &record,
        Some(&lease),
        Transition::ScheduleRetry,
        clock.now(),
    );
    retry.next_attempt_at = Some(clock.now() + Duration::from_secs(60));
    store.transition(retry).await.unwrap();
    let start = Instant::now();
    tokio::time::sleep(Duration::from_secs(31)).await;

    let remote = FakeRemote::new(clock);
    let outcome = rt
        .effect(NAME, "resume")
        .run(create(&remote, false))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert!(
        start.elapsed() >= Duration::from_secs(60),
        "waited for the schedule"
    );
    assert_eq!(
        transitions(&store, "resume").await,
        [
            Transition::StartAttempt,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
            Transition::Succeeded,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_concurrent_caller_can_wait_for_the_outcome() {
    let (_, rt, _) = setup();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let first = {
        let rt = rt.clone();
        let (started, release) = (started.clone(), release.clone());
        tokio::spawn(async move {
            rt.effect(NAME, "shared")
                .run(move |_| {
                    let (started, release) = (started.clone(), release.clone());
                    async move {
                        started.notify_one();
                        release.notified().await;
                        Ok::<_, EffectFailure>("done".to_string())
                    }
                })
                .await
        })
    };
    started.notified().await;

    let second = rt
        .effect(NAME, "shared")
        .run(|_| async { Ok::<_, EffectFailure>("duplicate".to_string()) })
        .await
        .unwrap();
    let EffectOutcome::InProgress { id } = second else {
        panic!("{second:?}");
    };
    assert_eq!(
        rt.wait::<String>(id, Duration::from_secs(5)).await.unwrap(),
        EffectOutcome::InProgress { id },
        "still running at the deadline"
    );

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        release.notify_one();
    });
    assert_eq!(
        rt.wait::<String>(id, Duration::from_secs(60))
            .await
            .unwrap(),
        EffectOutcome::Committed("done".into())
    );
    assert_eq!(
        first.await.unwrap().unwrap(),
        EffectOutcome::Committed("done".into())
    );
}
