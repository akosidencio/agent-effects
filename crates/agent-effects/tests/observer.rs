//! Observers see every effect recorded and every transition stored.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_effects::store::{EffectStore, NewEffect, TransitionRequest};
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    Clock, EffectFailure, EffectKey, EffectKind, EffectName, EffectObserver, EffectOutcome,
    EffectRecord, EffectStatus, FailureClass, LogicalKey, Observation, Resolution, RetryPolicy,
    Runtime, TokioClock, Transition, WorkerId,
};
use agent_effects_memory::MemoryStore;

/// What one observer saw.
#[derive(Clone, Debug, PartialEq)]
struct Seen {
    transition: Transition,
    from: EffectStatus,
    to: EffectStatus,
    in_previous_status: Duration,
    since_created: Duration,
}

#[derive(Clone, Default)]
struct Recorder {
    created: Arc<Mutex<Vec<String>>>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl EffectObserver for Recorder {
    fn on_created(&self, record: &EffectRecord) {
        self.created.lock().unwrap().push(record.key.to_string());
    }

    fn on_transition(&self, o: &Observation<'_>) {
        self.seen.lock().unwrap().push(Seen {
            transition: o.transition,
            from: o.from,
            to: o.to,
            in_previous_status: o.in_previous_status,
            since_created: o.since_created,
        });
    }
}

impl Recorder {
    fn transitions(&self) -> Vec<Transition> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.transition)
            .collect()
    }
}

const NO_JITTER: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_delay: Duration::from_secs(1),
    max_delay: Duration::from_secs(30),
    multiplier: 2.0,
    jitter: false,
};

#[tokio::test(start_paused = true)]
async fn an_effects_lifecycle_and_durations_are_observed() {
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let recorder = Recorder::default();
    let rt = Runtime::builder(MemoryStore::new())
        .clock(clock)
        .retry_policy(NO_JITTER)
        .observer(recorder.clone())
        .build();
    let run = || {
        let remote = remote.clone();
        rt.effect("op", "k").run(move |_| {
            let remote = remote.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(2)).await; // each attempt takes 2 s
                remote.create("res", None).await
            }
        })
    };
    assert_eq!(
        run().await.unwrap(),
        EffectOutcome::Committed("res#1".into())
    );
    run().await.unwrap(); // a replay: nothing new happens, nothing new is seen

    assert_eq!(*recorder.created.lock().unwrap(), ["op:k"]);
    let seen = recorder.seen.lock().unwrap().clone();
    let summary: Vec<_> = seen.iter().map(|s| (s.transition, s.from, s.to)).collect();
    assert_eq!(
        summary,
        [
            (
                Transition::StartAttempt,
                EffectStatus::Pending,
                EffectStatus::Executing
            ),
            (
                Transition::ScheduleRetry,
                EffectStatus::Executing,
                EffectStatus::Pending
            ),
            (
                Transition::StartAttempt,
                EffectStatus::Pending,
                EffectStatus::Executing
            ),
            (
                Transition::Succeeded,
                EffectStatus::Executing,
                EffectStatus::Committed
            ),
        ]
    );
    let secs = |d: Duration| d.as_secs();
    assert_eq!(
        secs(seen[1].in_previous_status),
        2,
        "first attempt's duration"
    );
    assert_eq!(secs(seen[2].in_previous_status), 1, "the backoff wait");
    assert_eq!(
        secs(seen[3].in_previous_status),
        2,
        "second attempt's duration"
    );
    assert_eq!(secs(seen[3].since_created), 5, "end to end");
}

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new("op").unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

#[tokio::test(start_paused = true)]
async fn recovery_and_operators_are_observed_including_time_spent_unknown() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let recorder = Recorder::default();
    // A worker died mid-attempt.
    let record = store
        .insert_or_get(NewEffect::new(
            key("dead"),
            EffectKind::IrreversibleWrite,
            clock.now(),
        ))
        .await
        .unwrap()
        .record;
    let lease = store
        .acquire_lease(
            record.id,
            &WorkerId::new("dead"),
            clock.now(),
            Duration::from_secs(30),
        )
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
    tokio::time::sleep(Duration::from_secs(30)).await;

    let rt = Runtime::builder(store.clone())
        .clock(clock)
        .observer(recorder.clone())
        .build();
    rt.recover().await.unwrap();
    tokio::time::sleep(Duration::from_secs(600)).await; // ten minutes unknown
    let outcome = rt
        .effect("op", "dead")
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    let EffectOutcome::NeedsIntervention { id } = outcome else {
        panic!("{outcome:?}");
    };
    rt.resolve(
        id,
        Resolution::NotApplied,
        "operator:dennis",
        "never arrived",
    )
    .await
    .unwrap();

    let seen = recorder.seen.lock().unwrap().clone();
    assert_eq!(
        recorder.transitions(),
        [
            Transition::LeaseExpired,
            Transition::Escalate,
            Transition::ResolvedNotApplied
        ]
    );
    assert_eq!(seen[1].from, EffectStatus::Unknown);
    assert_eq!(
        seen[1].in_previous_status.as_secs(),
        600,
        "time spent unknown"
    );
    assert!(
        recorder.created.lock().unwrap().is_empty(),
        "recorded before this runtime"
    );
}

#[tokio::test(start_paused = true)]
async fn approval_and_compensation_are_observed() {
    let recorder = Recorder::default();
    let rt = Runtime::builder(MemoryStore::new())
        .observer(recorder.clone())
        .build();
    let EffectOutcome::AwaitingApproval { id } = rt
        .effect("op", "k")
        .require_approval()
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap()
    else {
        panic!("expected to wait for approval");
    };
    rt.approve(id, "operator:dennis", "ok").await.unwrap();
    rt.effect("op", "k")
        .require_approval()
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    rt.compensation("op", "k")
        .run(|_, _: Option<()>| async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(
        recorder.transitions(),
        [
            Transition::RequestApproval,
            Transition::Approve,
            Transition::StartAttempt,
            Transition::Succeeded,
            Transition::StartCompensation,
            Transition::CompensationSucceeded,
        ]
    );
}

struct Panics;

impl EffectObserver for Panics {
    fn on_created(&self, _: &EffectRecord) {
        panic!("observer bug");
    }

    fn on_transition(&self, _: &Observation<'_>) {
        panic!("observer bug");
    }
}

#[tokio::test(start_paused = true)]
async fn a_panicking_observer_never_breaks_an_effect() {
    let recorder = Recorder::default();
    let rt = Runtime::builder(MemoryStore::new())
        .observer(Panics)
        .observer(recorder.clone())
        .build();
    let outcome = rt
        .effect("op", "k")
        .run(|_| async { Ok::<_, EffectFailure>(7) })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed(7));
    assert_eq!(
        recorder.transitions(),
        [Transition::StartAttempt, Transition::Succeeded],
        "the other observers still see everything"
    );
}
