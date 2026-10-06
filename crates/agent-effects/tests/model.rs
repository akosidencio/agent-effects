//! Model-based property test: random effect configurations, remote
//! behaviors, and sequences of calls, crashes, lease expiries and recovery
//! passes, checked against the transition table and the safety rules.
//!
//! After every case:
//!
//! 1. The audit trail replays through `EffectStatus::apply` from `Pending`,
//!    with contiguous sequence numbers, and ends at the record's status and
//!    version.
//! 2. An effect that is not naturally idempotent is created at most once,
//!    whatever crashed when.
//! 3. `Committed` means the remote created it; `Failed` on such an effect
//!    means it did not.

use std::sync::{Arc, Once};
use std::time::Duration;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::store::EffectStore;
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    EffectContext, EffectFailure, EffectKey, EffectKind, EffectName, EffectStatus, FailureClass,
    LogicalKey, Runtime, TokioClock, Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;
use proptest::prelude::*;
use proptest::sample::select;
use proptest::test_runner::TestCaseError;

const TTL: Duration = Duration::from_secs(30);
const RES: &str = "res";

const KINDS: [EffectKind; 4] = [
    EffectKind::Read,
    EffectKind::IdempotentWrite,
    EffectKind::ReversibleWrite,
    EffectKind::IrreversibleWrite,
];

const CLASSES: [FailureClass; 6] = [
    FailureClass::Transient,
    FailureClass::Permanent,
    FailureClass::RateLimited { retry_after: None },
    FailureClass::Authentication,
    FailureClass::Authorization,
    FailureClass::Validation,
];

const POINTS: [FaultPoint; 6] = [
    FaultPoint::BeforeInsert,
    FaultPoint::AfterInsert,
    FaultPoint::AfterAttemptPersisted,
    FaultPoint::AfterActionStarted,
    FaultPoint::AfterActionReturned,
    FaultPoint::AfterVerificationStarted,
];

#[derive(Clone, Copy, Debug)]
enum Step {
    /// A caller runs the effect on a fresh worker.
    Call,
    /// A caller runs it on a worker that crashes at the point.
    CrashAt(FaultPoint),
    /// A recovery pass.
    Recover,
    /// Time passes beyond a lease.
    Expire,
}

#[derive(Clone, Debug)]
struct Case {
    kind: EffectKind,
    remote_idempotency: bool,
    verify: bool,
    script: Vec<Behavior>,
    steps: Vec<Step>,
}

fn behavior() -> impl Strategy<Value = Behavior> {
    prop_oneof![
        3 => Just(Behavior::Succeed),
        2 => select(CLASSES.to_vec()).prop_map(Behavior::Fail),
        1 => Just(Behavior::Unreachable),
        2 => Just(Behavior::CommitThenDrop),
        2 => Just(Behavior::LoseRequest),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => Just(Step::Call),
        2 => select(POINTS.to_vec()).prop_map(Step::CrashAt),
        1 => Just(Step::Recover),
        2 => Just(Step::Expire),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    (
        select(KINDS.to_vec()),
        any::<bool>(),
        any::<bool>(),
        prop::collection::vec(behavior(), 0..8),
        prop::collection::vec(step(), 1..8),
    )
        .prop_map(|(kind, remote_idempotency, verify, script, steps)| Case {
            kind,
            remote_idempotency,
            verify,
            script,
            steps,
        })
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

async fn call(
    case: &Case,
    store: &MemoryStore,
    clock: TokioClock,
    remote: &FakeRemote,
    worker: String,
    faults: Option<Arc<FaultInjector>>,
) -> Result<(), String> {
    let mut builder = Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new(worker))
        .lease_ttl(TTL);
    if let Some(faults) = faults {
        builder = builder.fault_injector(faults);
    }
    let rt = builder.build();
    let send_key = case.remote_idempotency;
    let action = {
        let remote = remote.clone();
        move |ctx: EffectContext| {
            let remote = remote.clone();
            let key = send_key.then(|| ctx.idempotency_key());
            async move { remote.create(RES, key).await }
        }
    };
    let effect = rt
        .effect("model", "effect")
        .kind(case.kind)
        .remote_idempotency(case.remote_idempotency);
    let result = if case.verify {
        let lookup = remote.clone();
        effect
            .verify(move |_| {
                let lookup = lookup.clone();
                async move {
                    Ok::<_, EffectFailure>(match lookup.find(RES).await? {
                        Some(id) => Verification::Confirmed(id),
                        None => Verification::NotApplied,
                    })
                }
            })
            .run(action)
            .await
    } else {
        effect.run(action).await
    };
    result.map(|_| ()).map_err(|e| e.to_string())
}

async fn check(case: Case) -> Result<(), TestCaseError> {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock).script(case.script.clone());

    for (i, step) in case.steps.iter().enumerate() {
        let worker = format!("worker-{i}");
        match *step {
            Step::Call => {
                let result = call(&case, &store, clock, &remote, worker, None).await;
                prop_assert!(result.is_ok(), "step {i}: {result:?}");
            }
            Step::CrashAt(point) => {
                let injector = Arc::new(FaultInjector::new().at(point).crash());
                let result = call(
                    &case,
                    &store,
                    clock,
                    &remote,
                    worker,
                    Some(Arc::clone(&injector)),
                )
                .await;
                if injector.reached().contains(&point) {
                    prop_assert!(
                        result.as_ref().is_err_and(|e| e.contains("fault injected")),
                        "step {i}: {result:?}"
                    );
                } else {
                    prop_assert!(result.is_ok(), "step {i}: {result:?}");
                }
                // Let a request that was on the wire land.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Step::Recover => {
                let rt = Runtime::builder(store.clone())
                    .clock(clock)
                    .worker_id(WorkerId::new(worker))
                    .lease_ttl(TTL)
                    .build();
                prop_assert!(rt.recover().await.is_ok());
            }
            Step::Expire => tokio::time::sleep(TTL).await,
        }
    }

    let key = EffectKey::new(
        EffectName::new("model").unwrap(),
        LogicalKey::new("effect").unwrap(),
    );
    let created = remote.applications(RES);
    let Some(record) = store.get_by_key(&key).await.unwrap() else {
        prop_assert_eq!(created, 0, "created without a record");
        return Ok(());
    };

    let events = store.events(record.id).await.unwrap();
    let mut status = EffectStatus::Pending;
    for (i, event) in events.iter().enumerate() {
        prop_assert_eq!(event.sequence, i as u64 + 1, "contiguous sequence");
        prop_assert_eq!(
            event.from,
            status,
            "event {} starts where the last ended",
            i
        );
        prop_assert_eq!(
            status.apply(event.transition),
            Ok(event.to),
            "legal transition"
        );
        status = event.to;
    }
    prop_assert_eq!(status, record.status, "trail ends at the record's status");
    prop_assert_eq!(record.version, events.len() as u64);

    if !case.kind.is_naturally_idempotent() {
        prop_assert!(created <= 1, "created {} times: {:?}", created, events);
        if record.status == EffectStatus::Failed {
            prop_assert_eq!(created, 0, "failed, yet created: {:?}", events);
        }
    }
    if record.status == EffectStatus::Committed {
        prop_assert!(created >= 1, "committed without being created");
    }
    Ok(())
}

proptest! {
    // 512 cases by default; PROPTEST_CASES=50000 for a longer local soak.
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(512),
        ..ProptestConfig::default()
    })]

    #[test]
    fn histories_follow_the_state_machine_and_never_duplicate(case in case()) {
        quiet_crashes();
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(check(case))?;
    }
}
