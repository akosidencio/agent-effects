//! In-process crash suite: one test per crash point (design §11).
//!
//! Each test runs the effect on a runtime armed to crash at the point, lets
//! its lease expire, runs recovery, then re-runs the effect on a fresh
//! runtime over the same store, as the restarted process would. It does
//! this for each of three protection levels:
//!
//! - **verified**: irreversible, with an authoritative lookup;
//! - **idempotent**: irreversible, the remote deduplicates on the key;
//! - **unprotected**: irreversible, neither.
//!
//! Every combination must end with the resource created at most once, and
//! exactly once unless an operator has to decide.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Once};
use std::time::Duration;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::store::EffectStore;
use agent_effects::testkit::FakeRemote;
use agent_effects::{
    EffectContext, EffectFailure, EffectKey, EffectName, EffectOutcome, EffectStatus, LogicalKey,
    Runtime, RuntimeError, TokioClock, Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;

const TTL: Duration = Duration::from_secs(30);
const RES: &str = "res";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Protection {
    Verified,
    Idempotent,
    Unprotected,
}

const PROTECTIONS: [Protection; 3] = [
    Protection::Verified,
    Protection::Idempotent,
    Protection::Unprotected,
];

/// What a restarted caller must find.
#[derive(Debug, PartialEq, Eq)]
enum Expect {
    /// Committed, resource created once.
    CreatedOnce,
    /// Escalated, resource created `created` times (0 or 1).
    Operator { created: u32 },
}

/// Silences the panics that simulate crashes; everything else still prints.
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

type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T, EffectFailure>> + Send>>;

/// Runs the effect with `protection` on `rt`.
async fn run(
    rt: &Runtime<MemoryStore>,
    remote: &FakeRemote,
    protection: Protection,
) -> Result<EffectOutcome<String>, RuntimeError> {
    let send_key = protection == Protection::Idempotent;
    let action = {
        let remote = remote.clone();
        move |ctx: EffectContext| -> BoxFuture<String> {
            let remote = remote.clone();
            let key = send_key.then(|| ctx.idempotency_key());
            Box::pin(async move { remote.create(RES, key).await })
        }
    };
    let builder = rt.effect("op", "crash").remote_idempotency(send_key);
    if protection == Protection::Verified {
        let lookup = remote.clone();
        builder
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
        builder.run(action).await
    }
}

fn worker(
    store: &MemoryStore,
    clock: TokioClock,
    name: &str,
) -> agent_effects::RuntimeBuilder<MemoryStore> {
    Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new(name))
        .lease_ttl(TTL)
}

async fn crash_and_recover(point: FaultPoint, protection: Protection, expect: &Expect) {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);

    let injector = Arc::new(FaultInjector::new().at(point).crash());
    let doomed = worker(&store, clock, "doomed")
        .fault_injector(Arc::clone(&injector))
        .build();
    let first = run(&doomed, &remote, protection).await;
    let reached = injector.reached().contains(&point);
    let case = format!("{point:?} / {protection:?}");
    // Every point is on every effect's path, except verification, which
    // only verified effects reach. Guards against a suite that passes
    // because nothing crashed.
    let reachable =
        point != FaultPoint::AfterVerificationStarted || protection == Protection::Verified;
    assert_eq!(reached, reachable, "{case}: point reached");
    if reached {
        let err = first.expect_err(&case);
        assert!(err.to_string().contains("fault injected"), "{case}: {err}");
    } else {
        // Verification points are only reached by verified effects.
        assert_eq!(
            first.unwrap(),
            EffectOutcome::Committed("res#1".into()),
            "{case}"
        );
    }
    // An action started before the crash keeps running, like a request on
    // the wire: let it land before the process "restarts".
    tokio::time::sleep(Duration::from_millis(1)).await;

    // The process is gone; its lease runs out.
    tokio::time::sleep(TTL).await;
    let marked = worker(&store, clock, "recovery")
        .build()
        .recover()
        .await
        .unwrap();
    let key = EffectKey::new(
        EffectName::new("op").unwrap(),
        LogicalKey::new("crash").unwrap(),
    );
    let in_doubt = matches!(
        point,
        FaultPoint::AfterAttemptPersisted
            | FaultPoint::AfterActionStarted
            | FaultPoint::AfterActionReturned
            | FaultPoint::AfterVerificationStarted
    ) && reached;
    assert_eq!(
        marked.marked_unknown.len(),
        usize::from(in_doubt),
        "{case}: recovery"
    );
    if in_doubt {
        let record = store.get_by_key(&key).await.unwrap().unwrap();
        assert_eq!(record.status, EffectStatus::Unknown, "{case}");
    }

    let restarted = worker(&store, clock, "restarted").build();
    let outcome = run(&restarted, &remote, protection).await.unwrap();
    let expect = if reached {
        expect
    } else {
        &Expect::CreatedOnce
    };
    match expect {
        Expect::CreatedOnce => {
            assert_eq!(outcome, EffectOutcome::Committed("res#1".into()), "{case}");
            assert_eq!(remote.applications(RES), 1, "{case}: created once");
        }
        Expect::Operator { created } => {
            assert!(
                matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
                "{case}: {outcome:?}"
            );
            assert_eq!(
                remote.applications(RES),
                *created,
                "{case}: never re-run blindly"
            );
        }
    }
    // Whatever happened is now settled for every later caller too.
    assert_eq!(
        run(&restarted, &remote, protection).await.unwrap(),
        outcome,
        "{case}"
    );
}

async fn crash_point(point: FaultPoint, unprotected: Expect) {
    quiet_crashes();
    for protection in PROTECTIONS {
        let expect = match protection {
            Protection::Unprotected => &unprotected,
            _ => &Expect::CreatedOnce,
        };
        crash_and_recover(point, protection, expect).await;
    }
}

#[tokio::test(start_paused = true)]
async fn crash_before_insert() {
    // Nothing recorded, nothing sent: the restarted call starts fresh.
    crash_point(FaultPoint::BeforeInsert, Expect::CreatedOnce).await;
}

#[tokio::test(start_paused = true)]
async fn crash_after_insert() {
    // Pending, no lease: safe to start, nothing was sent.
    crash_point(FaultPoint::AfterInsert, Expect::CreatedOnce).await;
}

#[tokio::test(start_paused = true)]
async fn crash_after_the_attempt_is_persisted() {
    // Executing, the request never left. The runtime cannot know that, so
    // an unprotected effect goes to an operator, with nothing created.
    crash_point(
        FaultPoint::AfterAttemptPersisted,
        Expect::Operator { created: 0 },
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn crash_while_the_request_is_in_flight() {
    // The request lands after the crash.
    crash_point(
        FaultPoint::AfterActionStarted,
        Expect::Operator { created: 1 },
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn crash_after_the_response_before_persisting_it() {
    // Also covers "after the remote commit, before the response": either
    // way the remote applied it and the runtime never recorded the result.
    crash_point(
        FaultPoint::AfterActionReturned,
        Expect::Operator { created: 1 },
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn crash_during_verification() {
    // Only verified effects verify; the others commit normally.
    crash_point(FaultPoint::AfterVerificationStarted, Expect::CreatedOnce).await;
}
