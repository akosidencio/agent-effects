//! Durable handlers: running registered effects, and finishing them after a
//! crash with no caller at all.

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::handler::{EffectHandler, Handler, VerifiableEffect};
use agent_effects::store::{EffectStore, NewEffect, TransitionRequest};
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    Clock, EffectContext, EffectFailure, EffectKey, EffectKind, EffectName, EffectOutcome,
    EffectStatus, LogicalKey, Precondition, Runtime, RuntimeError, TokioClock, Transition,
    Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;
use serde_json::json;

const TTL: Duration = Duration::from_secs(30);
const NAME: &str = "remote.create";

/// Protection levels, as a const generic so each gets its own handler type
/// (only the verified one implements `VerifiableEffect`).
const VERIFIED: u8 = 0;
const IDEMPOTENT: u8 = 1;
const UNPROTECTED: u8 = 2;

/// Creates the resource named by its input on a `FakeRemote`.
struct Create<const P: u8> {
    remote: FakeRemote,
    executions: Arc<AtomicU32>,
}

impl<const P: u8> Create<P> {
    fn new(remote: &FakeRemote) -> Self {
        Self {
            remote: remote.clone(),
            executions: Arc::default(),
        }
    }
}

impl<const P: u8> EffectHandler for Create<P> {
    const NAME: &'static str = NAME;
    type Input = String;
    type Output = String;
    type Error = EffectFailure;

    fn kind(&self) -> EffectKind {
        EffectKind::IrreversibleWrite
    }

    fn remote_idempotency(&self) -> bool {
        P == IDEMPOTENT
    }

    async fn execute(
        &self,
        ctx: &EffectContext,
        resource: &String,
    ) -> Result<String, EffectFailure> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let key = (P == IDEMPOTENT).then(|| ctx.idempotency_key());
        self.remote.create(resource, key).await
    }
}

impl VerifiableEffect for Create<VERIFIED> {
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

fn handler<const P: u8>(remote: &FakeRemote) -> Handler<Create<P>> {
    Handler::new(Create::<P>::new(remote))
}

fn verified(remote: &FakeRemote) -> Handler<Create<VERIFIED>> {
    handler::<VERIFIED>(remote).verifiable()
}

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(NAME).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
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

fn builder(
    store: &MemoryStore,
    clock: TokioClock,
    worker: &str,
) -> agent_effects::RuntimeBuilder<MemoryStore> {
    Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new(worker))
        .lease_ttl(TTL)
}

#[tokio::test(start_paused = true)]
async fn a_submitted_effect_commits_and_replays() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let create = Create::<UNPROTECTED>::new(&remote);
    let executions = Arc::clone(&create.executions);
    let rt = builder(&store, clock, "w")
        .register(Handler::new(create))
        .build();

    let first = rt
        .submit::<Create<UNPROTECTED>>("k1", "res".into())
        .actor("agent:test")
        .await
        .unwrap();
    let again = rt
        .submit::<Create<UNPROTECTED>>("k1", "res".into())
        .await
        .unwrap();
    assert_eq!(first, EffectOutcome::Committed("res#1".into()));
    assert_eq!(again, first);
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    let record = store.get_by_key(&key("k1")).await.unwrap().unwrap();
    assert_eq!(
        record.input,
        Some(json!("res")),
        "the full input is stored for recovery"
    );
    assert_eq!(record.created_by.as_deref(), Some("agent:test"));

    let err = rt
        .submit::<Create<UNPROTECTED>>("k1", "other".into())
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::InputMismatch { .. }), "{err}");
}

#[tokio::test(start_paused = true)]
async fn submitting_to_an_unregistered_handler_is_an_error() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    // Same name, different handler type.
    let rt = builder(&store, clock, "w")
        .register(handler::<IDEMPOTENT>(&remote))
        .build();
    let err = rt
        .submit::<Create<UNPROTECTED>>("k", "res".into())
        .await
        .unwrap_err();
    assert!(
        matches!(err, RuntimeError::NotRegistered { name: NAME }),
        "{err}"
    );
}

#[test]
#[should_panic(expected = "already registered")]
fn registering_two_handlers_for_one_name_panics() {
    let remote = FakeRemote::new(agent_effects::SystemClock);
    let _ = Runtime::builder(MemoryStore::new())
        .register(handler::<IDEMPOTENT>(&remote))
        .register(handler::<UNPROTECTED>(&remote));
}

#[tokio::test(start_paused = true)]
async fn a_verifiable_handler_confirms_instead_of_rerunning() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock).script([Behavior::CommitThenDrop]);
    let rt = builder(&store, clock, "w")
        .register(verified(&remote))
        .build();
    let outcome = rt
        .submit::<Create<VERIFIED>>("k", "res".into())
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
    assert_eq!((remote.requests(), remote.applications("res")), (1, 1));
}

struct Gated;

impl EffectHandler for Gated {
    const NAME: &'static str = "gated";
    type Input = bool;
    type Output = ();
    type Error = EffectFailure;

    fn precondition(
        &self,
        _: &EffectContext,
        open: &bool,
    ) -> impl Future<Output = Precondition> + Send {
        std::future::ready(if *open {
            Precondition::Satisfied
        } else {
            Precondition::reject("gate closed")
        })
    }

    fn execute(
        &self,
        _: &EffectContext,
        _: &bool,
    ) -> impl Future<Output = Result<(), EffectFailure>> + Send {
        std::future::ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn a_handler_precondition_can_reject() {
    let rt = Runtime::new(MemoryStore::new());
    let rt = Runtime::builder(rt.store().clone())
        .register(Handler::new(Gated))
        .build();
    let outcome = rt.submit::<Gated>("g", false).await.unwrap();
    assert!(
        matches!(&outcome, EffectOutcome::Rejected(e) if e.message == "gate closed"),
        "{outcome:?}"
    );
}

/// Crash at `point` while submitting, then never call again: only recovery
/// runs. Returns the status recovery left, and how often the resource was
/// created.
async fn crash_then_recover_without_a_caller<const P: u8>(
    point: FaultPoint,
    register: impl Fn(&FakeRemote) -> Handler<Create<P>>,
) -> (Option<EffectStatus>, u32) {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);

    let injector = Arc::new(FaultInjector::new().at(point).crash());
    let doomed = builder(&store, clock, "doomed")
        .register(register(&remote))
        .fault_injector(Arc::clone(&injector))
        .build();
    let first = doomed.submit::<Create<P>>("k", "res".into()).await;
    if injector.reached().contains(&point) {
        assert!(first.is_err(), "{point:?}: crashed");
    }
    tokio::time::sleep(Duration::from_millis(1)).await;
    tokio::time::sleep(TTL).await;

    // A new process with the handler registered, and no caller.
    let recovery = builder(&store, clock, "recovery")
        .register(register(&remote))
        .build();
    let report = recovery.recover().await.unwrap();
    assert!(
        report.unhandled.is_empty() && report.resume_errors.is_empty(),
        "{report:?}"
    );
    let status = store.get_by_key(&key("k")).await.unwrap().map(|r| r.status);
    (status, remote.applications("res"))
}

const POINTS: [FaultPoint; 6] = [
    FaultPoint::BeforeInsert,
    FaultPoint::AfterInsert,
    FaultPoint::AfterAttemptPersisted,
    FaultPoint::AfterActionStarted,
    FaultPoint::AfterActionReturned,
    FaultPoint::AfterVerificationStarted,
];

#[tokio::test(start_paused = true)]
async fn recovery_finishes_protected_effects_with_no_caller() {
    quiet_crashes();
    for point in POINTS {
        let (status, created) =
            crash_then_recover_without_a_caller::<VERIFIED>(point, verified).await;
        let (idem_status, idem_created) =
            crash_then_recover_without_a_caller::<IDEMPOTENT>(point, handler::<IDEMPOTENT>).await;
        if point == FaultPoint::BeforeInsert {
            // Nothing was recorded, so there is nothing to recover.
            assert_eq!((status, created), (None, 0), "{point:?}");
            assert_eq!((idem_status, idem_created), (None, 0), "{point:?}");
            continue;
        }
        assert_eq!(
            (status, created),
            (Some(EffectStatus::Committed), 1),
            "{point:?} verified"
        );
        assert_eq!(
            (idem_status, idem_created),
            (Some(EffectStatus::Committed), 1),
            "{point:?} idempotent"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn recovery_escalates_unprotected_effects_it_cannot_settle() {
    quiet_crashes();
    for point in POINTS {
        let (status, created) =
            crash_then_recover_without_a_caller::<UNPROTECTED>(point, handler::<UNPROTECTED>).await;
        let expected = match point {
            FaultPoint::BeforeInsert => (None, 0),
            // Nothing was sent: recovery runs it.
            FaultPoint::AfterInsert => (Some(EffectStatus::Committed), 1),
            // In doubt: never re-run blindly.
            FaultPoint::AfterAttemptPersisted => (Some(EffectStatus::NeedsIntervention), 0),
            FaultPoint::AfterActionStarted | FaultPoint::AfterActionReturned => {
                (Some(EffectStatus::NeedsIntervention), 1)
            }
            // Not on an unverified effect's path: it committed normally.
            _ => (Some(EffectStatus::Committed), 1),
        };
        assert_eq!((status, created), expected, "{point:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn recovery_waits_for_a_scheduled_retry_to_fall_due() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    // A worker failed its first attempt, scheduled a retry in 60 s, died.
    let mut new = NewEffect::new(key("later"), EffectKind::IrreversibleWrite, clock.now());
    new.input = Some(json!("res"));
    new.input_fingerprint = None;
    let record = store.insert_or_get(new).await.unwrap().record;
    let lease = store
        .acquire_lease(record.id, &WorkerId::new("dead"), clock.now(), TTL)
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
    tokio::time::sleep(TTL).await;

    let rt = builder(&store, clock, "recovery")
        .register(handler::<UNPROTECTED>(&remote))
        .build();
    let early = rt.recover().await.unwrap();
    assert!(early.resumed.is_empty(), "not due yet: {early:?}");
    tokio::time::sleep(Duration::from_secs(31)).await;
    let due = rt.recover().await.unwrap();
    assert_eq!(due.resumed, [(record.id, EffectStatus::Committed)]);
}

#[tokio::test(start_paused = true)]
async fn an_input_that_no_longer_fits_is_reported_not_run() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let mut new = NewEffect::new(key("stale"), EffectKind::IrreversibleWrite, clock.now());
    new.input = Some(json!({ "an": "object" })); // the handler expects a string
    let record = store.insert_or_get(new).await.unwrap().record;

    let rt = builder(&store, clock, "recovery")
        .register(handler::<UNPROTECTED>(&remote))
        .build();
    let report = rt.recover().await.unwrap();
    assert_eq!(report.resume_errors.len(), 1, "{report:?}");
    assert_eq!(report.resume_errors[0].0, record.id);
    assert_eq!(remote.requests(), 0);
}

#[tokio::test(start_paused = true)]
async fn the_recovery_loop_finishes_effects_on_its_own() {
    quiet_crashes();
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let injector = Arc::new(
        FaultInjector::new()
            .at(FaultPoint::AfterAttemptPersisted)
            .crash(),
    );
    let doomed = builder(&store, clock, "doomed")
        .register(handler::<IDEMPOTENT>(&remote))
        .fault_injector(injector)
        .build();
    assert!(
        doomed
            .submit::<Create<IDEMPOTENT>>("loop", "res".into())
            .await
            .is_err()
    );

    let worker = builder(&store, clock, "recovery")
        .register(handler::<IDEMPOTENT>(&remote))
        .build();
    let task = tokio::spawn(async move { worker.run_recovery(Duration::from_secs(10)).await });
    tokio::time::sleep(TTL + Duration::from_secs(15)).await;
    task.abort();
    let record = store.get_by_key(&key("loop")).await.unwrap().unwrap();
    assert_eq!(record.status, EffectStatus::Committed);
    assert_eq!(remote.applications("res"), 1);
}
