//! Model-based property test: random effect configurations, remote
//! behaviors, and sequences of calls, crashes, lease expiries, recovery
//! passes, compensations, operator decisions and pruning, checked against
//! the transition table and the safety rules.
//!
//! An effect runs as a closure or as a registered durable handler, which
//! recovery finishes without a caller. Pruning a settled record starts a new
//! *generation*: the key is free, so the next call is a new effect against a
//! fresh remote. Before each prune and after the last step, the current
//! generation must satisfy:
//!
//! 1. The audit trail replays through `EffectStatus::apply` from `Pending`,
//!    with contiguous sequence numbers, and ends at the record's status and
//!    version.
//! 2. An effect that is not naturally idempotent is created at most once,
//!    whatever crashed when.
//! 3. `Committed` means the remote created it; `Failed` on such an effect
//!    means it did not.
//! 4. Policies hold: no automatic retry under a no-retry policy, no attempt
//!    before approval, nothing created after a denial, nothing cancelled
//!    before compensation started.
//! 5. Observers saw exactly the audit trail, crashes or not.
//! 6. The secret in the input appears nowhere in the record or the trail.
//!
//! And every prune removes exactly the records that are settled and not
//! under a live lease.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::handler::{CompensableEffect, EffectHandler, Handler, VerifiableEffect};
use agent_effects::store::EffectStore;
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    ApprovalDecision, ApprovalProvider, Clock, CompensationContext, EffectContext, EffectFailure,
    EffectKey, EffectKind, EffectName, EffectObserver, EffectRecord, EffectStatus, FailureClass,
    LogicalKey, Observation, PolicyBuilder, Resolution, RetentionPolicy, RiskLevel, Runtime,
    RuntimeError, Secret, StoreError, TokioClock, Transition, Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;
use proptest::prelude::*;
use proptest::sample::select;
use proptest::test_runner::TestCaseError;
use serde::{Deserialize, Serialize};
use serde_json::json;

const TTL: Duration = Duration::from_secs(30);
const RES: &str = "res";
const SECRET: &str = "s3cr3t-model-token";

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

const POINTS: [FaultPoint; 7] = [
    FaultPoint::BeforeInsert,
    FaultPoint::AfterInsert,
    FaultPoint::AfterApprovalRequested,
    FaultPoint::AfterAttemptPersisted,
    FaultPoint::AfterActionStarted,
    FaultPoint::AfterActionReturned,
    FaultPoint::AfterVerificationStarted,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// A caller runs the effect on a fresh worker.
    Call,
    /// A caller runs it on a worker that crashes at the point.
    CrashAt(FaultPoint),
    /// A recovery pass.
    Recover,
    /// Time passes beyond a lease.
    Expire,
    /// A caller undoes the effect (cancelling the resource idempotently).
    Compensate,
    /// A caller undoes it on a worker that crashes once the attempt is
    /// recorded.
    CompensateAndCrash,
    /// An operator approves it, if it is awaiting approval.
    OperatorApprove,
    /// An operator denies it, if it is awaiting approval.
    OperatorDeny,
    /// An operator resolves it truthfully, if it needs intervention:
    /// `Applied` if the remote has it, else `Retry` (true) or `NotApplied`.
    OperatorResolve { retry: bool },
    /// A retention pass that prunes every settled record.
    Prune,
}

// Independent switches of one generated configuration.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug)]
struct Case {
    kind: EffectKind,
    remote_idempotency: bool,
    verify: bool,
    /// Run as a registered durable handler instead of a closure.
    handler: bool,
    /// `Some`: the effect requires approval, and the provider answers with
    /// these decisions in order (then defers).
    approval: Option<Vec<Decision>>,
    /// The effect is `Critical` under a policy that forbids automatic retry.
    critical: bool,
    script: Vec<Behavior>,
    steps: Vec<Step>,
}

#[derive(Clone, Copy, Debug)]
enum Decision {
    Approve,
    Deny,
    Defer,
}

/// An approval provider answering from the case's script.
#[derive(Clone)]
struct ModelApprovals(Arc<Mutex<std::collections::VecDeque<Decision>>>);

impl ApprovalProvider for ModelApprovals {
    fn request(
        &self,
        _: agent_effects::ApprovalRequest,
    ) -> impl std::future::Future<Output = ApprovalDecision> + Send {
        std::future::ready(match self.0.lock().unwrap().pop_front() {
            Some(Decision::Approve) => ApprovalDecision::Approved {
                by: "provider".into(),
            },
            Some(Decision::Deny) => ApprovalDecision::Denied {
                by: "provider".into(),
                reason: "no".into(),
            },
            Some(Decision::Defer) | None => ApprovalDecision::Deferred,
        })
    }
}

/// Every runtime's observer in one generation reports here.
#[derive(Clone, Default)]
struct Recorder {
    created: Arc<AtomicU32>,
    seen: Arc<Mutex<Vec<(Transition, EffectStatus, EffectStatus)>>>,
}

impl EffectObserver for Recorder {
    fn on_created(&self, _: &EffectRecord) {
        self.created.fetch_add(1, Ordering::SeqCst);
    }

    fn on_transition(&self, o: &Observation<'_>) {
        self.seen.lock().unwrap().push((o.transition, o.from, o.to));
    }
}

/// The input: business data and a credential that must never be stored.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ModelInput {
    order: u32,
    token: Secret<String>,
}

fn input() -> ModelInput {
    ModelInput {
        order: 7,
        token: Secret::new(SECRET.to_owned()),
    }
}

/// The case's effect as a durable handler.
struct ModelHandler {
    remote: FakeRemote,
    kind: EffectKind,
    remote_idempotency: bool,
    critical: bool,
    approval: bool,
}

impl EffectHandler for ModelHandler {
    const NAME: &'static str = "model";
    type Input = ModelInput;
    type Output = String;
    type Error = EffectFailure;

    fn kind(&self) -> EffectKind {
        self.kind
    }

    fn remote_idempotency(&self) -> bool {
        self.remote_idempotency
    }

    fn risk(&self) -> RiskLevel {
        if self.critical {
            RiskLevel::Critical
        } else {
            RiskLevel::Low
        }
    }

    fn requires_approval(&self) -> bool {
        self.approval
    }

    async fn execute(&self, ctx: &EffectContext, _: &ModelInput) -> Result<String, EffectFailure> {
        let key = self.remote_idempotency.then(|| ctx.idempotency_key());
        self.remote.create(RES, key).await
    }
}

impl VerifiableEffect for ModelHandler {
    async fn verify(
        &self,
        _: &EffectContext,
        _: &ModelInput,
    ) -> Result<Verification<String>, EffectFailure> {
        Ok(match self.remote.find(RES).await? {
            Some(id) => Verification::Confirmed(id),
            None => Verification::NotApplied,
        })
    }
}

impl CompensableEffect for ModelHandler {
    async fn compensate(
        &self,
        ctx: &CompensationContext,
        _: &ModelInput,
        _: Option<&String>,
    ) -> Result<(), EffectFailure> {
        self.remote.cancel(RES, Some(ctx.idempotency_key())).await
    }
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
        3 => Just(Step::Recover),
        2 => Just(Step::Expire),
        2 => Just(Step::Compensate),
        1 => Just(Step::CompensateAndCrash),
        1 => Just(Step::OperatorApprove),
        1 => Just(Step::OperatorDeny),
        1 => any::<bool>().prop_map(|retry| Step::OperatorResolve { retry }),
        1 => Just(Step::Prune),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    (
        (
            select(KINDS.to_vec()),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        ),
        prop::option::of(prop::collection::vec(
            select(vec![Decision::Approve, Decision::Deny, Decision::Defer]),
            0..4,
        )),
        any::<bool>(),
        prop::collection::vec(behavior(), 0..8),
        prop::collection::vec(step(), 1..10),
    )
        .prop_map(
            |((kind, remote_idempotency, verify, handler), approval, critical, script, steps)| {
                Case {
                    kind,
                    remote_idempotency,
                    verify,
                    handler,
                    approval,
                    critical,
                    script,
                    steps,
                }
            },
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

fn effect_key() -> EffectKey {
    EffectKey::new(
        EffectName::new("model").unwrap(),
        LogicalKey::new("effect").unwrap(),
    )
}

/// One case's world. A prune that removes the record starts a new
/// generation: a fresh remote and recorder.
struct World<'a> {
    case: &'a Case,
    store: MemoryStore,
    clock: TokioClock,
    approvals: Option<ModelApprovals>,
    remote: FakeRemote,
    recorder: Recorder,
}

impl World<'_> {
    /// A runtime configured like every worker of the service.
    fn runtime(&self, worker: String, faults: Option<Arc<FaultInjector>>) -> Runtime<MemoryStore> {
        let case = self.case;
        let mut builder = Runtime::builder(self.store.clone())
            .clock(self.clock)
            .worker_id(WorkerId::new(worker))
            .lease_ttl(TTL)
            .observer(self.recorder.clone())
            .retention(RetentionPolicy::settled(Duration::ZERO));
        if let Some(faults) = faults {
            builder = builder.fault_injector(faults);
        }
        if let Some(approvals) = &self.approvals {
            builder = builder.approval_provider(approvals.clone());
        }
        if case.critical {
            builder = builder.risk_policy(
                PolicyBuilder::new()
                    .for_risk(RiskLevel::Critical)
                    .disable_automatic_retry()
                    .build(),
            );
        }
        if case.handler {
            let mut handler = Handler::new(ModelHandler {
                remote: self.remote.clone(),
                kind: case.kind,
                remote_idempotency: case.remote_idempotency,
                critical: case.critical,
                approval: case.approval.is_some(),
            })
            .compensable();
            if case.verify {
                handler = handler.verifiable();
            }
            builder = builder.register(handler);
        }
        builder.build()
    }

    async fn call(&self, rt: &Runtime<MemoryStore>) -> Result<(), String> {
        let case = self.case;
        if case.handler {
            return rt
                .submit::<ModelHandler>("effect", input())
                .await
                .map(|_| ())
                .map_err(|e| e.to_string());
        }
        let send_key = case.remote_idempotency;
        let action = {
            let remote = self.remote.clone();
            move |ctx: EffectContext| {
                let remote = remote.clone();
                let key = send_key.then(|| ctx.idempotency_key());
                async move { remote.create(RES, key).await }
            }
        };
        let mut effect = rt
            .effect("model", "effect")
            .input(&input())
            .kind(case.kind)
            .remote_idempotency(case.remote_idempotency);
        if case.approval.is_some() {
            effect = effect.require_approval();
        }
        if case.critical {
            effect = effect.risk(RiskLevel::Critical);
        }
        let result = if case.verify {
            let lookup = self.remote.clone();
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

    async fn compensate(&self, rt: &Runtime<MemoryStore>) -> Result<(), String> {
        let result = if self.case.handler {
            rt.compensate::<ModelHandler>("effect").await.map(|_| ())
        } else {
            let remote = self.remote.clone();
            rt.compensation("model", "effect")
                .run(move |ctx, _: Option<serde_json::Value>| {
                    let remote = remote.clone();
                    async move { remote.cancel(RES, Some(ctx.idempotency_key())).await }
                })
                .await
                .map(|_| ())
        };
        result.or_else(|e| match e {
            // Compensating before the effect exists is refused; fine.
            RuntimeError::NoSuchEffect { .. } => Ok(()),
            e => Err(e.to_string()),
        })
    }

    async fn record(&self) -> Option<EffectRecord> {
        self.store.get_by_key(&effect_key()).await.unwrap()
    }

    /// Runs one step, asserting what the step itself must satisfy.
    async fn step(&mut self, i: usize, step: Step) -> Result<(), TestCaseError> {
        let worker = format!("worker-{i}");
        match step {
            Step::Call => {
                let result = self.call(&self.runtime(worker, None)).await;
                prop_assert!(result.is_ok(), "step {i}: {result:?}");
            }
            Step::CrashAt(point) => {
                let injector = Arc::new(FaultInjector::new().at(point).crash());
                let rt = self.runtime(worker, Some(Arc::clone(&injector)));
                let result = self.call(&rt).await;
                crashed_at(i, point, &injector, &result)?;
                // Let a request that was on the wire land.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Step::Recover => {
                let report = self.runtime(worker, None).recover().await;
                prop_assert!(report.is_ok(), "step {i}: {report:?}");
                let report = report.unwrap();
                prop_assert!(
                    report.resume_errors.is_empty(),
                    "step {i}: {:?}",
                    report.resume_errors
                );
                if self.case.handler {
                    prop_assert!(
                        report.unhandled.is_empty(),
                        "step {i}: a handler effect left to a caller"
                    );
                }
            }
            Step::Expire => tokio::time::sleep(TTL).await,
            Step::Compensate => {
                let result = self.compensate(&self.runtime(worker, None)).await;
                prop_assert!(result.is_ok(), "step {i}: {result:?}");
            }
            Step::CompensateAndCrash => {
                let point = FaultPoint::AfterCompensationStarted;
                let injector = Arc::new(FaultInjector::new().at(point).crash());
                let rt = self.runtime(worker, Some(Arc::clone(&injector)));
                let result = self.compensate(&rt).await;
                crashed_at(i, point, &injector, &result)?;
            }
            Step::OperatorApprove | Step::OperatorDeny => {
                let Some(record) = self
                    .record()
                    .await
                    .filter(|r| r.status == EffectStatus::AwaitingApproval)
                else {
                    return Ok(());
                };
                let rt = self.runtime(worker, None);
                let decided = if step == Step::OperatorApprove {
                    rt.approve(record.id, "operator", "ok").await
                } else {
                    rt.deny(record.id, "operator", "no").await
                };
                prop_assert!(operator_ok(&decided), "step {i}: {decided:?}");
            }
            Step::OperatorResolve { retry } => {
                let Some(record) = self
                    .record()
                    .await
                    .filter(|r| r.status == EffectStatus::NeedsIntervention)
                else {
                    return Ok(());
                };
                // A truthful operator: checks the remote system first.
                let resolution = match self.remote.find(RES).await {
                    Ok(Some(id)) => Resolution::Applied {
                        output: Some(json!(id)),
                    },
                    _ if self.remote.applications(RES) > 0 => return Ok(()),
                    _ if retry => Resolution::Retry,
                    _ => Resolution::NotApplied,
                };
                let resolved = self
                    .runtime(worker, None)
                    .resolve(record.id, resolution, "operator", "checked the remote")
                    .await;
                prop_assert!(operator_ok(&resolved), "step {i}: {resolved:?}");
            }
            Step::Prune => self.prune(i).await?,
        }
        Ok(())
    }

    async fn prune(&mut self, i: usize) -> Result<(), TestCaseError> {
        let before = self.record().await;
        if let Some(record) = &before {
            self.check(record).await?;
        }
        let report = self.runtime(format!("worker-{i}"), None).prune().await;
        prop_assert!(report.is_ok(), "step {i}: {report:?}");
        let Some(before) = before else {
            return Ok(());
        };
        // Spelled out rather than `is_settled`, so the model does not
        // share the code's definition.
        let settled = matches!(
            before.status,
            EffectStatus::Committed
                | EffectStatus::Failed
                | EffectStatus::Rejected
                | EffectStatus::Compensated
        );
        let eligible = settled && before.live_lease_owner(self.clock.now()).is_none();
        let pruned = self.record().await.is_none();
        prop_assert_eq!(
            pruned,
            eligible,
            "step {}: pruned a {:?} record, or kept an eligible one",
            i,
            before.status
        );
        if pruned {
            self.remote = FakeRemote::new(self.clock);
            self.recorder = Recorder::default();
        }
        Ok(())
    }

    /// The invariants of the current generation, given its record.
    async fn check(&self, record: &EffectRecord) -> Result<(), TestCaseError> {
        let case = self.case;
        let created = self.remote.applications(RES);
        let events = self.store.events(record.id).await.unwrap();

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

        if case.critical {
            let attempts = events
                .iter()
                .filter(|e| e.transition == Transition::StartAttempt)
                .count();
            let operator_retry = events
                .iter()
                .any(|e| e.transition == Transition::ResolvedRetry);
            prop_assert!(
                attempts <= 1 || operator_retry,
                "automatic retry under a no-retry policy: {:?}",
                events
            );
        }

        if case.approval.is_some() {
            // Every attempt follows an approval; a denied effect never ran.
            let first_approval = events
                .iter()
                .position(|e| e.transition == Transition::Approve);
            let first_attempt = events
                .iter()
                .position(|e| e.transition == Transition::StartAttempt);
            if let Some(attempt) = first_attempt {
                prop_assert!(
                    first_approval.is_some_and(|approval| approval < attempt),
                    "attempted without approval: {:?}",
                    events
                );
            }
            if events.iter().any(|e| e.transition == Transition::Deny) {
                prop_assert_eq!(created, 0, "denied, yet created");
            }
        }

        if !case.kind.is_naturally_idempotent() {
            prop_assert!(created <= 1, "created {} times: {:?}", created, events);
            if record.status == EffectStatus::Failed {
                prop_assert_eq!(created, 0, "failed, yet created: {:?}", events);
            }
        }
        if record.status == EffectStatus::Committed {
            prop_assert!(created >= 1, "committed without being created");
        }
        let cancelled = self.remote.cancellations(RES);
        let compensation_started = matches!(
            record.status,
            EffectStatus::Compensating
                | EffectStatus::Compensated
                | EffectStatus::CompensationFailed
        );
        prop_assert!(
            cancelled == 0 || compensation_started,
            "cancelled an effect that never started compensating: {:?}",
            record.status
        );
        if record.status == EffectStatus::Compensated {
            prop_assert!(
                !self.remote.exists(RES),
                "compensated, yet the resource exists"
            );
        }

        // Observers saw exactly the trail, crashes or not: a transition is
        // stored and observed in one step, and crashes strike between steps.
        let seen = self.recorder.seen.lock().unwrap().clone();
        let trail: Vec<_> = events
            .iter()
            .map(|e| (e.transition, e.from, e.to))
            .collect();
        prop_assert_eq!(&seen, &trail, "observers saw the trail");
        prop_assert_eq!(
            self.recorder.created.load(Ordering::SeqCst),
            1,
            "observers saw the record created"
        );

        // The secret never reached storage.
        let stored = format!("{record:?}{events:?}");
        prop_assert!(!stored.contains(SECRET), "secret stored: {}", stored);
        Ok(())
    }
}

/// Asserts how a step that may have crashed at `point` ended.
fn crashed_at(
    i: usize,
    point: FaultPoint,
    injector: &FaultInjector,
    result: &Result<(), String>,
) -> Result<(), TestCaseError> {
    if injector.reached().contains(&point) {
        prop_assert!(
            result.as_ref().is_err_and(|e| e.contains("fault injected")),
            "step {i}: {result:?}"
        );
    } else {
        prop_assert!(result.is_ok(), "step {i}: {result:?}");
    }
    Ok(())
}

/// An operator may find a caller holding the effect; then they wait.
fn operator_ok(result: &Result<impl std::fmt::Debug, RuntimeError>) -> bool {
    matches!(
        result,
        Ok(_) | Err(RuntimeError::Store(StoreError::LeaseHeld { .. }))
    )
}

async fn check(case: Case) -> Result<(), TestCaseError> {
    let clock = TokioClock::new();
    let mut world = World {
        case: &case,
        store: MemoryStore::new(),
        clock,
        approvals: case.approval.as_ref().map(|decisions| {
            ModelApprovals(Arc::new(Mutex::new(decisions.iter().copied().collect())))
        }),
        remote: FakeRemote::new(clock).script(case.script.clone()),
        recorder: Recorder::default(),
    };
    for (i, step) in case.steps.iter().enumerate() {
        world.step(i, *step).await?;
    }
    if let Some(record) = world.record().await {
        world.check(&record).await?;
    } else {
        prop_assert_eq!(
            world.remote.applications(RES),
            0,
            "created without a record"
        );
    }
    prop_assert!(world.store.len() <= 1);
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
