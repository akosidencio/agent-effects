//! Risk policy: requirements by risk level and effect kind.

use std::sync::{Arc, Mutex};

use agent_effects::handler::{EffectHandler, Handler};
use agent_effects::store::{EffectStore, ListQuery};
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    ApprovalDecision, ApprovalProvider, ApprovalRequest, EffectContext, EffectFailure, EffectKind,
    EffectOutcome, FailureClass, PolicyBuilder, Resolution, RiskLevel, RiskPolicy, Runtime,
    RuntimeError, TokioClock, Verification,
};
use agent_effects_memory::MemoryStore;

const RES: &str = "res";

/// The spec's example policy, plus "irreversible writes must be verifiable".
fn policy() -> RiskPolicy {
    PolicyBuilder::new()
        .for_risk(RiskLevel::Low)
        .auto_execute()
        .for_risk(RiskLevel::High)
        .require_approval()
        .for_risk(RiskLevel::Critical)
        .require_approval()
        .disable_automatic_retry()
        .for_kind(EffectKind::IrreversibleWrite)
        .require_verification()
        .build()
}

fn runtime(store: &MemoryStore, clock: TokioClock, policy: RiskPolicy) -> Runtime<MemoryStore> {
    Runtime::builder(store.clone())
        .clock(clock)
        .risk_policy(policy)
        .build()
}

fn create(
    remote: &FakeRemote,
) -> impl Fn(
    EffectContext,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<String, EffectFailure>> + Send>,
> + Send
+ Sync
+ 'static {
    let remote = remote.clone();
    move |_| {
        let remote = remote.clone();
        Box::pin(async move { remote.create(RES, None).await })
    }
}

#[tokio::test(start_paused = true)]
async fn high_risk_effects_wait_for_approval() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let rt = runtime(&store, clock, policy());
    let outcome = rt
        .effect("crm.merge", "acct-1")
        .kind(EffectKind::ReversibleWrite)
        .risk(RiskLevel::High)
        .run(create(&remote))
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::AwaitingApproval { .. }),
        "{outcome:?}"
    );
    assert_eq!(remote.requests(), 0);

    let low = rt
        .effect("crm.tag", "acct-1")
        .kind(EffectKind::ReversibleWrite)
        .run(create(&remote))
        .await
        .unwrap();
    assert_eq!(
        low,
        EffectOutcome::Committed("res#1".into()),
        "low risk runs at once"
    );
}

#[tokio::test(start_paused = true)]
async fn an_effect_that_cannot_meet_the_policy_is_refused_before_anything_is_recorded() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let rt = runtime(&store, clock, policy());
    let err = rt
        .effect("payment.charge", "order-1")
        .kind(EffectKind::IrreversibleWrite)
        .run(create(&remote))
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            RuntimeError::PolicyViolation {
                requirement: "verification",
                ..
            }
        ),
        "{err}"
    );
    let recorded = store.list(ListQuery::statuses([])).await.unwrap();
    assert_eq!(recorded, Vec::new(), "nothing recorded");
    assert_eq!(remote.requests(), 0);

    // With a verification, the same effect runs.
    let lookup = remote.clone();
    let outcome = rt
        .effect("payment.charge", "order-1")
        .kind(EffectKind::IrreversibleWrite)
        .verify(move |_| {
            let lookup = lookup.clone();
            async move {
                Ok::<_, EffectFailure>(match lookup.find(RES).await? {
                    Some(id) => Verification::Confirmed(id),
                    None => Verification::NotApplied,
                })
            }
        })
        .run(create(&remote))
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
}

fn no_retry() -> RiskPolicy {
    PolicyBuilder::new()
        .for_risk(RiskLevel::Critical)
        .disable_automatic_retry()
        .build()
}

#[tokio::test(start_paused = true)]
async fn without_automatic_retry_a_transient_failure_is_final() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let rt = runtime(&store, clock, no_retry());
    let outcome = rt
        .effect("db.drop", "orders")
        .kind(EffectKind::IdempotentWrite)
        .risk(RiskLevel::Critical)
        .run(create(&remote))
        .await
        .unwrap();
    assert!(matches!(outcome, EffectOutcome::Failed(_)), "{outcome:?}");
    assert_eq!(remote.requests(), 1, "never retried");
}

#[tokio::test(start_paused = true)]
async fn without_automatic_retry_an_unknown_outcome_escalates_instead_of_rerunning() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock).script([Behavior::LoseRequest]);
    let rt = runtime(&store, clock, no_retry());
    let effect = || {
        rt.effect("db.drop", "orders")
            .kind(EffectKind::IdempotentWrite) // safe to re-run, but the policy forbids it
            .risk(RiskLevel::Critical)
            .run(create(&remote))
    };
    let EffectOutcome::NeedsIntervention { id } = effect().await.unwrap() else {
        panic!("expected escalation");
    };
    assert_eq!(remote.requests(), 1);

    // An operator's explicit retry is still allowed.
    rt.resolve(
        id,
        Resolution::Retry,
        "operator:dennis",
        "checked: it never arrived",
    )
    .await
    .unwrap();
    assert_eq!(
        effect().await.unwrap(),
        EffectOutcome::Committed("res#1".into())
    );
}

#[tokio::test(start_paused = true)]
async fn without_automatic_retry_a_verified_not_applied_fails_instead_of_rerunning() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock).script([Behavior::LoseRequest]);
    let rt = runtime(&store, clock, no_retry());
    let lookup = remote.clone();
    let outcome = rt
        .effect("db.drop", "orders")
        .risk(RiskLevel::Critical)
        .verify(move |_| {
            let lookup = lookup.clone();
            async move {
                Ok::<_, EffectFailure>(match lookup.find(RES).await? {
                    Some(id) => Verification::Confirmed(id),
                    None => Verification::NotApplied,
                })
            }
        })
        .run(create(&remote))
        .await
        .unwrap();
    assert!(matches!(outcome, EffectOutcome::Failed(_)), "{outcome:?}");
    assert_eq!(remote.requests(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_permissive_rule_never_loosens_an_effects_own_settings() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock);
    let permissive = PolicyBuilder::new()
        .for_risk(RiskLevel::Low)
        .auto_execute()
        .build();
    let rt = runtime(&store, clock, permissive);
    let outcome = rt
        .effect("crm.tag", "acct-1")
        .require_approval()
        .run(create(&remote))
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::AwaitingApproval { .. }),
        "{outcome:?}"
    );
}

/// Records the risk shown to the approver.
#[derive(Clone, Default)]
struct SeenRisk(Arc<Mutex<Vec<RiskLevel>>>);

impl ApprovalProvider for SeenRisk {
    fn request(
        &self,
        request: ApprovalRequest,
    ) -> impl std::future::Future<Output = ApprovalDecision> + Send {
        self.0.lock().unwrap().push(request.risk);
        std::future::ready(ApprovalDecision::Approved { by: "alice".into() })
    }
}

struct DropDatabase {
    remote: FakeRemote,
}

impl EffectHandler for DropDatabase {
    const NAME: &'static str = "db.drop";
    type Input = String;
    type Output = String;
    type Error = EffectFailure;

    fn kind(&self) -> EffectKind {
        EffectKind::IdempotentWrite
    }

    fn risk(&self) -> RiskLevel {
        RiskLevel::Critical
    }

    async fn execute(&self, _: &EffectContext, name: &String) -> Result<String, EffectFailure> {
        self.remote.create(name, None).await
    }
}

#[tokio::test(start_paused = true)]
async fn a_handlers_risk_drives_the_policy() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let seen = SeenRisk::default();
    let rt = Runtime::builder(store.clone())
        .clock(clock)
        .risk_policy(policy())
        .approval_provider(seen.clone())
        .register(Handler::new(DropDatabase {
            remote: remote.clone(),
        }))
        .build();
    let outcome = rt
        .submit::<DropDatabase>("orders", "orders".into())
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::Failed(_)),
        "approved, then no retry: {outcome:?}"
    );
    assert_eq!(
        *seen.0.lock().unwrap(),
        [RiskLevel::Critical],
        "the approver saw the risk"
    );
    assert_eq!(remote.requests(), 1);
}
