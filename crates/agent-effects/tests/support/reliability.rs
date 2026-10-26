//! Backend-independent failure scenarios with actual state changes and remote counts.
#![allow(dead_code)] // Each integration binary selects the scenarios it needs.

pub mod fault_store;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use agent_effects::testkit::FakeRemote;
use agent_effects::{
    CompensableEffect, CompensationContext, CompensationOutcome, EffectContext, EffectFailure,
    EffectHandler, EffectKey, EffectName, EffectOutcome, EffectRecord, EffectStatus, EffectStore,
    Handler, LogicalKey, ManualClock, RetryPolicy, Runtime, RuntimeError, Transition,
    VerifiableEffect, Verification, WorkerId,
};
use fault_store::{Edge, FaultStore, Operation};

pub const TTL: Duration = Duration::from_secs(30);
pub const RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    initial_delay: Duration::ZERO,
    max_delay: Duration::ZERO,
    multiplier: 1.0,
    jitter: false,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protection {
    None,
    Idempotent,
    Verified,
}

#[derive(Clone)]
pub struct Durable {
    pub remote: FakeRemote,
    pub protection: Protection,
}

impl EffectHandler for Durable {
    const NAME: &'static str = "matrix.action";
    type Input = String;
    type Output = String;
    type Error = EffectFailure;
    fn remote_idempotency(&self) -> bool {
        self.protection == Protection::Idempotent
    }
    async fn execute(
        &self,
        ctx: &EffectContext,
        resource: &String,
    ) -> Result<String, EffectFailure> {
        self.remote
            .create(
                resource,
                self.remote_idempotency().then(|| ctx.idempotency_key()),
            )
            .await
    }
}
impl VerifiableEffect for Durable {
    async fn verify(
        &self,
        _: &EffectContext,
        resource: &String,
    ) -> Result<Verification<String>, EffectFailure> {
        Ok(match self.remote.find(resource).await? {
            Some(output) => Verification::Confirmed(output),
            None => Verification::NotApplied,
        })
    }
}
impl CompensableEffect for Durable {
    async fn compensate(
        &self,
        ctx: &CompensationContext,
        resource: &String,
        _: Option<&String>,
    ) -> Result<(), EffectFailure> {
        self.remote
            .cancel(resource, Some(ctx.idempotency_key()))
            .await
    }
}

pub fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(Durable::NAME).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

pub fn runtime<S: EffectStore>(
    store: S,
    clock: Arc<ManualClock>,
    remote: FakeRemote,
    protection: Protection,
    worker: &str,
) -> Runtime<S> {
    let handler = Handler::new(Durable { remote, protection }).compensable();
    let handler = if protection == Protection::Verified {
        handler.verifiable()
    } else {
        handler
    };
    Runtime::builder(store)
        .clock(clock)
        .lease_ttl(TTL)
        .retry_policy(RETRY)
        .worker_id(WorkerId::new(worker))
        .register(handler)
        .build()
}

pub async fn record<S: EffectStore>(store: &S, logical: &str) -> EffectRecord {
    store.get_by_key(&key(logical)).await.unwrap().unwrap()
}

pub async fn assert_not_falsely_failed<S: EffectStore>(store: &S, id: agent_effects::EffectId) {
    let record = store.get(id).await.unwrap().unwrap();
    assert_ne!(record.status, EffectStatus::Failed);
    assert!(
        store
            .events(id)
            .await
            .unwrap()
            .iter()
            .all(|e| e.transition != Transition::FailedDefinitively)
    );
}

/// Lost acknowledgements are tested on both sides of real insert/lease/commit
/// operations, not by fabricating record states in the runtime.
pub async fn storage_matrix<S, F, Fut>(make: F)
where
    S: EffectStore,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    for protection in [
        Protection::None,
        Protection::Idempotent,
        Protection::Verified,
    ] {
        let store = FaultStore::new(make().await);
        let clock = Arc::new(ManualClock::default());
        let remote = FakeRemote::new(clock.clone());
        let rt = runtime(
            store.clone(),
            clock.clone(),
            remote.clone(),
            protection,
            "original",
        );
        for (index, operation) in operations(protection).into_iter().enumerate() {
            for edge in [Edge::Before, Edge::After] {
                let logical = format!("{protection:?}-{index}-{edge:?}");
                store.controls.fail(operation, edge);
                let outcome = rt.submit::<Durable>(&logical, logical.clone()).await;
                assert!(
                    matches!(outcome, Err(RuntimeError::Store(_))),
                    "{operation:?}/{edge:?}: {outcome:?}"
                );
                let before = store.inner.get_by_key(&key(&logical)).await.unwrap();
                let uncertain = before.as_ref().is_some_and(|r| {
                    r.status == EffectStatus::Executing || r.status == EffectStatus::Verifying
                });
                if operation == Operation::Insert && edge == Edge::Before {
                    assert!(before.is_none());
                    assert_eq!(remote.applications(&logical), 0);
                    assert!(matches!(
                        rt.submit::<Durable>(&logical, logical.clone())
                            .await
                            .unwrap(),
                        EffectOutcome::Committed(_)
                    ));
                } else {
                    clock.advance(TTL + Duration::from_secs(1));
                    let restarted = runtime(
                        store.clone(),
                        clock.clone(),
                        remote.clone(),
                        protection,
                        "restarted",
                    );
                    let report = restarted.recover().await.unwrap();
                    assert!(
                        report.resume_errors.is_empty(),
                        "{operation:?}/{edge:?}: {report:?}"
                    );
                    let recovered = record(store.inner.as_ref(), &logical).await;
                    let escalated = uncertain && protection == Protection::None;
                    assert_eq!(
                        recovered.status,
                        if escalated {
                            EffectStatus::NeedsIntervention
                        } else {
                            EffectStatus::Committed
                        },
                        "{operation:?}/{edge:?}"
                    );
                    if !escalated {
                        let requests = remote.requests();
                        assert!(matches!(
                            restarted
                                .submit::<Durable>(&logical, logical.clone())
                                .await
                                .unwrap(),
                            EffectOutcome::Committed(_)
                        ));
                        assert_eq!(
                            remote.requests(),
                            requests,
                            "committed replay must not call the remote"
                        );
                        assert_eq!(remote.applications(&logical), 1);
                    }
                    assert_not_falsely_failed(store.inner.as_ref(), recovered.id).await;
                }
                assert!(
                    remote.applications(&logical) <= 1,
                    "{operation:?}/{edge:?}: duplicate external application"
                );
            }
        }
    }
}

pub async fn compensation_storage_matrix<S, F, Fut>(make: F)
where
    S: EffectStore,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    let store = FaultStore::new(make().await);
    let clock = Arc::new(ManualClock::default());
    let remote = FakeRemote::new(clock.clone());
    let rt = runtime(
        store.clone(),
        clock.clone(),
        remote.clone(),
        Protection::Idempotent,
        "original",
    );
    for transition in [
        Transition::StartCompensation,
        Transition::CompensationSucceeded,
    ] {
        for edge in [Edge::Before, Edge::After] {
            let logical = format!("undo-{transition:?}-{edge:?}");
            rt.submit::<Durable>(&logical, logical.clone())
                .await
                .unwrap();
            store.controls.fail(Operation::Transition(transition), edge);
            assert!(matches!(
                rt.compensate::<Durable>(&logical).await,
                Err(RuntimeError::Store(_))
            ));
            clock.advance(TTL + Duration::from_secs(1));
            let restarted = runtime(
                store.clone(),
                clock.clone(),
                remote.clone(),
                Protection::Idempotent,
                "restarted",
            );
            let report = restarted.recover().await.unwrap();
            assert!(
                report.resume_errors.is_empty(),
                "{transition:?}/{edge:?}: {report:?}"
            );
            // A failure before compensation intent requires a new explicit
            // compensation request; recovery must not invent that intent.
            assert_eq!(
                restarted.compensate::<Durable>(&logical).await.unwrap(),
                CompensationOutcome::Compensated
            );
            assert_eq!(remote.cancellations(&logical), 1);
            assert_eq!(remote.applications(&logical), 1);
            assert_eq!(
                record(store.inner.as_ref(), &logical).await.status,
                EffectStatus::Compensated
            );
        }
    }
}

/// All store methods fail after the remote applies. Several replacement
/// workers race only after connectivity returns and the old lease expires.
pub async fn reconnect_with_simultaneous_recovery<S, F, Fut>(make: F)
where
    S: EffectStore,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    for protection in [Protection::None, Protection::Idempotent] {
        let store = FaultStore::new(make().await);
        let clock = Arc::new(ManualClock::default());
        let remote = FakeRemote::new(clock.clone());
        let rt = runtime(
            store.clone(),
            clock.clone(),
            remote.clone(),
            protection,
            "disconnected",
        );
        let outcome = rt
            .effect(Durable::NAME, "outage")
            .input(&"outage")
            .remote_idempotency(protection == Protection::Idempotent)
            .run({
                let (remote, controls) = (remote.clone(), store.controls.clone());
                move |ctx| {
                    let (remote, controls) = (remote.clone(), controls.clone());
                    async move {
                        let output = remote
                            .create(
                                "outage",
                                (protection == Protection::Idempotent)
                                    .then(|| ctx.idempotency_key()),
                            )
                            .await?;
                        controls.offline();
                        Ok::<_, EffectFailure>(output)
                    }
                }
            })
            .await;
        assert!(matches!(outcome, Err(RuntimeError::Store(_))));
        let interrupted = record(store.inner.as_ref(), "outage").await;
        assert_eq!(interrupted.status, EffectStatus::Executing);
        assert!(
            rt.recover().await.is_err(),
            "recovery must report the outage"
        );
        assert_eq!(remote.applications("outage"), 1);
        clock.advance(TTL + Duration::from_secs(1));
        store.controls.reconnect();
        let mut workers = tokio::task::JoinSet::new();
        for index in 0..4 {
            let restarted = runtime(
                store.clone(),
                clock.clone(),
                remote.clone(),
                protection,
                &format!("replacement-{index}"),
            );
            workers.spawn(async move { restarted.recover().await });
        }
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
        let settled = record(store.inner.as_ref(), "outage").await;
        assert_eq!(
            settled.status,
            if protection == Protection::Idempotent {
                EffectStatus::Committed
            } else {
                EffectStatus::NeedsIntervention
            }
        );
        assert_eq!(remote.applications("outage"), 1);
        if protection == Protection::None {
            assert_eq!(remote.requests(), 1);
        }
        assert_not_falsely_failed(store.inner.as_ref(), settled.id).await;
    }
}

fn operations(protection: Protection) -> Vec<Operation> {
    let mut operations = vec![
        Operation::Insert,
        Operation::Acquire,
        Operation::Transition(Transition::StartAttempt),
    ];
    if protection == Protection::Verified {
        operations.extend([
            Operation::Transition(Transition::StartVerification),
            Operation::Transition(Transition::VerificationConfirmed),
        ]);
    } else {
        operations.push(Operation::Transition(Transition::Succeeded));
    }
    operations
}
