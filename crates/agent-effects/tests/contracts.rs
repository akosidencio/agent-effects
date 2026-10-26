//! Safety assumptions must survive reattachment, recovery, and deployment.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use agent_effects::handler::{EffectHandler, Handler, VerifiableEffect};
use agent_effects::store::{ExecutionContract, NewEffect, VerificationContract};
use agent_effects::{
    EffectContext, EffectFailure, EffectKey, EffectKind, EffectName, EffectOutcome, EffectStatus,
    EffectStore, LogicalKey, RetryPolicy, Runtime, RuntimeError, Verification,
};
use agent_effects_memory::MemoryStore;

fn key() -> EffectKey {
    EffectKey::new(
        EffectName::new("contract.op").unwrap(),
        LogicalKey::new("one").unwrap(),
    )
}

async fn seed(store: &MemoryStore, contract: Option<ExecutionContract>) {
    let mut new = NewEffect::new(key(), EffectKind::IrreversibleWrite, SystemTime::now());
    new.execution_contract = contract;
    store.insert_or_get(new).await.unwrap();
}

#[tokio::test]
async fn idempotency_cannot_be_added_or_removed_on_reattach() {
    for stored in [false, true] {
        let store = MemoryStore::new();
        let rt = Runtime::new(store.clone());
        rt.effect("contract.op", "one")
            .remote_idempotency(stored)
            .require_approval()
            .run(|_| async { Ok::<_, EffectFailure>(()) })
            .await
            .unwrap();
        let before = store.get_by_key(&key()).await.unwrap().unwrap();
        let result = rt
            .effect("contract.op", "one")
            .remote_idempotency(!stored)
            .require_approval()
            .run(|_| async {
                panic!("incompatible action ran");
                #[allow(unreachable_code)]
                Ok::<_, EffectFailure>(())
            })
            .await;
        assert!(matches!(result, Err(RuntimeError::ContractMismatch { .. })));
        assert_eq!(store.get(before.id).await.unwrap().unwrap(), before);
    }
}

#[tokio::test]
async fn verification_modes_and_settle_delays_are_contractual() {
    let modes = [
        VerificationContract::None,
        VerificationContract::Authoritative,
        VerificationContract::EventuallyConsistent {
            settle: Duration::from_secs(60),
        },
    ];
    for mode in modes {
        let store = MemoryStore::new();
        let mut contract = ExecutionContract::new(EffectKind::IrreversibleWrite, 5);
        contract.verification = mode;
        seed(&store, Some(contract)).await;
        let rt = Runtime::new(store.clone());
        let result = rt
            .effect("contract.op", "one")
            .verify_eventually(Duration::from_secs(30), |_| async {
                Ok::<_, EffectFailure>(Verification::Confirmed(()))
            })
            .run(|_| async {
                panic!("incompatible action ran");
                #[allow(unreachable_code)]
                Ok::<_, EffectFailure>(())
            })
            .await;
        assert!(
            matches!(result, Err(RuntimeError::ContractMismatch { .. })),
            "{mode:?}"
        );
        assert!(
            store
                .events(store.get_by_key(&key()).await.unwrap().unwrap().id)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn kinds_versions_adapters_approval_and_attempt_ceilings_cannot_drift() {
    for field in 0..7 {
        let store = MemoryStore::new();
        let mut contract = ExecutionContract::new(EffectKind::IrreversibleWrite, 5);
        match field {
            0 => contract.handler_version = "2".into(),
            1 => contract.adapter = Some("provider-b".into()),
            2 => contract.max_attempts = 8,
            3 => contract.require_approval = true,
            4 => contract.automatic_retry = false,
            5 => contract.format_version = 2,
            _ => contract.kind = EffectKind::Read,
        }
        seed(&store, Some(contract)).await;
        let result = Runtime::new(store)
            .effect("contract.op", "one")
            .run(|_| async {
                panic!("incompatible action ran");
                #[allow(unreachable_code)]
                Ok::<_, EffectFailure>(())
            })
            .await;
        assert!(
            matches!(result, Err(RuntimeError::ContractMismatch { .. })),
            "field {field}: {result:?}"
        );
    }
    let store = MemoryStore::new();
    seed(
        &store,
        Some(ExecutionContract::new(EffectKind::IrreversibleWrite, 5)),
    )
    .await;
    assert!(matches!(
        Runtime::new(store)
            .effect("contract.op", "one")
            .kind(EffectKind::Read)
            .run(|_| async { Ok::<_, EffectFailure>(()) })
            .await,
        Err(RuntimeError::KindMismatch { .. })
    ));
}

#[tokio::test]
async fn legacy_unresolved_effects_are_never_given_new_capabilities() {
    let store = MemoryStore::new();
    seed(&store, None).await;
    for protected in [false, true] {
        assert!(matches!(
            Runtime::new(store.clone())
                .effect("contract.op", "one")
                .remote_idempotency(protected)
                .run(|_| async {
                    panic!("legacy action ran");
                    #[allow(unreachable_code)]
                    Ok::<_, EffectFailure>(())
                })
                .await,
            Err(RuntimeError::LegacyContract { .. })
        ));
    }
}

#[tokio::test]
async fn compatible_deployments_can_change_backoff_and_timeout() {
    let store = MemoryStore::new();
    seed(
        &store,
        Some(ExecutionContract::new(EffectKind::IrreversibleWrite, 5)),
    )
    .await;
    let retry = RetryPolicy {
        initial_delay: Duration::from_secs(4),
        ..RetryPolicy::default()
    };
    let outcome = Runtime::new(store.clone())
        .effect("contract.op", "one")
        .retry(retry)
        .attempt_timeout(Duration::from_secs(20))
        .run(|_| async { Ok::<_, EffectFailure>(42) })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed(42));
    let replay = Runtime::new(store)
        .effect("contract.op", "one")
        .run(|_| async {
            panic!("committed effect repeated");
            #[allow(unreachable_code)]
            Ok::<_, EffectFailure>(42)
        })
        .await
        .unwrap();
    assert_eq!(replay, outcome);
}

struct Deployed {
    version: &'static str,
    executions: Arc<AtomicU32>,
}

impl EffectHandler for Deployed {
    const NAME: &'static str = "contract.op";
    type Input = ();
    type Output = ();
    type Error = EffectFailure;
    fn contract_version(&self) -> &str {
        self.version
    }
    fn execute(
        &self,
        _: &EffectContext,
        _input: &(),
    ) -> impl std::future::Future<Output = Result<(), EffectFailure>> + Send {
        self.executions.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(()))
    }
}

impl VerifiableEffect for Deployed {
    fn verify(
        &self,
        _: &EffectContext,
        _input: &(),
    ) -> impl std::future::Future<Output = Result<Verification<()>, EffectFailure>> + Send {
        std::future::ready(Ok(Verification::Confirmed(())))
    }
}

impl agent_effects::CompensableEffect for Deployed {
    fn compensate(
        &self,
        _: &agent_effects::CompensationContext,
        _input: &(),
        _output: Option<&()>,
    ) -> impl std::future::Future<Output = Result<(), EffectFailure>> + Send {
        std::future::ready(Err(EffectFailure::permanent("unattested compensation ran")))
    }
}

#[tokio::test]
async fn replayable_legacy_commits_cannot_start_unattested_durable_compensation() {
    let store = MemoryStore::new();
    seed(&store, None).await;
    let record = store.get_by_key(&key()).await.unwrap().unwrap();
    let now = SystemTime::now();
    let lease = store
        .acquire_lease(
            record.id,
            &agent_effects::WorkerId::new("old"),
            now,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let started = store
        .transition(agent_effects::store::TransitionRequest::new(
            &record,
            Some(&lease),
            agent_effects::Transition::StartAttempt,
            now,
        ))
        .await
        .unwrap();
    let mut success = agent_effects::store::TransitionRequest::new(
        &started,
        Some(&lease),
        agent_effects::Transition::Succeeded,
        now,
    );
    success.output = Some(serde_json::Value::Null);
    store.transition(success).await.unwrap();
    store.release_lease(&lease).await.unwrap();
    let rt = Runtime::builder(store.clone())
        .register(
            Handler::new(Deployed {
                version: "1",
                executions: Arc::default(),
            })
            .compensable(),
        )
        .build();
    assert!(matches!(
        rt.compensate::<Deployed>("one").await,
        Err(RuntimeError::LegacyContract { .. })
    ));
    assert_eq!(
        store.get(record.id).await.unwrap().unwrap().status,
        EffectStatus::Committed
    );
}

#[tokio::test]
async fn recovery_refuses_an_incompatible_deployment_before_deserializing_or_acting() {
    let store = MemoryStore::new();
    let executions = Arc::<AtomicU32>::default();
    let mut contract = ExecutionContract::new(EffectKind::IrreversibleWrite, 5);
    contract.handler_version = "old".into();
    contract.verification = VerificationContract::Authoritative;
    seed(&store, Some(contract)).await;
    let rt = Runtime::builder(store.clone())
        .register(
            Handler::new(Deployed {
                version: "new",
                executions: executions.clone(),
            })
            .verifiable(),
        )
        .build();
    let report = rt.recover().await.unwrap();
    assert_eq!(report.resume_errors.len(), 1, "{report:?}");
    assert!(report.resume_errors[0].1.contains("contract"));
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.get_by_key(&key()).await.unwrap().unwrap().status,
        EffectStatus::Pending
    );
}

#[tokio::test]
async fn durable_handler_recovers_after_a_compatible_deployment() {
    let store = MemoryStore::new();
    let executions = Arc::<AtomicU32>::default();
    let mut contract = ExecutionContract::new(EffectKind::IrreversibleWrite, 5);
    contract.verification = VerificationContract::Authoritative;
    seed(&store, Some(contract)).await;
    let rt = Runtime::builder(store)
        .register(
            Handler::new(Deployed {
                version: "1",
                executions: executions.clone(),
            })
            .verifiable(),
        )
        .build();
    let report = rt.recover().await.unwrap();
    assert!(report.resume_errors.is_empty(), "{report:?}");
    assert_eq!(report.resumed.len(), 1);
    assert_eq!(report.resumed[0].1, EffectStatus::Committed);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}
