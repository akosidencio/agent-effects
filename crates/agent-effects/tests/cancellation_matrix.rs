//! Dropping a caller cannot interrupt an already spawned durable driver.
#[path = "support/reliability.rs"]
mod reliability;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    ApprovalDecision, ApprovalProvider, ApprovalRequest, EffectFailure, EffectHandler,
    EffectOutcome, EffectStatus, EffectStore, ManualClock, RetryPolicy, Runtime, Transition,
};
use agent_effects_memory::MemoryStore;
use reliability::fault_store::{Edge, FaultStore, Gate, Operation};
use reliability::{Durable, Protection, RETRY, TTL, key, record, runtime};

async fn settled(store: &MemoryStore, logical: &str, status: EffectStatus) {
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if let Some(record) = store.get_by_key(&key(logical)).await.unwrap()
                && record.status == status
                && record.lease_owner.is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("detached driver did not settle and release its lease");
}

#[tokio::test(start_paused = true)]
async fn dropping_an_unpolled_submission_never_records_or_executes() {
    let store = FaultStore::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::default());
    let remote = FakeRemote::new(clock.clone());
    let rt = runtime(
        store.clone(),
        clock,
        remote.clone(),
        Protection::None,
        "caller",
    );
    drop(rt.submit::<Durable>("unpolled", "unpolled".into()));
    tokio::task::yield_now().await;
    assert!(store.inner.is_empty());
    assert_eq!(remote.requests(), 0);
}

#[tokio::test(start_paused = true)]
async fn cancellation_at_store_checkpoints_keeps_the_driver_alive() {
    for recovery in [false, true] {
        for operation in [
            Operation::Insert,
            Operation::Acquire,
            Operation::Transition(Transition::StartAttempt),
            Operation::Transition(Transition::Succeeded),
        ] {
            for edge in [Edge::Before, Edge::After] {
                let store = FaultStore::new(MemoryStore::new());
                let clock = Arc::new(ManualClock::default());
                let remote = FakeRemote::new(clock.clone());
                let rt = runtime(
                    store.clone(),
                    clock.clone(),
                    remote.clone(),
                    Protection::Idempotent,
                    "caller",
                );
                if recovery {
                    let mut contract = agent_effects::store::ExecutionContract::new(
                        agent_effects::EffectKind::IrreversibleWrite,
                        RETRY.max_attempts,
                    );
                    contract.remote_idempotency = true;
                    let mut new = agent_effects::store::NewEffect::new(
                        key("cancelled"),
                        agent_effects::EffectKind::IrreversibleWrite,
                        agent_effects::Clock::now(&clock),
                    )
                    .with_contract(contract);
                    new.input = Some(serde_json::json!("cancelled"));
                    store.inner.insert_or_get(new).await.unwrap();
                }
                let gate = store.controls.pause(operation, edge);
                let caller = tokio::spawn(async move {
                    if recovery {
                        rt.recover().await.unwrap();
                    } else {
                        rt.submit::<Durable>("cancelled", "cancelled".into())
                            .await
                            .unwrap();
                    }
                });
                gate.entered().await;
                caller.abort();
                assert!(caller.await.unwrap_err().is_cancelled());
                gate.release();
                settled(&store.inner, "cancelled", EffectStatus::Committed).await;
                let record = record(store.inner.as_ref(), "cancelled").await;
                assert_eq!(
                    record.attempt_count, 1,
                    "{operation:?}/{edge:?}/recovery={recovery}"
                );
                assert_eq!(remote.applications("cancelled"), 1);
                assert_eq!(remote.requests(), 1);
            }
        }
    }
}

#[derive(Clone)]
struct Approver(Gate);
impl ApprovalProvider for Approver {
    async fn request(&self, _: ApprovalRequest) -> ApprovalDecision {
        self.0.wait().await;
        ApprovalDecision::Approved {
            by: "operator:test".into(),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_action_verification_and_approval_is_detached() {
    for stage in ["action", "verification", "approval"] {
        let store = FaultStore::new(MemoryStore::new());
        let clock = Arc::new(ManualClock::default());
        let remote = FakeRemote::new(clock.clone());
        let gate = Gate::default();
        let rt = Runtime::builder(store.clone())
            .clock(clock)
            .approval_provider(Approver(gate.clone()))
            .retry_policy(RETRY)
            .build();
        let caller = {
            let (remote, gate) = (remote.clone(), gate.clone());
            tokio::spawn(async move {
                match stage {
                    "verification" => {
                        let lookup = remote.clone();
                        rt.effect(Durable::NAME, stage)
                            .verify(move |_| {
                                let (lookup, gate) = (lookup.clone(), gate.clone());
                                async move {
                                    gate.wait().await;
                                    Ok::<_, EffectFailure>(agent_effects::Verification::Confirmed(
                                        lookup.find(stage).await?.unwrap(),
                                    ))
                                }
                            })
                            .run(move |_| {
                                let remote = remote.clone();
                                async move { remote.create(stage, None).await }
                            })
                            .await
                    }
                    "approval" => {
                        rt.effect(Durable::NAME, stage)
                            .require_approval()
                            .run(move |_| {
                                let remote = remote.clone();
                                async move { remote.create(stage, None).await }
                            })
                            .await
                    }
                    _ => {
                        rt.effect(Durable::NAME, stage)
                            .run(move |_| {
                                let (remote, gate) = (remote.clone(), gate.clone());
                                async move {
                                    gate.wait().await;
                                    remote.create(stage, None).await
                                }
                            })
                            .await
                    }
                }
            })
        };
        gate.entered().await;
        assert_eq!(
            remote.applications(stage),
            u32::from(stage == "verification")
        );
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        gate.release();
        settled(&store.inner, stage, EffectStatus::Committed).await;
        assert_eq!(remote.applications(stage), 1);
        if stage == "approval" {
            let record = record(store.inner.as_ref(), stage).await;
            let events = store.inner.events(record.id).await.unwrap();
            let decision = events
                .iter()
                .find(|e| e.transition == Transition::Approve)
                .unwrap();
            assert_eq!(decision.actor.as_deref(), Some("operator:test"));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_compensation_and_after_undo_success_does_not_repeat_it() {
    for after_success in [false, true] {
        let store = FaultStore::new(MemoryStore::new());
        let clock = Arc::new(ManualClock::default());
        let remote = FakeRemote::new(clock.clone());
        let rt = runtime(
            store.clone(),
            clock,
            remote.clone(),
            Protection::Idempotent,
            "caller",
        );
        rt.submit::<Durable>("undo", "undo".into()).await.unwrap();
        let gate = if after_success {
            store.controls.pause(
                Operation::Transition(Transition::CompensationSucceeded),
                Edge::Before,
            )
        } else {
            Gate::default()
        };
        let caller = tokio::spawn({
            let (gate, remote) = (gate.clone(), remote.clone());
            async move {
                rt.compensation(Durable::NAME, "undo")
                    .run(move |ctx, _: Option<String>| {
                        let (gate, remote) = (gate.clone(), remote.clone());
                        async move {
                            if !after_success {
                                gate.wait().await;
                            }
                            remote.cancel("undo", Some(ctx.idempotency_key())).await
                        }
                    })
                    .await
            }
        });
        gate.entered().await;
        assert_eq!(remote.cancellations("undo"), u32::from(after_success));
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        gate.release();
        settled(&store.inner, "undo", EffectStatus::Compensated).await;
        assert_eq!(remote.cancellations("undo"), 1);
        assert_eq!(
            record(store.inner.as_ref(), "undo")
                .await
                .compensation_attempts,
            1
        );
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_persisted_backoff_preserves_schedule_and_attempt_budget() {
    let store = FaultStore::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::default());
    let remote = FakeRemote::new(clock.clone())
        .script([Behavior::Fail(agent_effects::FailureClass::Transient)]);
    let retry = RetryPolicy {
        initial_delay: Duration::from_secs(60),
        max_delay: Duration::from_secs(60),
        ..RETRY
    };
    let rt = Runtime::builder(store.clone())
        .clock(clock.clone())
        .lease_ttl(Duration::from_secs(300))
        .retry_policy(retry)
        .register(agent_effects::Handler::new(Durable {
            remote: remote.clone(),
            protection: Protection::None,
        }))
        .build();
    let gate = store.controls.pause(
        Operation::Transition(Transition::ScheduleRetry),
        Edge::After,
    );
    let caller =
        tokio::spawn(async move { rt.submit::<Durable>("backoff", "backoff".into()).await });
    gate.entered().await;
    let waiting = record(store.inner.as_ref(), "backoff").await;
    assert_eq!(waiting.status, EffectStatus::Pending);
    assert!(waiting.next_attempt_at.is_some());
    assert_eq!(remote.applications("backoff"), 0);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    gate.release();
    clock.advance(Duration::from_secs(60));
    tokio::time::advance(Duration::from_secs(60)).await;
    settled(&store.inner, "backoff", EffectStatus::Committed).await;
    assert_eq!(
        record(store.inner.as_ref(), "backoff").await.attempt_count,
        2
    );
    assert_eq!(remote.applications("backoff"), 1);
}

#[tokio::test(start_paused = true)]
async fn renewal_outage_then_takeover_fences_the_stalled_worker() {
    for protection in [Protection::None, Protection::Idempotent] {
        let store = FaultStore::new(MemoryStore::new());
        let clock = Arc::new(ManualClock::default());
        let remote = FakeRemote::new(clock.clone());
        let rt = runtime(
            store.clone(),
            clock.clone(),
            remote.clone(),
            protection,
            "stalled",
        );
        let gate = Gate::default();
        let caller = tokio::spawn({
            let (remote, gate) = (remote.clone(), gate.clone());
            async move {
                rt.effect(Durable::NAME, "renewal")
                    .input(&"renewal")
                    .remote_idempotency(protection == Protection::Idempotent)
                    .run(move |ctx| {
                        let (remote, gate) = (remote.clone(), gate.clone());
                        async move {
                            let output = remote
                                .create(
                                    "renewal",
                                    (protection == Protection::Idempotent)
                                        .then(|| ctx.idempotency_key()),
                                )
                                .await?;
                            gate.wait().await;
                            Ok::<_, EffectFailure>(output)
                        }
                    })
                    .await
                    .unwrap()
            }
        });
        gate.entered().await;
        let original = record(store.inner.as_ref(), "renewal").await;
        store.controls.offline();
        tokio::time::advance(TTL / 3).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(store.controls.renewals.load(Ordering::SeqCst) > 0);
        assert_eq!(
            record(store.inner.as_ref(), "renewal")
                .await
                .lease_expires_at,
            original.lease_expires_at
        );
        clock.advance(TTL + Duration::from_secs(1));
        store.controls.reconnect();
        let replacement = runtime(
            store.clone(),
            clock.clone(),
            remote.clone(),
            protection,
            "replacement",
        );
        replacement.recover().await.unwrap();
        let authoritative = record(store.inner.as_ref(), "renewal").await;
        gate.release();
        let outcome = caller.await.unwrap();
        assert!(matches!(
            outcome,
            EffectOutcome::Committed(_) | EffectOutcome::NeedsIntervention { .. }
        ));
        assert_eq!(
            store.inner.get(authoritative.id).await.unwrap().unwrap(),
            authoritative
        );
        assert_eq!(remote.applications("renewal"), 1);
        let evidence = store
            .inner
            .late_observations(authoritative.id, None, 10)
            .await
            .unwrap();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].lease_epoch, original.lease_epoch);
        assert_eq!(
            evidence[0].outcome,
            agent_effects::store::LateOutcome::Succeeded
        );
    }
}
