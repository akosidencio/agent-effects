//! Deterministic races: pause one decision after reading its version, then
//! allow another worker to change the authoritative state before releasing it.
#[path = "support/reliability.rs"]
mod reliability;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use agent_effects::store::{LateObservation, LateOutcome, Lease, StoreError};
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    CompensationOutcome, EffectFailure, EffectHandler, EffectOutcome, EffectStatus, EffectStore,
    ManualClock, Resolution, Runtime, RuntimeError, Transition, Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;
use reliability::fault_store::{Edge, FaultStore, Operation};
use reliability::{Durable, Protection, record, runtime};

type Store = FaultStore<MemoryStore>;

fn setup(protection: Protection) -> (Store, Runtime<Store>, FakeRemote) {
    let store = FaultStore::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::default());
    let remote = FakeRemote::new(clock.clone());
    let rt = runtime(store.clone(), clock, remote.clone(), protection, "worker");
    (store, rt, remote)
}

async fn awaiting(rt: &Runtime<Store>) {
    assert!(matches!(
        rt.effect(Durable::NAME, "approval")
            .require_approval()
            .run(|_| async { Ok::<_, EffectFailure>("unused".to_string()) })
            .await
            .unwrap(),
        EffectOutcome::AwaitingApproval { .. }
    ));
}

async fn unknown(rt: &Runtime<Store>, remote: &FakeRemote) {
    let remote = remote.clone();
    assert!(matches!(
        rt.effect(Durable::NAME, "resolution")
            .input(&"resolution")
            .verify(|_| async { Ok::<_, EffectFailure>(Verification::<String>::Inconclusive) })
            .run(move |_| {
                let remote = remote.clone();
                async move {
                    remote.create("resolution", None).await?;
                    Err::<String, _>(EffectFailure::ambiguous("answer lost"))
                }
            })
            .await
            .unwrap(),
        EffectOutcome::Unknown { .. }
    ));
}

#[tokio::test]
async fn approve_versus_deny_accepts_and_audits_only_one_decision() {
    for approve_first in [false, true] {
        let (store, rt, _) = setup(Protection::None);
        awaiting(&rt).await;
        let pending = record(store.inner.as_ref(), "approval").await;
        let gate = store.controls.pause(
            Operation::Transition(if approve_first {
                Transition::Approve
            } else {
                Transition::Deny
            }),
            Edge::Before,
        );
        let stale = tokio::spawn({
            let rt = rt.clone();
            async move {
                if approve_first {
                    rt.approve(pending.id, "stale", "approve").await
                } else {
                    rt.deny(pending.id, "stale", "deny").await
                }
            }
        });
        gate.entered().await;
        if approve_first {
            rt.deny(pending.id, "winner", "deny").await.unwrap();
        } else {
            rt.approve(pending.id, "winner", "approve").await.unwrap();
        }
        gate.release();
        assert!(matches!(
            stale.await.unwrap(),
            Err(RuntimeError::Store(StoreError::VersionConflict { .. }))
        ));
        let calls = Arc::<AtomicU32>::default();
        let outcome = rt
            .effect(Durable::NAME, "approval")
            .require_approval()
            .run({
                let calls = calls.clone();
                move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok::<_, EffectFailure>("done".to_string()) }
                }
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), u32::from(!approve_first));
        assert!(matches!(
            outcome,
            EffectOutcome::Rejected(_) | EffectOutcome::Committed(_)
        ));
        let events = store.inner.events(pending.id).await.unwrap();
        let accepted: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.transition, Transition::Approve | Transition::Deny))
            .collect();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].actor.as_deref(), Some("winner"));
    }
}

#[tokio::test]
async fn execution_can_follow_a_committed_approval_before_its_acknowledgement() {
    let (store, rt, _) = setup(Protection::None);
    awaiting(&rt).await;
    let pending = record(store.inner.as_ref(), "approval").await;
    let gate = store
        .controls
        .pause(Operation::Transition(Transition::Approve), Edge::After);
    let approval = tokio::spawn({
        let rt = rt.clone();
        async move { rt.approve(pending.id, "approver", "reviewed").await }
    });
    gate.entered().await;
    assert!(matches!(
        rt.effect(Durable::NAME, "approval")
            .require_approval()
            .run(|_| async { Ok::<_, EffectFailure>("done".to_string()) })
            .await
            .unwrap(),
        EffectOutcome::Committed(_)
    ));
    gate.release();
    approval.await.unwrap().unwrap();
    assert_eq!(
        record(store.inner.as_ref(), "approval").await.status,
        EffectStatus::Committed
    );
    let events = store.inner.events(pending.id).await.unwrap();
    let approved = events
        .iter()
        .position(|e| e.transition == Transition::Approve)
        .unwrap();
    let executed = events
        .iter()
        .position(|e| e.transition == Transition::StartAttempt)
        .unwrap();
    assert!(approved < executed);
    assert!(rt.deny(pending.id, "late", "too late").await.is_err());
}

#[tokio::test]
async fn recovery_fences_a_resolution_that_read_an_older_version() {
    let (store, rt, remote) = setup(Protection::Verified);
    unknown(&rt, &remote).await;
    let pending = record(store.inner.as_ref(), "resolution").await;
    let gate = store.controls.pause(
        Operation::Transition(Transition::ResolvedApplied),
        Edge::Before,
    );
    let resolution = tokio::spawn({
        let rt = rt.clone();
        async move {
            rt.resolve(
                pending.id,
                Resolution::applied(&"resolution#1").unwrap(),
                "stale",
                "confirmed",
            )
            .await
        }
    });
    gate.entered().await;
    rt.recover().await.unwrap();
    gate.release();
    assert!(matches!(
        resolution.await.unwrap(),
        Err(RuntimeError::Store(StoreError::VersionConflict { .. }))
    ));
    assert_eq!(
        record(store.inner.as_ref(), "resolution").await.status,
        EffectStatus::Committed
    );
    assert_eq!(remote.applications("resolution"), 1);
    assert_eq!(remote.requests(), 1);
    assert!(
        store
            .inner
            .events(pending.id)
            .await
            .unwrap()
            .iter()
            .all(|e| e.transition != Transition::ResolvedApplied)
    );
}

#[tokio::test]
async fn an_operator_cannot_resolve_while_recovery_holds_the_lease() {
    let (store, rt, remote) = setup(Protection::Verified);
    unknown(&rt, &remote).await;
    let pending = record(store.inner.as_ref(), "resolution").await;
    let gate = store.controls.pause(
        Operation::Transition(Transition::StartVerification),
        Edge::Before,
    );
    let recovery = tokio::spawn({
        let rt = rt.clone();
        async move { rt.recover().await }
    });
    gate.entered().await;
    assert!(matches!(
        rt.resolve(
            pending.id,
            Resolution::NotApplied,
            "operator",
            "claimed absent"
        )
        .await,
        Err(RuntimeError::Store(StoreError::LeaseHeld { .. }))
    ));
    gate.release();
    recovery.await.unwrap().unwrap();
    assert_eq!(
        record(store.inner.as_ref(), "resolution").await.status,
        EffectStatus::Committed
    );
    assert_eq!(remote.applications("resolution"), 1);
}

#[tokio::test]
async fn supplementary_evidence_does_not_conflict_with_an_authorized_resolution() {
    let (store, rt, remote) = setup(Protection::Verified);
    unknown(&rt, &remote).await;
    let pending = record(store.inner.as_ref(), "resolution").await;
    let gate = store.controls.pause(
        Operation::Transition(Transition::ResolvedApplied),
        Edge::Before,
    );
    let resolution = tokio::spawn({
        let rt = rt.clone();
        async move {
            rt.resolve(
                pending.id,
                Resolution::applied(&"resolution#1").unwrap(),
                "operator",
                "provider confirmed",
            )
            .await
        }
    });
    gate.entered().await;
    let lease = Lease {
        effect_id: pending.id,
        owner: WorkerId::new("late-worker"),
        epoch: pending.lease_epoch,
        expires_at: pending.updated_at,
    };
    store
        .append_late_observation(LateObservation::new(
            &pending,
            &lease,
            LateOutcome::Succeeded,
            pending.updated_at + Duration::from_secs(1),
        ))
        .await
        .unwrap();
    assert_eq!(store.inner.get(pending.id).await.unwrap().unwrap(), pending);
    gate.release();
    resolution.await.unwrap().unwrap();
    assert_eq!(
        record(store.inner.as_ref(), "resolution").await.status,
        EffectStatus::Committed
    );
    assert_eq!(
        store
            .inner
            .late_observations(pending.id, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    let events = store.inner.events(pending.id).await.unwrap();
    let resolution = events
        .iter()
        .find(|e| e.transition == Transition::ResolvedApplied)
        .unwrap();
    assert_eq!(resolution.actor.as_deref(), Some("operator"));
}

#[tokio::test]
async fn conflicting_operator_resolutions_cannot_overwrite_the_winner() {
    let (store, rt, remote) = setup(Protection::Verified);
    unknown(&rt, &remote).await;
    let pending = record(store.inner.as_ref(), "resolution").await;
    let gate = store.controls.pause(
        Operation::Transition(Transition::ResolvedNotApplied),
        Edge::Before,
    );
    let stale = tokio::spawn({
        let rt = rt.clone();
        async move {
            rt.resolve(pending.id, Resolution::NotApplied, "stale", "not found")
                .await
        }
    });
    gate.entered().await;
    rt.resolve(
        pending.id,
        Resolution::applied(&"resolution#1").unwrap(),
        "winner",
        "provider confirmed",
    )
    .await
    .unwrap();
    gate.release();
    assert!(matches!(
        stale.await.unwrap(),
        Err(RuntimeError::Store(StoreError::VersionConflict { .. }))
    ));
    let committed = record(store.inner.as_ref(), "resolution").await;
    assert_eq!(committed.status, EffectStatus::Committed);
    assert!(
        rt.resolve(pending.id, Resolution::Retry, "late", "repeat")
            .await
            .is_err()
    );
    assert_eq!(
        store.inner.get(pending.id).await.unwrap().unwrap(),
        committed
    );
}

#[tokio::test]
async fn compensation_can_finish_before_operator_retry_acknowledgement() {
    let store = FaultStore::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::default());
    let remote = FakeRemote::new(clock.clone()).script([
        Behavior::Succeed,
        Behavior::Fail(agent_effects::FailureClass::Permanent),
    ]);
    let rt = runtime(
        store.clone(),
        clock,
        remote.clone(),
        Protection::Idempotent,
        "worker",
    );
    rt.submit::<Durable>("undo", "undo".into()).await.unwrap();
    assert!(matches!(
        rt.compensate::<Durable>("undo").await.unwrap(),
        CompensationOutcome::Failed(_)
    ));
    let failed = record(store.inner.as_ref(), "undo").await;
    let gate = store.controls.pause(
        Operation::Transition(Transition::ResolvedRetry),
        Edge::After,
    );
    let retry = tokio::spawn({
        let rt = rt.clone();
        async move {
            rt.resolve(failed.id, Resolution::Retry, "operator", "safe undo retry")
                .await
        }
    });
    gate.entered().await;
    assert_eq!(
        rt.compensate::<Durable>("undo").await.unwrap(),
        CompensationOutcome::Compensated
    );
    gate.release();
    retry.await.unwrap().unwrap();
    assert_eq!(
        record(store.inner.as_ref(), "undo").await.status,
        EffectStatus::Compensated
    );
    assert_eq!(remote.cancellations("undo"), 1);
    let events = store.inner.events(failed.id).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.transition == Transition::ResolvedRetry)
            .count(),
        1
    );
    assert!(
        rt.resolve(failed.id, Resolution::Retry, "late", "repeat")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn simultaneous_verification_workers_share_one_authoritative_result() {
    let (store, rt, remote) = setup(Protection::Verified);
    unknown(&rt, &remote).await;
    let id = record(store.inner.as_ref(), "resolution").await.id;
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..4 {
        let rt = runtime(
            store.clone(),
            Arc::new(ManualClock::default()),
            remote.clone(),
            Protection::Verified,
            &format!("verifier-{index}"),
        );
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            rt.recover().await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap().unwrap();
    }
    assert_eq!(
        record(store.inner.as_ref(), "resolution").await.status,
        EffectStatus::Committed
    );
    assert_eq!(remote.applications("resolution"), 1);
    assert_eq!(remote.requests(), 1);
    let events = store.inner.events(id).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.transition == Transition::VerificationConfirmed)
            .count(),
        1
    );
}
