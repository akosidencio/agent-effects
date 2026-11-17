//! `OperatorDecision`: decisions pinned to the record version an operator
//! reviewed, with the actor and note in the audit trail.

use agent_effects::store::{EffectStore, StoreError, Transition};
use agent_effects::{
    EffectFailure, EffectOutcome, EffectStatus, OperatorDecision, Resolution, Runtime, RuntimeError,
};
use agent_effects_memory::MemoryStore;

async fn awaiting(rt: &Runtime<MemoryStore>, key: &str) -> agent_effects::EffectId {
    match rt
        .effect("refund.issue", key)
        .require_approval()
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap()
    {
        EffectOutcome::AwaitingApproval { id } => id,
        other => panic!("{other:?}"),
    }
}

async fn escalated(rt: &Runtime<MemoryStore>, key: &str) -> agent_effects::EffectId {
    match rt
        .effect("payment.charge", key)
        .run(|_| async { Err::<(), _>(EffectFailure::ambiguous("reset")) })
        .await
        .unwrap()
    {
        EffectOutcome::NeedsIntervention { id } => id,
        other => panic!("{other:?}"),
    }
}

fn is_conflict(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::Store(StoreError::VersionConflict { .. })
    )
}

#[tokio::test]
async fn a_decision_on_a_version_that_moved_on_changes_nothing() {
    let rt = Runtime::new(MemoryStore::new());
    let id = awaiting(&rt, "r1").await;
    let seen = rt.store().get(id).await.unwrap().unwrap().version;
    // Someone else decides first.
    rt.deny(id, "operator:bob", "duplicate").await.unwrap();

    let late = OperatorDecision::new("operator:alice", "looks fine").expect_version(seen);
    let error = rt.approve_as(id, late).await.unwrap_err();
    assert!(is_conflict(&error), "{error}");
    let record = rt.store().get(id).await.unwrap().unwrap();
    assert_eq!(record.status, EffectStatus::Rejected);
    assert!(
        rt.store()
            .events(id)
            .await
            .unwrap()
            .iter()
            .all(|e| e.transition != Transition::Approve)
    );
}

#[tokio::test]
async fn a_decision_on_the_current_version_applies_with_its_audit_details() {
    let rt = Runtime::new(MemoryStore::new());
    let id = awaiting(&rt, "r1").await;
    let seen = rt.store().get(id).await.unwrap().unwrap().version;
    let decision = OperatorDecision::new("operator:alice", "ticket 42").expect_version(seen);
    let record = rt.approve_as(id, decision).await.unwrap();
    assert_eq!(record.status, EffectStatus::Pending);
    let event = rt.store().events(id).await.unwrap().pop().unwrap();
    assert_eq!(event.actor.as_deref(), Some("operator:alice"));
    assert_eq!(event.payload.unwrap()["note"], "ticket 42");

    let other = awaiting(&rt, "r2").await;
    let denied = rt
        .deny_as(
            other,
            OperatorDecision::new("operator:alice", "not eligible"),
        )
        .await
        .unwrap();
    assert_eq!(denied.last_error.unwrap().message, "not eligible");
}

#[tokio::test]
async fn resolve_as_pins_the_version_and_records_the_output_schema() {
    let rt = Runtime::new(MemoryStore::new());
    let id = escalated(&rt, "c1").await;
    let seen = rt.store().get(id).await.unwrap().unwrap().version;
    let stale = OperatorDecision::new("operator:alice", "x").expect_version(seen + 1);
    let error = rt
        .resolve_as(id, Resolution::NotApplied, stale)
        .await
        .unwrap_err();
    assert!(is_conflict(&error), "{error}");
    assert_eq!(
        rt.store().get(id).await.unwrap().unwrap().status,
        EffectStatus::NeedsIntervention
    );

    let decision = OperatorDecision::new("operator:alice", "found it")
        .expect_version(seen)
        .output_schema_version(2);
    let record = rt
        .resolve_as(id, Resolution::applied(&"ch_9").unwrap(), decision)
        .await
        .unwrap();
    assert_eq!(record.status, EffectStatus::Committed);
    assert_eq!(record.output_schema_version, 2);
}
