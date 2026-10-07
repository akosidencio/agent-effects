//! Conformance suite for [`EffectStore`] implementations.
//!
//! Enabled by the `testkit` feature. Add it as a dev-dependency feature:
//!
//! ```toml
//! [dev-dependencies]
//! agent-effects-store = { version = "0.1", features = ["testkit"] }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde_json::json;

use crate::failure::FailureClass;
use crate::id::{EffectId, EffectKey, EffectName, LogicalKey, WorkerId};
use crate::kind::EffectKind;
use crate::state::{ALL_STATUSES, ALL_TRANSITIONS, EffectStatus, Transition};
use crate::{
    EffectEvent, EffectRecord, EffectStore, ErrorRecord, Lease, ListQuery, NewEffect, PruneQuery,
    StoreError, TransitionRequest,
};

/// Runs the store conformance suite against stores built by `make_store`.
///
/// Every case gets a fresh store. A failing case panics. Some cases spawn
/// tasks, so call this from inside a Tokio runtime, ideally a multi-threaded
/// one so races are real.
///
/// ```ignore
/// #[tokio::test(flavor = "multi_thread")]
/// async fn conformance() {
///     agent_effects_store::testkit::conformance(|| async { MyStore::open_temp().await }).await;
/// }
/// ```
pub async fn conformance<S, F, Fut>(make_store: F)
where
    S: EffectStore,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    insert_and_read_back(make_store().await).await;
    insert_is_idempotent_per_key(make_store().await).await;
    concurrent_inserts_converge(make_store().await).await;
    missing_records_are_reported(make_store().await).await;
    leases_are_exclusive(make_store().await).await;
    takeover_fences_the_old_owner(make_store().await).await;
    renewal_extends_and_is_strict(make_store().await).await;
    release_frees_the_lease(make_store().await).await;
    transitions_persist_with_events(make_store().await).await;
    rejected_transitions_change_nothing(make_store().await).await;
    unleased_transitions_for_operators(make_store().await).await;
    terminal_records_are_final(make_store().await).await;
    listing_filters_and_pages(make_store().await).await;
    doubt_is_persisted_and_guards_failure(make_store().await).await;
    compensation_is_persisted(make_store().await).await;
    approval_is_persisted(make_store().await).await;
    pruning_removes_only_settled_idle_old_records(make_store().await).await;
}

const TTL: Duration = Duration::from_secs(30);

/// Test time `secs` seconds after a fixed base, at millisecond precision.
fn t(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000 + secs)
}

fn key(n: u32) -> EffectKey {
    EffectKey::new(
        EffectName::new("testkit.effect").unwrap(),
        LogicalKey::new(format!("key-{n}")).unwrap(),
    )
}

fn new_effect(n: u32) -> NewEffect {
    NewEffect {
        input: Some(json!({ "n": n, "nested": { "b": [1, 2], "a": null } })),
        input_fingerprint: Some(format!("fingerprint-{n}")),
        created_by: Some("agent:testkit".into()),
        ..NewEffect::new(key(n), EffectKind::IrreversibleWrite, t(0))
    }
}

fn worker(name: &str) -> WorkerId {
    WorkerId::new(name)
}

async fn insert<S: EffectStore>(store: &S, n: u32) -> EffectRecord {
    store.insert_or_get(new_effect(n)).await.unwrap().record
}

async fn reload<S: EffectStore>(store: &S, id: EffectId) -> EffectRecord {
    store.get(id).await.unwrap().expect("record exists")
}

/// Applies `transitions` in order under `lease` at time `now`.
async fn drive<S: EffectStore>(
    store: &S,
    mut record: EffectRecord,
    lease: Option<&Lease>,
    transitions: &[Transition],
    now: SystemTime,
) -> EffectRecord {
    for &transition in transitions {
        record = store
            .transition(TransitionRequest::new(&record, lease, transition, now))
            .await
            .unwrap_or_else(|e| panic!("{transition} from {}: {e}", record.status));
    }
    record
}

async fn insert_and_read_back<S: EffectStore>(store: S) {
    let new = new_effect(1);
    let outcome = store.insert_or_get(new.clone()).await.unwrap();
    assert!(outcome.inserted);
    let record = outcome.record;
    assert_eq!(record.id, new.id);
    assert_eq!(record.key, new.key);
    assert_eq!(record.kind, new.kind);
    assert_eq!(record.status, EffectStatus::Pending);
    assert_eq!(record.input, new.input);
    assert_eq!(record.input_fingerprint, new.input_fingerprint);
    assert_eq!(record.created_by, new.created_by);
    assert_eq!(
        (record.version, record.lease_epoch, record.attempt_count),
        (0, 0, 0)
    );
    assert_eq!((record.created_at, record.updated_at), (t(0), t(0)));
    assert_eq!(record.lease_owner, None);

    assert_eq!(store.get(record.id).await.unwrap(), Some(record.clone()));
    assert_eq!(
        store.get_by_key(&record.key).await.unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        store.events(record.id).await.unwrap(),
        Vec::<EffectEvent>::new()
    );
}

async fn insert_is_idempotent_per_key<S: EffectStore>(store: S) {
    let first = insert(&store, 1).await;
    let again = NewEffect {
        input: Some(json!("different")),
        ..NewEffect::new(key(1), EffectKind::Read, t(5))
    };
    let outcome = store.insert_or_get(again).await.unwrap();
    assert!(
        !outcome.inserted,
        "a second insert for the same key must not insert"
    );
    assert_eq!(
        outcome.record, first,
        "the existing record must come back untouched"
    );

    let other = insert(&store, 2).await;
    assert_ne!(other.id, first.id);
}

async fn concurrent_inserts_converge<S: EffectStore>(store: S) {
    let store = Arc::new(store);
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let store = Arc::clone(&store);
            tokio::spawn(async move { store.insert_or_get(new_effect(7)).await.unwrap() })
        })
        .collect();
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.unwrap());
    }
    let inserted = outcomes.iter().filter(|o| o.inserted).count();
    assert_eq!(inserted, 1, "exactly one concurrent insert may win");
    let id = outcomes[0].record.id;
    assert!(
        outcomes.iter().all(|o| o.record.id == id),
        "all callers must see one record"
    );
}

async fn missing_records_are_reported<S: EffectStore>(store: S) {
    let id = EffectId::new();
    assert_eq!(store.get(id).await.unwrap(), None);
    assert_eq!(store.get_by_key(&key(404)).await.unwrap(), None);
    assert!(matches!(
        store.acquire_lease(id, &worker("a"), t(0), TTL).await,
        Err(StoreError::NotFound(missing)) if missing == id
    ));
    let mut request = TransitionRequest::new(
        &insert(&store, 1).await,
        None,
        Transition::StartAttempt,
        t(0),
    );
    request.id = id;
    assert!(matches!(
        store.transition(request).await,
        Err(StoreError::NotFound(_))
    ));
    assert!(matches!(
        store.events(id).await,
        Err(StoreError::NotFound(_))
    ));
}

async fn leases_are_exclusive<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    assert_eq!((lease.epoch, lease.expires_at), (1, t(30)));
    assert_eq!(lease.owner, worker("a"));

    for contender in ["b", "a"] {
        match store
            .acquire_lease(record.id, &worker(contender), t(1), TTL)
            .await
        {
            Err(StoreError::LeaseHeld { owner, expires_at }) => {
                assert_eq!(owner, worker("a"));
                assert_eq!(expires_at, t(30));
            }
            other => panic!("{contender} acquired a held lease: {other:?}"),
        }
    }
}

async fn takeover_fences_the_old_owner<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    let old = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    assert!(matches!(
        store
            .acquire_lease(record.id, &worker("b"), t(29), TTL)
            .await,
        Err(StoreError::LeaseHeld { .. })
    ));
    let new = store
        .acquire_lease(record.id, &worker("b"), t(30), TTL)
        .await
        .unwrap();
    assert_eq!(new.epoch, 2);

    let stale = TransitionRequest::new(&record, Some(&old), Transition::StartAttempt, t(31));
    assert!(matches!(
        store.transition(stale).await,
        Err(StoreError::LeaseLost)
    ));
    assert!(matches!(
        store.renew_lease(&old, t(31), TTL).await,
        Err(StoreError::LeaseLost)
    ));
    store.release_lease(&old).await.unwrap();

    let current = reload(&store, record.id).await;
    assert_eq!(
        current.lease_owner,
        Some(worker("b")),
        "a stale release must not free b's lease"
    );
    assert_eq!((current.lease_epoch, current.version), (2, 0));
}

async fn renewal_extends_and_is_strict<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    let renewed = store.renew_lease(&lease, t(20), TTL).await.unwrap();
    assert_eq!((renewed.epoch, renewed.expires_at), (lease.epoch, t(50)));
    assert!(matches!(
        store
            .acquire_lease(record.id, &worker("b"), t(40), TTL)
            .await,
        Err(StoreError::LeaseHeld { .. })
    ));
    assert!(
        matches!(
            store.renew_lease(&renewed, t(50), TTL).await,
            Err(StoreError::LeaseLost)
        ),
        "an expired lease must not be revived"
    );
    assert_eq!(
        reload(&store, record.id).await.version,
        0,
        "lease operations must not bump the version"
    );
}

async fn release_frees_the_lease<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    store.release_lease(&lease).await.unwrap();
    store.release_lease(&lease).await.unwrap();
    let next = store
        .acquire_lease(record.id, &worker("b"), t(1), TTL)
        .await
        .unwrap();
    assert_eq!(next.epoch, 2);
}

async fn transitions_persist_with_events<St: EffectStore>(store: St) {
    use EffectStatus as S;
    use Transition as T;

    let record = insert(&store, 1).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    let record = drive(
        &store,
        record,
        Some(&lease),
        &[Transition::StartAttempt],
        t(1),
    )
    .await;
    assert_eq!(record.status, EffectStatus::Executing);
    assert_eq!(record.attempt_started_at, Some(t(1)));

    let error = ErrorRecord {
        class: Some(FailureClass::RateLimited {
            retry_after: Some(Duration::from_secs(9)),
        }),
        message: "429".into(),
    };
    let mut retry = TransitionRequest::new(&record, Some(&lease), Transition::ScheduleRetry, t(2));
    retry.next_attempt_at = Some(t(11));
    retry.error = Some(error.clone());
    retry.actor = Some("worker:a".into());
    retry.payload = Some(json!({ "delay_ms": 9000 }));
    let record = store.transition(retry).await.unwrap();
    assert_eq!(
        record,
        reload(&store, record.id).await,
        "transition must return the stored record"
    );
    assert_eq!(record.next_attempt_at, Some(t(11)));
    assert_eq!(record.attempt_ended_at, Some(t(2)));
    assert_eq!(record.last_error, Some(error));

    let record = drive(
        &store,
        record,
        Some(&lease),
        &[Transition::StartAttempt],
        t(11),
    )
    .await;
    let output = json!({ "payment_id": "pi_123", "amount": 4200 });
    let mut verify =
        TransitionRequest::new(&record, Some(&lease), Transition::StartVerification, t(12));
    verify.output = Some(output.clone());
    let record = store.transition(verify).await.unwrap();
    let record = drive(
        &store,
        record,
        Some(&lease),
        &[Transition::VerificationConfirmed],
        t(13),
    )
    .await;

    let stored = reload(&store, record.id).await;
    assert_eq!(stored.status, EffectStatus::Committed);
    assert_eq!(stored.output, Some(output));
    assert_eq!(stored.committed_at, Some(t(13)));
    assert_eq!((stored.attempt_count, stored.version), (2, 5));
    assert_eq!(stored.next_attempt_at, None);
    assert_eq!(stored.attempt_ended_at, Some(t(12)));

    let events = store.events(record.id).await.unwrap();
    let trail: Vec<_> = events
        .iter()
        .map(|e| (e.sequence, e.transition, e.from, e.to, e.attempt))
        .collect();
    assert_eq!(
        trail,
        [
            (1, T::StartAttempt, S::Pending, S::Executing, 1),
            (2, T::ScheduleRetry, S::Executing, S::Pending, 1),
            (3, T::StartAttempt, S::Pending, S::Executing, 2),
            (4, T::StartVerification, S::Executing, S::Verifying, 2),
            (5, T::VerificationConfirmed, S::Verifying, S::Committed, 2),
        ]
    );
    assert_eq!(events[1].actor.as_deref(), Some("worker:a"));
    assert_eq!(events[1].payload, Some(json!({ "delay_ms": 9000 })));
    assert_eq!(events[1].at, t(2));
}

async fn rejected_transitions_change_nothing<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    let record = reload(&store, record.id).await;

    let illegal = TransitionRequest::new(&record, Some(&lease), Transition::Succeeded, t(1));
    let stale_version = TransitionRequest {
        expected_version: 7,
        ..TransitionRequest::new(&record, Some(&lease), Transition::StartAttempt, t(1))
    };
    let unleased = TransitionRequest::new(&record, None, Transition::StartAttempt, t(1));
    let expired = TransitionRequest::new(&record, Some(&lease), Transition::StartAttempt, t(30));

    assert!(matches!(
        store.transition(illegal).await,
        Err(StoreError::InvalidTransition(_))
    ));
    assert!(matches!(
        store.transition(stale_version).await,
        Err(StoreError::VersionConflict {
            expected: 7,
            actual: 0
        })
    ));
    assert!(matches!(
        store.transition(unleased).await,
        Err(StoreError::LeaseHeld { .. })
    ));
    assert!(matches!(
        store.transition(expired).await,
        Err(StoreError::LeaseLost)
    ));

    assert_eq!(reload(&store, record.id).await, record);
    assert_eq!(
        store.events(record.id).await.unwrap(),
        Vec::<EffectEvent>::new()
    );
}

async fn unleased_transitions_for_operators<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    let record = drive(
        &store,
        record,
        Some(&lease),
        &[Transition::StartAttempt, Transition::OutcomeUnknown],
        t(1),
    )
    .await;
    store.release_lease(&lease).await.unwrap();

    let record = drive(&store, record, None, &[Transition::Escalate], t(2)).await;
    assert_eq!(record.status, EffectStatus::NeedsIntervention);
    let mut resolve = TransitionRequest::new(&record, None, Transition::ResolvedApplied, t(3));
    resolve.output = Some(json!("confirmed by operator"));
    resolve.actor = Some("operator:dennis".into());
    let record = store.transition(resolve).await.unwrap();
    assert_eq!(record.status, EffectStatus::Committed);
    assert_eq!(record.output, Some(json!("confirmed by operator")));
}

async fn terminal_records_are_final<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    let record = drive(
        &store,
        record,
        Some(&lease),
        &[Transition::StartAttempt, Transition::Succeeded],
        t(1),
    )
    .await;
    store.release_lease(&lease).await.unwrap();
    let record = reload(&store, record.id).await;
    // Committed has one exit, compensation; nothing else may leave it.
    for transition in ALL_TRANSITIONS
        .into_iter()
        .filter(|t| *t != Transition::StartCompensation)
    {
        let request = TransitionRequest::new(&record, None, transition, t(2));
        assert!(
            matches!(
                store.transition(request).await,
                Err(StoreError::InvalidTransition(_))
            ),
            "{transition} left a committed record"
        );
    }
    assert_eq!(reload(&store, record.id).await, record);

    // Compensated is terminal.
    let record = drive(
        &store,
        record,
        None,
        &[
            Transition::StartCompensation,
            Transition::CompensationSucceeded,
        ],
        t(3),
    )
    .await;
    for transition in ALL_TRANSITIONS {
        let request = TransitionRequest::new(&record, None, transition, t(4));
        assert!(
            matches!(
                store.transition(request).await,
                Err(StoreError::InvalidTransition(_))
            ),
            "{transition} left a compensated record"
        );
    }
}

async fn listing_filters_and_pages<S: EffectStore>(store: S) {
    let now = t(100);
    let mut ids = Vec::new();
    for n in 0..5 {
        ids.push(insert(&store, n).await.id);
    }
    // 0: Pending, no lease.
    // 1: Executing, live lease.
    let lease = store
        .acquire_lease(ids[1], &worker("a"), t(90), TTL)
        .await
        .unwrap();
    drive(
        &store,
        reload(&store, ids[1]).await,
        Some(&lease),
        &[Transition::StartAttempt],
        t(90),
    )
    .await;
    // 2: Executing, lease expired.
    let lease = store
        .acquire_lease(ids[2], &worker("a"), t(0), TTL)
        .await
        .unwrap();
    drive(
        &store,
        reload(&store, ids[2]).await,
        Some(&lease),
        &[Transition::StartAttempt],
        t(1),
    )
    .await;
    // 3: Verifying, lease released.
    let lease = store
        .acquire_lease(ids[3], &worker("a"), t(0), TTL)
        .await
        .unwrap();
    drive(
        &store,
        reload(&store, ids[3]).await,
        Some(&lease),
        &[Transition::StartAttempt, Transition::StartVerification],
        t(1),
    )
    .await;
    store.release_lease(&lease).await.unwrap();
    // 4: Unknown.
    let lease = store
        .acquire_lease(ids[4], &worker("a"), t(0), TTL)
        .await
        .unwrap();
    drive(
        &store,
        reload(&store, ids[4]).await,
        Some(&lease),
        &[Transition::StartAttempt, Transition::OutcomeUnknown],
        t(1),
    )
    .await;

    let listed = |query: ListQuery| {
        let store = &store;
        async move {
            store
                .list(query)
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.id)
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        listed(ListQuery::statuses([EffectStatus::Pending])).await,
        [ids[0]]
    );
    assert_eq!(
        listed(ListQuery::expired_leases(now)).await,
        [ids[2], ids[3]]
    );
    assert_eq!(
        listed(ListQuery::statuses([]).limit(2)).await,
        [ids[0], ids[1]]
    );
    assert_eq!(
        listed(ListQuery::statuses([]).after(ids[1])).await,
        [ids[2], ids[3], ids[4]]
    );
    assert_eq!(
        listed(ListQuery::expired_leases(now).after(ids[2])).await,
        [ids[3]]
    );

    let records = store
        .list(ListQuery::statuses([EffectStatus::Unknown]))
        .await
        .unwrap();
    assert_eq!(
        records,
        [reload(&store, ids[4]).await],
        "listed records must be complete"
    );
}

async fn doubt_is_persisted_and_guards_failure<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    assert!(!record.may_have_applied);
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    let record = drive(
        &store,
        record,
        Some(&lease),
        &[
            Transition::StartAttempt,
            Transition::OutcomeUnknown,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
        ],
        t(1),
    )
    .await;
    assert!(
        reload(&store, record.id).await.may_have_applied,
        "the store must persist may_have_applied"
    );
    let fail = TransitionRequest::new(&record, Some(&lease), Transition::FailedDefinitively, t(2));
    assert!(
        matches!(
            store.transition(fail).await,
            Err(StoreError::InvalidTransition(_))
        ),
        "a failed attempt must not fail an effect that may have applied"
    );
    assert_eq!(reload(&store, record.id).await, record);
}

async fn compensation_is_persisted<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(0), TTL)
        .await
        .unwrap();
    let record = drive(
        &store,
        record,
        Some(&lease),
        &[
            Transition::StartAttempt,
            Transition::Succeeded,
            Transition::StartCompensation,
        ],
        t(1),
    )
    .await;
    let mut retry = TransitionRequest::new(
        &record,
        Some(&lease),
        Transition::ScheduleCompensationRetry,
        t(2),
    );
    retry.next_attempt_at = Some(t(5));
    let record = store.transition(retry).await.unwrap();
    let stored = reload(&store, record.id).await;
    assert_eq!(stored, record, "transition must return the stored record");
    assert_eq!(stored.status, EffectStatus::Compensating);
    assert_eq!(
        stored.compensation_attempts, 1,
        "a scheduled retry is not an attempt until it starts"
    );
    assert_eq!(stored.next_attempt_at, Some(t(5)));

    let record = drive(
        &store,
        record,
        Some(&lease),
        &[Transition::StartCompensationRetry],
        t(5),
    )
    .await;
    let stored = reload(&store, record.id).await;
    assert_eq!(stored.compensation_attempts, 2);
    assert_eq!(stored.next_attempt_at, None);

    let record = drive(
        &store,
        record,
        Some(&lease),
        &[Transition::CompensationFailed],
        t(6),
    )
    .await;
    store.release_lease(&lease).await.unwrap();
    let record = drive(
        &store,
        reload(&store, record.id).await,
        None,
        &[Transition::ResolvedRetry],
        t(7),
    )
    .await;
    assert_eq!(
        record.status,
        EffectStatus::Compensating,
        "an operator can retry it"
    );
    let listed = store
        .list(ListQuery::statuses([EffectStatus::Compensating]))
        .await
        .unwrap();
    assert_eq!(listed, [record]);
}

async fn approval_is_persisted<S: EffectStore>(store: S) {
    let record = insert(&store, 1).await;
    assert!(!record.approved);
    let record = drive(&store, record, None, &[Transition::RequestApproval], t(1)).await;
    assert_eq!(record.status, EffectStatus::AwaitingApproval);
    let mut approve = TransitionRequest::new(&record, None, Transition::Approve, t(2));
    approve.actor = Some("operator:dennis".into());
    let record = store.transition(approve).await.unwrap();
    let stored = reload(&store, record.id).await;
    assert_eq!(stored, record, "transition must return the stored record");
    assert_eq!(stored.status, EffectStatus::Pending);
    assert!(stored.approved, "the store must persist approval");

    let other = insert(&store, 2).await;
    let other = drive(
        &store,
        other,
        None,
        &[Transition::RequestApproval, Transition::Deny],
        t(3),
    )
    .await;
    assert_eq!(other.status, EffectStatus::Rejected);
    assert!(!reload(&store, other.id).await.approved);
}

/// Runs effect `n` through `transitions` at time `at`, then releases its
/// lease.
async fn settle<S: EffectStore>(
    store: &S,
    n: u32,
    transitions: &[Transition],
    at: u64,
) -> EffectId {
    let record = insert(store, n).await;
    let lease = store
        .acquire_lease(record.id, &worker("a"), t(at), TTL)
        .await
        .unwrap();
    drive(store, record, Some(&lease), transitions, t(at)).await;
    store.release_lease(&lease).await.unwrap();
    lease.effect_id
}

async fn pruning_removes_only_settled_idle_old_records<S: EffectStore>(store: S) {
    let committed = [Transition::StartAttempt, Transition::Succeeded];
    let old_a = settle(&store, 0, &committed, 10).await;
    let failed = settle(
        &store,
        1,
        &[Transition::StartAttempt, Transition::FailedDefinitively],
        10,
    )
    .await;
    let young = settle(&store, 2, &committed, 50).await;
    let unknown = settle(
        &store,
        3,
        &[Transition::StartAttempt, Transition::OutcomeUnknown],
        10,
    )
    .await;
    let escalated = settle(
        &store,
        6,
        &[
            Transition::StartAttempt,
            Transition::OutcomeUnknown,
            Transition::Escalate,
        ],
        10,
    )
    .await;
    let leased = settle(&store, 4, &committed, 10).await;
    let old_b = settle(&store, 5, &committed, 10).await;
    // A compensation is about to start on `leased`: it holds a live lease.
    store
        .acquire_lease(leased, &worker("b"), t(95), TTL)
        .await
        .unwrap();

    let now = t(100);
    let minute = Duration::from_secs(60);
    let prune = |status, older_than, limit| {
        let store = &store;
        async move {
            store
                .prune(PruneQuery::new(status, older_than, now).limit(limit))
                .await
                .unwrap()
        }
    };
    let exists = |id| {
        let store = &store;
        async move { store.get(id).await.unwrap().is_some() }
    };

    assert_eq!(
        prune(EffectStatus::Committed, minute, 1).await,
        1,
        "the limit caps a batch"
    );
    assert!(!exists(old_a).await, "lowest id first");
    assert!(exists(old_b).await);
    assert_eq!(
        prune(EffectStatus::Committed, minute, 100).await,
        1,
        "only old_b is left to prune: young is too young, leased is leased"
    );
    assert!(!exists(old_b).await);
    assert!(exists(young).await && exists(leased).await);
    assert_eq!(prune(EffectStatus::Committed, minute, 100).await, 0);

    // Spelled out, not `is_settled`, so the suite does not share the
    // store's definition.
    let settled = [
        EffectStatus::Committed,
        EffectStatus::Failed,
        EffectStatus::Rejected,
        EffectStatus::Compensated,
    ];
    for status in ALL_STATUSES.into_iter().filter(|s| !settled.contains(s)) {
        assert_eq!(
            prune(status, Duration::ZERO, 100).await,
            0,
            "{status} is not settled and is never pruned"
        );
    }
    assert!(exists(unknown).await && exists(escalated).await);
    assert_eq!(
        prune(
            EffectStatus::Failed,
            Duration::from_secs(200_000_000_000),
            100
        )
        .await,
        0,
        "an age older than any record prunes nothing"
    );
    assert_eq!(prune(EffectStatus::Failed, minute, 100).await, 1);
    assert!(!exists(failed).await);

    pruned_records_leave_nothing_behind(&store, old_a, young).await;
}

/// After `pruned` (key 0) was pruned and `kept` was not.
async fn pruned_records_leave_nothing_behind<S: EffectStore>(
    store: &S,
    pruned: EffectId,
    kept: EffectId,
) {
    // The audit trail goes with the record; others keep theirs.
    assert!(matches!(
        store.events(pruned).await,
        Err(StoreError::NotFound(id)) if id == pruned
    ));
    assert_eq!(store.events(kept).await.unwrap().len(), 2);
    // The key is free again: the next request is a new effect.
    assert!(store.get_by_key(&key(0)).await.unwrap().is_none());
    let again = store.insert_or_get(new_effect(0)).await.unwrap();
    assert!(again.inserted);
    assert_ne!(again.record.id, pruned);
    assert_eq!(again.record.status, EffectStatus::Pending);
    assert_eq!(store.events(again.record.id).await.unwrap(), Vec::new());
}
