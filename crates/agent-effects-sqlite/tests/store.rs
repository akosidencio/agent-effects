//! The store contract, durability across reopening, and use from the runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use agent_effects::{EffectFailure, EffectOutcome, EffectStore, Runtime};
use agent_effects_sqlite::SqliteStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passes_the_conformance_suite() {
    let dir = tempfile::tempdir().unwrap();
    let next = AtomicU32::new(0);
    agent_effects_store::testkit::conformance(|| {
        let path = dir
            .path()
            .join(format!("case-{}.db", next.fetch_add(1, Ordering::SeqCst)));
        async move { SqliteStore::open(path).await.unwrap() }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_effects_survive_reopening_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("effects.db");
    let calls = Arc::new(AtomicU32::new(0));
    let charge = |store: SqliteStore| {
        let calls = Arc::clone(&calls);
        async move {
            Runtime::new(store)
                .effect("payment.charge", "order-1")
                .input(&42)
                .run(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok::<_, EffectFailure>("pi_1".to_string()) }
                })
                .await
                .unwrap()
        }
    };

    let first = charge(SqliteStore::open(&path).await.unwrap()).await;
    // A new process: a fresh pool on the same file.
    let reopened = SqliteStore::open(&path).await.unwrap();
    let second = charge(reopened.clone()).await;

    assert_eq!(first, EffectOutcome::Committed("pi_1".into()));
    assert_eq!(second, first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let record = reopened
        .list(agent_effects::store::ListQuery::statuses([]))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let events = reopened.events(record.id).await.unwrap();
    assert_eq!(events.len(), 2, "the audit trail persisted too");
}
