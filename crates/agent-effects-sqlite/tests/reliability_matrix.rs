//! Run the common fault matrix against SQLite transactions, not an emulated store.
#[path = "../../agent-effects/tests/support/reliability.rs"]
mod reliability;

use agent_effects_sqlite::SqliteStore;
use std::sync::atomic::{AtomicU32, Ordering};

#[tokio::test]
async fn sqlite_storage_failure_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let next = AtomicU32::new(0);
    reliability::storage_matrix(|| {
        let path = dir
            .path()
            .join(format!("fault-{}.db", next.fetch_add(1, Ordering::SeqCst)));
        async move { SqliteStore::open(path).await.unwrap() }
    })
    .await;
}

#[tokio::test]
async fn sqlite_compensation_storage_failure_matrix() {
    let dir = tempfile::tempdir().unwrap();
    reliability::compensation_storage_matrix(|| {
        let path = dir.path().join("compensation.db");
        async move { SqliteStore::open(path).await.unwrap() }
    })
    .await;
}

#[tokio::test]
async fn sqlite_reconnect_and_simultaneous_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let next = AtomicU32::new(0);
    reliability::reconnect_with_simultaneous_recovery(|| {
        let path = dir.path().join(format!(
            "recovery-{}.db",
            next.fetch_add(1, Ordering::SeqCst)
        ));
        async move { SqliteStore::open(path).await.unwrap() }
    })
    .await;
}

#[tokio::test]
async fn closed_driver_pool_after_external_success_recovers_on_reopen() {
    use agent_effects::testkit::FakeRemote;
    use agent_effects::{EffectHandler, EffectStatus, EffectStore, ManualClock, RuntimeError};
    use reliability::{Durable, Protection, TTL};
    use std::sync::Arc;
    for protection in [Protection::None, Protection::Idempotent] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("closed-pool.db");
        let store = SqliteStore::open(&path).await.unwrap();
        let clock = Arc::new(ManualClock::default());
        let remote = FakeRemote::new(clock.clone());
        let rt = reliability::runtime(
            store.clone(),
            clock.clone(),
            remote.clone(),
            protection,
            "disconnected",
        );
        let result = rt
            .effect(Durable::NAME, "closed")
            .input(&"closed")
            .remote_idempotency(protection == Protection::Idempotent)
            .run({
                let (store, remote) = (store.clone(), remote.clone());
                move |ctx| {
                    let (store, remote) = (store.clone(), remote.clone());
                    async move {
                        let value = remote
                            .create(
                                "closed",
                                (protection == Protection::Idempotent)
                                    .then(|| ctx.idempotency_key()),
                            )
                            .await?;
                        store.pool().close().await;
                        Ok::<_, agent_effects::EffectFailure>(value)
                    }
                }
            })
            .await;
        assert!(matches!(result, Err(RuntimeError::Store(_))));
        assert!(store.get_by_key(&reliability::key("closed")).await.is_err());
        clock.advance(TTL + std::time::Duration::from_secs(1));
        let reopened = SqliteStore::open(&path).await.unwrap();
        let rt = reliability::runtime(
            reopened.clone(),
            clock,
            remote.clone(),
            protection,
            "reconnected",
        );
        rt.recover().await.unwrap();
        let record = reliability::record(&reopened, "closed").await;
        assert_eq!(
            record.status,
            if protection == Protection::Idempotent {
                EffectStatus::Committed
            } else {
                EffectStatus::NeedsIntervention
            }
        );
        assert_eq!(remote.applications("closed"), 1);
        reliability::assert_not_falsely_failed(&reopened, record.id).await;
    }
}
