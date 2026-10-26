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

#[tokio::test]
async fn migration_preserves_legacy_records_without_inventing_safety_guarantees() {
    use agent_effects::{EffectId, EffectKey, EffectName, LogicalKey, RuntimeError};
    use sqlx::SqlSafeStr;
    use sqlx::migrate::{Migration, MigrationType, Migrator};

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let old = Migrator::with_migrations(vec![Migration::new(
        1,
        "effects".into(),
        MigrationType::Simple,
        include_str!("../migrations/0001_effects.sql").into_sql_str(),
        false,
    )]);
    old.run(&pool).await.unwrap();
    for (logical, status) in [("pending", "pending"), ("committed", "committed")] {
        sqlx::query("INSERT INTO effects (id, effect_name, logical_key, kind, status, attempt_count, may_have_applied, compensation_attempts, approved, lease_epoch, version, created_at, updated_at, output) VALUES (?, 'legacy.op', ?, 'irreversible_write', ?, 0, 0, 0, 0, 0, 0, 0, 0, '7')")
            .bind(EffectId::new().to_string()).bind(logical).bind(status)
            .execute(&pool).await.unwrap();
    }
    let store = SqliteStore::from_pool(pool).await.unwrap();
    for logical in ["pending", "committed"] {
        let key = EffectKey::new(
            EffectName::new("legacy.op").unwrap(),
            LogicalKey::new(logical).unwrap(),
        );
        assert!(
            store
                .get_by_key(&key)
                .await
                .unwrap()
                .unwrap()
                .execution_contract
                .is_none()
        );
    }
    let rt = Runtime::new(store);
    let pending = rt
        .effect("legacy.op", "pending")
        .run(|_| async {
            panic!("legacy effect ran");
            #[allow(unreachable_code)]
            Ok::<_, EffectFailure>(7)
        })
        .await;
    assert!(matches!(pending, Err(RuntimeError::LegacyContract { .. })));
    let replay = rt
        .effect("legacy.op", "committed")
        .run(|_| async {
            panic!("committed legacy effect ran");
            #[allow(unreachable_code)]
            Ok::<_, EffectFailure>(7)
        })
        .await
        .unwrap();
    assert_eq!(replay, EffectOutcome::Committed(7));
}
